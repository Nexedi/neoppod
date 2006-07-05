#include <pthread.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <uuid/uuid.h>
#include <getopt.h>
#include <syslog.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "neo_mysql.h"
#include "storage_request.h"
#include "storage_return.h"
#include "neo_socket.h"
#include "neo_struct.h"

#define DB 0
#define STORAGE_DATA_FILE "storage_data.txt"
#define STORAGE_ID_FILE "storage_id.txt"

struct master_node
{
  char id[UUID_LEN + 1];
  char ip[IP_LEN + 1];
  u_int16_t port;
};

static struct master_node *master;

static int soc = -1;
static int create_db = 0;
static uuid_t storage_id;
static char ID[UUID_LEN+1];	/* uuid is unparse into a 36 byte string */
static struct database *db;
char database_dir[BUF_SIZE]; /* path to the mysql db dir */
static pthread_mutex_t mutex_db = PTHREAD_MUTEX_INITIALIZER;

int
init_mutex ()
{
  pthread_mutex_init (&mutex_db, NULL);
  return 1;
}

int
lock (int mutex)
{
  if (mutex == DB)
    pthread_mutex_lock (&mutex_db);
  else
    {
      perror ("lock mutex");
      return 0;
    }
  return 1;
}

int
unlock (int mutex)
{
  if (mutex == DB)
    pthread_mutex_unlock (&mutex_db);
  else
    {
      perror ("unlock mutex");
      return 0;
    }
  return 1;
}

/* handler called when master close */
static void
cleanup_handler (int sig)
{
  database_close (db);
  storageClose (master->ip, master->port, ID);
  free (master);
  if (soc >= 0)
    close (soc);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_NOTICE), "storage close");
  closelog ();
  exit (0);
}


/* function that generate a uuid and store it on disk */
int
generate_id ()
{
  memset (ID, 0, UUID_LEN + 1);
  FILE *fid;
  /* check if a uuid ever exists */
  if ((fid = fopen (STORAGE_ID_FILE, "r")) != NULL)
    {
      fread (ID, UUID_LEN, 1, fid);
      uuid_parse (ID, storage_id);
      fclose (fid);
      return 1;
    }
  /* generate uuid */
  uuid_generate (storage_id);
  uuid_unparse (storage_id, ID);
  /* store it on disk */
  fid = fopen (STORAGE_ID_FILE, "w");
  fwrite (ID, UUID_LEN, 1, fid);
  fclose (fid);
  return 1;
}


static int
h_transaction (int conn, char *hbuf, u_int32_t * buf_len)	/* done  */
{
  u_int64_t tid, oid, serial, object_len;
  char rflags[FLAG_LEN];
  u_int16_t ulen, dlen, elen;
  u_int32_t len = 0, nb_objects, data_len = 0, i;
  int32_t crc;
  char *user = NULL, *desc = NULL, *ext = NULL, *data = NULL, *buf = NULL;

  /* header */
  memcpy (rflags, hbuf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (hbuf + INT16_LEN + FLAG_LEN)));
  buf = (char *) malloc (data_len);
  len = *buf_len - 8;
  while (len < data_len)
    if (!fill_buffer (conn, buf, &len, data_len - len))
      goto fail;

  len = 0;
  /* get transaction info */
  tid = ntohll (*((u_int64_t *) (buf + len)));
  len += INT64_LEN;
  ulen = ntohs (*((u_int16_t *) (buf + len)));
  len += INT16_LEN;
  dlen = ntohs (*((u_int16_t *) (buf + len)));
  len += INT16_LEN;
  elen = ntohs (*((u_int16_t *) (buf + len)));
  len += INT16_LEN;
  /* XXX use just one malloc for all */
  user = (char *) malloc (ulen + 1);
  desc = (char *) malloc (dlen + 1);
  ext = (char *) malloc (elen + 1);
  memset (user, 0, ulen + 1);
  memset (desc, 0, dlen + 1);
  memset (ext, 0, elen + 1);
  memcpy (user, buf + len, ulen);
  len += ulen;
  memcpy (desc, buf + len, dlen);
  len += dlen;
  memcpy (ext, buf + len, elen);
  len += elen;
  /* get object list */
  nb_objects = ntohl (*((u_int32_t *) (buf + len)));
  len += INT32_LEN;
  /* store transaction into db */
  lock (DB);
  if (!database_storage_put_trans (db, tid, user, desc, ext))
    {				/* must abort trans */
      unlock (DB);
      returnErrorMessage (conn, TRANSACTION, TRANS_NOT_FOUND,
			  "transaction not found");
      return 1;
    }
  unlock (DB);

  for (i = 0; i < nb_objects; i++)
    {
      oid = ntohll (*((u_int64_t *) (buf + len)));	/* oid */
      len += INT64_LEN;
      serial = ntohll (*((u_int64_t *) (buf + len)));	/* oid */
      len += INT64_LEN;
      object_len = ntohll (*((u_int64_t *) (buf + len)));	/* data length */
      len += INT64_LEN;
      data = (char *) malloc (object_len + 1);
      memset (data, 0, object_len + 1);
      memcpy (data, buf + len, object_len);	/* data */
      len += object_len;
      crc = ntohl (*((u_int32_t *) (buf + len)));	/* crc */
      len += INT32_LEN;
      /* store object in db */
      lock (DB);
      if (!database_storage_put_object (db, oid, serial, tid, (void *) data,
					strlen (data), crc))
	{			/* must abort trans , del object and transaction into db */
	  unlock (DB);
	  if (data != NULL)
	    free (data);
	  goto fail;
	}
      if (data != NULL)
	free (data);
      unlock (DB);
    }

  if (!returnTransaction (conn, TRANSACTION))
    goto fail;
  free (user);
  free (desc);
  free (ext);
  free (buf);
  return 1;

fail:
  if (user != NULL)
    free (user);
  if (desc != NULL)
    free (desc);
  if (ext != NULL)
    free (ext);
  if (buf != NULL)
    free (buf);

  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "return method failed");
  returnErrorMessage (conn, TRANSACTION, TMP_FAILURE, "Error storage node");
  return 0;
}


static int
h_undo (int conn, char *hbuf, u_int32_t * buf_len)
{
  u_int64_t tid;
  char rflags[FLAG_LEN], *buf;
  u_int32_t len = 0, data_len = 0, nb_oid;
  u_int64_t *oid_list;
/*   struct stringList *loid; */
  /* header */
  memcpy (rflags, hbuf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (hbuf + INT16_LEN + FLAG_LEN)));
  buf = (char *) malloc (data_len);

  len = *buf_len - 8;
  while (len < data_len)
    if (!fill_buffer (conn, buf, &len, data_len - len))
      goto fail;

  tid = ntohll (*((u_int64_t *) (buf)));
  /* get object list to return to client to invalidate cache */
  lock (DB);
  if (!database_storage_get_object_list (db, tid, &oid_list, &nb_oid))
    {				/* error in db */
      unlock (DB);
      goto fail;
    }

  /* don't delete transaction because needed for next undo */
/*   if (!database_storage_del_trans (db, tid)) */
/*     { */
/*       unlock (DB); */
/*       goto fail; */
/*     } */
  unlock (DB);

  if (!returnUndo (conn, UNDO, oid_list, nb_oid))
    {
      goto fail;
    }

  free (buf);
  return 1;
fail:
  free (buf);
  returnErrorMessage (conn, UNDO, TMP_FAILURE, "Error storage node");
  return 0;
}

static int
h_histInfo (int conn, char *hbuf, u_int32_t * buf_len)
{
  struct hist hist;
  u_int64_t oid, serial;
  char *buf;
  u_int32_t len = 0, data_len = 0;
  char rflags[FLAG_LEN];

  /* header */
  memcpy (rflags, hbuf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (hbuf + INT16_LEN + FLAG_LEN)));
  buf = (char *) malloc (data_len);

  len = *buf_len - 8;
  while (len < data_len)
    if (!fill_buffer (conn, buf, &len, data_len - len))
      goto fail;

  oid = ntohll (*((u_int64_t *) (buf)));
  serial = ntohll (*((u_int64_t *) (buf + INT64_LEN)));

  lock (DB);
  if (!database_storage_get_object_info (db, oid, serial, &hist))
    {
      unlock (DB);
      goto fail;
    }
  unlock (DB);
  /* create history entry */
  hist.time = 0;
  hist.serial = serial;

  if (!returnHistInfo (conn, HIST_INFO, hist))
    goto fail;
  free (hist.user);
  free (hist.desc);
  free (buf);
  return 1;
fail:
  if (hist.user != NULL)
    free (hist.user);
  if (hist.desc != NULL)
    free (hist.desc);
  if (buf != NULL)
    free (buf);
  returnErrorMessage (conn, HIST_INFO, TMP_FAILURE, "Error storage node");
  return 0;
}

static int
h_load (int conn, char *hbuf, u_int32_t * buf_len)
{
  u_int64_t oid, serial;
  char rflags[FLAG_LEN], *buf;
  u_int32_t len = 0, data_len = 0;
  int32_t crc;
  char *data;

  /* header */
  memcpy (rflags, hbuf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (hbuf + INT16_LEN + FLAG_LEN)));
  buf = (char *) malloc (data_len);

  len = *buf_len - 8;
  while (len < data_len)
    if (!fill_buffer (conn, buf, &len, data_len - len))
      goto fail;

  oid = ntohll (*((u_int64_t *) (buf)));
  serial = ntohll (*((u_int64_t *) (buf + INT64_LEN)));

  lock (DB);
  if (!database_storage_get_object (db, oid, serial, (void **) &data, &crc))
    {
      unlock (DB);
      returnErrorMessage (conn, LOAD, OID_NOT_FOUND,
			  "object not found in storage");
      return 0;
    }
  unlock (DB);
  if (!returnLoad (conn, LOAD, data, crc))
    {
      free (data);
      goto fail;
    }
  free (data);
  free (buf);
  return 1;
fail:
  free (buf);
  returnErrorMessage (conn, LOAD, TMP_FAILURE, "Error storage node");
  return 0;
}

static int
h_getSize (int conn, char *hbuf, u_int32_t * buf_len)
{
  u_int64_t size = 0;
  u_int32_t len = 0, data_len = 0;
  char rflags[FLAG_LEN], *buf;

  /* header */
  memcpy (rflags, hbuf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (hbuf + INT16_LEN + FLAG_LEN)));
  buf = (char *) malloc (data_len);

  len = *buf_len - 8;
  while (len < data_len)
    if (!fill_buffer (conn, buf, &len, data_len - len))
      goto fail;

  printf ("in get size\n");

  lock(DB);
  if (!database_storage_get_db_size (db, &size))
    {
      unlock (DB);
      printf ("get db failed\n");
      returnErrorMessage (conn, GET_SIZE, METHOD_ERROR,
                          "can not get size for storage");
      return 0;
    }
  unlock (DB);

  printf ("return get size %llu\n", size);

  if (!returnGetSize (conn, GET_SIZE, size))
    goto fail;
  free (buf);
  return 1;
fail:
  free (buf);
  returnErrorMessage (conn, GET_SIZE, TMP_FAILURE, "Error storage node");
  return 0;
}

static int
h_undoInfo (int conn, char *hbuf, u_int32_t * buf_len)
{
  u_int64_t tid;
  struct undoInfo info;
  u_int32_t len = 0, data_len = 0;
  char rflags[FLAG_LEN];
  char *buf;

  /* header */
  memcpy (rflags, hbuf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (hbuf + INT16_LEN + FLAG_LEN)));
  buf = (char *) malloc (data_len);

  len = *buf_len - 8;
  while (len < data_len)
    if (!fill_buffer (conn, buf, &len, data_len - len))
      goto fail;

  tid = ntohll (*((u_int64_t *) (buf)));
  /* get info */
  /* XXX use structure in database */
  lock (DB);
  if (!database_storage_get_trans_info (db, tid, &info))
    {
      unlock (DB);
      returnErrorMessage (conn, UNDO_INFO, TRANS_NOT_FOUND, "");
      return 1;
    }
  unlock (DB);

  /* init structure */
  info.time = 0;
  /* copy data */
  info.id = tid;

  if (!returnUndoInfo (conn, UNDO_INFO, info))
    goto fail;
  free (buf);
  free (info.desc);
  free (info.user);
  return 1;
 fail:
  free (info.desc);
  free (info.user);
  free (buf);
  returnErrorMessage (conn, UNDO_INFO, TMP_FAILURE, "Error storage node");
  return 0;
}


static int
h_masterClose (int conn, char *hbuf, u_int32_t * buf_len)
{
  u_int32_t len = 0, data_len = 0;
  char rflags[FLAG_LEN];
  char *buf, master_id[UUID_LEN + 1];

  /* header */
  memcpy (rflags, hbuf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (hbuf + INT16_LEN + FLAG_LEN)));
  buf = (char *) malloc (data_len);

  len = *buf_len - 8;
  while (len < data_len)
    if (!fill_buffer (conn, buf, &len, data_len - len))
      goto fail;

  memset (master_id, 0, UUID_LEN + 1);
  memcpy (master_id, buf, UUID_LEN);

  if (!returnMasterClose (conn, MASTER_CLOSE, ID))
    goto fail;
  free (buf);
  return 1;
fail:
  free (buf);
  returnErrorMessage (conn, MASTER_CLOSE, TMP_FAILURE, "Error storage node");
  return 0;
}

static int
h_masterChange (int conn, char *hbuf, u_int32_t * buf_len)
{
  u_int32_t len = 0, data_len = 0;
  char rflags[FLAG_LEN], master_id[UUID_LEN + 1], master_ip[IP_LEN + 1];
  u_int16_t master_port;
  char *buf;

  /* header */
  memcpy (rflags, hbuf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (hbuf + INT16_LEN + FLAG_LEN)));
  buf = (char *) malloc (data_len);

  len = *buf_len - 8;
  while (len < data_len)
    if (!fill_buffer (conn, buf, &len, data_len - len))
      goto fail;

  memset (master_id, 0, UUID_LEN + 1);
  memcpy (master_id, buf, UUID_LEN);
  buf += UUID_LEN;
  memset (master_ip, 0, IP_LEN + 1);
  memcpy (master_ip, buf, IP_LEN);
  buf += IP_LEN;
  master_port = ntohs (*((u_int16_t *) (buf)));

  if (!returnMasterChange (conn, MASTER_CHANGE, ID))
    goto fail;
  free (buf);
  return 1;
  /* XXX must close connection to master and connect to new master to check it ??? */
fail:
  free (buf);
  returnErrorMessage (conn, MASTER_CHANGE, TMP_FAILURE, "Error storage node");
  return 0;
}


static int
h_getTransData (int conn, char *hbuf, u_int32_t * buf_len)
{
  u_int64_t tid;
  u_int32_t len = 0, data_len = 0;
  char rflags[FLAG_LEN], *buf = NULL, *user = NULL, *desc = NULL, *ext = NULL;
  struct objectList *objects = NULL;

  /* method calleb by other storage to get all data from a transaction */

  /* header */
  memcpy (rflags, hbuf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (hbuf + INT16_LEN + FLAG_LEN)));
  buf = (char *) malloc (data_len);

  while (len < data_len)
    if (!fill_buffer (conn, buf, &len, data_len - len))
      goto fail;

  tid = ntohll (*((u_int64_t *) (buf)));

  lock (DB);
  if (!database_storage_get_trans_data
      (db, tid, &user, &desc, &ext, &objects))
    {
      unlock (DB);
      goto fail;
    }
  unlock (DB);

  if (!returnGetTransData
      (conn, GET_TRANS_DATA, tid, user, desc, ext, *objects))
    goto fail;

  free (desc);
  free (user);
  free (ext);
  free (buf);
  objectList_Free (objects);
  free (objects);
  return 1;

fail:
  if (buf != NULL)
    free (buf);
  if (desc != NULL)
    free (desc);
  if (ext != NULL)
    free (ext);
  if (user != NULL)
    free (user);
  if (objects != NULL)
    {
      objectList_Free (objects);
      free (objects);
    }
  returnErrorMessage (conn, GET_TRANS_DATA, TMP_FAILURE,
                      "Error storage node");
  return 0;
}

/* function that handle each new connection and call the appropriate function */
static void *
client_handler (void *data)
{
  int conn;
  char buf[HEADER_LEN];
  u_int32_t buf_len = 0;
  u_int16_t method;

  database_thread_init ();
  conn = *((int *) data);

  while (1)
    {
      buf_len = 0;
      if (!fill_buffer (conn, buf, &buf_len, HEADER_LEN))
        goto fail;

      method = ntohs (*((u_int16_t *) (buf)));
      syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "client %d call method %d",
	      conn, method);
      switch (method)
	{
	case TRANSACTION:
	  if (!h_transaction (conn, buf, &buf_len))
	    goto fail;
	  break;
	case UNDO:
	  if (!h_undo (conn, buf, &buf_len))
	    goto fail;
	  break;
	case HIST_INFO:
	  if (!h_histInfo (conn, buf, &buf_len))
	    goto fail;
	  break;
	case UNDO_INFO:
	  if (!h_undoInfo (conn, buf, &buf_len))
	    goto fail;
	  break;
	case LOAD:
	  if (!h_load (conn, buf, &buf_len))
	    goto fail;
	  break;
	case GET_SIZE:
	  if (!h_getSize (conn, buf, &buf_len))
	    goto fail;
	  break;
	case MASTER_CLOSE:
	  h_masterClose (conn, buf, &buf_len);
	  goto end;
	  break;
	case MASTER_CHANGE:
	  if (!h_masterChange (conn, buf, &buf_len))
	    goto fail;
	  break;
	case GET_TRANS_DATA:
	  if (!h_getTransData (conn, buf, &buf_len))
	    goto fail;
	  break;
	}
    }

fail:
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "return method failed");
  syslog (LOG_MAKEPRI (LOG_USER, LOG_INFO), "End client connection %d", conn);
  database_thread_end ();
  close (conn);
  free (data);
  return NULL;

end:				/* end storage node */
  syslog (LOG_MAKEPRI (LOG_USER, LOG_NOTICE), "Close storage");
  database_thread_end ();
  close (conn);
  free (data);
  exit (1);
}

/**
 * function that restore a transaction from data coming from
 * another storage node
 */
int
restore_transaction (int conn)
{
  u_int64_t tid, oid, serial, data_size;
  char hbuf[RHEADER_LEN], rflags[FLAG_LEN], *buf = NULL, *data = NULL, *user =
    NULL, *desc = NULL, *ext = NULL;
  u_int32_t buf_len = 0, len = 0, data_len, nb_object, i;
  u_int16_t return_code, tlen;
  int32_t crc = 0;

  if (!wait_packet (conn, hbuf, &buf_len, RHEADER_LEN))
    return 0;

  if ((ntohs (*((u_int16_t *) (hbuf)))) != GET_TRANS_DATA)
    {
      perror ("receive");
      return 0;
    }
  memcpy (rflags, hbuf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (hbuf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (hbuf + INT16_LEN + FLAG_LEN + INT32_LEN)));

  buf = (char *) malloc (data_len);
  if (!buf)
    return 0;

  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	{
	  free (buf);
	  return 0;
	}
    }
  if (return_code != 0)
    return return_code;

  tid = ntohll (*((u_int64_t *) (buf)));
  buf += INT64_LEN;
  /* user */
  tlen = ntohs (*((u_int16_t *) (buf)));
  buf += INT16_LEN;
  user = (char *) malloc (tlen + 1);
  memset (user, 0, tlen + 1);
  memcpy (user, buf, tlen);
  buf += tlen;
  /* desc */
  tlen = ntohs (*((u_int16_t *) (buf)));
  buf += INT16_LEN;
  desc = (char *) malloc (tlen + 1);
  memset (desc, 0, tlen + 1);
  memcpy (desc, buf, tlen);
  buf += tlen;
  /* ext */
  tlen = ntohs (*((u_int16_t *) (buf)));
  buf += INT16_LEN;
  ext = (char *) malloc (tlen + 1);
  memset (ext, 0, tlen + 1);
  memcpy (ext, buf, tlen);
  buf += tlen;
  /* store transaction in database */
  database_storage_store_trans (db, tid, user, desc, ext);

  /* object list */
  nb_object = ntohl (*((u_int32_t *) (buf)));
  buf += INT32_LEN;

  for (i = 0; i < nb_object; i++)
    {
      /* oid */
      oid = ntohll (*((u_int64_t *) (buf)));
      buf += INT64_LEN;
      /* serial */
      serial = ntohll (*((u_int64_t *) (buf)));
      buf += INT64_LEN;
      /* data */
      data_size = ntohll (*((u_int64_t *) (buf)));
      buf += INT64_LEN;
      data = (char *) malloc (data_size + 1);
      memset (data, 0, data_size + 1);
      memcpy (data, buf, data_size);
      buf += data_size;
      database_storage_put_object (db, oid, serial, tid, (void *) data,
				   strlen (data), crc);
      free (data);
    }
  free (buf - data_len);
  free (ext);
  free (desc);
  free (user);
  return 1;
}

/**
 * function that get list of storage node for each transaction
 * known to be stored by the storage node
 */
static int
make_storage_sane (int conn)
{
  u_int64_t tid;
  char hbuf[RHEADER_LEN], rflags[FLAG_LEN], *buf;
  u_int32_t buf_len = 0, len =
    0, data_len, trans_nb, storage_nb, nb, i, storage_conn;
  u_int16_t return_code, storage_port, exist = 0;
  char storage_ip[IP_LEN + 1];

  if (!wait_packet (conn, hbuf, &buf_len, RHEADER_LEN))
    return 0;

  if ((ntohs (*((u_int16_t *) (hbuf)))) != SANITY_CHECK)
    {
      perror ("receive");
      return 0;
    }
  memcpy (rflags, hbuf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (hbuf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (hbuf + INT16_LEN + FLAG_LEN + INT32_LEN)));
  if (return_code != 0)
    return return_code;

  buf = (char *) malloc (data_len);
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	return 0;
    }

  trans_nb = ntohl (*((u_int32_t *) (buf)));
  len = INT32_LEN;
  /**
   * get all data from other storage and store them
   */
  for (nb = 0; nb < trans_nb; nb++)
    {
      /* get transaction id */
      tid = ntohll (*((u_int64_t *) (buf + len)));
      len += INT64_LEN;
      /* check if transaction exist */
/*       database_storage_trans_exist (db, tid, &exist); */
      if (!exist)
	{
	  /* number of storage for the transaction */
	  storage_nb = ntohl (*((u_int32_t *) (buf + len)));
	  len += INT32_LEN;
	  /* get storages list */
	  for (i = 0; i < storage_nb; i++)
	    {
	      /* storage ip */
	      memset (storage_ip, 0, IP_LEN + 1);
	      memcpy (storage_ip, buf + len, IP_LEN);
	      len += IP_LEN;
	      /* storage port */
	      storage_port = ntohs (*((u_int16_t *) (buf + len)));
	      len += INT16_LEN;
	      /* get data from another storage */
        storage_conn = connectTo(storage_ip, storage_port);
	      if (getTransData (storage_conn, GET_TRANS_DATA, tid))
            /* restore data */
            if (restore_transaction (storage_conn))
              {
                /* it's ok, go to next transaction */
                len += (INT16_LEN + IP_LEN) * (storage_nb - i - 1);
                close (storage_conn);
                break;
              }
        close (storage_conn);
      }
	}
      else
	len += (INT16_LEN + IP_LEN) * (storage_nb);
    }
  free (buf);
  return 1;
}

/**
 * send request to master to get storage node list
 * for the given transaction
 */
static int
restore_data (int conn, u_int64_t tid)
{
  u_int64_t n_tid;
  u_int32_t data_len, meth;
  char *send_buf;
  char hbuf[RHEADER_LEN], rflags[FLAG_LEN], *buf;
  u_int32_t buf_len = 0, len = 0, storage_nb, i, storage_conn;
  u_int16_t return_code, storage_port;
  char storage_ip[IP_LEN + 1];

  /* send request to master */
  meth = htons (GET_STORAGE_FOR_TRANS);
  data_len = htonl (INT64_LEN + UUID_LEN);
  send_buf = (char *) malloc (HEADER_LEN + INT64_LEN + UUID_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  memcpy (send_buf + HEADER_LEN, ID, UUID_LEN);
  n_tid = htonll (tid);
  memcpy (send_buf + HEADER_LEN + UUID_LEN, &n_tid, INT64_LEN);
  if (!send_all (conn, send_buf, HEADER_LEN + INT64_LEN + UUID_LEN))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  /* get answer from master node */
  if (!wait_packet (conn, hbuf, &buf_len, RHEADER_LEN))
    return 0;
  /* header */
  if ((ntohs (*((u_int16_t *) (hbuf)))) != GET_STORAGE_FOR_TRANS)
    {
      perror ("receive");
      return 0;
    }
  memcpy (rflags, hbuf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (hbuf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (hbuf + INT16_LEN + FLAG_LEN + INT32_LEN)));

  buf = (char *) malloc (data_len);
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	return 0;
    }
  if (return_code != 0)
    return return_code;

  /* data  */
  len = 0;
  storage_nb = ntohl (*((u_int32_t *) (buf + len)));
  len = INT32_LEN;
  /* get storages list */
  for (i = 0; i < storage_nb; i++)
    {
      /* storage ip */
      memset (storage_ip, 0, IP_LEN + 1);
      memcpy (storage_ip, buf + len, IP_LEN);
      len += IP_LEN;
      /* storage port */
      storage_port = ntohs (*((u_int16_t *) (buf + len)));
      len += INT16_LEN;
      /* get data from another storage */
      storage_conn = connectTo(storage_ip, storage_port);
      if (getTransData
          (storage_conn, GET_TRANS_DATA, tid))
        /* restore data */
        if (restore_transaction (storage_conn))
          {
            close (storage_conn);
            free (buf);
            return 1;
          }
      close (storage_conn);
    }
  /* never get here otherwise transaction isn't restored */
  free (buf);
  return 0;
}

/**
 * check if data stored by storage and master node index match
 */
static int
check_storage_sanity (int conn)
{
  /* get index of transaction with oid from master and check if all is ok */
  char hbuf[RHEADER_LEN], *buf, rflags[FLAG_LEN];
  u_int32_t data_len, len = 0, buf_len = 0, nb_oid, oid_nb, nb_txn, txn_nb;
  u_int64_t tid, oid;
  u_int16_t return_code;

  if (!wait_packet (conn, hbuf, &buf_len, RHEADER_LEN))
    return 0;
  /* get header */
  if ((ntohs (*((u_int16_t *) (hbuf)))) != STORAGE_INDEX)
    {
      perror ("receive");
      return 0;
    }
  memcpy (rflags, hbuf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (hbuf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (hbuf + INT16_LEN + FLAG_LEN + INT32_LEN)));

  buf = (char *) malloc (data_len);
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	return 0;
    }
  if (return_code != 0)
    return return_code;
  /* get data */
  len = 0;
  nb_txn = ntohl (*((u_int64_t *) (buf + len)));
  len += INT32_LEN;
  for (txn_nb = 0; txn_nb < nb_txn; txn_nb++)
    {
      tid = ntohll (*((u_int64_t *) (buf + len)));
      len += INT64_LEN;
      nb_oid = ntohl (*((u_int32_t *) (buf + len)));
      len += INT32_LEN;
      /* XXX must construct list of oid before checking with DB */
      for (oid_nb = 0; oid_nb < nb_oid; oid_nb++)
	{
	  oid = ntohll (*((u_int64_t *) (buf + len)));
	  len += INT64_LEN;
	  /* now check with database if allright */
	  if (!database_storage_check_sanity (db, tid, oid))
	    {
	      /* must get data from another storage */
	      restore_data (conn, tid);
	    }
	}
    }
  free (buf);
  return 1;
}


/* send broadcast message to find where is the master */
static int
search_master ()
{
  int soc, bd = 1, size;
  char send_buf[HEADER_LEN], rcv_buf[RHEADER_LEN + 1], rflags[FLAG_LEN];
  struct sockaddr_in addr;
  u_int16_t meth, return_code;
  u_int32_t data_len;
  size_t send, received;

  soc = socket (PF_INET, SOCK_DGRAM, IPPROTO_UDP);
  setsockopt (soc, SOL_SOCKET, SO_BROADCAST, (int *) &bd, sizeof (bd));

  addr.sin_family = AF_INET;
  addr.sin_port = htons (10825);
  addr.sin_addr.s_addr = htonl (INADDR_BROADCAST);

  /* create packet */
  meth = htons (SEARCH_MASTER);
  data_len = htonl (0);
  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  send = sendto (soc, (char *)send_buf, HEADER_LEN, 0, (struct sockaddr *)&addr, sizeof(addr));
  /* wait for a response */
  size = sizeof (addr);
  memset (rcv_buf, 0, RHEADER_LEN);
  received = recvfrom (soc, (char *)rcv_buf, RHEADER_LEN, 0, (struct sockaddr *)&addr, &size);
  /* check response */
  if ((ntohs (*((u_int16_t *) (rcv_buf)))) != SEARCH_MASTER)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING), "return method error from master in udp broadcast");
      return METHOD_ERROR;
    }
  memcpy (rflags, rcv_buf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (rcv_buf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (rcv_buf + INT16_LEN + FLAG_LEN + INT32_LEN)));
  if (return_code != 0)
    return return_code;

  /* get master ip */
  memset (master->ip, 0, IP_LEN + 1);
  strncpy (master->ip, inet_ntoa(addr.sin_addr), IP_LEN);
  printf ("master found on %s\n", master->ip);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_NOTICE), "master found on %s", master->ip);
  close(soc);

  return 0;
}




static int
parse_arg (int argc, char **argv)
{
  int c, option_index = 0;

  while (1)
    {
      static struct option long_options[] = {
	{"create-database", no_argument, 0, 'c'},
	{"log-level", required_argument, 0, 'l'},
	{"database", required_argument, 0, 'd'},
	{0, 0, 0, 0}
      };
      option_index = 0;

      c = getopt_long (argc, argv, "cl:d:", long_options, &option_index);

      /* Detect the end of the options. */
      if (c == -1)
	break;

      switch (c)
	{
	case 'c':
	  create_db = 1;
	  break;
	case 'l':
	  switch (atoi (optarg))
	    {
	    case 3:
	      setlogmask (LOG_UPTO (LOG_DEBUG));
	      break;
	    case 2:
	      setlogmask (LOG_UPTO (LOG_INFO));
	      break;
	    case 1:
	      setlogmask (LOG_UPTO (LOG_NOTICE));
	      break;
	    }
	  break;
  case 'd': /* XXX must test len of dir! */
    strncpy (database_dir, optarg, BUF_SIZE-1);
    break;
	case '?':
	  /* getopt_long already printed an error message. */
	  break;
	default:
	  abort ();
	}
    }
  /* Print any remaining command line arguments (not options). */
  if (optind < argc)
    {
      printf ("non-option ARGV-elements: ");
      while (optind < argc)
	printf ("%s ", argv[optind++]);
      putchar ('\n');
      return 0;
    }
  return 1;
}

int
main (int argc, char **argv)
{
  struct sockaddr_in addr;
  pthread_attr_t attr;
  char old_MasterID[UUID_LEN+1];
  u_int16_t port = 10000;	/* default port */
  int *connp, status, master_conn;
  pthread_t th;
  FILE *fid;

  /* init default before geting args */
  setlogmask (LOG_UPTO (LOG_ERR));
  strncpy (database_dir, "./db", BUF_SIZE-1);


  master = (struct master_node *) malloc (sizeof (struct master_node));

  if (!parse_arg (argc, argv))
    return -1;

  /* open syslog */
  openlog ("neostorage", 0, 0);

  signal (SIGINT, cleanup_handler);
  signal (SIGTERM, cleanup_handler);

  if (!generate_id ())
    goto fail;

  syslog (LOG_MAKEPRI (LOG_USER, LOG_NOTICE), "id : %s", ID);

  /* open database */
  if ((db = database_storage_open (create_db)) == 0)
    return -1;

  syslog (LOG_MAKEPRI (LOG_USER, LOG_NOTICE),
	  "connection to database opened");

  /* search for master */
  if (search_master() != 0)
    {
      printf ("master not found\n");
      return -1;
    }

  init_mutex ();
  /* set attributes to thread */
  if (pthread_attr_init (&attr) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "pthread_attr_init %s",
	      strerror (errno));
      goto fail;
    }

  if (pthread_attr_setdetachstate (&attr, PTHREAD_CREATE_DETACHED) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR),
	      "pthread_attr_setdetachstate %s", strerror (errno));
      goto fail;
    }

  /* create socket */
  soc = socket (PF_INET, SOCK_STREAM, 0);
  if (soc < 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "socket %s", strerror (errno));
      goto fail;
    }

  addr.sin_family = AF_INET;
  addr.sin_port = htons (port);
  addr.sin_addr.s_addr = htonl (INADDR_ANY);

  /* try to open a port for storage server */
bind:
  if (bind (soc, (struct sockaddr *) &addr, sizeof (addr)) != 0)
    {
      port++;
      addr.sin_port = htons (port);
      goto bind;
    }

  if (listen (soc, 5) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "listen %s", strerror (errno));
      goto fail;
    }

  syslog (LOG_MAKEPRI (LOG_USER, LOG_NOTICE), "listening on port %d", port);

  /* connect to master */
  master->port = MASTER_PORT;
  master_conn = connectTo(master->ip, master->port);
  storageStart (master_conn, ID, "127.0.0.1", port, master->id, &status);

  memset(old_MasterID, 0, UUID_LEN+1);
  /* Check if a master id already exists */
  if ((fid = fopen (STORAGE_DATA_FILE, "r")) != NULL)
    {
      fread (old_MasterID, UUID_LEN, 1, fid);
      fclose (fid);
      if (strncmp (old_MasterID, master->id, UUID_LEN) != 0)
	{
	  /* master change anyway the storage is make sane just after */
    /* just store the new master id */
	  fid = fopen (STORAGE_DATA_FILE, "w");
	  fwrite (master->id, UUID_LEN, 1, fid);
	  fclose (fid);
	}
    }

  /* check the status of the storage */
  printf ("status %d\n", status);
  if (status == 2)		/* storage marked as unsane */
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_NOTICE), "storage unreliable");
      if (!make_storage_sane (master_conn))
	{
    syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "impossible to make the storage reliable");
     goto fail;
	}
    }
  else if (status == 1)
    /* must check index with master */
    if (!check_storage_sanity (master_conn))
      {
        syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "storage index check failed");
        goto fail;
      }
  /* storage is ready to handle request */
  if (!storageReady (master_conn, ID))
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "impossible to start storage");
      goto fail;
    }
  close (master_conn);
  printf ("storage ready\n");

  /* now wait for connection */
  while (1)
    {
      connp = (int *) malloc (sizeof (int));
      if (!connp)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "malloc %s",
		  strerror (errno));
	  goto fail;
	}

      *connp = accept (soc, 0, 0);
      if (*connp < 0)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "accept %s",
		  strerror (errno));
	  free (connp);
	  goto fail;
	}

      syslog (LOG_MAKEPRI (LOG_USER, LOG_INFO),
	      "New client connected with %d", *connp);

      if (pthread_create (&th, &attr, client_handler, connp) != 0)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "pthread_create %s",
		  strerror (errno));
	  perror ("pthread_create");
	  free (connp);
	  goto fail;
	}
    }

  close (soc);
  return 0;

fail:
  free (master);
  if (soc >= 0)
    close (soc);
  database_close (db);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_NOTICE), "Close storage");
  closelog ();
  return 1;
}
