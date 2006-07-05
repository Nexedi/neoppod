#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <netinet/in.h>
#include <stdio.h>
#include <sys/syslog.h>

#include "neo_socket.h"
#include "neo_struct.h"
#include "master_return.h"

static u_int16_t rcode = 0;

int
returnGetObjectByOid (int conn, int method, u_int64_t serial,
		      struct stringList storages)
{
  u_int64_t n_serial;
  u_int32_t data_len, n_nb_storage, i;
  u_int16_t meth;
  char *send_buf, *id;
  int len = 0;

  meth = htons (method);
  /* compute len */
  n_nb_storage = htonl (storages.last);
  data_len = htonl (storages.last * UUID_LEN + INT64_LEN + INT32_LEN);
  send_buf =
    (char *) malloc (RHEADER_LEN + storages.last * UUID_LEN + INT64_LEN +
		     INT32_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);
  /* data */
  n_serial = htonll (serial);
  memcpy (send_buf + RHEADER_LEN, &n_serial, INT64_LEN);
  len += RHEADER_LEN + INT64_LEN;
  memcpy (send_buf + len, &n_nb_storage, INT32_LEN);
  len += INT32_LEN;
  /* storage list */
  for (i = 0; i < storages.last; i++)
    {
      stringList_GetItem (&storages, &id, i);
      memcpy (send_buf + len, id, UUID_LEN);
      free (id);
      len += UUID_LEN;
    }

  if (!send_all
      (conn, send_buf,
       RHEADER_LEN + storages.last * UUID_LEN + INT64_LEN + INT32_LEN))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  return 1;
}


int
returnGetObjectBySerial (int conn, int method, struct stringList storages)
{
  u_int32_t data_len, n_nb_storage, i;
  u_int16_t meth;
  char *send_buf, *id;
  int len = 0;

  meth = htons (method);
  /* compute len */
  data_len = htonl (storages.last * UUID_LEN + INT32_LEN);
  send_buf =
    (char *) malloc (RHEADER_LEN + storages.last * UUID_LEN + INT32_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);

  /* data */
  n_nb_storage = htonl (storages.last);
  memcpy (send_buf + RHEADER_LEN, &n_nb_storage, INT32_LEN);
  len = RHEADER_LEN + INT32_LEN;
  /* list of storages */
  for (i = 0; i < storages.last; i++)
    {
      stringList_GetItem (&storages, &id, i);
      memcpy (send_buf + len, id, UUID_LEN);
      free (id);
      len += UUID_LEN;
    }

  if (!send_all
      (conn, send_buf, RHEADER_LEN + storages.last * UUID_LEN + INT32_LEN))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  return 1;
}


int
returnGetSerial (int conn, int method, u_int64_t serial)
{
  u_int64_t n_serial;
  u_int32_t data_len;
  u_int16_t meth;
  char *send_buf;

  meth = htons (method);
  data_len = htonl (INT64_LEN);
  send_buf = (char *) malloc (RHEADER_LEN + INT64_LEN);
  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);

  /* serial */
  n_serial = htonll (serial);
  memcpy (send_buf + RHEADER_LEN, &n_serial, INT64_LEN);

  if (!send_all (conn, send_buf, RHEADER_LEN + INT64_LEN))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  return 1;
}


int
returnGetLastTransaction (int conn, int method, u_int64_t tid)
{
  u_int64_t n_tid;
  u_int32_t buf_len, data_len;
  u_int16_t meth;
  char *send_buf;

  meth = htons (method);
  data_len = htonl (INT64_LEN);
  buf_len = RHEADER_LEN + ntohl (data_len);
  send_buf = (char *) malloc (buf_len);
  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);
  /* data */
  n_tid = htonll (tid);
  memcpy (send_buf + RHEADER_LEN, &n_tid, INT64_LEN);

  if (!send_all (conn, send_buf, RHEADER_LEN + INT64_LEN))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  return 1;
}


int
returnGetOidInt (int conn, int method, struct oidList list)
{
  u_int32_t buf_len, data_len;
  u_int16_t meth, net_int, n_nb_oid, i, j;
  char *send_buf;
  int tlen = 0, oid[8];

  meth = htons (method);
  data_len = htonl (list.last * ID_LEN * INT16_LEN + INT16_LEN);
  buf_len = RHEADER_LEN + ntohl (data_len);
  send_buf = (char *) malloc (buf_len);
  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);
  /* data */
  n_nb_oid = htons (list.last);
  memcpy (send_buf + 10, &n_nb_oid, INT16_LEN);
  /* list of oid */
  tlen = RHEADER_LEN + INT16_LEN;
  for (i = 0; i < list.last; i++)
    {
      oidList_GetItem (&list, oid, i);
      for (j = 0; j < ID_LEN; j++)
	{
	  net_int = htons (oid[j]);
	  memcpy (send_buf + tlen, &net_int, INT16_LEN);
	  tlen += INT16_LEN;
	}
    }

  if (!send_all (conn, send_buf, buf_len))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  return 1;
}

int
returnClientStart (int conn, int method, struct storageInfo info)
{
  u_int32_t buf_len, data_len, meth;
  u_int16_t name_len, ext_len, len, sV, sU, sTU, rO;
  char *send_buf;

  /* send information back */
  meth = htons (method);
  /* compute len */
  data_len = htonl (strlen (info.name) + strlen (info.ext) + 12);
  name_len = htons (strlen (info.name));
  ext_len = htons (strlen (info.ext));
  sV = htons (info.supportVersion);
  sU = htons (info.supportUndo);
  sTU = htons (info.supportTransUndo);
  rO = htons (info.readOnly);
  buf_len = RHEADER_LEN + ntohl (data_len);
  send_buf = (char *) malloc (buf_len);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);

  /* information about storage */
  len = RHEADER_LEN;
  memcpy (send_buf + len, &name_len, INT16_LEN);
  len += INT16_LEN;
  memcpy (send_buf + len, info.name, strlen (info.name));
  len += strlen (info.name);
  memcpy (send_buf + len, &sV, INT16_LEN);
  len += INT16_LEN;
  memcpy (send_buf + len, &sU, INT16_LEN);
  len += INT16_LEN;
  memcpy (send_buf + len, &sTU, INT16_LEN);
  len += INT16_LEN;
  memcpy (send_buf + len, &rO, INT16_LEN);
  len += INT16_LEN;
  memcpy (send_buf + len, &ext_len, INT16_LEN);
  len += INT16_LEN;
  memcpy (send_buf + len, info.ext, strlen (info.ext));

  if (!send_all (conn, send_buf, buf_len))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  return 1;
}


int
returnGetTransSN (int conn, int method, struct tlist tl)
{
  u_int64_t n_tid;
  u_int32_t buf_len, data_len, j, len = 0, size = 0, n_nb_storage;
  u_int16_t meth, n_nb_trans, i;
  char *send_buf;

  meth = htons (method);
  /* compute len for each transaction */
  for (i = 0; i < tl.last; i++)
    size += tl.objects[i].list.last * UUID_LEN;

  data_len = htonl (size + INT16_LEN + tl.last * (INT64_LEN + INT32_LEN));
  buf_len =
    RHEADER_LEN + size + INT16_LEN + tl.last * (INT64_LEN + INT32_LEN);
  send_buf = (char *) malloc (buf_len);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);
  /* data */
  n_nb_trans = htons (tl.last);
  memcpy (send_buf + RHEADER_LEN, &n_nb_trans, INT16_LEN);
  /* list of transaction */
  len = RHEADER_LEN + INT16_LEN;
  for (i = 0; i < tl.last; i++)
    {
      /* tid */
      n_tid = htonll (tl.objects[i].tid);
      memcpy (send_buf + len, &n_tid, INT64_LEN);
      len += INT64_LEN;
      /* storage nb */
      n_nb_storage = htonl (tl.objects[i].list.last);
      memcpy (send_buf + len, &n_nb_storage, INT32_LEN);
      len += INT32_LEN;
      for (j = 0; j < tl.objects[i].list.last; j++)
	{			/* storage id */
	  memcpy (send_buf + len, tl.objects[i].list.objects[j].data,
		  UUID_LEN);
	  len += UUID_LEN;
	}
    }

  if (!send_all (conn, send_buf, buf_len))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  return 1;
}


int
returnGetObjectHist (int conn, int method, struct tlist tl)
{
  u_int64_t n_serial;
  u_int32_t buf_len, data_len, j, len = 0, size = 0, n_nb_storage;
  u_int16_t meth, i, n_nb_trans;
  char *send_buf;

  meth = htons (method);
  /* compute len , same as returnGetTransSN */
  size = 0;
  for (i = 0; i < tl.last; i++)
    size += tl.objects[i].list.last * UUID_LEN;

  data_len = htonl (size + INT16_LEN + tl.last * (INT64_LEN + INT32_LEN));
  buf_len =
    RHEADER_LEN + size + INT16_LEN + tl.last * (INT64_LEN + INT32_LEN);
  send_buf = (char *) malloc (buf_len);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);
  /* list of storages for each object entry */
  n_nb_trans = htons (tl.last);
  memcpy (send_buf + RHEADER_LEN, &n_nb_trans, INT16_LEN);
  len = RHEADER_LEN + INT16_LEN;
  for (i = 0; i < tl.last; i++)
    {
      /* serial */
      n_serial = htonll (tl.objects[i].tid);
      memcpy (send_buf + len, &n_serial, INT64_LEN);
      len += INT64_LEN;
      /* storage nb */
      n_nb_storage = htonl (tl.objects[i].list.last);
      memcpy (send_buf + len, &n_nb_storage, INT32_LEN);
      len += INT32_LEN;
      for (j = 0; j < tl.objects[i].list.last; j++)
	{			/* storage id */
	  memcpy (send_buf + len, tl.objects[i].list.objects[j].data,
		  UUID_LEN);
	  len += UUID_LEN;
	}
    }

  if (!send_all (conn, send_buf, buf_len))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  return 1;
}


int
returnUndoTrans (int conn, int method, struct stringList storages)
{
  u_int32_t data_len, n_nb_storage, i;
  u_int16_t meth;
  char *send_buf, *id;
  int len = 0;

  meth = htons (method);
  data_len = htonl (storages.last * UUID_LEN + INT32_LEN);
  send_buf =
    (char *) malloc (RHEADER_LEN + storages.last * UUID_LEN + INT32_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);
  /* list of storages */
  n_nb_storage = htonl (storages.last);
  memcpy (send_buf + RHEADER_LEN, &n_nb_storage, INT32_LEN);
  len = RHEADER_LEN + INT32_LEN;
  for (i = 0; i < storages.last; i++)
    {
      stringList_GetItem (&storages, &id, i);
      memcpy (send_buf + len, id, UUID_LEN);
      free (id);
      len += UUID_LEN;
    }

  if (!send_all
      (conn, send_buf, RHEADER_LEN + storages.last * UUID_LEN + INT32_LEN))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  return 1;
}


int
returnBeginTrans (int conn, int method, u_int16_t tid[ID_LEN],
		  struct stringList storages)
{
  u_int32_t buf_len, data_len, i, n_nb_storage;
  u_int16_t meth, n_tid;
  int len = 0;
  char *send_buf, *oid;

  meth = htons (method);
  data_len =
    htonl (storages.last * UUID_LEN + INT16_LEN * ID_LEN + INT32_LEN);
  buf_len =
    RHEADER_LEN + storages.last * UUID_LEN + INT16_LEN * ID_LEN + INT32_LEN;
  send_buf = (char *) malloc (buf_len);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);
  /* transaction id */
  len = RHEADER_LEN;
  for (i = 0; i < ID_LEN; i++)
    {
      n_tid = htons (tid[i]);
      memcpy (send_buf + len, &n_tid, INT16_LEN);
      len += INT16_LEN;
    }
  /* list of storages */
  n_nb_storage = htonl (storages.last);
  memcpy (send_buf + len, &n_nb_storage, INT32_LEN);
  len += INT32_LEN;
  for (i = 0; i < storages.last; i++)
    {				/* storage id */
      stringList_GetItem (&storages, &oid, i);
      memcpy (send_buf + len, oid, UUID_LEN);
      free (oid);
      len += UUID_LEN;
    }

  if (!send_all (conn, send_buf, buf_len))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  return 1;
}


int
returnEndTrans (int conn, int method)
{
  u_int32_t data_len;
  u_int16_t meth;
  char *send_buf;

  meth = htons (method);
  data_len = htonl (0);
  send_buf = (char *) malloc (RHEADER_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);
  if (!send_all (conn, send_buf, RHEADER_LEN))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  return 1;
}


int
returnGetAllSN (int conn, int method, struct nlist storages)
{
  u_int32_t buf_len, data_len, n_nb_storage, i;
  u_int16_t meth, n_port;
  char *send_buf;
  int len = 0;

  meth = htons (method);
  data_len =
    htonl (storages.last * (UUID_LEN + IP_LEN + INT16_LEN) + INT32_LEN);
  buf_len =
    RHEADER_LEN + storages.last * (UUID_LEN + IP_LEN + INT16_LEN) + INT32_LEN;
  send_buf = (char *) malloc (buf_len);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);

  /* list of all storage nodes */
  len = RHEADER_LEN;
  n_nb_storage = htonl (storages.last);
  memcpy (send_buf + len, &n_nb_storage, INT32_LEN);
  len += INT32_LEN;
  for (i = 0; i < storages.last; i++)
    {
      memcpy (send_buf + len, storages.objects[i].id, UUID_LEN);
      len += UUID_LEN;
      strncpy (send_buf + len, storages.objects[i].addr, IP_LEN);
/*       memcpy (send_buf + len, storages.objects[i].addr, IP_LEN); */
      len += IP_LEN;
      n_port = htons (storages.objects[i].port);
      memcpy (send_buf + len, &n_port, INT16_LEN);
      len += INT16_LEN;
    }

  if (!send_all (conn, send_buf, buf_len))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  return 1;
}


int
returnGetAllCN (int conn, int method, struct nlist storages)
{
  u_int32_t buf_len, data_len, n_nb_storage, i;
  u_int16_t meth, n_port;
  char *send_buf;
  int len = 0;

  meth = htons (method);
  data_len =
    htonl (storages.last * (UUID_LEN + IP_LEN + INT16_LEN) + INT32_LEN);
  buf_len =
    RHEADER_LEN + storages.last * (UUID_LEN + IP_LEN + INT16_LEN) + INT32_LEN;
  send_buf = (char *) malloc (buf_len);
  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);
  /* list of all client nodes */
  n_nb_storage = htonl (storages.last);
  memcpy (send_buf + 10, &n_nb_storage, INT32_LEN);
  len = RHEADER_LEN + INT32_LEN;
  for (i = 0; i < storages.last; i++)
    {
      memcpy (send_buf + len, storages.objects[i].id, UUID_LEN);
      len += UUID_LEN;
      strncpy (send_buf + len, storages.objects[i].addr, IP_LEN);
/*       memcpy (send_buf + len, storages.objects[i].addr, IP_LEN); */
      len += IP_LEN;
      n_port = htons (storages.objects[i].port);
      memcpy (send_buf + len, &n_port, INT16_LEN);
      len += INT16_LEN;
    }

  if (!send_all (conn, send_buf, buf_len))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  return 1;

}


int
returnGetSNInfo (int conn, int method, struct node info)
{
  u_int32_t buf_len, data_len, meth;
  u_int16_t id_len, addr_len, len;
  char *send_buf;

  meth = htons (method);
  addr_len = htons (strlen (info.addr));
  id_len = htons (strlen (info.id));
  data_len = htonl (2 + strlen (info.addr) + 2 + 2 + strlen (info.id));
  buf_len = RHEADER_LEN + ntohl (data_len);
  send_buf = (char *) malloc (buf_len);
  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);
  /* header */
  memcpy (send_buf, &meth, 2);
  memcpy (send_buf + 2, flags, 2);
  memcpy (send_buf + 4, &data_len, 4);
  memcpy (send_buf + 8, &rcode, 2);
  /* id, ip and port for a given storage */
  len = 10;
  memcpy (send_buf + len, &id_len, 2);
  len += 2;
  memcpy (send_buf + len, info.id, strlen (info.id));
  len += strlen (info.id);
  memcpy (send_buf + len, &addr_len, 2);
  len += 2;
  memcpy (send_buf + len, info.addr, strlen (info.addr));
  len += strlen (info.addr);
  memcpy (send_buf + len, &(info.port), 2);

  if (!send_all (conn, send_buf, buf_len))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  return 1;
}

int
returnStorageStart (int conn, int method, char id[UUID_LEN], u_int16_t status)
{
  u_int32_t data_len, meth;
  u_int16_t n_status;
  char *send_buf;

  meth = htons (method);
  data_len = htonl (UUID_LEN + INT16_LEN);
  send_buf = (char *) malloc (RHEADER_LEN + UUID_LEN + INT16_LEN);
  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);
  /* return master id */
  memcpy (send_buf + RHEADER_LEN, id, UUID_LEN);
  n_status = htons (status);
  memcpy (send_buf + RHEADER_LEN + UUID_LEN, &n_status, INT16_LEN);

  if (!send_all (conn, send_buf, RHEADER_LEN + UUID_LEN + INT16_LEN))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  return 1;
}


int
returnErrorMessage (int conn, int method, int code, char *message)
{
  u_int32_t data_len;
  u_int16_t n_msg_len, n_error_code, meth;
  char *send_buf;

  syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING),
	  "return error message for conn %d : method %d and code %d", conn,
	  method, code);
  meth = htons (method);
  data_len = htonl (INT16_LEN + strlen (message));
  send_buf = (char *) malloc (RHEADER_LEN + INT16_LEN + strlen (message));
  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  n_error_code = htons (code);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &n_error_code,
	  INT16_LEN);
  /* error message */
  n_msg_len = htons (strlen (message));
  memcpy (send_buf + RHEADER_LEN, &n_msg_len, INT16_LEN);
  memcpy (send_buf + RHEADER_LEN + INT16_LEN, message, strlen (message));

  if (!send_all (conn, send_buf, RHEADER_LEN + INT16_LEN + strlen (message)))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  return 1;
}


int
returnStorageSanity (int conn, int method, struct tlist tl)
{
  u_int64_t n_tid;
  u_int32_t data_len, n_nb_trans, size = 0, len, n_nb_storage;
  u_int16_t meth, i, j, n_port;
  char *send_buf;

  meth = htons (method);
  /* compute len , same as returnGetTransSN */

  for (i = 0; i < tl.last; i++)
    size +=
      tl.objects[i].list.last * (IP_LEN + INT16_LEN) + INT64_LEN + INT32_LEN;

  data_len = htonl (INT32_LEN + size);
  send_buf = (char *) malloc (RHEADER_LEN + INT32_LEN + size);
  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);
  /* list of storages for each transaction */
  n_nb_trans = htonl (tl.last);
  memcpy (send_buf + RHEADER_LEN, &n_nb_trans, INT32_LEN);
  len = RHEADER_LEN + INT32_LEN;
  for (i = 0; i < tl.last; i++)
    {
      /* transaction id */
      n_tid = htonll (tl.objects[i].tid);
      memcpy (send_buf + len, &n_tid, INT64_LEN);
      len += INT64_LEN;
      /* storage nb */
      n_nb_storage = htonl (tl.objects[i].list.last / 2);
      memcpy (send_buf + len, &n_nb_storage, INT32_LEN);
      len += INT32_LEN;
      for (j = 0; j < tl.objects[i].list.last; j++)
	{			/* storage ip */
	  memcpy (send_buf + len, tl.objects[i].list.objects[j].data, IP_LEN);
	  len += IP_LEN;
	  j++;
	  /* storage port */
	  n_port =
	    htons (strtol (tl.objects[i].list.objects[j].data, NULL, 10));
	  memcpy (send_buf + len, &n_port, INT16_LEN);
	  len += INT16_LEN;
	}
    }

  if (!send_all (conn, send_buf, RHEADER_LEN + INT32_LEN + size))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  return 1;
}


int
returnStorageIndex (int conn, int method, struct transactionList *txn)
{
  u_int64_t n_tid, n_oid;
  u_int16_t meth;
  u_int32_t data_len, size =
    0, buf_len, len, n_nb_oid, obj_nb, n_txn_nb, txn_nb;
  char *send_buf;

  for (txn_nb = 0; txn_nb < txn->last; txn_nb++)
    size += txn->txn[txn_nb].objects.last * INT64_LEN;

  meth = htons (method);
  data_len = htonl (INT32_LEN + txn->last * (INT64_LEN + INT32_LEN) + size);
  buf_len =
    RHEADER_LEN + INT32_LEN + txn->last * (INT64_LEN + INT32_LEN) + size;
  send_buf = (char *) malloc (buf_len);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);

  /* data */
  len = RHEADER_LEN;
  n_txn_nb = htonl (txn->last);
  memcpy (send_buf + len, &n_txn_nb, INT32_LEN);
  len += INT32_LEN;
  for (txn_nb = 0; txn_nb < txn->last; txn_nb++)
    {
      n_tid = htonll (txn->txn[txn_nb].tid);
      memcpy (send_buf + len, &n_tid, INT64_LEN);
      len += INT64_LEN;
      n_nb_oid = htonl (txn->txn[txn_nb].objects.last);
      memcpy (send_buf + len, &n_nb_oid, INT32_LEN);
      len += INT32_LEN;
      for (obj_nb = 0; obj_nb < txn->txn[txn_nb].objects.last; obj_nb++)
	{
	  n_oid = htonll (txn->txn[txn_nb].objects.list[obj_nb].oid);
	  memcpy (send_buf + len, &n_oid, INT64_LEN);
	  len += INT64_LEN;
	}
    }
  if (!send_all (conn, send_buf, buf_len))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  return 1;
}


int
returnGetStorageForTrans (int conn, int method, struct stringList storages)
{
  u_int32_t data_len, n_nb_storage, i;
  u_int16_t meth, port, n_port;
  char *send_buf, *ip, *s_port;
  int len = 0;

  meth = htons (method);
  /* compute len */
  n_nb_storage = htonl (storages.last / 2);
  data_len = htonl (storages.last / 2 * (IP_LEN + INT16_LEN) + INT32_LEN);
  send_buf =
    (char *) malloc (RHEADER_LEN + storages.last / 2 * (IP_LEN + INT16_LEN) +
		     INT32_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);
  /* data */
  len = RHEADER_LEN;
  memcpy (send_buf + len, &n_nb_storage, INT32_LEN);
  len += INT32_LEN;
  /* storage list */
  for (i = 0; i < storages.last; i++)
    {
      stringList_GetItem (&storages, &ip, i);
      memcpy (send_buf + len, ip, IP_LEN);
      len += IP_LEN;
      i++;
      stringList_GetItem (&storages, &s_port, i);
      sscanf (s_port, "%hu", &port);
      n_port = htons (port);
      memcpy (send_buf + len, &n_port, INT16_LEN);
      len += INT16_LEN;
      free (ip);
      free (s_port);
    }


  if (!send_all
      (conn, send_buf,
       RHEADER_LEN + storages.last / 2 * (IP_LEN + INT16_LEN) + INT32_LEN))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  return 1;
}
