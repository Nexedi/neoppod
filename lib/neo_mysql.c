#define _GNU_SOURCE

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <mysql/mysql.h>
#include <syslog.h>
#include "neo_struct.h"
#include "neo_mysql.h"
#include "neo_struct.h"

#include <time.h>


extern char database_dir[BUF_SIZE];

/* common functions */
int
database_thread_init ()
{
  return mysql_thread_init ();
}

void
database_thread_end ()
{
  return mysql_thread_end ();
}

int
database_close (struct database *orig_db)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;

  mysql_close (db->sql);
  free (db);
  mysql_server_end ();
  return 1;
}


/* part for the storage node */
struct database *
database_storage_open (int create)
{
  struct database_mysql *db;

  char datadir_opt[BUF_SIZE + 11];
  memset(datadir_opt, 0, BUF_SIZE+11);
  strncpy (datadir_opt, "--datadir=", 10);
  strncat (datadir_opt, database_dir, BUF_SIZE);

  char *server_args[] = {
    "storage", /* this string is not used */
    datadir_opt,
    "--skip-innodb",
    "--key-buffer=256M",
    "--record-buffer=32M"
  };
  static char *server_groups[] = {
    "embedded",
    "server",
    "storage_SERVER",
    (char *) NULL
  };

  db = malloc (sizeof (struct database_mysql));
  if (!db)
    return 0;

  mysql_server_init (sizeof (server_args) / sizeof (char *), server_args,
		     server_groups);

  db->sql = mysql_init (NULL);
  if (!db->sql)
    return 0;

  if (!mysql_real_connect (db->sql, NULL, "root", NULL, NULL, 0, NULL, 0))
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  if (mysql_query (db->sql, "CREATE DATABASE IF NOT EXISTS storage") != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  if (mysql_select_db (db->sql, "storage") != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  if (create)
    {
      if (mysql_query (db->sql, "DROP TABLE IF EXISTS object") != 0)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
		  mysql_error (db->sql));
	  return 0;
	}

      if (mysql_query (db->sql, "DROP TABLE IF EXISTS transaction") != 0)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
		  mysql_error (db->sql));
	  return 0;
	}

      if (mysql_query
	  (db->sql,
	   "CREATE TABLE transaction (tid BIGINT UNSIGNED NOT NULL, user VARCHAR(20), description VARCHAR(50), extension VARCHAR(100), PRIMARY KEY(tid)) TYPE=MYISAM")
	  != 0)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
		  mysql_error (db->sql));
	  return 0;
	}

      if (mysql_query
	  (db->sql,
	   "CREATE TABLE object (id BIGINT UNSIGNED NOT NULL, serial BIGINT UNSIGNED NOT NULL, tid BIGINT UNSIGNED NOT NULL, FOREIGN KEY (tid) REFERENCES transaction(tid) on delete cascade, crc INT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(id,serial)) TYPE=MYISAM")
	  != 0)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
		  mysql_error (db->sql));
	  return 0;
	}
    }

  return (struct database *) db;
}


int
database_storage_put_object (struct database *orig_db, u_int64_t oid,
			     u_int64_t serial, u_int64_t tid,
			     void *object, u_int32_t size, int32_t crc)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char *buf;
  char *p;

  /* allocate buffer for size of object + rest of the request */
  /* size is * 2 for the mysql_real_escape_string (see mysql doc) */
  buf = (char *) malloc (size * 2 + 500);
  memset (buf, 0, size * 2 +  500);
  p = buf;
  p +=
    sprintf (buf,
	     "INSERT INTO object VALUES ('%llu', '%llu', '%llu', '%d', '",
	     oid, serial, tid, crc);
  p += mysql_real_escape_string (db->sql, p, object, size);
  p = stpcpy (p, "')");
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : PutObject %s", buf);

  if (mysql_real_query (db->sql, buf, p - buf) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s, size of object : %d, size of buffer %d, size of object %d",
	      mysql_error (db->sql), size, strlen(buf), strlen(object));
      free (buf);
      return 0;
    }
  free (buf);
  return 1;
};


int
database_storage_put_trans (struct database *orig_db, u_int64_t tid,
			    char *user, char *desc, char *ext)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  char *p;

  p = buf;
  memset (buf, 0, 4000);
  p +=
    sprintf (buf, "INSERT INTO transaction VALUES ('%llu', '%s', '%s', '%s')",
	     tid, user, desc, ext);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : PutTrans %s", buf);
  if (mysql_real_query (db->sql, buf, p - buf) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }
  return 1;
};

int
database_storage_del_trans (struct database *orig_db, u_int64_t tid)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  char *p;
  memset (buf, 0, 4000);
  p = buf;
  p += sprintf (buf, "DELETE FROM transaction WHERE tid = '%llu'", tid);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : DeleteTrans %s", buf);
  if (mysql_real_query (db->sql, buf, p - buf) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }
  return 1;
};

int
database_storage_store_trans (struct database *orig_db, u_int64_t tid,
			      char *user, char *desc, char *ext)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  char *p;

  p = buf;
  memset (buf, 0, 4000);

  /* delete trans */
  database_storage_del_trans (orig_db, tid);
  /* add new */
  p +=
    sprintf (buf, "INSERT INTO transaction VALUES ('%llu', '%s', '%s', '%s')",
	     tid, user, desc, ext);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : StoreTrans %s", buf);
  if (mysql_real_query (db->sql, buf, p - buf) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }
  return 1;
};

int
database_storage_check_sanity (struct database *orig_db, u_int64_t tid,
			       u_int64_t oid)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  char *p;
  MYSQL_RES *res;
  MYSQL_ROW row;

  p = buf;
  memset (buf, 0, 4000);

  p +=
    sprintf (buf,
	     "SELECT COUNT(*) FROM object WHERE id = '%llu' AND tid = '%llu' ",
	     oid, tid);

  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : check sanity %s", buf);
  if (mysql_real_query (db->sql, buf, p - buf) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }
  res = mysql_store_result (db->sql);
  if (!res)
    return 0;

  row = mysql_fetch_row (res);
  if (!row)
    return 0;

  if (mysql_num_rows (res) == 0)
    return 0;

  mysql_free_result (res);
  return 1;
};

int
database_storage_get_db_size (struct database *orig_db, u_int64_t *size)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  unsigned long len;
  MYSQL_RES *res;
  MYSQL_ROW row;

  memset (buf, 0, 4000);
  /* XXX doesn't work */
  len =
    sprintf (buf, "SELECT SUM(len(data)) FROM object");
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : GetDBSize %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  res = mysql_store_result (db->sql);
  if (!res)
    return 0;

  row = mysql_fetch_row (res);
  if (!row)
    return 0;

  sscanf (row[0], "%llu", size);

  mysql_free_result (res);

  return 1;
};


int
database_storage_get_object (struct database *orig_db, u_int64_t oid,
			     u_int64_t serial, void **object, int32_t * crc)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  unsigned long len;
  MYSQL_RES *res;
  MYSQL_ROW row;
  unsigned long *lengths;
  memset (buf, 0, 4000);

  len =
    sprintf (buf,
	     "SELECT data, crc FROM object WHERE id = '%llu' AND serial = '%llu'",
	     oid, serial);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : GetObject %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  res = mysql_store_result (db->sql);
  if (!res)
    return 0;

  row = mysql_fetch_row (res);
  if (!row)
    return 0;

  lengths = mysql_fetch_lengths (res);

  len = lengths[0];
  *object = malloc (len + 1);
  memset (*object, 0, len + 1);
  if (!*object)
    return 0;
  memcpy (*object, row[0], len);

  sscanf (row[1], "%d", crc);

  mysql_free_result (res);

  return 1;
};


int
database_storage_get_trans_info (struct database *orig_db, u_int64_t tid,
				 struct undoInfo *hist)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  char *p;
  unsigned long long len;
  MYSQL_RES *res;
  MYSQL_ROW row;
  unsigned long *lengths;
  memset (buf, 0, 4000);
  p = buf;
  len =
    sprintf (buf,
	     "SELECT user,description FROM transaction WHERE tid = '%llu'",
	     tid);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : GetTransInfo %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  res = mysql_store_result (db->sql);
  if (!res)
    return 0;

  row = mysql_fetch_row (res);
  if (!row)
    return 0;

  lengths = mysql_fetch_lengths (res);
  /* user */
  len = lengths[0];
  hist->user = (char *) malloc (len + 1);
  memset (hist->user, 0, len + 1);
  if (!hist->user)
    return 0;
  memcpy (hist->user, row[0], len);

  /* desc */
  len = lengths[1];
  hist->desc = (char *) malloc (len + 1);
  memset (hist->desc, 0, len + 1);
  if (!hist->desc)
    return 0;
  memcpy (hist->desc, row[1], len);

  mysql_free_result (res);
  return 1;
};


int
database_storage_get_object_info (struct database *orig_db, u_int64_t oid,
				  u_int64_t serial, struct hist *hist)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  char *p;
  unsigned long long len;
  MYSQL_RES *res;
  MYSQL_ROW row;
  unsigned long *lengths;
  memset (buf, 0, 4000);
  p = buf;
  len =
    sprintf (buf,
	     "SELECT transaction.user,transaction.description, object.data FROM object, transaction WHERE object.id = '%llu' AND object.serial = '%llu' AND transaction.tid = object.tid",
	     oid, serial);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : getObjectInfo %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  res = mysql_store_result (db->sql);
  if (!res)
    return 0;

  row = mysql_fetch_row (res);
  if (!row)
    return 0;

  lengths = mysql_fetch_lengths (res);
  /* user */
  len = lengths[0];
  hist->user = (char *) malloc (len + 1);
  if (!hist->user)
    return 0;
  memset (hist->user, 0, len + 1);
  memcpy (hist->user, row[0], len);
  /* desc */
  len = lengths[1];
  hist->desc = (char *) malloc (len + 1);
  if (!hist->desc)
    return 0;
  memset (hist->desc, 0, len + 1);
  memcpy (hist->desc, row[1], len);
  /* size */
  hist->size = lengths[2];
  mysql_free_result (res);

  return 1;
};

int
database_storage_get_trans_data (struct database *orig_db, u_int64_t tid,
				 char **user, char **desc, char **ext,
				 struct objectList **list)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  unsigned long len;
  MYSQL_RES *res, *ress;
  MYSQL_ROW row;
  unsigned long *lengths;
  u_int32_t size, nb_rows, i;
  u_int64_t oid, serial;

  memset (buf, 0, 4000);
  len =
    sprintf (buf,
	     "SELECT user, description, extension FROM transaction WHERE tid = '%llu'",
	     tid);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : getTransaData %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  res = mysql_store_result (db->sql);
  if (!res)
    return 0;

  row = mysql_fetch_row (res);
  if (!row)
    return 0;
  /* return user descritpion and size for the object */
  lengths = mysql_fetch_lengths (res);
  /* user */
  size = lengths[0];
  *user = (char *) malloc (size + 1);
  if (!*user)
    return 0;
  memset (*user, 0, size + 1);
  memcpy (*user, row[0], size);
  /* desc */
  size = lengths[1];
  *desc = (char *) malloc (size + 1);
  if (!*desc)
    return 0;
  memset (*desc, 0, size + 1);
  memcpy (*desc, row[1], size);
  /* extension */
  size = lengths[2];
  *ext = (char *) malloc (size + 1);
  if (!*ext)
    return 0;
  memset (*ext, 0, size + 1);
  memcpy (*ext, row[2], size);

  mysql_free_result (res);

  /* now get list object with data... */
  memset (buf, 0, 4000);
  len =
    sprintf (buf,
	     "SELECT id, serial, data FROM object WHERE tid = '%llu'", tid);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : getTransData %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  ress = mysql_store_result (db->sql);
  if (!ress)
    return 0;

  nb_rows = mysql_num_rows (ress);
  /* init list */
  *list = (struct objectList *) malloc (sizeof (struct objectList));
  objectList_New (*list, nb_rows);

  /* add oid, serial and data to list */
  for (i = 0; i < nb_rows; i++)
    {
      /* get next row */
      row = mysql_fetch_row (ress);
      if (!row)
	return 0;
      sscanf (row[0], "%llu", &oid);
      sscanf (row[1], "%llu", &serial);
      objectList_Append (*list, oid, serial, row[2]);
    }

  mysql_free_result (ress);

  return 1;
};


int
database_storage_get_object_list (struct database *orig_db, u_int64_t tid,
				  u_int64_t ** list, u_int32_t * nb_oid)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  unsigned long len, nb_rows, i;
  MYSQL_RES *res;
  MYSQL_ROW row;
  u_int64_t *oid;

  *nb_oid = 0;
  memset (buf, 0, 4000);

  len = sprintf (buf, "SELECT id FROM object WHERE tid = '%llu'", tid);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : getObjectList %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  res = mysql_store_result (db->sql);
  if (!res)
    return 0;

  nb_rows = mysql_num_rows (res);

  /* init list */
  oid = (u_int64_t *) malloc (sizeof (u_int64_t) * nb_rows);

  /* add oid to list */
  for (i = 0; i < nb_rows; i++)
    {
      /* get next row */
      row = mysql_fetch_row (res);
      if (!row)
	return 0;
      sscanf (row[0], "%llu", &(oid[i]));
      (*nb_oid)++;
    }
  mysql_free_result (res);
  *list = oid;
  return 1;
};


int
database_storage_trans_exist (struct database *orig_db, u_int64_t tid,
			      int *exist)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  unsigned long len;
  MYSQL_RES *res;

  memset (buf, 0, 4000);

  *exist = 0;

  len = sprintf (buf, "SELECT user FROM transaction WHERE tid = '%llu'", tid);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : transExist %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  res = mysql_store_result (db->sql);
  if (!res)
    return 0;

  if (mysql_num_rows (res) != 0)
    *exist = 1;

  return 1;
};




/* part for the master node */
struct database *
database_master_open (int create)
{
  struct database_mysql *db;

  char datadir_opt[BUF_SIZE + 11];
  memset(datadir_opt, 0, BUF_SIZE+11);
  strncpy (datadir_opt, "--datadir=", 10);
  strncat (datadir_opt, database_dir, BUF_SIZE);

  char *server_args[] = {
    "master",
    datadir_opt,
    "--skip-innodb",
    "--key-buffer=256M",
    "--record-buffer=32M",
  };
  static char *server_groups[] = {
    "embedded",
    "server",
    "master_SERVER",
    (char *) NULL
  };

  db = malloc (sizeof (struct database_mysql));
  if (!db)
    return 0;

  mysql_server_init (sizeof (server_args) / sizeof (char *), server_args,
		     server_groups);

  db->sql = mysql_init (NULL);
  if (!db->sql)
    return 0;

  if (!mysql_real_connect (db->sql, NULL, "root", NULL, NULL, 0, NULL, 0))
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  if (mysql_query (db->sql, "CREATE DATABASE IF NOT EXISTS master") != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  if (mysql_select_db (db->sql, "master") != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  if (create)			/* start with a new database */
    {
      if (mysql_query (db->sql, "DROP TABLE IF EXISTS object") != 0)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
		  mysql_error (db->sql));
	  return 0;
	}

      if (mysql_query (db->sql, "DROP TABLE IF EXISTS transaction") != 0)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
		  mysql_error (db->sql));
	  return 0;
	}

      if (mysql_query (db->sql, "DROP TABLE IF EXISTS storage") != 0)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
		  mysql_error (db->sql));
	  return 0;
	}

      if (mysql_query (db->sql, "DROP TABLE IF EXISTS client") != 0)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
		  mysql_error (db->sql));
	  return 0;
	}

      if (mysql_query (db->sql, "DROP TABLE IF EXISTS location") != 0)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
		  mysql_error (db->sql));
	  return 0;
	}

      if (mysql_query
	  (db->sql,
	   "CREATE TABLE transaction (tid BIGINT UNSIGNED NOT NULL, previous BIGINT UNSIGNED, status TINYINT UNSIGNED, PRIMARY KEY(tid)) TYPE=MYISAM")
	  != 0)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
		  mysql_error (db->sql));
	  return 0;
	}

      if (mysql_query
	  (db->sql,
	   "CREATE TABLE object (id BIGINT UNSIGNED NOT NULL, serial BIGINT UNSIGNED NOT NULL, tid BIGINT UNSIGNED NOT NULL, FOREIGN KEY (tid) REFERENCES transaction(tid) on delete cascade, PRIMARY KEY(id,serial)) TYPE=MYISAM")
	  != 0)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
		  mysql_error (db->sql));
	  return 0;
	}

      if (mysql_query
	  (db->sql,
	   "CREATE TABLE storage (id VARCHAR(36) NOT NULL, ip VARCHAR(15), port SMALLINT UNSIGNED, status TINYINT UNSIGNED, PRIMARY KEY(id)) TYPE=MYISAM")
	  != 0)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
		  mysql_error (db->sql));
	  return 0;
	}

      if (mysql_query
	  (db->sql,
	   "CREATE TABLE client (id VARCHAR(36) NOT NULL, ip VARCHAR(15), port SMALLINT UNSIGNED, connection_descriptor SMALLINT UNSIGNED, PRIMARY KEY(id)) TYPE=MYISAM")
	  != 0)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
		  mysql_error (db->sql));
	  return 0;
	}

      if (mysql_query
	  (db->sql,
	   "CREATE TABLE location (tid BIGINT UNSIGNED NOT NULL, sid VARCHAR(36) NOT NULL, FOREIGN KEY (tid) REFERENCES transaction(tid) on delete cascade, FOREIGN KEY (sid) REFERENCES storage(id)) TYPE=MYISAM")
	  != 0)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
		  mysql_error (db->sql));
	  return 0;
	}
      syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : database recreated");
    }

  return (struct database *) db;
}

int
database_master_get_object_by_oid (struct database *orig_db, u_int64_t oid,
				   u_int64_t * serial,
				   struct stringList **list)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000], id[UUID_LEN + 1];
  int i, nb_storage_id = 0;
  unsigned long len;
  MYSQL_RES *res;
  MYSQL_ROW row;

  /* then get storages for the last serial of oid */
  memset (buf, 0, 4000);
  len =
    sprintf (buf,
	     "SELECT object.serial, storage.id FROM object, location, storage WHERE object.id = '%llu' AND location.tid = object.tid AND storage.id = location.sid AND storage.status = 0 AND object.serial = (SELECT MAX(object.serial) FROM object, transaction WHERE object.id = '%llu' AND transaction.tid = object.tid AND transaction.status = 0)",
	     oid, oid);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : ObjectByOid %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  res = mysql_store_result (db->sql);
  if (!res)
    return 0;

  if ((nb_storage_id = mysql_num_rows (res)) == 0)
    {
      mysql_free_result (res);
      return -1;
    }

  row = mysql_fetch_row (res);
  if (!row)
    return 0;

  sscanf (row[0], "%llu", serial);

  /* get storage id list */
  *list = (struct stringList *) malloc (sizeof (struct stringList));
  stringList_New (*list, nb_storage_id, UUID_LEN);

  for (i = 0; i < nb_storage_id; i++)
    {
      memset (id, 0, UUID_LEN + 1);
      memcpy (id, row[1], UUID_LEN);
      stringList_Append (*list, id);
      row = mysql_fetch_row (res);
      if (!row)
	break;
    }
  mysql_free_result (res);
  return 1;
}

int
database_master_get_object_by_serial (struct database *orig_db,
				      u_int64_t oid, u_int64_t serial,
				      struct stringList **list)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000], id[UUID_LEN + 1];
  unsigned long len;
  int i, nb_storage_id = 0;
  MYSQL_RES *res;
  MYSQL_ROW row;

  memset (buf, 0, 4000);
  len =
    sprintf (buf,
	     "SELECT storage.id FROM object, transaction, location, storage WHERE object.id = '%llu' AND object.serial = '%llu' AND transaction.tid = object.tid AND location.tid = transaction.tid AND storage.id = location.sid AND storage.status = 0 AND transaction.status = 0",
	     oid, serial);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : ObjectBySerial %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  res = mysql_store_result (db->sql);
  if (!res)
    return 0;

  if ((nb_storage_id = mysql_num_rows (res)) == 0)
    {
      mysql_free_result (res);
      return -1;
    }

  row = mysql_fetch_row (res);
  if (!row)
    return 0;

  /* get storage id list */
  *list = (struct stringList *) malloc (sizeof (struct stringList));
  stringList_New (*list, nb_storage_id, UUID_LEN);
  for (i = 0; i < nb_storage_id; i++)
    {
      memset (id, 0, UUID_LEN + 1);
      memcpy (id, row[0], UUID_LEN);
      stringList_Append (*list, id);
      row = mysql_fetch_row (res);
      if (!row)
	break;
    }
  mysql_free_result (res);
  return 1;
}

int
database_master_get_serial (struct database *orig_db, u_int64_t oid,
			    u_int64_t * serial)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  unsigned long len;
  MYSQL_RES *res;
  MYSQL_ROW row;

  memset (buf, 0, 4000);
  len =
    sprintf (buf, "SELECT MAX(serial) FROM object, transaction WHERE object.id = '%llu' AND transaction.tid = object.tid AND transaction.status = 0",
	     oid);
  /* tester le null */
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : GetSerial %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  res = mysql_store_result (db->sql);
  if (!res)
    return 0;

  if (mysql_num_rows (res) == 0)
    {
      mysql_free_result (res);
      return -1;
    }

  row = mysql_fetch_row (res);
  if (!row)
    return 0;
  if (row[0] == NULL)
    {
      mysql_free_result (res);
      return -1;
    }
  /* get serial */
  sscanf (row[0], "%llu", serial);
  mysql_free_result (res);
  return 1;
}


int
database_master_get_trans_storages (struct database *orig_db,
				    u_int64_t tid, int16_t first,
				    int16_t last, struct tlist **list)
{
  u_int64_t ltid;
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000], id[UUID_LEN + 1];
  unsigned long len;
  int16_t i = 0;
  int nb_storage_id = 0, j, k = 0;
  MYSQL_RES *res;
  MYSQL_ROW row;

  ltid = tid;
  *list = (struct tlist *) malloc (sizeof (struct tlist));
  init_tlist (*list, last - first);
  while (i < last)
    {
      if (i < first)
	{
	  /* only get the previous transaction id */
	  memset (buf, 0, 4000);
	  len =
	    sprintf (buf,
		     "SELECT previous FROM transaction WHERE tid = '%llu'",
		     ltid);
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : GetTransSN %s",
		  buf);

	  if (mysql_real_query (db->sql, buf, len) != 0)
	    {
	      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
		      mysql_error (db->sql));
	      return 0;
	    }

	  res = mysql_store_result (db->sql);
	  if (!res)
	    return 0;
	  row = mysql_fetch_row (res);
	  if (!row)
	    return 0;
	  sscanf (row[0], "%llu", &ltid);
	  mysql_free_result (res);
	}
      else if (i >= first)
	{
	  /* get and add transaction and informations to list */
	  memset (buf, 0, 4000);
	  len =
	    sprintf (buf,
		     "SELECT transaction.previous, storage.id FROM transaction, location, storage WHERE location.tid = '%llu' AND transaction.tid = '%llu' AND storage.id = location.sid AND storage.status = 0",
		     ltid, ltid);
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : GetTransSN %s",
		  buf);

	  if (mysql_real_query (db->sql, buf, len) != 0)
	    {
	      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
		      mysql_error (db->sql));
	      return 0;
	    }
	  res = mysql_store_result (db->sql);
	  if (!res)
	    return 0;
	  /* add the current transaction */
	  nb_storage_id = mysql_num_rows (res);
	  add_trans (list, ltid, nb_storage_id);
	  for (j = 0; j < nb_storage_id; j++)
	    {
	      row = mysql_fetch_row (res);
	      if (!row)
          return 0;
	      memset (id, 0, UUID_LEN + 1);
	      memcpy (id, row[1], UUID_LEN);
	      /* add storage id */
	      add_cobject (&((*list)->objects[k].list), id, "");
	    }
    /* check for row here... */
	  sscanf (row[0], "%llu", &ltid);
	  k++;
	  mysql_free_result (res);
	}
      i++;
    }
  return 1;
}

int
database_master_get_object_hist (struct database *orig_db, u_int64_t oid,
				 u_int16_t length, struct tlist **list)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000], id[UUID_LEN + 1];
  unsigned long len;
  u_int16_t i = 0, nb_storage_id = 0, j;
  u_int64_t serial;
  clock_t c1, c2;
  float t;
  static float time = 0;
  MYSQL_RES *res, *result;
  MYSQL_ROW row;

  *list = (struct tlist *) malloc (sizeof (struct tlist));
  init_tlist (*list, length);

  /* get and add transaction and informations to list */
  memset (buf, 0, 4000);
  /* first get all serial ordered for the client */
  len =
    sprintf (buf,
	     "SELECT serial FROM object WHERE id = '%llu' ORDER BY serial DESC",
	     oid);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : GetObjectHist %s", buf);
  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  res = mysql_store_result (db->sql);
  if (!res)
    return 0;
  if (mysql_num_rows (res) == 0)
    return -1;
  while (i < length)
    {
      /* get serial */
      row = mysql_fetch_row (res);
      if (!row)
        break;
      sscanf (row[0], "%llu", &serial);
      /* get storage for this serial */
      memset (buf, 0, 4000);
      len =
        sprintf (buf,
                 "SELECT storage.id, transaction.tid FROM object, transaction, location, storage WHERE object.id = '%llu' AND object.serial = '%llu' AND transaction.tid = object.tid AND transaction.status = 0 AND location.tid = transaction.tid AND storage.id = location.sid AND storage.status = 0",
                 oid, serial);
      syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : GetObjectHist %s",
              buf);
      /* time */
      c1 = clock();
      if (mysql_real_query (db->sql, buf, len) != 0)
        {
          syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
                  mysql_error (db->sql));
          return 0;
        }
      /* time */
      c2 = clock();
      t =  (float)(c2 - c1) / CLOCKS_PER_SEC;
      if (t > 0)
        {
          time += t;
          printf ("SQL time : %f, time %f\n", t, time);
        }
      result = mysql_store_result (db->sql);
      if (!result)
        continue;

      if ((nb_storage_id = mysql_num_rows (result)) == 0)
        continue; /* maybe an undone transaction */
      add_trans (list, serial, nb_storage_id);
      /* storage list */
      for (j = 0; j < nb_storage_id; j++)
        {
          row = mysql_fetch_row (result);
          if (!row)
            break;
          /* storage id */
          memset (id, 0, UUID_LEN + 1);
          memcpy (id, row[0], UUID_LEN);
          add_cobject (&((*list)->objects[i].list), id, "");
        }
      i++;
      mysql_free_result (result);
    }
  mysql_free_result (res);
  return 1;
}


int
database_master_undo_trans (struct database *orig_db, u_int64_t tid,
			    struct stringList **list)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000], id[UUID_LEN];
  unsigned long len;
  u_int16_t nb_storage_id, i;
  int rows, changed, warn;
  MYSQL_RES *res;
  MYSQL_ROW row;

  memset (buf, 0, 4000);
  len =
    sprintf (buf, "UPDATE transaction SET status = 1 WHERE tid = '%llu'",
	     tid);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : undoTrans %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  /* check if something has been updated otherwise it is unknown transaction */
  sscanf (mysql_info (db->sql), "Rows matched: %d Changed: %d  Warnings: %d",
	  &rows, &changed, &warn);
  if (rows == 0)
    return -1;

  /* also return a list of storages to undo transaction on them */
  memset (buf, 0, 4000);
  len =
    sprintf (buf,
	     "SELECT storage.id FROM storage, location WHERE location.tid = '%llu' AND storage.id = location.sid AND storage.status = 0",
	     tid);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : undoTrans %s", buf);
  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  res = mysql_store_result (db->sql);
  if (!res)
    return 0;

  if ((nb_storage_id = mysql_num_rows (res)) == 0)
    return -1;
  row = mysql_fetch_row (res);
  if (!row)
    return 0;

  /* get storage id list */
  *list = (struct stringList *) malloc (sizeof (struct stringList));
  stringList_New (*list, nb_storage_id, UUID_LEN);
  for (i = 0; i < nb_storage_id; i++)
    {
      memset (id, 0, UUID_LEN + 1);
      memcpy (id, row[0], UUID_LEN);
      stringList_Append (*list, id);
      row = mysql_fetch_row (res);
      if (!row)
	break;
    }
  mysql_free_result (res);

  return 1;
}

int
database_master_begin_trans (struct database *orig_db, u_int64_t tid,
			     u_int64_t ltid, struct stringList list)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  u_int32_t i;
  unsigned long len;
  char *id;

  memset (buf, 0, 4000);
  /* insert new transaction in table */
  /* transaction status : 0 : ok, 1 : undone, 2 : not validated */
  len =
    sprintf (buf, "INSERT INTO transaction VALUES ('%llu', '%llu', 2)", tid,
	     ltid);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : BeginTrans %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }
  /* insert pair tid-storage_id into location table */
  for (i = 0; i < list.last; i++)
    {
      memset (buf, 0, 4000);
      stringList_GetItem (&list, &id, i);
      len =
	sprintf (buf, "INSERT INTO location VALUES ('%llu', '%s')", tid, id);
      free (id);
      syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : BeginTrans %s", buf);

      if (mysql_real_query (db->sql, buf, len) != 0)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
		  mysql_error (db->sql));
	  return 0;
	}
    }
  return 1;
}

int
database_master_put_object (struct database *orig_db, u_int64_t tid,
			    u_int64_t oid, u_int64_t serial)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  unsigned long len;
  u_int64_t last_serial;
  int res;

  memset (buf, 0, 4000);
  /* check if serial is the last one for the object, if not client try to validate a too old version of version */
  if ((res = database_master_get_serial (orig_db, oid, &last_serial)) == 0)
    return 0;
  else if (res == 1)
    if (serial != last_serial)
      {
	syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING),
		"SQL : Invalid transaction %llu for object %llu, serial %llu != %llu",
		tid, oid, serial, last_serial);
	return 0;		/* transaction invalid */
      }

  /* insert new object into table */
  len =
    sprintf (buf, "INSERT INTO object VALUES ('%llu', '%llu', '%llu')", oid,
	     tid, tid);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : PutObject %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }
  return 1;
}


int
database_master_end_trans (struct database *orig_db, u_int64_t tid)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  unsigned long len;
  /* update status of transaction */
  memset (buf, 0, 4000);
  len =
    sprintf (buf, "UPDATE transaction SET status = 0 WHERE tid = '%llu'",
	     tid);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : EndTrans %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }
  return 1;
}

int
database_master_delete_client (struct database *orig_db, char oid[UUID_LEN+1])
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  unsigned long len;

  memset (buf, 0, 4000);
  len = sprintf (buf, "DELETE FROM client WHERE id = '%s'", oid);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : DeleteClient %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  return 1;
}

int
database_master_remove_client (struct database *orig_db, u_int16_t conn)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  unsigned long len;

  memset (buf, 0, 4000);
  len = sprintf (buf, "DELETE FROM client WHERE connection_descriptor = '%d'", conn);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : Remove Client %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  return 1;
}


int
database_master_delete_object (struct database *orig_db, u_int64_t oid,
			       u_int64_t serial)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  unsigned long len;

  memset (buf, 0, 4000);
  len =
    sprintf (buf, "DELETE FROM object WHERE id = '%llu' AND serial='%llu'",
	     oid, serial);

  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : DeleteObject %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  return 1;
}


int
database_master_close_storage (struct database *orig_db, char id[UUID_LEN+1])
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  unsigned long len;

  memset (buf, 0, 4000);
  /* set status to close for the storage */
  len = sprintf (buf, "UPDATE storage SET status = 1 WHERE id = '%s'", id);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : CloseStorage %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }
  return 1;
}

int
database_master_unreliable_storage (struct database *orig_db,
				    char id[UUID_LEN+1])
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  unsigned long len;

  memset (buf, 0, 4000);
  /* set status to unreliable for the storage */
  len = sprintf (buf, "UPDATE storage SET status = 2 WHERE id = '%s'", id);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : UnreliableStorage %s",
	  buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }
  return 1;
}


int
database_master_storage_ready (struct database *orig_db, char id[UUID_LEN+1])
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  unsigned long len;

  memset (buf, 0, 4000);
  /* set status to ready for the storage */
  len = sprintf (buf, "UPDATE storage SET status = 0 WHERE id = '%s'", id);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : StorageReady %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }
  return 1;
}


int
database_master_storage_starting (struct database *orig_db, char id[UUID_LEN+1],
				  char ip[IP_LEN], u_int16_t port,
				  u_int16_t * status)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  unsigned long len;
  MYSQL_RES *res;
  MYSQL_ROW row;

  /* status of storage node : 0 -> reliable, 1 -> closed, 2 -> unreliable, 3 -> starting */

  memset (buf, 0, 4000);
  /* try to get storage from database to see if already exists */
  len = sprintf (buf, "SELECT status FROM storage WHERE id = '%s'", id);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : StorageStart %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  res = mysql_store_result (db->sql);
  if (mysql_num_rows (res) != 0)
    {
      /* get status */
      row = mysql_fetch_row (res);
      if (!row)
	return 0;
      sscanf (row[0], "%hu", status);
      mysql_free_result (res);
      /* storage ever exist in database, update information */
      memset (buf, 0, 4000);
      len =
	sprintf (buf,
		 "UPDATE storage SET ip = '%s', port = %d, status = 0 WHERE id = '%s'",
		 ip, port, id);
      syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : StorageStart %s",
	      buf);

      if (mysql_real_query (db->sql, buf, len) != 0)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
		  mysql_error (db->sql));
	  return 0;
	}
    }
  else
    {
      mysql_free_result (res);
      /* storage doesn't exist in database, add it */
      memset (buf, 0, 4000);
      len =
	sprintf (buf, "INSERT INTO storage  VALUES ('%s', '%s', %d, 0)", id,
		 ip, port);
      syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : StartStorage %s",
	      buf);

      if (mysql_real_query (db->sql, buf, len) != 0)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
		  mysql_error (db->sql));
	  return 0;
	}
    }
  return 1;
}

int
database_master_get_storage_infos (struct database *orig_db,
				   char id[UUID_LEN+1], char **ip,
				   u_int16_t * port)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  unsigned long len;
  MYSQL_RES *res;
  MYSQL_ROW row;

  memset (buf, 0, 4000);
  len = sprintf (buf, "SELECT ip, port FROM storage WHERE id = '%s'", id);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : GetStorageInfo %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  res = mysql_store_result (db->sql);
  if (!res)
    return 0;

  row = mysql_fetch_row (res);
  if (!row)
    return 0;

  /* get ip */
  *ip = (char *) malloc (IP_LEN + 1);
  memset (*ip, 0, IP_LEN + 1);
  memcpy (*ip, row[0], IP_LEN);
  /* get port */
  *port = strtol (row[1], NULL, 10);
  mysql_free_result (res);
  return 1;
}


int
database_master_start_client (struct database *orig_db, char id[UUID_LEN+1],
			      char ip[IP_LEN], u_int16_t port, u_int16_t conn)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  unsigned long len;

  /* add client in database */
  memset (buf, 0, 4000);
  len =
    sprintf (buf, "INSERT INTO client  VALUES ('%s', '%s', %d, %d)", id, ip,
	     port, conn);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : StartClient %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }
  return 1;
}



int
database_master_get_all_storages_infos (struct database *orig_db,
					struct nlist **list)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  int nb_storage_id, i, port;
  unsigned long len;
  MYSQL_RES *res;
  MYSQL_ROW row;
  char id[UUID_LEN + 1], ip[IP_LEN + 1];

  memset (buf, 0, 4000);
  len = sprintf (buf, "SELECT id, ip, port FROM storage WHERE status = 0");

  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : GetAllStoragesInfo %s",
	  buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  res = mysql_store_result (db->sql);
  if (!res)
    return 0;

  nb_storage_id = mysql_num_rows (res);
  if (nb_storage_id == 0)
    return -1;

  row = mysql_fetch_row (res);
  if (!row)
    return 0;
  /* get storage id list */
  *list = (struct nlist *) malloc (sizeof (struct nlist));
  init_nlist (*list, nb_storage_id);
  for (i = 0; i < nb_storage_id; i++)
    {
      memset (id, 0, UUID_LEN + 1);
      memset (ip, 0, IP_LEN + 1);
      memcpy (id, row[0], UUID_LEN);
      memcpy (ip, row[1], IP_LEN);
      port = strtol (row[2], NULL, 10);
      add_node (*list, id, ip, port);
      row = mysql_fetch_row (res);
      if (!row)
	break;
    }
  mysql_free_result (res);
  return 1;
}



int
database_master_get_all_clients_infos (struct database *orig_db,
				       struct nlist **list)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  int nb_storage_id, i, port;
  unsigned long len;
  MYSQL_RES *res;
  MYSQL_ROW row;
  char id[UUID_LEN + 1], ip[IP_LEN + 1];

  memset (buf, 0, 4000);
  len = sprintf (buf, "SELECT id, ip, port FROM client");
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : GetAllClientsInfo %s",
	  buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  res = mysql_store_result (db->sql);
  if (!res)
    return 0;

  if ((nb_storage_id = mysql_num_rows (res)) == 0)
    {
      mysql_free_result (res);
      return -1;
    }

  row = mysql_fetch_row (res);
  if (!row)
    return 0;

  /* get storage id list */
  *list = (struct nlist *) malloc (sizeof (struct nlist));
  init_nlist (*list, nb_storage_id);
  for (i = 0; i < nb_storage_id; i++)
    {
      memset (id, 0, UUID_LEN + 1);
      memset (ip, 0, IP_LEN + 1);
      memcpy (id, row[0], UUID_LEN);
      memcpy (ip, row[1], IP_LEN);
      port = strtol (row[2], NULL, 10);
      add_node (*list, id, ip, port);
      row = mysql_fetch_row (res);
      if (!row)
	break;
    }
  mysql_free_result (res);
  return 1;
}


int
database_master_get_trans_list (struct database *orig_db,
				char storage_id[UUID_LEN+1],
				struct tlist **list)
{
  u_int64_t tid;
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  int nb_trans_id, i, j, nb_storage_id;
  unsigned long len;
  u_int16_t port;
  MYSQL_RES *res, *result;
  MYSQL_ROW row;
  char ip[IP_LEN + 1], string_port[IP_LEN + 1];

  memset (buf, 0, 4000);
  len =
    sprintf (buf,
	     "SELECT location.tid FROM location, transaction WHERE location.sid = '%s' AND transaction.tid = location.tid AND transaction.status = 0",
	     storage_id);

  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : GetTransList %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  res = mysql_store_result (db->sql);
  if (!res)
    return 0;

  nb_trans_id = mysql_num_rows (res);

  *list = (struct tlist *) malloc (sizeof (struct tlist));
  init_tlist (*list, nb_trans_id);

  /* XXX just for test... */
  if (nb_trans_id == 0)
    return 1;


  for (i = 0; i < nb_trans_id; i++)
    {
      /* get tid */
      row = mysql_fetch_row (res);
      if (!row)
	return 0;
      sscanf (row[0], "%llu", &tid);
      /* get storage list for this trans */
      memset (buf, 0, 4000);
      len =
	sprintf (buf,
		 "SELECT storage.ip, storage.port FROM location, storage WHERE location.tid = '%llu' AND location.sid != '%s' AND storage.id = location.sid AND storage.status = 0",
		 tid, storage_id);
      syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : GetTranList %s", buf);

      if (mysql_real_query (db->sql, buf, len) != 0)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
		  mysql_error (db->sql));
	  return 0;
	}

      result = mysql_store_result (db->sql);
      if (!result)
	return 0;

      if ((nb_storage_id = mysql_num_rows (result)) == 0)
	break;			/* no storage for the tid */

      add_trans (list, tid, nb_storage_id * 2);	/* XXX hack for the moment to store ip and port */
      /* storage list */
      for (j = 0; j < nb_storage_id; j++)
	{
	  row = mysql_fetch_row (result);
	  if (!row)
	    break;
	  /* storage ip & port */
	  memset (ip, 0, IP_LEN + 1);
	  memcpy (ip, row[0], IP_LEN);
	  memset (string_port, 0, IP_LEN + 1);
	  memcpy (string_port, row[1], IP_LEN);
	  port = strtol (row[1], NULL, 10);
	  add_cobject (&((*list)->objects[i].list), ip, "");
	  add_cobject (&((*list)->objects[i].list), string_port, "");
	}
      mysql_free_result (result);
    }
  mysql_free_result (res);
  return 1;
}


int
database_master_get_storage_index (struct database *orig_db,
				   char storage_id[UUID_LEN+1],
				   struct transactionList **txn,
				   u_int64_t * nb_txn)
{
  u_int64_t tid, object_id, i;
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  int j, nb_oid;
  unsigned long len;
  MYSQL_RES *res, *result;
  MYSQL_ROW row;
  struct objectList objects;

  memset (buf, 0, 4000);
  len =
    sprintf (buf, "SELECT tid FROM location WHERE sid = '%s'", storage_id);

  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG),
	  "SQL : Get storage for index check %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  res = mysql_store_result (db->sql);
  if (!res)
    return 0;

  *nb_txn = mysql_num_rows (res);
  *txn = (struct transactionList *) malloc (sizeof (struct transactionList));
  transactionList_New (*txn, *nb_txn);

  for (i = 0; i < *nb_txn; i++)
    {
      /* get tid */
      row = mysql_fetch_row (res);
      if (!row)
	return 0;
      sscanf (row[0], "%llu", &tid);
      /* get list for this trans */
      memset (buf, 0, 4000);
      len = sprintf (buf, "SELECT id FROM object WHERE tid = '%llu'", tid);
      syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG),
	      "SQL : GetTranList for index check %s", buf);

      if (mysql_real_query (db->sql, buf, len) != 0)
	{
	  syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
		  mysql_error (db->sql));
	  return 0;
	}

      result = mysql_store_result (db->sql);
      if (!result)
	return 0;

      nb_oid = mysql_num_rows (result);
      objectList_New (&objects, nb_oid);

      /* get storage list */
      for (j = 0; j < nb_oid; j++)
	{
	  row = mysql_fetch_row (result);
	  if (!row)
	    break;
	  /* storage ip & port */
	  sscanf (row[0], "%llu", &object_id);
	  objectList_Append (&objects, object_id, tid, "");
	}
      transactionList_Append (*txn, tid, objects);
      objectList_Free (&objects);
      mysql_free_result (result);
    }
  mysql_free_result (res);

  return 1;
}

int
database_master_create_storage_index (struct database *orig_db,
				      struct stringList **index)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  int nb_storage_id, i;
  unsigned long len;
  MYSQL_RES *res;
  MYSQL_ROW row;
  char id[UUID_LEN + 1];

  memset (buf, 0, 4000);
  len = sprintf (buf, "SELECT id FROM storage WHERE status = 0");

  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : create storage index %s",
	  buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  res = mysql_store_result (db->sql);
  if (!res)
    return 0;

  nb_storage_id = mysql_num_rows (res);
  stringList_New (*index, nb_storage_id, UUID_LEN);
  if (nb_storage_id == 0)
    return 1;

  row = mysql_fetch_row (res);
  if (!row)
    {
      mysql_free_result (res);
      return 0;
    }
  /* get storage id list */
  for (i = 0; i < nb_storage_id; i++)
    {
      memset (id, 0, UUID_LEN + 1);
      memcpy (id, row[0], UUID_LEN);
      stringList_Append (*index, id);

      row = mysql_fetch_row (res);
      if (!row)
	break;
    }
  mysql_free_result (res);
  return 1;
}


int
database_master_get_storages_for_trans (struct database *orig_db,
					u_int64_t tid,
					char storage_id[UUID_LEN+1],
					struct stringList **list)
{
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000], ip[IP_LEN + 1], port[IP_LEN + 1];
  unsigned long len;
  int i, nb_storage_id = 0;
  MYSQL_RES *res;
  MYSQL_ROW row;
  memset (buf, 0, 4000);
  len =
    sprintf (buf,
	     "SELECT storage.ip, storage.port FROM location, storage WHERE location.tid = '%llu' AND location.sid != '%s' AND storage.id = location.sid AND storage.status = 0",
	     tid, storage_id);
  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG), "SQL : get storage for trans %s",
	  buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  res = mysql_store_result (db->sql);
  if (!res)
    return 0;

  if ((nb_storage_id = mysql_num_rows (res)) == 0)
    {
      /* maybe return error message to storage node instead */
      *list = (struct stringList *) malloc (sizeof (struct stringList));
      stringList_New (*list, nb_storage_id, UUID_LEN);
      return 1;
    }

  row = mysql_fetch_row (res);
  if (!row)
    return 0;

  /* get storage id list */
  *list = (struct stringList *) malloc (sizeof (struct stringList));
  stringList_New (*list, nb_storage_id * 2, UUID_LEN);
  for (i = 0; i < nb_storage_id; i++)
    {
      memset (ip, 0, IP_LEN + 1);
      memcpy (ip, row[0], IP_LEN);
      memset (port, 0, IP_LEN + 1);
      memcpy (port, row[1], IP_LEN);
      stringList_Append (*list, ip);
      stringList_Append (*list, port);
      row = mysql_fetch_row (res);
      if (!row)
	break;
    }
  mysql_free_result (res);
  return 1;
}

/**
 * check if all objects stores by storage nodes are accessible
 * if yes master can begin to handle client request
 * otherwise must wait until another storage connect and redo the check
 */
int
database_master_all_objects_accessible (struct database *orig_db)
{
  u_int64_t oid, serial, i, nb_objects;
  struct database_mysql *db = (struct database_mysql *) orig_db;
  char buf[4000];
  unsigned long len;
  MYSQL_RES *res, *result;
  MYSQL_ROW row;

  memset (buf, 0, 4000);
  len =
    sprintf (buf, "SELECT id, serial FROM object");

  syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG),
	  "SQL : all object accessible %s", buf);

  if (mysql_real_query (db->sql, buf, len) != 0)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
	      mysql_error (db->sql));
      return 0;
    }

  res = mysql_store_result (db->sql);
  if (!res)
    return 0;

  nb_objects = mysql_num_rows (res);
  for (i = 0; i < nb_objects; i++)
    {
      /* get tid */
      row = mysql_fetch_row (res);
      if (!row)
        return 0;
      sscanf (row[0], "%llu", &oid);
      sscanf (row[1], "%llu", &serial);
      /* get list for this trans */
      memset (buf, 0, 4000);
      len = sprintf (buf, "SELECT COUNT(*) FROM object, transaction, location, storage WHERE object.id = '%llu' AND object.serial = '%llu' AND transaction.tid = object.tid AND location.tid = transaction.tid AND storage.id = location.sid AND storage.status = 0", oid, serial);
      syslog (LOG_MAKEPRI (LOG_USER, LOG_DEBUG),
              "SQL : Get sStorage for object %s", buf);

      if (mysql_real_query (db->sql, buf, len) != 0)
        {
          syslog (LOG_MAKEPRI (LOG_USER, LOG_ERR), "SQL Error : %s",
                  mysql_error (db->sql));
          return 0;
        }

      result = mysql_store_result (db->sql);
      if (!result)
        return 0;

      if (mysql_num_rows (result) == 0)
        {
          mysql_free_result (result);
          mysql_free_result (res);
          return 0;
        }
      mysql_free_result (result);
    }
  mysql_free_result (res);

  return 1;
}
