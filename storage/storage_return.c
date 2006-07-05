#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <netinet/in.h>
#include <stdio.h>

#include "neo_socket.h"
#include "neo_struct.h"

static u_int16_t rcode = 0;

int
returnTransaction (int conn, int method)
{
  u_int32_t data_len;
  u_int16_t meth;
  char *send_buf;

  /* send request to master */
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
returnUndo (int conn, int method, u_int64_t * oid_list, u_int32_t nb_oid)
{
  u_int64_t n_oid;
  u_int32_t buf_len, data_len, n_nb_oid, i;
  u_int16_t meth;
  char *send_buf;
  int len = 0;

  /* send request to master */
  meth = htons (method);

  data_len = htonl (nb_oid * INT64_LEN + INT32_LEN);

  buf_len = RHEADER_LEN + ntohl (data_len);
  send_buf = (char *) malloc (buf_len);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);
  /* data */
  n_nb_oid = htonl (nb_oid);
  memcpy (send_buf + RHEADER_LEN, &n_nb_oid, INT32_LEN);
  len = RHEADER_LEN + INT32_LEN;
  for (i = 0; i < nb_oid; i++)
    {
      n_oid = htonll (oid_list[i]);
      memcpy (send_buf + len, &n_oid, INT64_LEN);
      len += INT64_LEN;
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
returnHistInfo (int conn, int method, struct hist hist)	// done
{
  u_int64_t n_serial, n_size;
  u_int32_t buf_len, data_len, meth;
  u_int16_t time_len, user_len, desc_len, len;
  char *send_buf, time[10];

  snprintf (time, 10, "%10f", hist.time);

  /* send request to master */
  meth = htons (method);
  data_len =
    htonl (3 * INT16_LEN + 2 * INT64_LEN + strlen (time) +
	   strlen (hist.user) + strlen (hist.desc));
  buf_len = RHEADER_LEN + ntohl (data_len);
  send_buf = (char *) malloc (buf_len);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);
  /* data */
  len = RHEADER_LEN;
  /* time */
  time_len = htons (strlen (time));
  memcpy (send_buf + len, &time_len, INT16_LEN);
  len += INT16_LEN;
  memcpy (send_buf + len, time, strlen (time));
  len += strlen (time);
  /* user */
  user_len = htons (strlen (hist.user));
  memcpy (send_buf + len, &user_len, INT16_LEN);
  len += INT16_LEN;
  memcpy (send_buf + len, hist.user, strlen (hist.user));
  len += strlen (hist.user);
  /* desc */
  desc_len = htons (strlen (hist.desc));
  memcpy (send_buf + len, &desc_len, INT16_LEN);
  len += INT16_LEN;
  memcpy (send_buf + len, hist.desc, strlen (hist.desc));
  len += strlen (hist.desc);
  /* serial */
  n_serial = htonll (hist.serial);
  memcpy (send_buf + len, &n_serial, INT64_LEN);
  len += INT64_LEN;
  /* size */
  n_size = htonll (hist.size);
  memcpy (send_buf + len, &n_size, INT64_LEN);

  if (!send_all (conn, send_buf, buf_len))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);

  return 1;
}


int
returnUndoInfo (int conn, int method, struct undoInfo hist)	// done
{
  u_int64_t n_id;
  u_int32_t buf_len, data_len, meth;
  u_int16_t time_len, user_len, desc_len, len;
  char *send_buf, time[10];

  sprintf (time, "%10f", hist.time);
  /* send request to master */
  meth = htons (method);
  data_len =
    htonl (strlen (time) + strlen (hist.user) + strlen (hist.desc) +
	   INT64_LEN + 3 * INT16_LEN);

  buf_len = RHEADER_LEN + ntohl (data_len);
  send_buf = (char *) malloc (buf_len);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);
  /* data */
  len = RHEADER_LEN;
  /* time */
  time_len = htons (strlen (time));
  memcpy (send_buf + len, &time_len, INT16_LEN);
  len += INT16_LEN;
  memcpy (send_buf + len, time, strlen (time));
  len += strlen (time);
  /* user */
  user_len = htons (strlen (hist.user));
  memcpy (send_buf + len, &user_len, INT16_LEN);
  len += INT16_LEN;
  memcpy (send_buf + len, hist.user, strlen (hist.user));
  len += strlen (hist.user);
  /* desc */
  desc_len = htons (strlen (hist.desc));
  memcpy (send_buf + len, &desc_len, INT16_LEN);
  len += INT16_LEN;
  memcpy (send_buf + len, hist.desc, strlen (hist.desc));
  len += strlen (hist.desc);
  /* id */
  n_id = htonll (hist.id);
  memcpy (send_buf + len, &n_id, INT64_LEN);;

  if (!send_all (conn, send_buf, buf_len))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);

  return 1;
}


int
returnLoad (int conn, int method, char *object, int32_t crc)
{
  u_int64_t object_len;
  u_int32_t buf_len, data_len, meth;
  int32_t n_crc;
  char *send_buf;

  /* send request to master */
  meth = htons (method);
  data_len = htonl (INT64_LEN + strlen (object) + INT32_LEN);
  object_len = htonll (strlen (object));
  buf_len = RHEADER_LEN + ntohl (data_len);
  send_buf = (char *) malloc (buf_len);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);
  /* data */
  memcpy (send_buf + RHEADER_LEN, &object_len, INT64_LEN);
  memcpy (send_buf + RHEADER_LEN + INT64_LEN, object, strlen (object));
  /* crc */
  n_crc = htonl (crc);
  memcpy (send_buf + RHEADER_LEN + INT64_LEN + strlen (object), &n_crc,
	  INT32_LEN);

  if (!send_all (conn, send_buf, buf_len))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);

  return 1;
}


int
returnGetSize (int conn, int method, u_int64_t size)
{
  u_int32_t buf_len, data_len, meth;
  u_int64_t ssize;
  char *send_buf;

  /* send request to master */
  meth = htons (method);
  data_len = htonl(INT64_LEN);
  ssize = htonll (size);	

  send_buf = (char *) malloc (RHEADER_LEN + INT64_LEN);
  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);
  /* data */
  memcpy (send_buf + RHEADER_LEN, &ssize, INT64_LEN);

  if (!send_all (conn, send_buf, RHEADER_LEN + INT64_LEN))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);

  return 1;
}

int
returnMasterClose (int conn, int method, char *id)	// done
{
  u_int32_t data_len, meth;
  char *send_buf;

  /* send request to master */
  meth = htons (method);
  data_len = htonl (UUID_LEN);

  send_buf = (char *) malloc (RHEADER_LEN + UUID_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);
  /* data */
  memcpy (send_buf + RHEADER_LEN, id, UUID_LEN);

  if (!send_all (conn, send_buf, RHEADER_LEN + UUID_LEN))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);

  return 1;
}

int
returnMasterChange (int conn, int method, char *id)	// done
{
  u_int32_t data_len, meth;
  char *send_buf;

  /* send request to master */
  meth = htons (method);
  data_len = htonl (UUID_LEN);

  send_buf = (char *) malloc (RHEADER_LEN + UUID_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);
  /* data */
  memcpy (send_buf + RHEADER_LEN, id, UUID_LEN);

  if (!send_all (conn, send_buf, RHEADER_LEN + UUID_LEN))
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

  u_int32_t buf_len, data_len, meth;
  u_int16_t msg_len, err_code;
  char *send_buf;

  /* send request to nodes */
  meth = htons (method);
  data_len = htonl (INT16_LEN + strlen (message));
  err_code = htons (code);
  msg_len = htons (strlen (message));

  buf_len = RHEADER_LEN + ntohl (data_len);
  send_buf = (char *) malloc (buf_len);
  
  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &err_code, INT16_LEN);
  /* data */
  memcpy (send_buf + RHEADER_LEN, &msg_len, INT16_LEN);
  memcpy (send_buf + RHEADER_LEN + INT16_LEN, message, strlen (message));

  if (!send_all (conn, send_buf, buf_len))
    {
      free (send_buf);
      return 0;
    }

  free (send_buf);
  return 1;

}


int
returnGetTransData (int conn, int method, u_int64_t tid, char *user,
		    char *desc, char *ext, struct objectList list)
{
  u_int64_t n_oid, n_serial, n_tid, n_data_size;
  u_int32_t buf_len, data_len, nb_object, i, size = 0;
  u_int16_t meth, user_len, desc_len, ext_len;
  char *send_buf;
  int tlen = 0;

  /* send request to master */
  meth = htons (method);
  user_len = htons (strlen (user));
  desc_len = htons (strlen (desc));
  ext_len = htons (strlen (ext));
  // get data from struct
  nb_object = htonl (list.last);

  /* must get data size for packet */
  for (i = 0; i < list.last; i++)
    size += strlen (list.list[i].data);

  data_len =
    htonl (list.last * (2 * INT64_LEN + INT64_LEN) + size + INT32_LEN +
	   INT64_LEN + 3 * INT16_LEN + strlen (user) + strlen (desc) +
	   strlen (ext));

  buf_len = RHEADER_LEN + ntohl (data_len);
  send_buf = (char *) malloc (buf_len);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &rcode, INT16_LEN);
  /* data */
  tlen = RHEADER_LEN;
  n_tid = htonll (tid);
  memcpy (send_buf + tlen, &n_tid, ID_LEN);
  tlen += INT64_LEN;
  /* transaction info */
  memcpy (send_buf + tlen, &user_len, INT16_LEN);
  tlen += INT16_LEN;
  memcpy (send_buf + tlen, user, strlen (user));
  tlen += strlen (user);
  memcpy (send_buf + tlen, &desc_len, INT16_LEN);
  tlen += INT16_LEN;
  memcpy (send_buf + tlen, desc, strlen (desc));
  tlen += strlen (desc);
  memcpy (send_buf + tlen, &ext_len, INT16_LEN);
  tlen += INT16_LEN;
  memcpy (send_buf + tlen, ext, strlen (ext));
  tlen += strlen (ext);
  /* object list */
  memcpy (send_buf + tlen, &nb_object, INT32_LEN);
  tlen += INT32_LEN;
  for (i = 0; i < list.last; i++)
    {
      n_oid = htonll (list.list[i].oid);
      memcpy (send_buf + tlen, &n_oid, INT64_LEN);
      tlen += INT64_LEN;
      n_serial = htonll (list.list[i].serial);
      memcpy (send_buf + tlen, &n_serial, INT64_LEN);
      tlen += INT64_LEN;
      n_data_size = htonll (strlen (list.list[i].data));
      memcpy (send_buf + tlen, &n_data_size, INT64_LEN);
      tlen += INT64_LEN;
      memcpy (send_buf + tlen, list.list[i].data, strlen (list.list[i].data));
      tlen += strlen (list.list[i].data);
    }

  if (!send_all (conn, send_buf, buf_len))
    {
      free (send_buf);
      return 0;
    }

  free (send_buf);
  return 1;
}
