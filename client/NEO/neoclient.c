#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <netinet/in.h>
#include <uuid/uuid.h>
#include <syslog.h>
#include <Python.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "neo_socket.h"
#include "neo_struct.h"


/* generate client id */
int
generate_id (char **ID)
{
  uuid_t client_id;
  uuid_generate (client_id);
  *ID = (char *) malloc (UUID_LEN + 1);
  memset (*ID, 0, UUID_LEN + 1);
  uuid_unparse (client_id, *ID);
  return 0;
}


/* send broadcast message to find where is the master */
int
search_master (int method, char **master_ip)
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
  meth = htons (method);
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
  if ((ntohs (*((u_int16_t *) (rcv_buf)))) != method)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING), "return method error");
      return METHOD_ERROR;
    }
  memcpy (rflags, rcv_buf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (rcv_buf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (rcv_buf + INT16_LEN + FLAG_LEN + INT32_LEN)));
  if (return_code != 0)
    return return_code;

  /* get master ip */
  *master_ip = (char *) malloc(strlen(inet_ntoa(addr.sin_addr))+1);
  memset (*master_ip, 0, strlen(inet_ntoa(addr.sin_addr))+1);
  memcpy (*master_ip, inet_ntoa(addr.sin_addr), strlen(inet_ntoa(addr.sin_addr))+1);

  close(soc);
  return 0;
}



/* Master call */
int
getObjectByOid (int conn, int method, u_int64_t oid, PyObject ** resultlist)
{
  u_int64_t n_oid, serial;
  u_int32_t buf_len = 0, data_len = 0, len = 0, storage_nb, nb;
  u_int16_t meth, return_code = 0;
  char buf[BUF_SIZE], rflags[FLAG_LEN], storage_id[UUID_LEN + 1];
  char *send_buf;

  /* send request to master */
  meth = htons (method);
  data_len = htonl (INT64_LEN);
  send_buf = (char *) malloc (HEADER_LEN + INT64_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  n_oid = htonll (oid);
  memcpy (send_buf + HEADER_LEN, &n_oid, INT64_LEN);

  if (!send_all (conn, send_buf, HEADER_LEN + INT64_LEN))
    {
      free (send_buf);
      return CONNECTION_FAILURE;
    }
  free (send_buf);

  /* Wait for response */
  if (!wait_packet (conn, buf, &buf_len, RHEADER_LEN))
    return CONNECTION_FAILURE;
  /* header */
  if ((ntohs (*((u_int16_t *) (buf)))) != method)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING), "return method error");
      return METHOD_ERROR;
    }
  memcpy (rflags, buf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (buf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (buf + INT16_LEN + FLAG_LEN + INT32_LEN)));
  len = buf_len - RHEADER_LEN;
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	return CONNECTION_FAILURE;
    }
  if (return_code != 0)
    return return_code;
  /* data */
  len = 0;
  serial = ntohll (*((u_int64_t *) (buf)));
  len += INT64_LEN;
  storage_nb = ntohl (*((u_int32_t *) (buf + len)));
  len += INT32_LEN;
  /* construct list of storage id */
  *resultlist = PyList_New (0);
  /* add serial */
  PyList_Append (*resultlist, PyLong_FromUnsignedLongLong (serial));
  /* add storage */
  memset (storage_id, 0, UUID_LEN + 1);
  for (nb = 0; nb < storage_nb; nb++)
    {
      memcpy (storage_id, buf + len, UUID_LEN);
      len += UUID_LEN;
      PyList_Append (*resultlist, PyString_FromString (storage_id));
    }

  return return_code;
}


int
getObjectBySerial (int conn, int method, u_int64_t oid, u_int64_t serial,
		   PyObject ** resultlist)
{
  u_int64_t n_oid, n_serial;
  u_int32_t buf_len = 0, data_len = 0, storage_nb = 0, len = 0, nb;
  u_int16_t meth, return_code = 0;
  char buf[BUF_SIZE], rflags[FLAG_LEN], storage_id[UUID_LEN + 1];
  char *send_buf;

  /* send request to master */
  meth = htons (method);
  data_len = htonl (INT64_LEN * 2);
  send_buf = (char *) malloc (HEADER_LEN + INT64_LEN * 2);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  n_oid = htonll (oid);
  memcpy (send_buf + HEADER_LEN, &n_oid, INT64_LEN);
  n_serial = htonll (serial);
  memcpy (send_buf + HEADER_LEN + INT64_LEN, &n_serial, INT64_LEN);

  if (!send_all (conn, send_buf, HEADER_LEN + INT64_LEN * 2))
    {
      free (send_buf);
      return CONNECTION_FAILURE;
    }
  free (send_buf);

  /* Wait for response */
  if (!wait_packet (conn, buf, &buf_len, RHEADER_LEN))
    return CONNECTION_FAILURE;
  /* header */
  if ((ntohs (*((u_int16_t *) (buf)))) != method)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING),
	      "BySerial return method error %d instead of %d",
	      ntohs (*((u_int16_t *) (buf))), method);
      return METHOD_ERROR;
    }
  memcpy (rflags, buf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (buf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (buf + INT16_LEN + FLAG_LEN + INT32_LEN)));
  len = buf_len - RHEADER_LEN;
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	return CONNECTION_FAILURE;
    }
  if (return_code != 0)
    return return_code;
  /* data */

  storage_nb = ntohl (*((u_int32_t *) (buf)));
  len = INT32_LEN;
  /* construct list of storage id */
  *resultlist = PyList_New (0);
  memset (storage_id, 0, UUID_LEN + 1);
  for (nb = 0; nb < storage_nb; nb++)
    {
      memcpy (storage_id, buf + len, UUID_LEN);
      len += UUID_LEN;
      PyList_Append (*resultlist, PyString_FromString (storage_id));
    }

  return return_code;
}


int
getSerial (int conn, int method, u_int64_t oid, u_int64_t * serial)
{
  u_int64_t n_oid;
  u_int32_t buf_len = 0, data_len = 0, len = 0;
  u_int16_t meth, return_code;
  char buf[BUF_SIZE], rflags[FLAG_LEN];
  char *send_buf;

  /* send request to master */
  meth = htons (method);
  data_len = htonl (INT64_LEN);
  send_buf = (char *) malloc (HEADER_LEN + INT64_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  n_oid = htonll (oid);
  memcpy (send_buf + HEADER_LEN, &n_oid, INT64_LEN);

  if (!send_all (conn, send_buf, HEADER_LEN + INT64_LEN))
    {
      free (send_buf);
      return CONNECTION_FAILURE;
    }
  free (send_buf);

  /* Wait for response */
  if (!wait_packet (conn, buf, &buf_len, RHEADER_LEN))
    return CONNECTION_FAILURE;
  /* header */
  if ((ntohs (*((u_int16_t *) (buf)))) != method)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING), "return method error");
      return METHOD_ERROR;
    }
  memcpy (rflags, buf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (buf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (buf + INT16_LEN + FLAG_LEN + INT32_LEN)));
  len = buf_len - RHEADER_LEN;
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	return 7;
    }
  if (return_code != 0)
    return return_code;
  /* data */
  *serial = ntohll (*((u_int64_t *) (buf)));

  return return_code;
}

int
getLastTransaction (int conn, int method, u_int64_t * tid)
{
  u_int32_t data_len = 0, buf_len = 0, len = 0;
  u_int16_t meth, return_code;
  char buf[BUF_SIZE], rflags[FLAG_LEN];
  char *send_buf;

  /* send request to master */
  meth = htons (method);
  data_len = 0;
  send_buf = (char *) malloc (HEADER_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* no data */

  if (!send_all (conn, send_buf, HEADER_LEN))
    {
      free (send_buf);
      return CONNECTION_FAILURE;
    }
  free (send_buf);

  /* Wait for response */
  if (!wait_packet (conn, buf, &buf_len, RHEADER_LEN))
    return CONNECTION_FAILURE;
  /* header */
  if ((ntohs (*((u_int16_t *) (buf)))) != method)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING), "return method error");
      return METHOD_ERROR;
    }
  memcpy (rflags, buf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (buf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (buf + INT16_LEN + FLAG_LEN + INT32_LEN)));
  len = buf_len - RHEADER_LEN;
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	return CONNECTION_FAILURE;
    }
  if (return_code != 0)
    return return_code;

  /* transaction id */
  *tid = ntohll (*((u_int64_t *) (buf)));

  return return_code;
}

int
getOid (int conn, int method, u_int16_t nb_oid, PyObject ** list)
{
  u_int32_t buf_len = 0, data_len = 0, len = 0;
  u_int16_t meth, return_code, ascii, i, j, n_nb_oid;
  char buf[BUF_SIZE], rflags[FLAG_LEN];
  char *send_buf;
  PyObject *ss_tuple;

  /* send request to master */
  meth = htons (method);
  data_len = htonl (INT16_LEN);
  send_buf = (char *) malloc (HEADER_LEN + INT16_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  n_nb_oid = htons (nb_oid);
  memcpy (send_buf + HEADER_LEN, &n_nb_oid, INT16_LEN);

  if (!send_all (conn, send_buf, HEADER_LEN + INT16_LEN))
    {
      free (send_buf);
      return CONNECTION_FAILURE;
    }
  free (send_buf);

  /* Wait for response */
  if (!wait_packet (conn, buf, &buf_len, RHEADER_LEN))
    return CONNECTION_FAILURE;
  /* header */
  if ((ntohs (*((u_int16_t *) (buf)))) != method)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING), "return method error");
      return METHOD_ERROR;
    }
  memcpy (rflags, buf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (buf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (buf + INT16_LEN + FLAG_LEN + INT32_LEN)));
  len = buf_len - RHEADER_LEN;
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	return CONNECTION_FAILURE;
    }
  if (return_code != 0)
    return return_code;

  /* data */
  nb_oid = ntohs (*((u_int16_t *) (buf)));
  *list = PyList_New (0);
  len = INT16_LEN;
  /* list of oid */
  for (j = 0; j < nb_oid; j++)
    {
      ss_tuple = PyList_New (0);
      for (i = 0; i < ID_LEN; i++)
	{
	  ascii = ntohs (*((u_int16_t *) (buf + len)));
	  len += INT16_LEN;
	  PyList_Append (ss_tuple, PyInt_FromLong (ascii));
	}
      PyList_Append (*list, ss_tuple);
      Py_XDECREF (ss_tuple);
    }

  return return_code;
}


int
clientStart (int conn, int method, char id[UUID_LEN], char ip[IP_LEN],
	     u_int16_t port, PyObject ** list)
{
  u_int32_t buf_len = 0, data_len = 0, len = 0;
  u_int16_t meth, n_port, return_code, bool;
  char buf[BUF_SIZE], rflags[FLAG_LEN];;
  char *send_buf, *data;

  /* send request to master */
  meth = htons (method);
  data_len = htonl (UUID_LEN + IP_LEN + INT16_LEN);
  send_buf = (char *) malloc (HEADER_LEN + UUID_LEN + IP_LEN + INT16_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  memcpy (send_buf + HEADER_LEN, id, UUID_LEN);
  memcpy (send_buf + HEADER_LEN + UUID_LEN, ip, IP_LEN);
  n_port = htons (port);
  memcpy (send_buf + HEADER_LEN + UUID_LEN + IP_LEN, &n_port, INT16_LEN);

  if (!send_all (conn, send_buf, HEADER_LEN + UUID_LEN + IP_LEN + INT16_LEN))
    {
      free (send_buf);
      return CONNECTION_FAILURE;
    }
  free (send_buf);

  /* Wait for response */
  if (!wait_packet (conn, buf, &buf_len, RHEADER_LEN))
    return CONNECTION_FAILURE;
  /* header */
  if ((ntohs (*((u_int16_t *) (buf)))) != method)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING), "return method error");
      return METHOD_ERROR;
    }
  memcpy (rflags, buf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (buf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (buf + INT16_LEN + FLAG_LEN + INT32_LEN)));
  len = buf_len - RHEADER_LEN;
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	return CONNECTION_FAILURE;
    }
  if (return_code != 0)
    return return_code;
  /* data */

  len = 0;
  /* get info about storage and it into a list */
  *list = PyList_New (0);
  /* name */
  data_len = ntohs (*((u_int16_t *) (buf + len)));
  data = (char *) malloc (data_len + 1);
  memset (data, 0, data_len + 1);
  len += INT16_LEN;
  memcpy (data, buf + len, data_len);
  len += data_len;
  PyList_Append (*list, PyString_FromString (data));
  free (data);
  /* support version */
  bool = ntohs (*((u_int16_t *) (buf + len)));
  PyList_Append (*list, PyInt_FromLong (bool));
  len += INT16_LEN;
  /* support undo */
  bool = ntohs (*((u_int16_t *) (buf + len)));
  PyList_Append (*list, PyInt_FromLong (bool));
  len += INT16_LEN;
  /* support trans undo */
  bool = ntohs (*((u_int16_t *) (buf + len)));
  PyList_Append (*list, PyInt_FromLong (bool));
  len += INT16_LEN;
  /* read only */
  bool = ntohs (*((u_int16_t *) (buf + len)));
  PyList_Append (*list, PyInt_FromLong (bool));
  len += INT16_LEN;
  /* extension */
  data_len = ntohs (*((u_int16_t *) (buf + len)));
  data = (char *) malloc (data_len + 1);
  memset (data, 0, data_len + 1);
  len += INT16_LEN;
  memcpy (data, buf + len, data_len);
  PyList_Append (*list, PyString_FromString (data));
  free (data);

  return return_code;
}


int
getTransSN (int conn, int method, int16_t first, int16_t last,
	    PyObject ** list)
{
  u_int64_t tid;
  u_int32_t buf_len = 0, data_len = 0, len = 0, storage_nb;
  u_int16_t meth, return_code, trans_nb, nb, i;
  int16_t n_first, n_last;
  char hbuf[RHEADER_LEN], rflags[FLAG_LEN], storage_id[UUID_LEN + 1], *buf;
  char *send_buf;
  PyObject *ss_tuple;

  /* send request to master */
  meth = htons (method);
  data_len = htonl (INT16_LEN * 2);
  send_buf = (char *) malloc (HEADER_LEN + INT16_LEN * 2);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  n_first = htons (first);
  memcpy (send_buf + HEADER_LEN, &n_first, INT16_LEN);
  n_last = htons (last);
  memcpy (send_buf + HEADER_LEN + INT16_LEN, &n_last, INT16_LEN);

  if (!send_all (conn, send_buf, HEADER_LEN + INT16_LEN * 2))
    {
      free (send_buf);
      return CONNECTION_FAILURE;
    }
  free (send_buf);

  /* Wait for response */
  if (!wait_packet (conn, hbuf, &buf_len, RHEADER_LEN))
    return CONNECTION_FAILURE;
  /* header */
  if ((ntohs (*((u_int16_t *) (hbuf)))) != method)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING), "return method error");
      return METHOD_ERROR;
    }
  memcpy (rflags, hbuf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (hbuf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (hbuf + INT16_LEN + FLAG_LEN + INT32_LEN)));
/*   len = buf_len - RHEADER_LEN; */

  buf = (char *) malloc (data_len + 1);
  memset (buf, 0, data_len + 1);

  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
        {
          free (buf);
          return CONNECTION_FAILURE;
        }
    }
  if (return_code != 0)
    {
      free(buf);
      return return_code;
    }

  /* get tid and storage node for each transaction */
  trans_nb = ntohs (*((u_int16_t *) (buf)));
  *list = PyList_New (0);
  len = INT16_LEN;
  memset (storage_id, 0, UUID_LEN + 1);
  for (nb = 0; nb < trans_nb; nb++)
    {
      /* create a new tuple */
      ss_tuple = PyList_New (0);
      tid = ntohll (*((u_int64_t *) (buf + len)));
      len += INT64_LEN;
      /* add tid */
      PyList_Append (ss_tuple, PyLong_FromUnsignedLongLong (tid));
      /* get storages infos */
      storage_nb = ntohl (*((u_int32_t *) (buf + len)));
      len += INT32_LEN;
      /* get storages */
      for (i = 0; i < storage_nb; i++)
	{
	  memcpy (storage_id, buf + len, UUID_LEN);
	  len += UUID_LEN;
	  PyList_Append (ss_tuple, PyString_FromString (storage_id));
	}
      /* add tuple to result list */
      PyList_Append (*list, ss_tuple);
      Py_XDECREF (ss_tuple);
    }
  free(buf);
  return return_code;
}


int
getObjectHist (int conn, int method, u_int64_t oid, u_int16_t hist_length,
	       PyObject ** list)
{
  u_int64_t n_oid, serial;
  u_int32_t buf_len = 0, data_len = 0, len = 0, storage_nb, i;
  u_int16_t return_code, meth, n_hist_length, serial_nb, nb;
  char hbuf[RHEADER_LEN], rflags[FLAG_LEN], storage_id[UUID_LEN + 1], *buf;
  char *send_buf;
  PyObject *ss_tuple;

  /* send request to master */
  meth = htons (method);
  data_len = htonl (INT64_LEN + INT16_LEN);
  send_buf = (char *) malloc (HEADER_LEN + INT64_LEN + INT16_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  n_oid = htonll (oid);
  memcpy (send_buf + HEADER_LEN, &n_oid, INT64_LEN);
  n_hist_length = htons (hist_length);
  memcpy (send_buf + HEADER_LEN + INT64_LEN, &n_hist_length, INT16_LEN);

  if (!send_all (conn, send_buf, HEADER_LEN + INT64_LEN + INT16_LEN))
    {
      free (send_buf);
      return CONNECTION_FAILURE;
    }
  free (send_buf);

  /* Wait for response */
  if (!wait_packet (conn, hbuf, &buf_len, RHEADER_LEN))
    return CONNECTION_FAILURE;
  /* header */
  if ((ntohs (*((u_int16_t *) (hbuf)))) != method)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING), "return method error");
      return METHOD_ERROR;
    }
  memcpy (rflags, hbuf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (hbuf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (hbuf + INT16_LEN + FLAG_LEN + INT32_LEN)));
  
  buf = (char *) malloc (data_len + 1);
  memset (buf, 0, data_len + 1);

  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
        {
          free(buf);
          return CONNECTION_FAILURE;
        }
    }
  if (return_code != 0)
    {
      free(buf);
      return return_code;
    }
  memset (storage_id, 0, UUID_LEN + 1);
  /* data */
  serial_nb = ntohs (*((u_int16_t *) (buf)));
  *list = PyList_New (0);
  len = INT16_LEN;
  for (nb = 0; nb < serial_nb; nb++)
    {
      /* create a new tuple for the version of the object */
      ss_tuple = PyList_New (0);
      /* get serial */
      serial = ntohll (*((u_int64_t *) (buf + len)));
      len += INT64_LEN;
      PyList_Append (ss_tuple, PyLong_FromUnsignedLongLong (serial));
      /* get number of storages */
      storage_nb = ntohl (*((u_int32_t *) (buf + len)));
      len += INT32_LEN;
      /* get storages id */
      for (i = 0; i < storage_nb; i++)
	{
	  memcpy (storage_id, buf + len, UUID_LEN);
	  len += UUID_LEN;
	  PyList_Append (ss_tuple, PyString_FromString (storage_id));
	}
      PyList_Append (*list, ss_tuple);
      Py_XDECREF (ss_tuple);
    }
  free(buf);
  return return_code;
}

int
undoTrans (int conn, int method, u_int64_t tid, PyObject ** list)
{
  u_int64_t n_tid;
  u_int32_t buf_len = 0, data_len = 0, len = 0, storage_nb, nb;
  u_int16_t meth, return_code;
  char hbuf[RHEADER_LEN], rflags[FLAG_LEN], storage_id[UUID_LEN + 1];
  char *send_buf, *buf;

  /* send request to master */
  meth = htons (method);
  data_len = htonl (INT64_LEN);
  send_buf = (char *) malloc (HEADER_LEN + INT64_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  n_tid = htonll (tid);
  memcpy (send_buf + HEADER_LEN, &n_tid, INT64_LEN);

  if (!send_all (conn, send_buf, HEADER_LEN + INT64_LEN))
    {
      free (send_buf);
      return CONNECTION_FAILURE;
    }
  free (send_buf);

  /* Wait for response */
  if (!wait_packet (conn, hbuf, &buf_len, RHEADER_LEN))
    return CONNECTION_FAILURE;
  /* header */
  if ((ntohs (*((u_int16_t *) (hbuf)))) != method)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING), "return method error");
      return METHOD_ERROR;
    }
  memcpy (rflags, hbuf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (hbuf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (hbuf + INT16_LEN + FLAG_LEN + INT32_LEN)));

  buf = (char *)malloc (data_len + 1);
  memset (buf, 0, data_len + 1);

  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
        {
          free (buf);
          return CONNECTION_FAILURE;
        }
    }
  if (return_code != 0)
    {
      free (buf);
      return return_code;
    }

  /* data */
  storage_nb = ntohl (*((u_int32_t *) (buf)));
  len = INT32_LEN;
  /* build a list of storage id */
  *list = PyList_New (0);
  memset (storage_id, 0, UUID_LEN + 1);
  for (nb = 0; nb < storage_nb; nb++)
    {
      memcpy (storage_id, buf + len, UUID_LEN);
      len += UUID_LEN;
      PyList_Append (*list, PyString_FromString (storage_id));
    }
  free(buf);
  return return_code;
}

int
beginTrans (int conn, int method, u_int64_t tid, PyObject ** list)
{
  u_int64_t n_tid;
  u_int32_t buf_len = 0, data_len = 0, len = 0, nb, storage_nb;
  u_int16_t meth, i, id, return_code = 0;
  char buf[BUF_SIZE], rflags[FLAG_LEN], storage_id[UUID_LEN + 1];
  char *send_buf;
  PyObject *ss_tuple;

  /* send request to master */
  meth = htons (method);
  data_len = htonl (INT64_LEN);
  send_buf = (char *) malloc (HEADER_LEN + INT64_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  n_tid = htonll (tid);
  memcpy (send_buf + HEADER_LEN, &n_tid, INT64_LEN);

  if (!send_all (conn, send_buf, HEADER_LEN + INT64_LEN))
    {
      free (send_buf);
      return CONNECTION_FAILURE;
    }
  free (send_buf);

  /* wait for response */
  if (!wait_packet (conn, buf, &buf_len, RHEADER_LEN))
    return CONNECTION_FAILURE;
  /* header */
  if ((ntohs (*((u_int16_t *) (buf)))) != method)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING), "return method error");
      return METHOD_ERROR;
    }
  memcpy (rflags, buf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (buf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (buf + INT16_LEN + FLAG_LEN + INT32_LEN)));
  len = buf_len - RHEADER_LEN;
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	return CONNECTION_FAILURE;
    }
  if (return_code != 0)
    return return_code;

  /* data */
  len = 0;
  *list = PyList_New (0);
  ss_tuple = PyList_New (0);
  /* get tid */
  for (i = 0; i < ID_LEN; i++)
    {
      id = ntohs (*((u_int16_t *) (buf + len)));
      len += INT16_LEN;
      PyList_Append (ss_tuple, PyInt_FromLong (id));
    }
  PyList_Append (*list, ss_tuple);
  Py_XDECREF (ss_tuple);
  /* get storage list */
  storage_nb = ntohl (*((u_int32_t *) (buf + len)));
  len += INT32_LEN;
  memset (storage_id, 0, UUID_LEN + 1);
  for (nb = 0; nb < storage_nb; nb++)
    {
      memcpy (storage_id, buf + len, UUID_LEN);
      len += UUID_LEN;
      PyList_Append (*list, PyString_FromString (storage_id));
    }
  return return_code;
}


int
endTrans (int conn, int method, u_int64_t tid, PyObject * oid_list,
	  PyObject * storage_list)
{
  u_int64_t n_tid, n_oid, n_serial, oid, serial;
  u_int32_t buf_len = 0, data_len = 0, len =
    0, n_nb_oid, nb_oid, nb_storage, n_nb_storage, i;
  u_int16_t meth, return_code = 0;
  char buf[BUF_SIZE], rflags[FLAG_LEN];
  char *send_buf, *storage_id;
  PyObject *ntuple, *nlist;

  /* send request to master */
  meth = htons (method);
  nb_oid = PyTuple_Size (oid_list);
  nb_storage = PyTuple_Size (storage_list);
  data_len =
    htonl (nb_oid * 2 * INT64_LEN + 2 * INT32_LEN + nb_storage * UUID_LEN +
	   INT64_LEN);
  buf_len = HEADER_LEN + ntohl (data_len);
  send_buf = (char *) malloc (buf_len);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  n_tid = htonll (tid);
  memcpy (send_buf + HEADER_LEN, &n_tid, INT64_LEN);
  len = HEADER_LEN + INT64_LEN;

  n_nb_storage = htonl (nb_storage);
  /* send list of storages */
  memcpy (send_buf + len, &n_nb_storage, INT32_LEN);
  len += INT32_LEN;
  for (i = 0; i < nb_storage; i++)
    {
      ntuple = PyTuple_GetItem (storage_list, i);
      nlist = PySequence_Tuple (ntuple);
      PyArg_ParseTuple (nlist, (char *) "s", &storage_id);
      Py_XDECREF (nlist);
      memcpy (send_buf + len, storage_id, UUID_LEN);
      len += UUID_LEN;
    }

  n_nb_oid = htonl (nb_oid);
  /* send list of oid and serial */
  memcpy (send_buf + len, &n_nb_oid, INT32_LEN);
  len += INT32_LEN;
  for (i = 0; i < nb_oid; i++)
    {
      ntuple = PyTuple_GetItem (oid_list, i);
      nlist = PySequence_Tuple (ntuple);
      PyArg_ParseTuple (nlist, (char *) "LL", &oid, &serial);
      Py_XDECREF (nlist);
      /* oid */
      n_oid = htonll (oid);
      memcpy (send_buf + len, &n_oid, INT64_LEN);
      len += INT64_LEN;
      /* serial */
      n_serial = htonll (serial);
      memcpy (send_buf + len, &n_serial, INT64_LEN);
      len += INT64_LEN;
    }

  if (!send_all (conn, send_buf, buf_len))
    {
      free (send_buf);
      return CONNECTION_FAILURE;
    }
  free (send_buf);

  /* Wait for response */
  buf_len = 0;
  if (!wait_packet (conn, buf, &buf_len, RHEADER_LEN))
    return CONNECTION_FAILURE;
  /* header */
  if ((ntohs (*((u_int16_t *) (buf)))) != method)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING), "return method error");
      return METHOD_ERROR;
    }
  memcpy (rflags, buf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (buf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (buf + INT16_LEN + FLAG_LEN + INT32_LEN)));
  len = buf_len - RHEADER_LEN;
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	return CONNECTION_FAILURE;
    }

  return return_code;
}


int
getAllSN (int conn, int method, PyObject ** list)
{
  u_int32_t buf_len = 0, data_len = 0, len = 0, storage_nb, nb;
  u_int16_t port, return_code, meth;
  char hbuf[RHEADER_LEN], rflags[FLAG_LEN], id[UUID_LEN + 1], ip[IP_LEN + 1];
  char *send_buf, *buf;
  PyObject *ss_tuple;

  /* send request to master */
  meth = htons (method);
  data_len = 0;
  send_buf = (char *) malloc (HEADER_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* no data */
  if (!send_all (conn, send_buf, HEADER_LEN))
    {
      free (send_buf);
      return CONNECTION_FAILURE;
    }
  free (send_buf);

  /* Wait for response */
  if (!wait_packet (conn, hbuf, &buf_len, RHEADER_LEN))
    return CONNECTION_FAILURE;
  /* header */
  if ((ntohs (*((u_int16_t *) (hbuf)))) != method)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING), "return method error");
      return METHOD_ERROR;
    }
  memcpy (rflags, hbuf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (hbuf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (hbuf + INT16_LEN + FLAG_LEN + INT32_LEN)));
  
  buf = (char *)malloc (data_len + 1);
  memset (buf, 0, data_len + 1);

  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
        {
          free (buf);
          return CONNECTION_FAILURE;
        }
    }
  if (return_code != 0)
    {
      free (buf);
      return return_code;
    }
  /* data */

  /* store storages infos in a list */
  memset (id, 0, UUID_LEN + 1);
  memset (ip, 0, IP_LEN + 1);
  *list = PyList_New (0);
  storage_nb = ntohl (*((u_int32_t *) (buf)));
  len = INT32_LEN;
  for (nb = 0; nb < storage_nb; nb++)
    {
      /* make a tuple for each storage */
      ss_tuple = PyList_New (0);
      /* id */
      memcpy (id, buf + len, UUID_LEN);
      len += UUID_LEN;
      PyList_Append (ss_tuple, PyString_FromString (id));
      memcpy (ip, buf + len, IP_LEN);
      len += IP_LEN;
      PyList_Append (ss_tuple, PyString_FromString (ip));
      port = ntohs (*((u_int16_t *) (buf + len)));
      len += INT16_LEN;
      PyList_Append (ss_tuple, PyInt_FromLong (port));
      /* add tuple to list */
      PyList_Append (*list, ss_tuple);
      Py_XDECREF (ss_tuple);
    }
  free (buf);
  return return_code;
}


int
getAllCN (int conn, int method, PyObject ** list)
{
  u_int32_t buf_len = 0, data_len = 0, len = 0, client_nb, nb;
  u_int16_t return_code, port, meth;
  char hbuf[RHEADER_LEN], rflags[FLAG_LEN], id[UUID_LEN + 1], ip[IP_LEN + 1];
  char *send_buf, *buf;
  PyObject *ss_tuple;

  /* send request to master */
  meth = htons (method);
  data_len = 0;
  send_buf = (char *) malloc (HEADER_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* no data */

  if (!send_all (conn, send_buf, HEADER_LEN))
    {
      free (send_buf);
      return CONNECTION_FAILURE;
    }
  free (send_buf);

  /* Wait for response */
  if (!wait_packet (conn, hbuf, &buf_len, RHEADER_LEN))
    return CONNECTION_FAILURE;
  /* header */
  if ((ntohs (*((u_int16_t *) (hbuf)))) != method)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING), "return method error");
      return METHOD_ERROR;
    }
  memcpy (rflags, hbuf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (hbuf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (hbuf + INT16_LEN + FLAG_LEN + INT32_LEN)));

  buf = (char *)malloc (data_len + 1);
  memset (buf, 0, data_len + 1);

  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
        {
          free (buf);
          return CONNECTION_FAILURE;
        }
    }
  if (return_code != 0)
    {
      free (buf);
      return return_code;
    }

  /* data */
  client_nb = ntohl (*((u_int32_t *) (buf)));
  *list = PyList_New (0);
  len = INT32_LEN;
  memset (id, 0, UUID_LEN + 1);
  memset (ip, 0, IP_LEN + 1);
  for (nb = 0; nb < client_nb; nb++)
    {
      ss_tuple = PyList_New (0);
      memcpy (id, buf + len, UUID_LEN);	/* id */
      len += UUID_LEN;
      PyList_Append (ss_tuple, PyString_FromString (id));
      memcpy (ip, buf + len, IP_LEN);	/* ip */
      len += IP_LEN;
      PyList_Append (ss_tuple, PyString_FromString (ip));
      port = ntohs (*((u_int16_t *) (buf + len)));
      len += INT16_LEN;
      PyList_Append (ss_tuple, PyInt_FromLong (port));
      PyList_Append (*list, ss_tuple);
      Py_XDECREF (ss_tuple);
    }
  free (buf);
  return return_code;
}


int
failure (int conn, int method, char sn[UUID_LEN])
{
  u_int32_t data_len, meth;
  char *send_buf;

  /* send request to master */
  meth = htons (method);
  data_len = htonl (UUID_LEN);
  send_buf = (char *) malloc (HEADER_LEN + UUID_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  memcpy (send_buf + HEADER_LEN, sn, UUID_LEN);
  if (!send_all (conn, send_buf, HEADER_LEN + UUID_LEN))
    {
      free (send_buf);
      return CONNECTION_FAILURE;
    }
  free (send_buf);
  return 0;
}




/* Storage call */
int
transaction (int conn, int method, char *user, char *desc,
	     char *ext, u_int64_t tid, unsigned long total_object_data_size,
	     PyObject * list, char **mmsg)
{
  u_int64_t oid, serial, n_oid, n_serial, n_tid, data_size;
  u_int32_t buf_len = 0, data_len = 0, nb_obj, n_nb_obj, len = 0, i;
  int32_t crc, n_crc;
  u_int16_t meth, user_len, desc_len, ext_len, return_code;
  char buf[BUF_SIZE], rflags[FLAG_LEN];
  char *send_buf, *data;
  PyObject *nlist, *ntuple;

  /* send request to master */
  meth = htons (method);
  user_len = htons (strlen (user));
  desc_len = htons (strlen (desc));
  ext_len = htons (strlen (ext));
  nb_obj = PyTuple_Size (list);
  data_len = htonl (INT64_LEN + 3 * INT16_LEN + strlen (user)
		    + strlen (desc) + strlen (ext) + INT32_LEN
		    + nb_obj * (3 * INT64_LEN + INT32_LEN) +
		    total_object_data_size);

  buf_len = HEADER_LEN + ntohl (data_len);
  send_buf = (char *) malloc (buf_len);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  len += HEADER_LEN;
  n_tid = htonll (tid);
  memcpy (send_buf + len, &n_tid, INT64_LEN);
  len += INT64_LEN;
  /* len and data about transaction */
  memcpy (send_buf + len, &user_len, INT16_LEN);
  len += INT16_LEN;
  memcpy (send_buf + len, &desc_len, INT16_LEN);
  len += INT16_LEN;
  memcpy (send_buf + len, &ext_len, INT16_LEN);
  len += INT16_LEN;
  memcpy (send_buf + len, user, strlen (user));
  len += strlen (user);
  memcpy (send_buf + len, desc, strlen (desc));
  len += strlen (desc);
  memcpy (send_buf + len, ext, strlen (ext));
  len += strlen (ext);

  /* object list */
  n_nb_obj = htonl (nb_obj);
  memcpy (send_buf + len, &n_nb_obj, INT32_LEN);
  len += INT32_LEN;
  for (i = 0; i < nb_obj; i++)
    {
      ntuple = PyTuple_GetItem (list, i);
      nlist = PySequence_Tuple (ntuple);
      /* XXX maybe use s# in case of null byte in data */
      PyArg_ParseTuple (nlist, (char *) "LLsl", &oid, &serial, &data, &crc);
      Py_XDECREF (nlist);
      /*  oid */
      n_oid = htonll (oid);
      memcpy (send_buf + len, &n_oid, INT64_LEN);
      len += INT64_LEN;
      /*  serial */
      n_serial = htonll (serial);
      memcpy (send_buf + len, &n_serial, INT64_LEN);
      len += INT64_LEN;
      /*  data */
      data_size = htonll (strlen (data));	/* must be 64int */
      memcpy (send_buf + len, &data_size, INT64_LEN);
      len += INT64_LEN;
      memcpy (send_buf + len, data, strlen (data));
      len += strlen (data);
      /* crc */
      n_crc = htonl (crc);
      memcpy (send_buf + len, &n_crc, INT32_LEN);
      len += INT32_LEN;
    }
  if (!send_all (conn, send_buf, buf_len))
    {
      free (send_buf);
      return CONNECTION_FAILURE;
    }
  free (send_buf);

  /* Wait for response */
  buf_len = 0;
  if (!wait_packet (conn, buf, &buf_len, RHEADER_LEN))
    return CONNECTION_FAILURE;
  /* header */
  if ((ntohs (*((u_int16_t *) (buf)))) != method)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING), "return method error");
      return METHOD_ERROR;
    }
  memcpy (rflags, buf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (buf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (buf + INT16_LEN + FLAG_LEN + INT32_LEN)));
  len = buf_len - RHEADER_LEN;
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	return CONNECTION_FAILURE;
    }

  if (return_code != 0)
    return return_code;
  /* data */
/*   msg_len = ntohs (*((u_int16_t *) (buf))); */
/*   *mmsg = (char *) malloc (msg_len + 1); */
/*   memset (*mmsg, 0, msg_len + 1); */
/*   memcpy (*mmsg, buf + 2, msg_len); */

  return return_code;
}

int
undo (int conn, int method, u_int64_t tid, PyObject ** list)
{
  u_int64_t n_tid, oid;
  u_int32_t buf_len = 0, data_len = 0, len = 0, nb_oid, nb;
  u_int16_t meth, return_code;
  char buf[BUF_SIZE], rflags[FLAG_LEN];
  char *send_buf;

  /* send request to master */
  meth = htons (method);
  data_len = htonl (INT64_LEN);
  send_buf = (char *) malloc (HEADER_LEN + INT64_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  n_tid = htonll (tid);
  memcpy (send_buf + HEADER_LEN, &n_tid, INT64_LEN);

  if (!send_all (conn, send_buf, HEADER_LEN + INT64_LEN))
    {
      free (send_buf);
      return CONNECTION_FAILURE;
    }
  free (send_buf);

  /* Wait for response */
  if (!wait_packet (conn, buf, &buf_len, RHEADER_LEN))
    return CONNECTION_FAILURE;
  /* header */
  if ((ntohs (*((u_int16_t *) (buf)))) != method)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING), "return method error");
      return METHOD_ERROR;
    }
  memcpy (rflags, buf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (buf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (buf + INT16_LEN + FLAG_LEN + INT32_LEN)));
  len = buf_len - RHEADER_LEN;
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	return CONNECTION_FAILURE;
    }

  if (return_code != 0)
    return return_code;
  /* data */

  /* get list of oid that have been undone */
  nb_oid = ntohl (*((u_int32_t *) (buf)));
  len = INT32_LEN;
  *list = PyList_New (0);
  for (nb = 0; nb < nb_oid; nb++)
    {
      oid = ntohll (*((u_int64_t *) (buf + len)));
      len += INT64_LEN;
      PyList_Append (*list, PyLong_FromUnsignedLongLong (oid));
    }
  return return_code;
}



int
histInfo (int conn, int method, u_int64_t oid, u_int64_t serial,
	  PyObject ** list)
{
  u_int64_t n_oid, n_serial, object_size;
  u_int32_t buf_len = 0, data_len = 0, len = 0;
  u_int16_t meth, return_code;
  char buf[BUF_SIZE], rflags[FLAG_LEN];
  char *data, *send_buf;

  /* send request to master */
  meth = htons (method);
  data_len = htonl (2 * INT64_LEN);
  send_buf = (char *) malloc (HEADER_LEN + 2 * INT64_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  len = HEADER_LEN;
  n_oid = htonll (oid);
  memcpy (send_buf + len, &n_oid, INT64_LEN);
  len += INT64_LEN;
  n_serial = htonll (serial);
  memcpy (send_buf + len, &n_serial, INT64_LEN);
  len += INT64_LEN;

  if (!send_all (conn, send_buf, HEADER_LEN + 2 * INT64_LEN))
    {
      free (send_buf);
      return CONNECTION_FAILURE;
    }
  free (send_buf);

  /* Wait for response */
  if (!wait_packet (conn, buf, &buf_len, RHEADER_LEN))
    return CONNECTION_FAILURE;
  /* header */
  if ((ntohs (*((u_int16_t *) (buf)))) != method)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING), "return method error");
      return METHOD_ERROR;
    }
  memcpy (rflags, buf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (buf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (buf + INT16_LEN + FLAG_LEN + INT32_LEN)));
  len = buf_len - RHEADER_LEN;
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	return CONNECTION_FAILURE;
    }
  if (return_code != 0)
    return return_code;
  /* data */

  *list = PyList_New (0);
  len = 0;
  /* time */
  data_len = ntohs (*((u_int16_t *) (buf + len)));
  len += INT16_LEN;
  data = (char *) malloc (data_len + 1);
  memset (data, 0, data_len + 1);
  memcpy (data, buf + len, data_len);
  PyList_Append (*list, PyString_FromString (data));
  len += data_len;
  free (data);
  /* user */
  data_len = ntohs (*((u_int16_t *) (buf + len)));
  len += INT16_LEN;
  data = (char *) malloc (data_len + 1);
  memset (data, 0, data_len + 1);
  memcpy (data, buf + len, data_len);
  PyList_Append (*list, PyString_FromString (data));
  len += data_len;
  free (data);
  /* description */
  data_len = ntohs (*((u_int16_t *) (buf + len)));
  len += INT16_LEN;
  data = (char *) malloc (data_len + 1);
  memset (data, 0, data_len + 1);
  memcpy (data, buf + len, data_len);
  PyList_Append (*list, PyString_FromString (data));
  len += data_len;
  free (data);
  /* serial */
  serial = ntohll (*((u_int64_t *) (buf + len)));
  PyList_Append (*list, PyLong_FromUnsignedLongLong (serial));
  len += INT64_LEN;
  /* size */
  object_size = ntohll (*((u_int64_t *) (buf + len)));
  PyList_Append (*list, PyLong_FromUnsignedLongLong (object_size));

  return return_code;
}


int
undoInfo (int conn, int method, u_int64_t tid, PyObject ** list)
{
  u_int64_t n_tid, id;
  u_int32_t buf_len = 0, data_len = 0, len = 0;
  u_int16_t meth, return_code;
  char buf[BUF_SIZE], rflags[FLAG_LEN];
  char *send_buf, *data;

  /* send request to master */
  meth = htons (method);
  data_len = htonl (INT64_LEN);
  send_buf = (char *) malloc (HEADER_LEN + INT64_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  n_tid = htonll (tid);
  memcpy (send_buf + HEADER_LEN, &n_tid, INT64_LEN);

  if (!send_all (conn, send_buf, HEADER_LEN + INT64_LEN))
    {
      free (send_buf);
      return CONNECTION_FAILURE;
    }
  free (send_buf);

  /* Wait for response */
  if (!wait_packet (conn, buf, &buf_len, RHEADER_LEN))
    return CONNECTION_FAILURE;
  /* header */
  if ((ntohs (*((u_int16_t *) (buf)))) != method)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING), "return method error");
      return METHOD_ERROR;
    }
  memcpy (rflags, buf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (buf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (buf + INT16_LEN + FLAG_LEN + INT32_LEN)));
  len = buf_len - RHEADER_LEN;
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	return CONNECTION_FAILURE;
    }
  if (return_code != 0)
    return return_code;

  /* data */
  *list = PyList_New (0);
  len = 0;
  /* time */
  data_len = ntohs (*((u_int16_t *) (buf + len)));
  len += INT16_LEN;
  data = (char *) malloc (data_len + 1);
  memset (data, 0, data_len + 1);
  memcpy (data, buf + len, data_len);
  PyList_Append (*list, PyString_FromString (data));
  len += data_len;
  free (data);
  /* user */
  data_len = ntohs (*((u_int16_t *) (buf + len)));
  len += INT16_LEN;
  data = (char *) malloc (data_len + 1);
  memset (data, 0, data_len + 1);
  memcpy (data, buf + len, data_len);
  PyList_Append (*list, PyString_FromString (data));
  len += data_len;
  free (data);
  /* desc */
  data_len = ntohs (*((u_int16_t *) (buf + len)));
  len += INT16_LEN;
  data = (char *) malloc (data_len + 1);
  memset (data, 0, data_len + 1);
  memcpy (data, buf + len, data_len);
  PyList_Append (*list, PyString_FromString (data));
  len += data_len;
  free (data);
  /* id  */
  id = ntohll (*((u_int64_t *) (buf + len)));
  PyList_Append (*list, PyLong_FromUnsignedLongLong (id));

  return return_code;
}


int
load (int conn, int method, u_int64_t oid, u_int64_t serial,
      PyObject ** object)
{
  u_int64_t n_oid, n_serial, object_len;
  u_int32_t buf_len = 0, data_len = 0, len = 0;
  u_int16_t meth, return_code;
  int32_t crc;
  char *buf, rflags[FLAG_LEN], hbuf[RHEADER_LEN];
  char *send_buf, *data;

  /* send request to master */
  meth = htons (method);
  data_len = htonl (2 * INT64_LEN);
  send_buf = (char *) malloc (HEADER_LEN + 2 * INT64_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  n_oid = ntohll (oid);
  memcpy (send_buf + HEADER_LEN, &n_oid, INT64_LEN);
  n_serial = ntohll (serial);
  memcpy (send_buf + HEADER_LEN + INT64_LEN, &n_serial, INT64_LEN);

  if (!send_all (conn, send_buf, HEADER_LEN + 2 * INT64_LEN))
    {
      free (send_buf);
      return CONNECTION_FAILURE;
    }
  free (send_buf);
  /* Wait for response */
  if (!wait_packet (conn, hbuf, &buf_len, RHEADER_LEN))
    return CONNECTION_FAILURE;
  /* header */
  if ((ntohs (*((u_int16_t *) (hbuf)))) != method)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING), "return method error");
      return METHOD_ERROR;
    }
  memcpy (rflags, hbuf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (hbuf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (hbuf + INT16_LEN + FLAG_LEN + INT32_LEN)));
  len = buf_len - RHEADER_LEN;

  buf = (char *) malloc (data_len+1);
  memset (buf, 0, data_len+1);
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
        {
          free (buf);
          return CONNECTION_FAILURE;
        }
    }
  if (return_code != 0)
    {
      free (buf);
      return return_code;
    }
  
  /* data */
  object_len = ntohll (*((u_int64_t *) (buf)));
  data = (char *) malloc (object_len + 1);
  memset (data, 0, object_len + 1);
  memcpy (data, buf + INT64_LEN, object_len);
  /* crc */
  crc = ntohl (*((u_int32_t *) (buf + INT64_LEN + object_len)));
  *object = PyList_New (0);
  PyList_Append (*object, PyString_FromString (data));
  PyList_Append (*object, PyLong_FromLong (crc));
  free (data);
  free (buf);
  return return_code;
}


int
getSize (int conn, int method, u_int64_t * size)
{
  u_int32_t buf_len = 0, data_len = 0, len = 0;
  u_int16_t meth, return_code;
  char buf[BUF_SIZE], rflags[FLAG_LEN];
  char *send_buf;

  /* send request to master */
  meth = htons (method);
  data_len = 0;
  send_buf = (char *) malloc (HEADER_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* no data */
  if (!send_all (conn, send_buf, HEADER_LEN))
    {
      free (send_buf);
      return CONNECTION_FAILURE;
    }
  free (send_buf);

  /* Wait for response */
  if (!wait_packet (conn, buf, &buf_len, RHEADER_LEN))
    return CONNECTION_FAILURE;
  /* header */
  if ((ntohs (*((u_int16_t *) (buf)))) != method)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING), "return method error");
      return METHOD_ERROR;
    }
  memcpy (rflags, buf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (buf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (buf + INT16_LEN + FLAG_LEN + INT32_LEN)));
  len = buf_len - RHEADER_LEN;
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	return CONNECTION_FAILURE;
    }
  close (conn);
  if (return_code != 0)
    return return_code;
  /* data */
  *size = ntohll (*((u_int64_t *) (buf)));

  return return_code;
}


/* Other client call */
int
checkCache (int method, u_int64_t serial, PyObject * oid, PyObject * client)
{
  u_int64_t n_serial, n_oid, id;
  u_int32_t buf_len = 0, data_len = 0, nb_oid, n_nb_oid, len = 0, i;
  u_int16_t meth, conn, port, client_nb;
  char *send_buf, *ip;
  PyObject *ntuple, *nlist;

  /* send request to master */
  meth = htons (method);
  nb_oid = PyTuple_Size (oid);
  data_len = htonl (nb_oid * INT64_LEN + INT64_LEN + INT32_LEN);
  buf_len = HEADER_LEN + nb_oid * INT64_LEN + INT64_LEN + INT32_LEN;
  send_buf = (char *) malloc (buf_len);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  n_serial = htonll (serial);
  memcpy (send_buf + HEADER_LEN, &n_serial, INT64_LEN);
  len = HEADER_LEN + INT64_LEN;
  n_nb_oid = htonl (nb_oid);
  memcpy (send_buf + len, &n_nb_oid, INT32_LEN);
  len += INT32_LEN;

  for (i = 0; i < nb_oid; i++)
    {
      ntuple = PyTuple_GetItem (oid, i);
      nlist = PySequence_Tuple (ntuple);
      PyArg_ParseTuple (nlist, (char *) "L", &id);
      Py_XDECREF (nlist);
      n_oid = ntohll (id);
      memcpy (send_buf + len, &n_oid, INT64_LEN);
      len += INT64_LEN;
    }

  /* send message to each client */
  client_nb = PyTuple_Size (client);
  for (i = 0; i < client_nb; i++)
    {
      ntuple = PyTuple_GetItem (client, i);
      nlist = PySequence_Tuple (ntuple);
      PyArg_ParseTuple (nlist, (char *) "si", &ip, &port);
      Py_XDECREF (nlist);
      /* establish connection */
      if (!(conn = connectTo (ip, port)))
	continue;
      if (!send_all (conn, send_buf, buf_len))
	continue;
      close (conn);
    }

  free (send_buf);

  return 0;
}

/* XXX dont use for the moment */
int
getSNInfo (int conn, int method, char *id, struct node *sn)
{
  u_int32_t buf_len, data_len;
  u_int16_t meth, id_len;
  char buf[BUF_SIZE];
  u_int16_t return_code, addr_len;
  u_int32_t len = 0;
  char rflags[2];
  char *send_buf;

  /* send request to master */
  meth = htons (method);
  data_len = htonl (2 + strlen (id));
  id_len = htons (strlen (id));

  buf_len = HEADER_LEN + ntohl (data_len);
  send_buf = (char *) malloc (buf_len);

  /* header */
  memcpy (send_buf, &meth, 2);
  memcpy (send_buf + 2, flags, 2);
  memcpy (send_buf + 4, &data_len, 4);
  /* data */
  memcpy (send_buf + 8, &id_len, 2);
  memcpy (send_buf + 10, id, strlen (id));

  if (!send_all (conn, send_buf, buf_len))
    {
      free (send_buf);
      return 7;
    }
  free (send_buf);

  /* Wait for response */
  buf_len = 0, data_len = 0;
  if (!wait_packet (conn, buf, &buf_len, RHEADER_LEN))
    return 7;
  /* header */
  if ((ntohs (*((u_int16_t *) (buf)))) != method)
    {
      syslog (LOG_MAKEPRI (LOG_USER, LOG_WARNING), "return method error");
      return 8;
    }
  memcpy (rflags, buf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (buf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (buf + INT16_LEN + FLAG_LEN + INT32_LEN)));
  len = buf_len - RHEADER_LEN;
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	return 7;
    }
  if (return_code != 0)
    return return_code;
  /* data */

  /* Wait for response */
  data_len = 0;
  buf_len = 0;

  if (!wait_packet (conn, buf, &buf_len, RHEADER_LEN))
    return 7;

  if ((ntohs (*((u_int16_t *) (buf)))) != method)
    {
      perror ("receive");
      return 8;
    }

  memcpy (rflags, buf + 2, 2);
  data_len = ntohl (*((u_int32_t *) (buf + 4)));
  return_code = ntohs (*((u_int16_t *) (buf + 8)));

  len = buf_len - 10;
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	return 7;
    }
  if (return_code != 0)
    return return_code;

  len = 0;
  /* id */
  id_len = ntohs (*((u_int16_t *) (buf + len)));
  len += 2;
  sn->id = (char *) malloc (id_len + 1);
  memset (sn->id, 0, id_len + 1);
  memcpy (sn->id, buf + len, id_len);
  len += id_len;
  /* ip */
  addr_len = ntohs (*((u_int16_t *) (buf + len)));
  len += 2;
  sn->addr = (char *) malloc (addr_len + 1);
  memset (sn->addr, 0, addr_len + 1);
  memcpy (sn->addr, buf + len, addr_len);
  len += addr_len;
  /* port */
  memcpy (&(sn->port), buf + len, 2);

  return return_code;
}


/* method called by client server to wait for a message */
int
wait_msg (int conn, PyObject ** list)
{

  u_int64_t serial, oid;
  char buf[BUF_SIZE];
  u_int32_t len = 0, data_len = 0, buf_len = 0, oid_nb, nb;
  u_int16_t method, port;
  char rflags[FLAG_LEN];
  char id[UUID_LEN + 1], ip[IP_LEN + 1];

  /* Wait for response */
  if (!wait_packet (conn, buf, &buf_len, HEADER_LEN))
    return CONNECTION_FAILURE;
  /* header */
  method = ntohs (*((u_int16_t *) (buf)));
  memcpy (rflags, buf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (buf + INT16_LEN + FLAG_LEN)));
  len = buf_len - HEADER_LEN;
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	return CONNECTION_FAILURE;
    }
  printf ("receive mesage with method %d, data len %d\n", method, data_len);
  /* data */
  len = 0;
  *list = PyList_New (0);
  PyList_Append (*list, PyInt_FromLong (method));

  if (method == 19)		/* check cache message */
    {
      /* get tid */
      serial = ntohll (*((u_int64_t *) (buf)));
      PyList_Append (*list, PyLong_FromUnsignedLongLong (serial));
      len += INT64_LEN;
      /* get list of oid */
      oid_nb = ntohl (*((u_int32_t *) (buf + len)));
      len += INT32_LEN;
      for (nb = 0; nb < oid_nb; nb++)
	{
	  oid = ntohll (*((u_int64_t *) (buf + len)));
	  PyList_Append (*list, PyLong_FromUnsignedLongLong (oid));
	  len += INT64_LEN;
	}
      return method;
    }

  else if (method == 13 || method == 14 || method == 32)	/* add node message or change master */
    {
      /* id */
      memset (id, 0, UUID_LEN + 1);
      memcpy (id, buf + len, UUID_LEN);
      PyList_Append (*list, PyString_FromString (id));
      len += UUID_LEN;
      /* ip */
      memset (ip, 0, IP_LEN + 1);
      memcpy (ip, buf + len, IP_LEN);
      PyList_Append (*list, PyString_FromString (ip));
      len += IP_LEN;
      /* port */
      port = ntohs (*((u_int16_t *) (buf + len)));
      PyList_Append (*list, PyInt_FromLong (port));

      return method;
    }

  else if (method == 15 || method == 16 || method == 33 || method == 28)	/* del node message or failure node */
    {
      /* id */
      memset (id, 0, UUID_LEN + 1);
      memcpy (id, buf + len, UUID_LEN);
      PyList_Append (*list, PyString_FromString (id));

      return method;
    }
  return 0;
}


int
clientClose (int conn, int method, char id[UUID_LEN])
{
  u_int32_t data_len;
  u_int16_t meth;
  char *send_buf;
  /* send request to master */
  meth = htons (method);
  data_len = htonl (UUID_LEN);
  send_buf = (char *) malloc (HEADER_LEN + UUID_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);

  /* data */
  memcpy (send_buf + HEADER_LEN, id, UUID_LEN);

  if (!send_all (conn, send_buf, HEADER_LEN + UUID_LEN))
    {
      free (send_buf);
      return CONNECTION_FAILURE;
    }
  free (send_buf);
  return 0;
}


int
replyClose (int conn, int method, char id[UUID_LEN])
{
  u_int32_t data_len;
  u_int16_t meth, return_code = 0;
  char *send_buf;

  /* send request to master */
  meth = htons (method);
  data_len = htonl (UUID_LEN);
  send_buf = (char *) malloc (UUID_LEN + RHEADER_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN + INT32_LEN, &return_code,
	  FLAG_LEN);
  /* data */
  memcpy (send_buf + RHEADER_LEN, id, UUID_LEN);

  if (!send_all (conn, send_buf, UUID_LEN + RHEADER_LEN))
    {
      free (send_buf);
      return 7;
    }
  free (send_buf);
  return 0;
}

