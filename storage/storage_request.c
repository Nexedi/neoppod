#include <netinet/in.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

#include "neo_socket.h"
#include "neo_struct.h"

#define STORAGE_START 29


int
storageClose (char master_ip[IP_LEN], u_int16_t master_port,
	      char storage_id[UUID_LEN])
{
  int conn;
  u_int32_t data_len, meth;
  char *send_buf;

  /* send request to master */
  meth = htons (STORAGE_CLOSE);
  data_len = htonl (UUID_LEN);
  send_buf = (char *) malloc (HEADER_LEN + UUID_LEN);


  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  memcpy (send_buf + HEADER_LEN, storage_id, UUID_LEN);

  if (!(conn = connectTo (master_ip, master_port)))
    return 0;

  if (!send_all (conn, send_buf, HEADER_LEN + UUID_LEN))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);

  close (conn);

  return 1;
}


int
storageStart (int conn, char storage_id[UUID_LEN], char storage_ip[IP_LEN],
              u_int16_t storage_port, char master_id[UUID_LEN], int *status)
{
  u_int32_t buf_len, data_len, meth, len = 0;
  u_int16_t n_storage_port;
  char buf[BUF_SIZE];
  u_int16_t return_code;
  char rflags[FLAG_LEN];
  char *send_buf;


  /* send request to master */
  meth = htons (STORAGE_START);
  data_len = htonl (UUID_LEN + IP_LEN + INT16_LEN);
  send_buf = (char *) malloc (HEADER_LEN + UUID_LEN + IP_LEN + INT16_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);

  /* data */
  len = HEADER_LEN;
  memcpy (send_buf + len, storage_id, UUID_LEN);
  len += UUID_LEN;
  memcpy (send_buf + len, storage_ip, IP_LEN);
  len += IP_LEN;
  n_storage_port = htons (storage_port);
  memcpy (send_buf + len, &n_storage_port, INT16_LEN);

  if (!send_all (conn, send_buf, HEADER_LEN + UUID_LEN + IP_LEN + INT16_LEN))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);

  /* Wait for response */
  data_len = 0;
  buf_len = 0;

  if (!wait_packet (conn, buf, &buf_len, RHEADER_LEN))
    return 0;

  if ((ntohs (*((u_int16_t *) (buf)))) != STORAGE_START)
    {
      perror ("receive");
      return 0;
    }

  memcpy (rflags, buf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (buf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (buf + INT16_LEN + FLAG_LEN + INT32_LEN)));

  if (return_code != 0)
    return return_code;

  len = buf_len - 10;
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	return 0;
    }
  memset (master_id, 0, UUID_LEN + 1);
  memcpy (master_id, buf, UUID_LEN);
  *status = ntohs (*((u_int16_t *) (buf + UUID_LEN)));

  return 1;
}


int
storageReady (int conn, char storage_id[UUID_LEN])
{
  u_int32_t data_len, meth;
  char *send_buf;

  /* send request to master */
  meth = htons (STORAGE_READY);
  data_len = htonl (UUID_LEN);
  send_buf = (char *) malloc (HEADER_LEN + UUID_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  memcpy (send_buf + HEADER_LEN, storage_id, UUID_LEN);

  if (!send_all (conn, send_buf, HEADER_LEN + UUID_LEN))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  return 1;
}


int
getTransData (int conn, int method, u_int64_t tid)
{
  u_int64_t n_tid;
  u_int32_t buf_len, data_len, meth, len = 0;
  char *send_buf;

  /* send request to master */
  meth = htons (GET_TRANS_DATA);
  data_len = htonl (ID_LEN);

  buf_len = HEADER_LEN + ntohl (data_len);
  send_buf = (char *) malloc (buf_len);

  /* header */
  memcpy (send_buf, &meth, 2);
  memcpy (send_buf + 2, flags, 2);
  memcpy (send_buf + 4, &data_len, 4);
  /* data */
  len = 8;

  n_tid = htonll (tid);
  memcpy (send_buf + len, &n_tid, INT64_LEN);

  if (!send_all (conn, send_buf, buf_len))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);

  return 1;
}
