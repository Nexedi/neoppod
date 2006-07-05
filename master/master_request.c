#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <netinet/in.h>
#include <stdio.h>

#include "neo_socket.h"
#include "neo_struct.h"
#include "master_request.h"


int
masterClose (char node_ip[IP_LEN], u_int16_t node_port, int method,
	     char master_id[UUID_LEN], char **node_id)
{
  u_int32_t buf_len, data_len, meth;
  u_int16_t conn;
  char *send_buf;
  u_int32_t len = 0;
  char buf[BUF_SIZE];
  u_int16_t return_code;
  char rflags[2];

  /* send request to nodes */
  meth = htons (method);
  data_len = htonl (UUID_LEN);
  send_buf = (char *) malloc (HEADER_LEN + UUID_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  memcpy (send_buf + HEADER_LEN, master_id, UUID_LEN);

  if (!(conn = connectTo (node_ip, node_port)))
    return 0;

  if (!send_all (conn, send_buf, HEADER_LEN + UUID_LEN))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);

  /* wait response from nodes */
  buf_len = 0, data_len = 0;

  if (!wait_packet (conn, buf, &buf_len, RHEADER_LEN))
    return 0;

  if ((ntohs (*((u_int16_t *) (buf)))) != method)
    {
      perror ("receive");
      return 0;
    }

  memcpy (rflags, buf + INT16_LEN, FLAG_LEN);
  data_len = ntohl (*((u_int32_t *) (buf + INT16_LEN + FLAG_LEN)));
  return_code =
    ntohs (*((u_int16_t *) (buf + INT16_LEN + FLAG_LEN + INT32_LEN)));
  len = buf_len - RHEADER_LEN;
  while (len < data_len)
    {
      if (!fill_buffer (conn, buf, &len, data_len - len))
	return 0;
    }

  *node_id = (char *) malloc (UUID_LEN + 1);
  memset (*node_id, 0, UUID_LEN + 1);
  memcpy (*node_id, buf, UUID_LEN);

  return 1;
}


char *
masterChange (char node_ip[IP_LEN], u_int16_t node_port, int method,
	      char id[UUID_LEN], char ip[IP_LEN], u_int16_t port)
{
  u_int32_t buf_len, data_len, meth, rlen = 0;
  u_int16_t len, conn, n_port;
  char *send_buf;
  char buf[BUF_SIZE];
  u_int16_t return_code;
  char *idr;
  char rflags[2];

  /* send request to client */
  meth = htons (method);
  data_len = htonl (UUID_LEN + IP_LEN + INT16_LEN);
  send_buf = (char *) malloc (HEADER_LEN + UUID_LEN + IP_LEN + INT16_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  memcpy (send_buf + HEADER_LEN, id, UUID_LEN);
  len = HEADER_LEN + UUID_LEN;
  memcpy (send_buf + HEADER_LEN, ip, IP_LEN);
  len += IP_LEN;
  n_port = htons (port);
  memcpy (send_buf + len, &n_port, INT16_LEN);

  if (!(conn = connectTo (node_ip, node_port)))
    return 0;

  if (!send_all (conn, send_buf, HEADER_LEN + UUID_LEN + IP_LEN + INT16_LEN))
    {
      free (send_buf);
      return NULL;
    }
  free (send_buf);

  /* wait response from nodes */
  buf_len = 0, data_len = 0;

  if (!wait_packet (conn, buf, &buf_len, RHEADER_LEN))
    return 0;

  if ((ntohs (*((u_int16_t *) (buf)))) != method)
    {
      perror ("receive");
      return 0;
    }

  memcpy (rflags, buf + 2, 2);
  data_len = ntohl (*((u_int32_t *) (buf + 4)));
  return_code = ntohs (*((u_int16_t *) (buf + 8)));
  rlen = buf_len - 10;

  while (rlen < data_len)
    {
      if (!fill_buffer (conn, buf, &rlen, data_len - rlen))
	return 0;
    }
  close (conn);
  idr = (char *) malloc (UUID_LEN + 1);
  memset (idr, 0, UUID_LEN + 1);
  memcpy (idr, buf, UUID_LEN);

  return idr;
}

int
addStorage (char node_ip[IP_LEN], u_int16_t node_port, int method,
	    char storage_id[UUID_LEN], char storage_ip[IP_LEN],
	    u_int16_t storage_port)
{
  u_int32_t data_len, conn;
  u_int16_t len, meth, n_storage_port;
  char *send_buf;

  if (!(conn = connectTo (node_ip, node_port)))
    return 0;

  /* send request to client */
  meth = htons (method);
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
  strncpy (send_buf + len, storage_ip, IP_LEN);
  len += IP_LEN;
  n_storage_port = htons (storage_port);
  memcpy (send_buf + len, &n_storage_port, INT16_LEN);

  if (!send_all (conn, send_buf, HEADER_LEN + UUID_LEN + IP_LEN + INT16_LEN))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);

  close (conn);

  return 1;
}


int
addClient (char node_ip[IP_LEN], u_int16_t node_port, int method,
	   char client_id[UUID_LEN], char client_ip[IP_LEN],
	   u_int16_t client_port)
{
  u_int32_t data_len, conn;
  u_int16_t len, meth, n_client_port;
  char *send_buf;

  if (!(conn = connectTo (node_ip, node_port)))
    return 0;

  /* send request to client */
  meth = htons (method);
  data_len = htonl (UUID_LEN + IP_LEN + INT16_LEN);
  send_buf = (char *) malloc (HEADER_LEN + UUID_LEN + IP_LEN + INT16_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);

  /* data */
  len = HEADER_LEN;
  memcpy (send_buf + len, client_id, UUID_LEN);
  len += UUID_LEN;
  memcpy (send_buf + len, client_ip, IP_LEN);
  len += IP_LEN;
  n_client_port = htons (client_port);
  memcpy (send_buf + len, &n_client_port, INT16_LEN);

  if (!send_all (conn, send_buf, HEADER_LEN + UUID_LEN + IP_LEN + INT16_LEN))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);
  close (conn);

  return 1;
}



int
delStorage (char node_ip[IP_LEN], u_int16_t node_port, int method,
	    char storage_id[UUID_LEN])
{
  u_int32_t data_len, conn;
  u_int16_t meth;
  char *send_buf;

  if (!(conn = connectTo (node_ip, node_port)))
    return 0;

  /* send request to client */
  meth = htons (method);
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

  close (conn);

  return 1;

}

int
delClient (char node_ip[IP_LEN], u_int16_t node_port, int method,
	   char client_id[UUID_LEN])
{
  u_int32_t data_len, conn;
  u_int16_t meth;
  char *send_buf;

  if (!(conn = connectTo (node_ip, node_port)))
    return 0;

  /* send request to client */
  meth = htons (method);
  data_len = htonl (UUID_LEN);
  send_buf = (char *) malloc (HEADER_LEN + UUID_LEN);

  /* header */
  memcpy (send_buf, &meth, INT16_LEN);
  memcpy (send_buf + INT16_LEN, flags, FLAG_LEN);
  memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN);
  /* data */
  memcpy (send_buf + HEADER_LEN, client_id, UUID_LEN);

  if (!send_all (conn, send_buf, HEADER_LEN + UUID_LEN))
    {
      free (send_buf);
      return 0;
    }
  free (send_buf);

  close (conn);

  return 1;

}



/* int */
/* unreliableStorage (char *ip, int port, int method, char *id) */
/* { */
/*   u_int32_t buf_len, data_len, conn; */
/*   u_int16_t id_len, meth; */
/*   char *send_buf; */

/*   conn = connectTo (ip, port); */

/*   /\* send request to client *\/ */
/*   meth = htons (method); */
/*   data_len = htonl (2 + strlen (id)); */
/*   id_len = htons (strlen (id)); */

/*   buf_len = HEADER_LEN + ntohl (data_len); */
/*   send_buf = (char *) malloc (buf_len); */

/*   /\* header *\/ */
/*   memcpy (send_buf, &meth, INT16_LEN); */
/*   memcpy (send_buf + INT16_LEN, flags, FLAG_LEN); */
/*   memcpy (send_buf + INT16_LEN + FLAG_LEN, &data_len, INT32_LEN); */
/*   memcpy (send_buf, &meth, 2); */
/*   memcpy (send_buf + 2, flags, 2); */
/*   memcpy (send_buf + 4, &data_len, 4); */
/*   /\* data *\/ */
/*   memcpy (send_buf + 8, &id_len, 2); */
/*   memcpy (send_buf + 10, id, strlen (id)); */

/*   if (!send_all (conn, send_buf, buf_len)) */
/*     { */
/*       free (send_buf); */
/*       return 0; */
/*     } */
/*   free (send_buf); */

/*   close (conn); */

/*   return 1; */

/* } */
