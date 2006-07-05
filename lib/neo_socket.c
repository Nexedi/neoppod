#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <netdb.h>

#include "neo_socket.h"

#define NDEBUG 0


/**
 * function that return the client ip for a given connection
 * although clients give their ip, we prefer
 * to get it from the socket connection
 */
int
getIp (int ds, char **ip)
{
  struct sockaddr_in addr;
  int size = sizeof (addr);
  /* get remote address */
  if (getpeername (ds, (struct sockaddr *) &addr, &size) < 0)
    return 0;
  *ip = (char *) malloc (IP_LEN);
  *ip = (char *) inet_ntoa (addr.sin_addr);
  if (*ip == NULL)
    return 0;
  return 1;
}


/**
 * function that convert 64 byte integer in endian or byte order
 */

unsigned long long
ntohll (unsigned long long n)
{
#if __BYTE_ORDER == __BIG_ENDIAN
  return n;
#else
  return (((unsigned long long) ntohl (n)) << 32) + ntohl (n >> 32);
#endif
}

unsigned long long
htonll (unsigned long long n)
{
#if __BYTE_ORDER == __BIG_ENDIAN
  return n;
#else
  return (((unsigned long long) htonl (n)) << 32) + htonl (n >> 32);
#endif
}



int
send_all (int conn, char *buf, size_t len)
{
  while (len > 0)
    {
      ssize_t size;
      size = write (conn, buf, len);

      if (size == 0)
	{
/* 	  fprintf (stderr, "unexpected EOF when writing\n"); */
	  return 0;
	}
      else if (size < 0)
	{
	  if (errno == EINTR)
	    continue;

	  perror ("write");
	  return 0;
	}
#if NDEBUG
      fprintf (stdout, "sending %d bytes\n", size);
#endif
      len -= (size_t) size;
      buf += size;
    }
  return 1;
}

int
wait_packet (int conn, char *buf, u_int32_t * len, u_int32_t max)
{
  while (1)
    {
      if (fill_buffer (conn, buf, len, max))
	break;
      else
	return 0;
    }
  return 1;
}

int
fill_buffer (int conn, char *buf, u_int32_t * len, u_int32_t max)
{
  ssize_t size;
again:
  if (((max - *len) < 65635))
    size = read (conn, buf + *len, max - *len);
  else
    size = read (conn, buf + *len, 65635);

  if (size == 0)
    {
/*       fprintf (stderr, "unexpected EOF when reading\n"); */
      return 0;
    }
  else if (size < 0)
    {
/*       printf ("read socket error %d\n", errno); */
      if (errno == EINTR)
	goto again;
      if (errno == EAGAIN)
	goto again;

      perror ("read");
      return 0;
    }
  *len += (u_int32_t) size;

#if NDEBUG
  fprintf (stderr, "the buffer has %u bytes\n", *len);
#endif

  return 1;
}


int
connectTo (char ip[IP_LEN], u_int16_t port)
{
  int soc, rc;
  struct sockaddr_in serv_addr;
  struct hostent *server;

  server = gethostbyname (ip);
  bzero ((char *) &serv_addr, sizeof (serv_addr));
  serv_addr.sin_family = AF_INET;
  bcopy ((char *) server->h_addr, (char *) &serv_addr.sin_addr.s_addr,
	 server->h_length);
  serv_addr.sin_port = htons (port);

  soc = socket (PF_INET, SOCK_STREAM, 0);
  if (soc < 0)
    return 0;

  rc = connect (soc, (struct sockaddr *) &serv_addr, sizeof (serv_addr));
  if (rc < 0)
    return 0;

  return soc;
}
