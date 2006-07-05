#ifndef neo_socket_h
#define neo_socket_h

#include "neo_struct.h"

int send_all (int conn, char *buf, size_t len);
int fill_buffer (int conn, char *buf, u_int32_t * len, u_int32_t max);
int wait_packet (int conn, char *buf, u_int32_t * len, u_int32_t max);
int connectTo (char ip[IP_LEN], u_int16_t port);
unsigned long long htonll (unsigned long long n);
unsigned long long ntohll (unsigned long long n);
int getIp (int ds, char **ip);

#endif
