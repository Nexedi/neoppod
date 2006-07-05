#ifndef master_request_h
#define master_request_h

#include "neo_struct.h"

int masterClose (char node_ip[IP_LEN], u_int16_t node_port, int method,
		 char master_id[UUID_LEN], char **storage_id);

char *masterChange (char node_ip[IP_LEN], u_int16_t node_port, int method,
		    char master_ip[IP_LEN], char master_id[UUID_LEN],
		    u_int16_t master_port);

int addStorage (char node_ip[IP_LEN], u_int16_t node_port, int method,
		char storage_id[UUID_LEN], char storage_ip[IP_LEN],
		u_int16_t storage_port);

int addClient (char node_ip[IP_LEN], u_int16_t node_port, int method,
	       char client_id[UUID_LEN], char client_ip[IP_LEN],
	       u_int16_t client_port);

int delStorage (char node_ip[IP_LEN], u_int16_t node_port, int method,
		char storage_id[UUID_LEN]);

int delClient (char node_ip[IP_LEN], u_int16_t node_port, int method,
	       char client_id[UUID_LEN]);

/* int unreliableStorage (char *ip, int port, int method, char *id); */

#endif
