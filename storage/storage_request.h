#ifndef storage_request_h
#define storage_request_h

int storageClose (char master_ip[IP_LEN], u_int16_t port,
                  char storage_id[UUID_LEN]);

int storageStart (int conn,
                  char storage_id[UUID_LEN], char storage_ip[IP_LEN],
                  u_int16_t storage_port, char master_id[UUID_LEN],
                  int *status);

int storageReady (int conn,
                  char storage_id[UUID_LEN]);

int getTransData (int conn, int method,
                  u_int64_t tid);

#endif
