#ifndef mater_return_h
#define mater_return_h

#include "neo_struct.h"

int returnGetObjectByOid (int conn, int method, u_int64_t serial,
			  struct stringList storages);

int returnGetObjectBySerial (int conn, int method,
			     struct stringList storages);

int returnGetSerial (int conn, int method, u_int64_t serial);

int returnGetLastTransaction (int conn, int method, u_int64_t tid);

int returnGetOid (int conn, int method, struct list list);
int returnGetOidInt (int conn, int method, struct oidList list);

int returnClientStart (int conn, int method, struct storageInfo info);

int returnGetTransSN (int conn, int method, struct tlist tl);

int returnGetObjectHist (int conn, int method, struct tlist tl);

int returnUndoTrans (int conn, int method, struct stringList storages);

int returnBeginTrans (int conn, int method, u_int16_t tid[ID_LEN],
		      struct stringList storages);

int returnEndTrans (int conn, int method);

int returnGetAllSN (int conn, int method, struct nlist storages);

int returnGetAllCN (int conn, int method, struct nlist storages);

int returnGetSNInfo (int conn, int method, struct node info);

int returnStorageStart (int conn, int method, char id[UUID_LEN],
			u_int16_t status);

int returnErrorMessage (int conn, int method, int code, char *message);

int returnStorageSanity (int conn, int method, struct tlist tl);

int returnStorageIndex (int conn, int method, struct transactionList *txn);

int returnGetStorageForTrans (int conn, int method,
			      struct stringList storages);

#endif
