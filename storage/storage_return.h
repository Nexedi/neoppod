#ifndef storage_return_h
#define storage_return_h

int returnTransaction (int conn, int method);
int returnUndo (int conn, int method, u_int64_t * oid, u_int32_t nb_oid);
int returnHistInfo (int conn, int method, struct hist hist);
int returnUndoInfo (int conn, int method, struct undoInfo hist);
int returnLoad (int conn, int method, char *object, int32_t crc);
int returnGetSize (int conn, int method, u_int64_t size);
int returnMasterClose (int conn, int method, char *id);
int returnMasterChange (int conn, int method, char *id);
int returnErrorMessage (int conn, int method, int code, char *message);
int returnGetTransData (int conn, int method, u_int64_t tid, char *user,
			char *desc, char *ext, struct objectList objects);

#endif
