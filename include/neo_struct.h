#ifndef neo_struct_h
#define neo_struct_h
/* define some constant */
#define ID_LEN 8
#define UUID_LEN 36
#define IP_LEN 15
#define HEADER_LEN 8		/* 2 byte : id, 2 byte : flag, 4 byte : data_len */
#define RHEADER_LEN 10		/* idem + 2 byte : return code */
#define BUF_SIZE 1024
#define flags "  "
#define NDEBUG 0
#define MASTER_PORT 10823
#define FLAG_LEN 2
#define INT16_LEN 2
#define INT32_LEN 4
#define INT64_LEN 8


/* list of error */
enum error
{
  TMP_FAILURE = 1,
  OID_NOT_FOUND,
  SERIAL_NOT_FOUND,
  TRANS_NOT_FOUND,
  ABORT_TRANS,
  TRANS_NOT_VALID,
  CONNECTION_FAILURE,
  METHOD_ERROR,
  NOT_READY,
};

/* list of function call */
enum functions
{
  GET_OBJECT_BY_OID = 1,
  GET_OBJECT_BY_SERIAL,
  GET_SERIAL,
  GET_LAST_TRANS,
  GET_OID,
  GET_TRANS_SN,
  GET_OBJECT_HIST,
  UNDO_TRANS,
  BEGIN_TRANS,
  END_TRANS, /* 10 */
  GET_ALL_SN,
  GET_ALL_CN,
  ADD_SN,
  ADD_CN,
  DEL_SN,
  DEL_CN,
  GET_SN_INFO,
  FAILURE,
  CHECK_CACHE,
  TRANSACTION, /* 20 */
  UNDO,
  HIST_INFO,
  UNDO_INFO,
  LOAD,
  GET_SIZE,
  CLIENT_CLOSE,
  STORAGE_CLOSE,
  MASTER_CLOSE,
  STORAGE_START,
  STORAGE_READY, /* 30 */
  CLIENT_START,
  MASTER_CHANGE,
  UNRELIABLE_STORAGE,
  SANITY_CHECK,
  GET_TRANS_DATA,
  STORAGE_INDEX,
  GET_STORAGE_FOR_TRANS,
  SEARCH_MASTER,
};


/* generic object for oid, storage and client */
struct cobject
{
  char *data;
};

/* generic list for oid , storage and lcient list*/
struct list
{
  u_int16_t len;
  u_int32_t last;
  struct cobject *objects;
};


/* structure for transaction */
struct trans
{
  u_int64_t tid;
  struct list list;
};

/* transaction, serial list */
struct tlist
{
  u_int16_t len;
  u_int32_t last;
  struct trans *objects;
};

struct hist
{
  float time;
  char *user;
  char *desc;
  u_int64_t serial;
  u_int64_t size;
};

struct undoInfo
{
  float time;
  char *user;
  char *desc;
  u_int64_t id;
};

struct storageInfo
{
  u_int16_t supportVersion;
  u_int16_t supportUndo;
  u_int16_t supportTransUndo;
  u_int16_t readOnly;
  char *name;
  char *ext;
};

struct node
{
  char *id;
  char *addr;
  u_int16_t port;
};

/* node list */
struct nlist
{
  u_int16_t len;
  u_int32_t last;
  struct node *objects;
};

/* structure for storage index in master node */
struct index
{
  int len;
  int last;
  int id_size;
  char *id;
};

struct oidList
{
  int len;
  int last;
  int *list;
};

int oidList_New (struct oidList *o, int list_size);
int oidList_Append (struct oidList *o, int oid[8]);
int oidList_GetItem (struct oidList *o, int oid[8], int pos);
int oidList_Free (struct oidList *o);

int init_tlist (struct tlist *pl, u_int16_t size);
int add_trans (struct tlist **pl, u_int64_t tid, u_int16_t nb_s);
int free_tlist (struct tlist *pl);

int init_list (struct list *pl, u_int16_t size);
int add_cobject (struct list *pl, char *cdata, char *sdata);
int free_list (struct list *pl);

int init_nlist (struct nlist *pl, u_int16_t size);
int add_node (struct nlist *pl, char *id, char *addr, u_int16_t port);
int free_nlist (struct nlist *pl);

int init_index (struct index *i, u_int16_t id_size);
int index_add (struct index *i, char *id);
int index_get (struct index *i, char **id, int pos);



struct stringList
{
  u_int32_t len;
  u_int32_t last;
  u_int32_t item_size;
  char *list;
};

int stringList_New (struct stringList *o, u_int32_t list_size,
		    u_int32_t item_size);
int stringList_Append (struct stringList *o, char *item);
int stringList_GetItem (struct stringList *o, char **item, u_int32_t pos);
int stringList_Free (struct stringList *o);

struct object
{
  u_int64_t oid;
  u_int64_t serial;
  char *data;
};

struct objectList
{
  u_int32_t len;
  u_int32_t last;
  struct object *list;
};

struct transaction
{
  u_int64_t tid;
  struct objectList objects;
};

struct transactionList
{
  u_int32_t len;
  u_int32_t last;
  struct transaction *txn;
};


int objectList_New (struct objectList *o, u_int32_t list_size);
int objectList_Append (struct objectList *o, u_int64_t oid,
		       u_int64_t serial, char *data);
int objectList_GetItem (struct objectList *o, u_int64_t * oid,
			u_int64_t serial, char **data);
int objectList_Free (struct objectList *o);
/* int transaction_New (struct transaction *t, u_int64_t tid, u_int32_t nb_objects); */
/* int transaction_AppendObject (struct transaction *t, u_int64_t oid, u_int64_t serial); */
/* int transaction_Free (struct transaction *t); */

int transactionList_New (struct transactionList *t, u_int32_t nb_txn);
int transactionList_Append (struct transactionList *t, u_int64_t tid,
			    struct objectList object);
int transactionList_Free (struct transactionList *t);
#endif
