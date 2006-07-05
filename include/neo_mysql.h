#ifndef neo_mysql_h
#define neo_mysql_h

#include <mysql/mysql.h>
#include "neo_struct.h"

struct database;

struct database_mysql
{
  MYSQL *sql;
};

/* common functions */
int database_close (struct database *orig_db);
int database_thread_init (void);
void database_thread_end (void);

/* storage functions */
struct database *database_storage_open (int create);

int database_storage_get_object (struct database *orig_db, u_int64_t oid,
				 u_int64_t serial, void **object,
				 int32_t * crc);

int database_storage_get_trans_info (struct database *orig_db,
				     u_int64_t tid, struct undoInfo *info);

int database_storage_get_object_info (struct database *orig_db,
				      u_int64_t oid, u_int64_t serial,
				      struct hist *hist);

int database_storage_get_object_list (struct database *orig_db,
				      u_int64_t tid,
				      u_int64_t ** list, u_int32_t * nb_oid);

int database_storage_del_trans (struct database *orig_db, u_int64_t tid);

int database_storage_put_object (struct database *orig_db, u_int64_t oid,
				 u_int64_t serial, u_int64_t tid,
				 void *object, u_int32_t size, int32_t crc);

int database_storage_put_trans (struct database *orig_db, u_int64_t tid,
				char *user, char *desc, char *ext);


int database_storage_get_index_list (struct database *orig_db,
				     struct stringList **oidl,
				     struct stringList **tidl);
int database_storage_get_trans_data (struct database *orig_db,
				     u_int64_t tid, char **user,
				     char **desc, char **ext,
				     struct objectList **list);

int database_storage_store_trans (struct database *orig_db, u_int64_t tid,
				  char *user, char *desc, char *ext);

int database_storage_trans_exist (struct database *orig_db, u_int64_t tid,
				  int *exist);

/* master functions */
struct database *database_master_open (int create);
int database_master_get_object_by_oid (struct database *orig_db,
				       u_int64_t oid, u_int64_t * serial,
				       struct stringList **list);
int database_master_get_object_by_serial (struct database *orig_db,
					  u_int64_t oid,
					  u_int64_t serial,
					  struct stringList **list);
int database_master_get_serial (struct database *orig_db, u_int64_t oid,
				u_int64_t * serial);
int database_master_get_object_hist (struct database *orig_db,
				     u_int64_t oid, u_int16_t length,
				     struct tlist **list);
int database_master_undo_trans (struct database *orig_db, u_int64_t tid,
				struct stringList **list);
int database_master_begin_trans (struct database *orig_db, u_int64_t tid,
				 u_int64_t ltid, struct stringList list);
int database_master_end_trans (struct database *orig_db, u_int64_t tid);
int database_master_remove_client (struct database *orig_db,
                                   u_int16_t conn);
int database_master_delete_client (struct database *orig_db,
				   char id[UUID_LEN]);
int database_master_close_storage (struct database *orig_db,
				   char id[UUID_LEN]);
int database_master_storage_starting (struct database *orig_db,
				      char id[UUID_LEN], char ip[IP_LEN],
				      u_int16_t port, u_int16_t * status);
int database_master_get_storage_infos (struct database *orig_db,
				       char id[UUID_LEN], char **ip,
				       u_int16_t * port);
int database_master_start_client (struct database *orig_db, char id[UUID_LEN],
				  char ip[IP_LEN], u_int16_t port, u_int16_t conn);
int database_master_get_all_storages_infos (struct database *orig_db,
					    struct nlist **list);
int database_master_get_all_clients_infos (struct database *orig_db,
					   struct nlist **list);
int database_master_storage_ready (struct database *orig_db,
				   char id[UUID_LEN]);
int database_master_get_trans_storages (struct database *orig_db,
					u_int64_t tid, int16_t first,
					int16_t last, struct tlist **list);
int database_master_put_object (struct database *orig_db, u_int64_t tid,
				u_int64_t oid, u_int64_t serial);
int database_master_delete_object (struct database *orig_db, u_int64_t oid,
				   u_int64_t serial);
int database_master_get_trans_list (struct database *orig_db,
				    char storage_id[UUID_LEN],
				    struct tlist **list);
int database_master_unreliable_storage (struct database *orig_db,
					char id[UUID_LEN]);
int database_master_get_storage_index (struct database *orig_db,
				       char id[UUID_LEN],
				       struct transactionList **txn,
				       u_int64_t * nb_txn);

int database_master_create_storage_index (struct database *orig_db,
					  struct stringList **index);

int database_master_get_storages_for_trans (struct database *orig_db,
					    u_int64_t tid,
					    char storage_id[UUID_LEN],
					    struct stringList **list);

int database_storage_check_sanity (struct database *orig_db, u_int64_t tid,
				   u_int64_t oid);

int database_storage_get_db_size (struct database *orig_db, u_int64_t *size);

int database_master_all_objects_accessible(struct database *orig_db);

#endif
