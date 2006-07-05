#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "neo_struct.h"


/* generic list use for storage, oid, and client*/
int
init_list (struct list *pl, u_int16_t size)
{
  pl->len = size;
  pl->last = 0;
  pl->objects = (struct cobject *) malloc (sizeof (struct cobject) * pl->len);
  return 1;
}

int
double_list (struct list *pl)
{
  int i;
  pl->len *= 2;
  struct cobject *tmp;
  tmp = (struct cobject *) malloc (sizeof (struct cobject) * pl->len);
  for (i = 0; i < pl->len / 2; i++)
    tmp[i] = pl->objects[i];
  free (pl->objects);
  pl->objects = tmp;
  return 1;
}


int
add_cobject (struct list *pl, char *cdata, char *sdata)
{
  if (pl->len == pl->last)
    {
      double_list (pl);
    }

  pl->objects[pl->last].data = (char *) malloc (strlen (cdata) + 1);
  memset (pl->objects[pl->last].data, 0, strlen (cdata) + 1);
  strncpy (pl->objects[pl->last].data, cdata, strlen (cdata));
  pl->last++;
  return 1;
}

int
free_list (struct list *pl)
{
  free (pl->objects);
  return 1;
}


/* function for transaction */
int
init_tlist (struct tlist *pl, u_int16_t size)
{
  pl->len = size;
  pl->last = 0;
  pl->objects = (struct trans *) malloc (sizeof (struct trans) * pl->len);
  return 1;
}

int
double_tlist (struct tlist *pl)
{
  int i;
  pl->len *= 2;
  struct trans *tmp;
  tmp = (struct trans *) malloc (sizeof (struct trans) * pl->len);
  for (i = 0; i < pl->len / 2; i++)
    tmp[i] = pl->objects[i];
  free (pl->objects);
  pl->objects = tmp;
  return 1;
}

int
free_tlist (struct tlist *pl)
{
  u_int16_t i;
  for (i = 0; i < pl->last; i++)
    free_list (&(pl->objects[i].list));
  free (pl->objects);
  return 1;
}

int
add_trans (struct tlist **pl, u_int64_t tid, u_int16_t nb_s)
{
  if ((*pl)->len == (*pl)->last)
    {
      perror ("add object");
      return 0;
    }
  (*pl)->objects[(*pl)->last].tid = tid;

  init_list (&((*pl)->objects[(*pl)->last].list), nb_s);
  (*pl)->last++;
  return 1;
}

/* function for node list */
int
double_nlist (struct nlist *pl)
{
  int i;
  pl->len *= 2;
  struct node *tmp;
  tmp = (struct node *) malloc (sizeof (struct node) * pl->len);
  for (i = 0; i < pl->len / 2; i++)
    tmp[i] = pl->objects[i];
  free (pl->objects);
  pl->objects = tmp;
  return 1;
}


int
add_node (struct nlist *pl, char *id, char *addr, u_int16_t port)
{
  if (pl->len == pl->last)
    {
      double_nlist (pl);
    }
  pl->objects[pl->last].id = (char *) malloc (strlen (id) + 1);
  pl->objects[pl->last].addr = (char *) malloc (strlen (addr) + 1);
  memset (pl->objects[pl->last].id, 0, strlen (id) + 1);
  memset (pl->objects[pl->last].addr, 0, strlen (addr) + 1);
  strncpy (pl->objects[pl->last].id, id, strlen (id));
  strncpy (pl->objects[pl->last].addr, addr, strlen (addr));
  pl->objects[pl->last].port = port;
  pl->last++;
  return 1;
}

int
free_nlist (struct nlist *pl)
{
  u_int32_t node_nb;
  for (node_nb = 0; node_nb < pl->last; node_nb++)
    {
      free (pl->objects[node_nb].id);
      free (pl->objects[node_nb].addr);
    }

  free (pl->objects);
  return 1;
}

int
init_nlist (struct nlist *pl, u_int16_t size)
{
  pl->len = size;
  pl->last = 0;
  pl->objects = (struct node *) malloc (sizeof (struct node) * pl->len);
  return 1;
}


/* functions for index in master node */
int
double_index (struct index *i)
{
  int j;
  char *tmpid;
  i->len = i->len * 2;
  tmpid = (char *) malloc (i->id_size * i->len);
  for (j = 0; j < (i->len) / 2; j++)
    {
      strcpy (&(tmpid[j * (i->id_size + 1)]), &(i->id[j * (i->id_size + 1)]));
    }
  free (i->id);
  i->id = tmpid;

  return 1;
}

int
init_index (struct index *i, u_int16_t id_size)
{
  i->len = 5;
  i->last = 0;
  i->id_size = id_size;
  i->id = (char *) malloc ((i->id_size + 1) * i->len);
  memset (i->id, 0, (i->id_size + 1) * i->len);

  return 1;
}

int
index_add (struct index *i, char *id)
{
  if (i->len == i->last)
    double_index (i);
  strncpy (&(i->id[i->last * (i->id_size + 1)]), id, strlen (id));
  i->last++;
  printf ("cstruct : index add last = %d\n", i->last);

  return 1;
}

int
index_get (struct index *i, char **id, int pos)
{
  *id = (char *) malloc (strlen (&(i->id[pos * (i->id_size + 1)])) + 1);
  memset (*id, 0, strlen (&(i->id[pos * (i->id_size + 1)])) + 1);
  strncpy (*id, &(i->id[pos * (i->id_size + 1)]),
	   strlen (&(i->id[pos * (i->id_size + 1)])));

  return 1;
}


/* special function for oid list */
int
oidList_New (struct oidList *o, int list_size)
{
  o->last = 0;
  o->len = list_size;

  o->list = (int *) malloc (8 * o->len * sizeof (int));

  return 1;
}

int
oidList_Append (struct oidList *o, int oid[8])
{
  int i;
  if (o->last > o->len)
    return 0;

  for (i = 0; i < 8; i++)
    o->list[o->last * 8 + i] = oid[i];

  o->last++;
  return 1;
}

int
oidList_GetItem (struct oidList *o, int oid[8], int pos)
{
  int i;
  if (pos > o->last)
    return 0;

  for (i = 0; i < 8; i++)
    oid[i] = o->list[pos * 8 + i];

  return 1;
}

int
oidList_Free (struct oidList *o)
{
  free (o->list);
  return 1;
}

/* new structure function */
int
stringList_New (struct stringList *o, u_int32_t list_size,
		u_int32_t item_size)
{
  o->last = 0;
  o->len = list_size;
  o->item_size = item_size;

  o->list = (char *) malloc (o->len * (o->item_size + 1));
  memset (o->list, 0, o->len * (o->item_size + 1));

  return 1;
}

int
stringList_Append (struct stringList *o, char *item)
{
  if (o->last > o->len)
    return 0;
  strncpy (&(o->list[o->last * (o->item_size + 1)]), item, o->item_size);

  o->last++;
  return 1;
}

int
stringList_GetItem (struct stringList *o, char **item, u_int32_t pos)
{
  if (pos > o->last)
    return 0;
  *item = (char *) malloc (o->item_size + 1);
  memset (*item, 0, o->item_size + 1);
  strncpy (*item, &(o->list[pos * (o->item_size + 1)]), o->item_size);

  return 1;
}

int
stringList_Free (struct stringList *o)
{
  if (o->list != NULL)
    free (o->list);
  return 1;
}



int
objectList_New (struct objectList *o, u_int32_t list_size)
{
  o->len = list_size;
  o->last = 0;
  o->list = (struct object *) malloc (sizeof (struct object) * list_size);
  return 1;
}



int
objectList_Append (struct objectList *o, u_int64_t oid,
		   u_int64_t serial, char *data)
{
  o->list[o->last].oid = oid;
  o->list[o->last].serial = serial;
  o->list[o->last].data = (char *) malloc (strlen (data) + 1);
  memset (o->list[o->last].data, 0, strlen (data) + 1);
  strncpy (o->list[o->last].data, data, strlen (data));
  o->last++;
  return 1;
}

int
objectList_Free (struct objectList *o)
{
  u_int32_t i;

  for (i = 0; i < o->last; i++)
    free (o->list[i].data);
  free (o->list);
  return 1;
}


int
transaction_New (struct transaction *t, u_int64_t tid, u_int32_t nb_objects)
{
  printf ("sizeof %d, %d\n", sizeof (tid), sizeof (t->tid));
  t->tid = tid;
  objectList_New (&(t->objects), nb_objects);
  return 1;
}

int
transaction_AppendObject (struct transaction *t, u_int64_t oid,
			  u_int64_t serial)
{
  if (!objectList_Append (&(t->objects), oid, serial, ""))
    return 0;

  return 1;
}


int
transaction_Free (struct transaction *t)
{
  objectList_Free (&(t->objects));
  return 1;
}


/* list of transaction */
int
transactionList_New (struct transactionList *t, u_int32_t nb_txn)
{
  t->len = nb_txn;
  t->last = 0;
  t->txn =
    (struct transaction *) malloc (sizeof (struct transaction) * nb_txn);
  return 1;
}

int
transactionList_Append (struct transactionList *t, u_int64_t tid,
			struct objectList o)
{
  u_int32_t object_nb;
  t->txn[t->last].tid = tid;
  objectList_New (&t->txn[t->last].objects, o.last);

  for (object_nb = 0; object_nb < o.last; object_nb++)
    objectList_Append (&t->txn[t->last].objects, o.list[object_nb].oid,
		       o.list[object_nb].serial, o.list[object_nb].data);

  t->last++;
  return 1;
}

int
transactionList_Free (struct transactionList *t)
{
  u_int32_t txn_nb;

  for (txn_nb = 0; txn_nb < t->last; txn_nb++)
    objectList_Free (&t->txn[txn_nb].objects);

  free (t->txn);
  return 1;
}
