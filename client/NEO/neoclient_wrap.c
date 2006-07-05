#include <Python.h>
#include "neo_struct.h"

extern int search_master (int, char **);
extern int generate_id (char **);
extern int getObjectByOid (int, int, u_int64_t, PyObject **);
extern int getObjectBySerial (int, int, u_int64_t, u_int64_t, PyObject **);
extern int getSerial (int, int, u_int64_t, u_int64_t *);
extern int getLastTransaction (int, int, u_int64_t *);
extern int getOid (int, int, u_int16_t, PyObject **);
extern int clientStart (int, int, char *, char *, u_int16_t, PyObject **);
extern int getTransSN (int, int, int16_t, int16_t, PyObject **);
extern int getObjectHist (int, int, u_int64_t, u_int16_t, PyObject **);
extern int undoTrans (int, int, u_int64_t, PyObject **);
extern int beginTrans (int, int, u_int64_t, PyObject **);
extern int endTrans (int, int, u_int64_t, PyObject *, PyObject *);
extern int failure (int, int, char[UUID_LEN]);
extern int transaction (int, int, char *, char *, char *, u_int64_t,
			unsigned long, PyObject *, char **);
extern int undo (int, int, u_int64_t, PyObject **);
extern int histInfo (int, int, u_int64_t, u_int64_t, PyObject **);
extern int undoInfo (int, int, u_int64_t, PyObject **);
extern int load (int, int, u_int64_t, u_int64_t, PyObject **);
extern int getSize (int, int, u_int64_t *);
extern int checkCache (int, u_int64_t, PyObject *, PyObject *);
extern int getSNInfo (int, int, char *, struct node *);
extern int getAllSN (int, int, PyObject **);
extern int getAllCN (int, int, PyObject **);
extern int wait_msg (int, PyObject **);
extern int clientClose (int, int, char *);
extern int replyClose (int, int, char *);

static int
handleError (int rcode)
{
  switch (rcode)
    {
    case TMP_FAILURE:
      PyErr_SetString (PyExc_SystemError, "temporary failure");
      break;
    case OID_NOT_FOUND:
      PyErr_SetString (PyExc_ValueError, "oid not found");
      break;
    case SERIAL_NOT_FOUND:
      PyErr_SetString (PyExc_ValueError, "serial not found");
      break;
    case TRANS_NOT_FOUND:
      PyErr_SetString (PyExc_ValueError, "transaction not found");
      break;
    case ABORT_TRANS:
      PyErr_SetString (PyExc_RuntimeError, "abort transaction");
      break;
    case TRANS_NOT_VALID:
      PyErr_SetString (PyExc_RuntimeError, "transaction not valid");
      break;
    case CONNECTION_FAILURE:
      PyErr_SetString (PyExc_SystemError, "connection failure");
      break;
    case METHOD_ERROR:
      PyErr_SetString (PyExc_SystemError, "wrong message");
      break;
    case NOT_READY:
      PyErr_SetString (PyExc_RuntimeError, "master not ready");
      break;
    }
  return 0;
}

static PyObject *
_wrap_generate_id (PyObject * self, PyObject * args)
{
  PyObject *resultobj;
  char *result;
  int rcode;
  rcode = generate_id (&result);
  if (rcode != 0)		/* raise exception */
    {
      handleError (rcode);
      return NULL;
    }
  resultobj =
    result ? PyString_FromString (result) : Py_BuildValue ((char *) "");
  free (result);
  return resultobj;

}


static PyObject *
_wrap_search_master (PyObject * self, PyObject * args)
{
  PyObject *resultobj;
  int method;
  char *result;
  int rcode;

  if (!PyArg_ParseTuple
      (args, (char *) "i:search_master", &method))
    goto fail;

  rcode = search_master (method, &result);
  if (rcode != 0)		/* raise exception */
    {
      handleError (rcode);
      return NULL;
    }
  resultobj =
    result ? PyString_FromString (result) : Py_BuildValue ((char *) "");
  free (result);
  return resultobj;

fail:
  return NULL;
}


static PyObject *
_wrap_getObjectByOid (PyObject * self, PyObject * args)
{
  int conn, method;
  u_int64_t oid;
  u_int16_t rcode;
  PyObject *resultlist;

  if (!PyArg_ParseTuple
      (args, (char *) "iiK:getObjectByOid", &conn, &method, &oid))
    goto fail;

  rcode = getObjectByOid (conn, method, oid, &resultlist);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  return resultlist;
fail:
  return NULL;
}


static PyObject *
_wrap_getObjectBySerial (PyObject * self, PyObject * args)
{
  PyObject *resultlist;
  int conn, method;
  u_int64_t oid, serial;
  int rcode;

  if (!PyArg_ParseTuple
      (args, (char *) "iiKK:getObjectBySerial", &conn, &method, &oid,
       &serial))
    goto fail;
  rcode = getObjectBySerial (conn, method, oid, serial, &resultlist);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  return resultlist;
fail:
  return NULL;
}


static PyObject *
_wrap_getSerial (PyObject * self, PyObject * args)
{
  int conn, method;
  u_int64_t oid, serial;
  int rcode;

  if (!PyArg_ParseTuple
      (args, (char *) "iiK:getSerial", &conn, &method, &oid))
    goto fail;
  rcode = getSerial (conn, method, oid, &serial);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  return PyLong_FromUnsignedLongLong (serial);
fail:
  return NULL;
}


static PyObject *
_wrap_getLastTransaction (PyObject * self, PyObject * args)
{
  int conn, method;
  u_int64_t tid;
  int rcode;

  if (!PyArg_ParseTuple
      (args, (char *) "ii:getLastTransaction", &conn, &method))
    goto fail;
  rcode = getLastTransaction (conn, method, &tid);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  return PyLong_FromUnsignedLongLong (tid);
fail:
  return NULL;
}


static PyObject *
_wrap_getOid (PyObject * self, PyObject * args)
{
  PyObject *resultlist;
  int conn, method;
  u_int16_t rcode, nb_oid;

  if (!PyArg_ParseTuple
      (args, (char *) "iiH:getOid", &conn, &method, &nb_oid))
    goto fail;
  rcode = getOid (conn, method, nb_oid, &resultlist);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  return resultlist;
fail:
  return NULL;
}


static PyObject *
_wrap_clientStart (PyObject * self, PyObject * args)
{
  PyObject *resultlist;
  int conn, method;
  char *id, *ip;
  u_int16_t port;
  int rcode;

  if (!PyArg_ParseTuple
      (args, (char *) "iissH:clientStart", &conn, &method, &id, &ip, &port))
    goto fail;
  rcode = clientStart (conn, method, id, ip, port, &resultlist);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  return resultlist;
fail:
  return NULL;
}


static PyObject *
_wrap_getTransSN (PyObject * self, PyObject * args)
{
  PyObject *resultlist;
  int conn, method;
  int16_t rcode, first, last;

  if (!PyArg_ParseTuple
      (args, (char *) "iihh:getTransSN", &conn, &method, &first, &last))
    goto fail;
  rcode = getTransSN (conn, method, first, last, &resultlist);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  return resultlist;
fail:
  return NULL;
}


static PyObject *
_wrap_getObjectHist (PyObject * self, PyObject * args)
{
  PyObject *resultlist;
  int conn, method;
  u_int64_t oid;
  u_int16_t length;
  u_int16_t rcode;

  if (!PyArg_ParseTuple
      (args, (char *) "iiKH:getObjectHist", &conn, &method, &oid, &length))
    goto fail;

  rcode = getObjectHist (conn, method, oid, length, &resultlist);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  return resultlist;
fail:
  return NULL;
}


static PyObject *
_wrap_undoTrans (PyObject * self, PyObject * args)
{
  PyObject *resultlist;
  int conn, method;
  u_int64_t tid;
  u_int16_t rcode;

  if (!PyArg_ParseTuple
      (args, (char *) "iiK:undoTrans", &conn, &method, &tid))
    goto fail;
  rcode = (int) undoTrans (conn, method, tid, &resultlist);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  return resultlist;

fail:
  return NULL;
}


static PyObject *
_wrap_beginTrans (PyObject * self, PyObject * args)
{
  int conn, method;
  u_int64_t tid;
  u_int16_t rcode;
  PyObject *resultlist;

  if (!PyArg_ParseTuple
      (args, (char *) "iiK:beginTrans", &conn, &method, &tid))
    goto fail;
  rcode = beginTrans (conn, method, tid, &resultlist);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  return resultlist;
fail:
  return NULL;
}


static PyObject *
_wrap_endTrans (PyObject * self, PyObject * args)
{
  int conn, method, rcode = 0;
  u_int64_t tid;
  PyObject *oid_list, *oid_obj, *storage_obj, *storage_list;

  if (!PyArg_ParseTuple
      (args, (char *) "iiKOO", &conn, &method, &tid, &oid_obj, &storage_obj))
    goto fail;


  oid_list = PyTuple_New (1);
  oid_list = PySequence_Tuple (oid_obj);

  storage_list = PyTuple_New (1);
  storage_list = PySequence_Tuple (storage_obj);
  rcode = (int) endTrans (conn, method, tid, oid_list, storage_list);

  Py_XDECREF (oid_list);
  Py_XDECREF (storage_list);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  return PyInt_FromLong ((long) rcode);
fail:
  return NULL;
}


static PyObject *
_wrap_failure (PyObject * self, PyObject * args)
{
  PyObject *resultobj;
  int conn, method;
  char *storage_id;
  int rcode;
  if (!PyArg_ParseTuple
      (args, (char *) "iis:failure", &conn, &method, &storage_id))
    goto fail;
  rcode = (int) failure (conn, method, storage_id);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  resultobj = PyInt_FromLong ((long) rcode);
  return resultobj;
fail:
  return NULL;
}

static PyObject *
_wrap_transaction (PyObject * self, PyObject * args)
{
  int method, rcode = 0, conn;
  char *user, *desc, *ext, *result;
  unsigned long data_size;
  u_int64_t tid;
  PyObject *tuple, *list;

  if (!PyArg_ParseTuple
      (args, (char *) "iisssKkO", &conn, &method, &user, &desc, &ext,
       &tid, &data_size, &tuple))
    goto fail;

  list = PyTuple_New (1);
  list = PySequence_Tuple (tuple);

  rcode =
    (int) transaction (conn, method, user, desc, ext, tid, data_size,
		       list, &result);

  Py_XDECREF (list);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  return PyInt_FromLong ((long) rcode);
fail:
  return NULL;
}


static PyObject *
_wrap_undo (PyObject * self, PyObject * args)
{
  PyObject *resultlist;
  int method, conn;
  u_int64_t tid;
  u_int16_t rcode;
  if (!PyArg_ParseTuple (args, (char *) "iiK:undo", &conn, &method, &tid))
    goto fail;
  rcode = (int) undo (conn, method, tid, &resultlist);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  return resultlist;
fail:
  return NULL;
}


static PyObject *
_wrap_histInfo (PyObject * self, PyObject * args)
{
  PyObject *resultobj;
  int method, conn;
  u_int64_t oid, serial;
  int rcode;

  if (!PyArg_ParseTuple
      (args, (char *) "iiKK:histInfo", &conn, &method, &oid, &serial))
    goto fail;
  rcode = histInfo (conn, method, oid, serial, &resultobj);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  return resultobj;
fail:
  return NULL;
}


static PyObject *
_wrap_undoInfo (PyObject * self, PyObject * args)
{
  PyObject *resultobj;
  int method, conn;
  u_int64_t tid;
  int rcode;

  if (!PyArg_ParseTuple (args, (char *) "iiK:undoInfo", &conn, &method, &tid))
    goto fail;
  rcode = undoInfo (conn, method, tid, &resultobj);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  return resultobj;
fail:
  return NULL;
}


static PyObject *
_wrap_load (PyObject * self, PyObject * args)
{
  PyObject *resultobj;
  int conn, method;
  u_int64_t oid, serial;
  int rcode;

  if (!PyArg_ParseTuple
      (args, (char *) "iiKK:load", &conn, &method, &oid, &serial))
    goto fail;
  rcode = load (conn, method, oid, serial, &resultobj);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  return resultobj;
fail:
  return NULL;
}


static PyObject *
_wrap_getSize (PyObject * self, PyObject * args)
{
  PyObject *resultobj;
  int conn, method;
  u_int64_t result;
  int rcode;

  if (!PyArg_ParseTuple (args, (char *) "ii:getSize", &conn, &method))
    goto fail;
  rcode = (int) getSize (conn, method, &result);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  resultobj = PyLong_FromUnsignedLongLong ((long long) result);
  return resultobj;
fail:
  return NULL;
}


static PyObject *
_wrap_checkCache (PyObject * self, PyObject * args)
{
  int method, rcode;
  u_int64_t serial;
  PyObject *oid, *client, *resultobj, *oid_list, *client_list;

  if (!PyArg_ParseTuple
      (args, (char *) "iKOO", &method, &serial, &oid, &client))
    goto fail;

  oid_list = PyTuple_New (1);
  oid_list = PySequence_Tuple (oid);

  client_list = PyTuple_New (1);
  client_list = PySequence_Tuple (client);

  rcode = (int) checkCache (method, serial, oid_list, client_list);

  resultobj = PyInt_FromLong ((long) rcode);
  return resultobj;
fail:
  return NULL;
}


static PyObject *
_wrap_getSNInfo (PyObject * self, PyObject * args)
{
  PyObject *resultobj;
  int arg1;
  int arg2;
  char *arg3;
  struct node result;
  int rcode;

  if (!PyArg_ParseTuple (args, (char *) "iis:getSNInfo", &arg1, &arg2, &arg3))
    goto fail;
  rcode = getSNInfo (arg1, arg2, arg3, &result);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  resultobj = Py_BuildValue ("ssi", result.id, result.addr, result.port);

  return resultobj;
fail:
  return NULL;
}


static PyObject *
_wrap_getAllSN (PyObject * self, PyObject * args)
{
  PyObject *resultlist;
  int arg1;
  int arg2;
  u_int16_t rcode;

  if (!PyArg_ParseTuple (args, (char *) "ii:getAllSN", &arg1, &arg2))
    goto fail;
  rcode = getAllSN (arg1, arg2, &resultlist);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  return resultlist;
fail:
  return NULL;
}



static PyObject *
_wrap_getAllCN (PyObject * self, PyObject * args)
{
  PyObject *resultlist;
  int arg1;
  int arg2;
  u_int16_t rcode;

  if (!PyArg_ParseTuple (args, (char *) "ii:getAllCN", &arg1, &arg2))
    goto fail;
  rcode = getAllCN (arg1, arg2, &resultlist);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  return resultlist;
fail:
  return NULL;
}


static PyObject *
_wrap_clientClose (PyObject * self, PyObject * args)
{
  PyObject *resultobj;
  int arg1;
  int arg2;
  char *arg3;
  int rcode;

  if (!PyArg_ParseTuple
      (args, (char *) "iis:clientClose", &arg1, &arg2, &arg3))
    goto fail;
  rcode = clientClose (arg1, arg2, arg3);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  resultobj = PyInt_FromLong ((long) rcode);
  return resultobj;
fail:
  return NULL;
}


static PyObject *
_wrap_replyClose (PyObject * self, PyObject * args)
{
  PyObject *resultobj;
  int conn, method;
  char *id;
  int rcode;

  if (!PyArg_ParseTuple
      (args, (char *) "iis:clientClose", &conn, &method, &id))
    goto fail;
  rcode = replyClose (conn, method, id);

  if (rcode != 0)
    {
      handleError (rcode);
      return NULL;
    }

  resultobj = PyInt_FromLong ((long) rcode);
  return resultobj;
fail:
  return NULL;
}




static PyObject *
_wrap_wait (PyObject * self, PyObject * args)
{
  PyObject *resultlist;
  int arg1;
  u_int16_t rcode;

  if (!PyArg_ParseTuple (args, (char *) "i:wait", &arg1))
    goto fail;

  rcode = wait_msg (arg1, &resultlist);
  if (rcode != 0)
    return resultlist;

fail:
  return NULL;

}



static PyMethodDef SwigMethods[] = {
  {(char *) "generate_id", _wrap_generate_id, METH_VARARGS},
  {(char *) "search_master", _wrap_search_master, METH_VARARGS},
  {(char *) "getObjectByOid", _wrap_getObjectByOid, METH_VARARGS},
  {(char *) "getObjectBySerial", _wrap_getObjectBySerial, METH_VARARGS},
  {(char *) "getSerial", _wrap_getSerial, METH_VARARGS},
  {(char *) "getLastTransaction", _wrap_getLastTransaction, METH_VARARGS},
  {(char *) "getOid", _wrap_getOid, METH_VARARGS},
  {(char *) "clientStart", _wrap_clientStart, METH_VARARGS},
  {(char *) "getTransSN", _wrap_getTransSN, METH_VARARGS},
  {(char *) "getObjectHist", _wrap_getObjectHist, METH_VARARGS},
  {(char *) "undoTrans", _wrap_undoTrans, METH_VARARGS},
  {(char *) "beginTrans", _wrap_beginTrans, METH_VARARGS},
  {(char *) "endTrans", _wrap_endTrans, METH_VARARGS},
  {(char *) "failure", _wrap_failure, METH_VARARGS},
  {(char *) "transaction", _wrap_transaction, METH_VARARGS},
  {(char *) "undo", _wrap_undo, METH_VARARGS},
  {(char *) "histInfo", _wrap_histInfo, METH_VARARGS},
  {(char *) "undoInfo", _wrap_undoInfo, METH_VARARGS},
  {(char *) "load", _wrap_load, METH_VARARGS},
  {(char *) "getSize", _wrap_getSize, METH_VARARGS},
  {(char *) "checkCache", _wrap_checkCache, METH_VARARGS},
  {(char *) "getSNInfo", _wrap_getSNInfo, METH_VARARGS},
  {(char *) "getAllSN", _wrap_getAllSN, METH_VARARGS},
  {(char *) "getAllCN", _wrap_getAllCN, METH_VARARGS},
  {(char *) "clientClose", _wrap_clientClose, METH_VARARGS},
  {(char *) "wait", _wrap_wait, METH_VARARGS},
  {(char *) "replyClose", _wrap_replyClose, METH_VARARGS},
  {NULL, NULL}
};


void
initneoclient ()
{
  (void) Py_InitModule ("neoclient", SwigMethods);
}
