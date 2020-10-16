#include <Python.h>
#include <stdlib.h>
#include <string.h>
#include "./polodb.h"

#define KEY_DATABASE "Database"
#define KEY_DOCUMENT "DbDocument"
#define KEY_DOC_ITER "DbDocumentIter"
#define KEY_VALUE "DbValue"
#define KEY_DB_HANDLE "DbHandle"
#define KEY_ARRAY "Array"
#define KEY_OBJECT_ID "ObjectId"

#define POLO_CALL(EXPR) \
  ec = (EXPR); \
  if (ec < 0) { \
    PyErr_SetString(PyExc_Exception, PLDB_error_msg()); \
    return NULL; \
  }

#define CHECK_NULL(EXPR) \
  if ((EXPR) == NULL) { \
    PyErr_SetString(PyExc_RuntimeError, "pointer is null: " #EXPR); \
    return NULL; \
  }


typedef struct {
  PyObject_HEAD
  Database* db;
} DatabaseObject;

static PyObject* DatabaseObject_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
  DatabaseObject* self;
  self = (DatabaseObject*) type->tp_alloc(type, 0);
  if (self != NULL) {
    self->db = NULL;
  }
  return (PyObject*)self;
}

static void DatabaseObject_dealloc(DatabaseObject* self) {
  if (self->db != NULL) {
    PLDB_close(self->db);
    self->db = NULL;
  }
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static int DatabaseObject_init(DatabaseObject* self, PyObject *args, PyObject* kwds) {
  const char* path = NULL;
  if (!PyArg_ParseTuple(args, "s", &path)) {
    return -1;
  }

  Database* db = PLDB_open(path);
  if (db == NULL) {
    PyErr_SetString(PyExc_Exception, PLDB_error_msg());
    return -1;
  }

  self->db = db;

  return 0;
}

static PyObject* DatabaseObject_close(DatabaseObject* self, PyObject* Py_UNUSED(ignored)) {
  if (self->db == NULL) {
    PyErr_SetString(PyExc_Exception, "database is not opened");
    return NULL;
  }

  PLDB_close(self->db);

  self->db = NULL;

  Py_RETURN_NONE;
}

static PyMethodDef DatabaseObject_methods[] = {
  {"close", (PyCFunction)DatabaseObject_close, METH_NOARGS,
   "close the database"
  },
  {NULL}  /* Sentinel */
};

static PyTypeObject DatabaseObjectType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "polodb.Database",
    .tp_doc = "Database object",
    .tp_basicsize = sizeof(DatabaseObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_new = DatabaseObject_new,
    .tp_init = (initproc) DatabaseObject_init,
    .tp_dealloc = (destructor) DatabaseObject_dealloc,
    .tp_methods = DatabaseObject_methods,
};

static void doc_dctor(PyObject* obj) {
  DbDocument* doc = (DbDocument*)PyCapsule_GetPointer(obj, KEY_DOCUMENT);
  PLDB_free_doc(doc);
}

static void value_dctor(PyObject* obj) {
  DbValue* val = (DbValue*)PyCapsule_GetPointer(obj, KEY_VALUE);
  PLDB_free_value(val);
}

static void database_dctor(PyObject* obj) {
  Database* db = PyCapsule_GetPointer(obj, KEY_DATABASE);
  if (db != NULL) {
    PLDB_close(db);
  }
}

static void doc_iter_dctor(PyObject* obj) {
  DbDocumentIter* iter = (DbDocumentIter*)PyCapsule_GetPointer(obj, KEY_DOC_ITER);
  PLDB_free_doc_iter(iter);
}

static void db_handle_dctor(PyObject* obj) {
  DbHandle* handle = PyCapsule_GetPointer(obj, KEY_DB_HANDLE);
  PLDB_free_handle(handle);
}

static void array_dctor(PyObject* obj) {
  DbArray* arr = PyCapsule_GetPointer(obj, KEY_ARRAY);
  PLDB_free_arr(arr);
}

static void object_id_dctor(PyObject* obj) {
  DbObjectId* oid = PyCapsule_GetPointer(obj, KEY_OBJECT_ID);
  PLDB_free_object_id(oid);
}

static PyObject* py_open_database(PyObject* self, PyObject* args) {
  const char* path;
  if (!PyArg_ParseTuple(args, "s", &path)) {
    return NULL;
  }

  Database* db = PLDB_open(path);
  if (db == NULL) {
    PyErr_SetString(PyExc_Exception, PLDB_error_msg());
    return NULL;
  }

  PyObject* result = PyCapsule_New(db, KEY_DATABASE, database_dctor);

  return result;
}

static PyObject* py_close_database(PyObject* self, PyObject* args) {
  PyObject* obj;
  if (!PyArg_ParseTuple(args, "O", &obj)) {
    return NULL;
  }

  Database* db = PyCapsule_GetPointer(obj, KEY_DATABASE);
  PLDB_close(db);

  PyCapsule_SetPointer(obj, NULL);

  Py_RETURN_NONE;
}

static PyObject* py_start_transaction(PyObject* self, PyObject* args) {
  PyObject* obj;
  int flags;
  if (!PyArg_ParseTuple(args, "Oi", &obj, &flags)) {
    return NULL;
  }

  Database* db = PyCapsule_GetPointer(obj, KEY_DATABASE);
  CHECK_NULL(db);

  int ec = 0;
  POLO_CALL(PLDB_start_transaction(db, flags));

  Py_RETURN_NONE;
}

static PyObject* py_commit(PyObject* self, PyObject* args) {
  PyObject* obj;
  if (!PyArg_ParseTuple(args, "O", &obj)) {
    return NULL;
  }

  Database* db = PyCapsule_GetPointer(obj, KEY_DATABASE);
  CHECK_NULL(db);
  
  int ec = 0;
  POLO_CALL(PLDB_commit(db))

  Py_RETURN_NONE;
}

static PyObject* py_rollback(PyObject* self, PyObject* args) {
  PyObject* obj;
  if (!PyArg_ParseTuple(args, "O", &obj)) {
    return NULL;
  }

  Database* db = PyCapsule_GetPointer(obj, KEY_DATABASE);
  CHECK_NULL(db);
  
  int ec = 0;
  POLO_CALL(PLDB_rollback(db))

  Py_RETURN_NONE;
}

static PyObject* py_create_collection(PyObject* self, PyObject* args) {
  PyObject* obj = NULL;
  const char* name = NULL;
  if (!PyArg_ParseTuple(args, "Os", &obj, &name)) {
    return NULL;
  }

  Database* db = PyCapsule_GetPointer(obj, KEY_DATABASE);
  CHECK_NULL(db);

  int ec = 0;
  POLO_CALL(PLDB_create_collection(db, name));

  Py_RETURN_NONE;
}

static PyObject* py_insert(PyObject* self, PyObject* args) {
  PyObject* db_obj = NULL;
  const char* col_name = NULL;
  PyObject* val_doc = NULL;

  if (!PyArg_ParseTuple(args, "OsO", &db_obj, &col_name, &val_doc)) {
    return NULL;
  }

  Database* db = PyCapsule_GetPointer(db_obj, KEY_DATABASE);
  DbDocument* query = PyCapsule_GetPointer(val_doc, KEY_DOCUMENT);

  int ec = 0;
  POLO_CALL(PLDB_insert(db, col_name, query));

  Py_RETURN_NONE;
}

static PyObject* py_find(PyObject* self, PyObject* args) {
  PyObject* db_obj = NULL;
  const char* col_name = NULL;
  PyObject* query_obj = NULL;

  if (!PyArg_ParseTuple(args, "OsO", &db_obj, &col_name, &query_obj)) {
    return NULL;
  }

  Database* db = PyCapsule_GetPointer(db_obj, KEY_DATABASE);
  CHECK_NULL(db);

  DbDocument* query = NULL;
  if (query_obj != Py_None) {
    DbDocument* query = PyCapsule_GetPointer(query_obj, KEY_DOCUMENT);
    CHECK_NULL(query);
  }

  DbHandle* handle = NULL;

  int ec = 0;
  POLO_CALL(PLDB_find(db, col_name, query, &handle));

  PyObject* result = PyCapsule_New(handle, KEY_DB_HANDLE, db_handle_dctor);

  return result;
}

static PyObject* py_handle_step(PyObject* self, PyObject* args) {
  PyObject* obj = NULL;
  if (!PyArg_ParseTuple(args, "O", &obj)) {
    return NULL;
  }

  DbHandle* handle = PyCapsule_GetPointer(obj, KEY_DB_HANDLE);
  CHECK_NULL(handle);

  int ec = 0;
  POLO_CALL(PLDB_handle_step(handle));

  Py_RETURN_NONE;
}

static PyObject* py_handle_to_str(PyObject* self, PyObject* args) {
  PyObject* obj = NULL;
  if (!PyArg_ParseTuple(args, "O", &obj)) {
    return NULL;
  }

  DbHandle* handle = PyCapsule_GetPointer(obj, KEY_DB_HANDLE);
  CHECK_NULL(handle);

  char* buffer = malloc(4096);
  memset(buffer, 0, 4096);

  int ec = PLDB_handle_to_str(handle, buffer, 4096);
  if (ec < 0) {
    PyErr_SetString(PyExc_Exception, PLDB_error_msg());
    free(buffer);
    return NULL;
  }

  PyObject* result = PyUnicode_FromStringAndSize(buffer, ec);

  free(buffer);

  return result;
}

static PyObject* py_handle_get(PyObject* self, PyObject* args) {
  PyObject* obj = NULL;
  if (!PyArg_ParseTuple(args, "O", &obj)) {
    return NULL;
  }

  DbHandle* handle = PyCapsule_GetPointer(obj, KEY_DB_HANDLE);
  CHECK_NULL(handle);

  DbValue* val = NULL;

  PLDB_handle_get(handle, &val);

  PyObject* result = PyCapsule_New(val, KEY_VALUE, value_dctor);

  return result;
}

static PyObject* py_update(PyObject* self, PyObject* args) {
  PyObject* db_obj = NULL;
  const char* col_name = NULL;
  PyObject* query_obj = NULL;
  PyObject* update_obj = NULL;

  if (!PyArg_ParseTuple(args, "OsOO", &db_obj, &col_name, &query_obj, &update_obj)) {
    return NULL;
  }

  Database* db = PyCapsule_GetPointer(db_obj, KEY_DATABASE);
  CHECK_NULL(db);

  DbDocument* query = NULL;
  if (query_obj != Py_None) {
    DbDocument* query = PyCapsule_GetPointer(query_obj, KEY_DOCUMENT);
    CHECK_NULL(query);
  }

  DbDocument* update = PyCapsule_GetPointer(update_obj, KEY_DOCUMENT);
  CHECK_NULL(update);

  long long ec = 0;
  POLO_CALL(PLDB_update(db, col_name, query, update));

  return PyLong_FromLongLong(ec);
}

static PyObject* py_delete(PyObject* self, PyObject* args) {
  PyObject* db_obj = NULL;
  const char* name = NULL;
  PyObject* query_obj = NULL;

  if (!PyArg_ParseTuple(args, "OsO", &db_obj, &name, &query_obj)) {
    return NULL;
  }

  Database* db = PyCapsule_GetPointer(db_obj, KEY_DATABASE);
  CHECK_NULL(db);

  DbDocument* query = PyCapsule_GetPointer(query_obj, KEY_DOCUMENT);
  CHECK_NULL(query);

  long long ec = 0;
  POLO_CALL(PLDB_delete(db, name, query));

  return PyLong_FromLongLong(ec);
}

static PyObject* py_delete_all(PyObject* self, PyObject* args) {
  PyObject* db_obj = NULL;
  const char* name = NULL;

  if (!PyArg_ParseTuple(args, "Os", &db_obj, &name)) {
    return NULL;
  }

  Database* db = PyCapsule_GetPointer(db_obj, KEY_DATABASE);
  CHECK_NULL(db);

  long long ec = 0;
  POLO_CALL(PLDB_delete_all(db, name));

  return PyLong_FromLongLong(ec);
}

static PyObject* py_version(PyObject* self, PyObject* args) {
  static char buffer[1024];
  memset(buffer, 0, 1024);

  int ec = 0;
  POLO_CALL(PLDB_version(buffer, 1024));

  return PyUnicode_FromString(buffer);
}

static PyObject* py_mk_doc(PyObject* self, PyObject* args) {
  DbDocument* doc = PLDB_mk_doc();

  PyObject* obj = PyCapsule_New((void*)doc, KEY_DOCUMENT, doc_dctor);

  return obj;
}

static PyObject* py_doc_set(PyObject* self, PyObject* args) {
  PyObject* db_obj;
  const char* name;
  PyObject* val_obj;

  if (!PyArg_ParseTuple(args, "OsO", &db_obj, &name, &val_obj)) {
    return NULL;
  }

  DbDocument* doc = PyCapsule_GetPointer(db_obj, KEY_DOCUMENT);
  CHECK_NULL(doc);

  DbValue* val = PyCapsule_GetPointer(val_obj, KEY_VALUE);
  CHECK_NULL(val);

  PLDB_doc_set(doc, name, val);

  Py_RETURN_NONE;
}

static PyObject* py_doc_get(PyObject* self, PyObject* args) {
  PyObject* db_obj;
  const char* name;

  if (!PyArg_ParseTuple(args, "Os", &db_obj, &name)) {
    return NULL;
  }

  DbDocument* doc = PyCapsule_GetPointer(db_obj, KEY_DOCUMENT);
  CHECK_NULL(doc);

  DbValue* result = NULL;

  if (PLDB_doc_get(doc, name, &result)) {
    PyObject* result_obj = PyCapsule_New(result, KEY_VALUE, value_dctor);

    return result_obj;
  }

  Py_RETURN_NONE;
}

static PyObject* py_doc_len(PyObject* self, PyObject* args) {
  PyObject* db_obj;
  if (!PyArg_ParseTuple(args, "O", &db_obj)) {
    return NULL;
  }

  DbDocument* doc = PyCapsule_GetPointer(db_obj, KEY_DOCUMENT);
  CHECK_NULL(doc);

  int len = PLDB_doc_len(doc);

  return PyLong_FromLong(len);
}

static PyObject* py_doc_iter(PyObject* self, PyObject* args) {
  PyObject* db_obj;
  if (!PyArg_ParseTuple(args, "O", &db_obj)) {
    return NULL;
  }

  DbDocument* doc = PyCapsule_GetPointer(db_obj, KEY_DOCUMENT);
  CHECK_NULL(doc);

  DbDocumentIter* iter = PLDB_doc_iter(doc);

  PyObject* result_obj = PyCapsule_New(iter, KEY_DOC_ITER, doc_iter_dctor);

  return result_obj;
}

static PyObject* py_doc_iter_next(PyObject* self, PyObject* args) {
  PyObject* iter_obj;
  if (!PyArg_ParseTuple(args, "O", &iter_obj)) {
    return NULL;
  }

  DbDocumentIter* iter = PyCapsule_GetPointer(iter_obj, KEY_DOC_ITER);
  CHECK_NULL(iter);

  // TODO: dynamic check buffer
  char* buffer = malloc(512);
  memset(buffer, 0, 512);

  DbValue* val = NULL;

  int ec = 0;
  POLO_CALL(PLDB_doc_iter_next(iter, buffer, 512, &val));

  if (ec != 0) {
    PyObject* key = PyUnicode_FromString(buffer);
    PyObject* value_result = PyCapsule_New(val, KEY_VALUE, value_dctor);

    free(buffer);

    PyObject* result = PyTuple_New(2);
    if (PyTuple_SetItem(result, 0, key)) {
      return NULL;
    }
    if (PyTuple_SetItem(result, 1, value_result)) {
      return NULL;
    }
    return result;
  }

  free(buffer);
  Py_RETURN_NONE;
}

static PyObject* py_doc_to_value(PyObject* self, PyObject* args) {
  PyObject* py_doc;
  if (!PyArg_ParseTuple(args, "O", &py_doc)) {
    return NULL;
  }

  DbDocument* doc = PyCapsule_GetPointer(py_doc, KEY_DOCUMENT);

  DbValue* val = PLDB_doc_into_value(doc);

  PyObject* result = PyCapsule_New(val, KEY_VALUE, value_dctor);

  return result;
}

static PyObject* py_mk_object_id(PyObject* self, PyObject* args) {
  PyObject* py_db;
  if (!PyArg_ParseTuple(args, "O", &py_db)) {
    return NULL;
  }

  Database* db = PyCapsule_GetPointer(py_db, KEY_DATABASE);
  DbObjectId* oid = PLDB_mk_object_id(db);

  PyObject* result = PyCapsule_New(oid, KEY_OBJECT_ID, object_id_dctor);

  return result;
}

static PyObject* py_object_id_to_hex(PyObject* self, PyObject* args) {
  PyObject* py_oid;
  if (!PyArg_ParseTuple(args, "O", &py_oid)) {
    return NULL;
  }

  DbObjectId* oid = PyCapsule_GetPointer(py_oid, KEY_OBJECT_ID);

  static char buffer[64];
  memset(buffer, 0, 64);

  int ec = 0;
  POLO_CALL(PLDB_object_id_to_hex(oid, buffer, 64));

  return PyUnicode_FromStringAndSize(buffer, ec);
}

static PyObject* py_mk_int(PyObject *self, PyObject *args) {
  long long int_value;
  if (!PyArg_ParseTuple(args, "L", &int_value)) {
      return NULL;
  }

  DbValue* val = PLDB_mk_int(int_value);

  PyObject* result = PyCapsule_New(val, KEY_VALUE, value_dctor);

  return result;
}

static PyObject* py_mk_double(PyObject *self, PyObject *args) {
  double db_value;
  if (!PyArg_ParseTuple(args, "d", &db_value)) {
    return NULL;
  }

  DbValue* val = PLDB_mk_double(db_value);

  PyObject* result = PyCapsule_New(val, KEY_VALUE, value_dctor);

  return result;
}

static PyObject* py_mk_bool(PyObject* self, PyObject* args) {
  int bl_value;
  if (!PyArg_ParseTuple(args, "p", &bl_value)) {
    return NULL;
  }

  DbValue* val = PLDB_mk_bool(bl_value);

  PyObject* result = PyCapsule_New(val, KEY_VALUE, value_dctor);

  return result;
}

static PyObject* py_mk_null(PyObject* self, PyObject* args) {
  DbValue* val = PLDB_mk_null();

  PyObject* obj = PyCapsule_New((void*)val, KEY_VALUE, value_dctor);

  return obj;
}

static PyObject* py_mk_str(PyObject* self, PyObject* args) {
  const char* content;
  if (!PyArg_ParseTuple(args, "s", &content)) {
    return NULL;
  }

  DbValue* val = PLDB_mk_str(content);

  PyObject* result = PyCapsule_New(val, KEY_VALUE, value_dctor);

  return result;
}

static PyObject* py_mk_arr(PyObject* self, PyObject* args) {
  DbArray* arr = PLDB_mk_arr();

  PyObject* result = PyCapsule_New(arr, KEY_ARRAY, array_dctor);

  return result;
}

static PyObject* py_arr_len(PyObject* self, PyObject* args) {
  PyObject* arr_obj;
  if (!PyArg_ParseTuple(args, "O", &arr_obj)) {
    return NULL;
  }

  DbArray* arr = PyCapsule_GetPointer(arr_obj, KEY_ARRAY);
  CHECK_NULL(arr);

  unsigned int len = PLDB_arr_len(arr);

  return PyLong_FromUnsignedLong(len);
}

static PyObject* py_arr_push(PyObject* self, PyObject* args) {
  PyObject* arr_obj;
  PyObject* val_obj;
  if (!PyArg_ParseTuple(args, "OO", &arr_obj, &val_obj)) {
    return NULL;
  }

  DbArray* arr = PyCapsule_GetPointer(arr_obj, KEY_ARRAY);
  CHECK_NULL(arr);

  DbValue* val = PyCapsule_GetPointer(val_obj, KEY_VALUE);
  CHECK_NULL(val);

  PLDB_arr_push(arr, val);

  Py_RETURN_NONE;
}

static PyObject* py_arr_get(PyObject* self, PyObject* args) {
  PyObject* arr_obj;
  unsigned int index;
  if (!PyArg_ParseTuple(args, "OI", &arr_obj, &index)) {
    return NULL;
  }

  DbArray* arr = PyCapsule_GetPointer(arr_obj, KEY_ARRAY);
  CHECK_NULL(arr);

  DbValue* val = NULL;

  int ec = 0;
  POLO_CALL(PLDB_arr_get(arr, index, &val))

  PyObject* result = PyCapsule_New(val, KEY_VALUE, value_dctor);

  return result;
}

static PyObject* py_arr_to_value(PyObject* self, PyObject* args) {
  PyObject* arr_obj;
  if (!PyArg_ParseTuple(args, "O", &arr_obj)) {
    return NULL;
  }

  DbArray* arr = PyCapsule_GetPointer(arr_obj, KEY_ARRAY);
  CHECK_NULL(arr);

  DbValue* val = PLDB_arr_into_value(arr);

  PyObject* result = PyCapsule_New(val, KEY_VALUE, value_dctor);

  return result;
}

static PyObject* py_value_get_type(PyObject* self, PyObject* args) {
  PyObject* obj;
  if (!PyArg_ParseTuple(args, "O", &obj)) {
    return NULL;
  }
  
  DbValue* val = PyCapsule_GetPointer(obj, KEY_VALUE);
  CHECK_NULL(val);

  int ty = PLDB_value_type(val);

  return PyLong_FromLong(ty);
}

static PyObject* py_value_get_i64(PyObject* self, PyObject* args) {
  PyObject* obj;
  if (!PyArg_ParseTuple(args, "O", &obj)) {
    return NULL;
  }
  
  DbValue* val = PyCapsule_GetPointer(obj, KEY_VALUE);
  CHECK_NULL(val);

  long long result = 0;
  int ec = 0;
  POLO_CALL(PLDB_value_get_i64(val, &result));

  return PyLong_FromLongLong(result);
}

static PyObject* py_value_get_string(PyObject* self, PyObject* args) {
  PyObject* obj;
  if (!PyArg_ParseTuple(args, "O", &obj)) {
    return NULL;
  }

  DbValue* val = PyCapsule_GetPointer(obj, KEY_VALUE);
  CHECK_NULL(val);

  const char* content = NULL;
  int ec = 0;
  POLO_CALL(PLDB_value_get_string_utf8(val, &content));

  return PyUnicode_FromStringAndSize(content, ec);
}

static PyObject* py_value_get_bool(PyObject* self, PyObject* args) {
  PyObject* obj;
  if (!PyArg_ParseTuple(args, "O", &obj)) {
    return NULL;
  }

  DbValue* val = PyCapsule_GetPointer(obj, KEY_VALUE);
  CHECK_NULL(val);

  int ec = 0;
  POLO_CALL(PLDB_value_get_bool(val));

  return PyBool_FromLong(ec);
}

static PyObject* py_value_get_double(PyObject* self, PyObject* args) {
  PyObject* obj;
  if (!PyArg_ParseTuple(args, "O", &obj)) {
    return NULL;
  }

  DbValue* val = PyCapsule_GetPointer(obj, KEY_VALUE);
  CHECK_NULL(val);

  double out = 0;
  int ec = 0;
  POLO_CALL(PLDB_value_get_double(val, &out));

  return PyFloat_FromDouble(out);
}

static PyObject* py_value_get_arr(PyObject* self, PyObject* args) {
  PyObject* obj;
  if (!PyArg_ParseTuple(args, "O", &obj)) {
    return NULL;
  }

  DbValue* val = PyCapsule_GetPointer(obj, KEY_VALUE);
  CHECK_NULL(val);

  DbArray* arr = NULL;
  int ec = 0;
  POLO_CALL(PLDB_value_get_array(val, &arr))

  PyObject* arr_obj = PyCapsule_New((void*)arr, KEY_ARRAY, array_dctor);

  return arr_obj;
}

static PyObject* py_value_get_doc(PyObject* self, PyObject* args) {
  PyObject* obj;
  if (!PyArg_ParseTuple(args, "O", &obj)) {
    return NULL;
  }

  DbValue* val = PyCapsule_GetPointer(obj, KEY_VALUE);
  CHECK_NULL(val);

  DbDocument* doc = NULL;
  int ec = 0;
  POLO_CALL(PLDB_value_get_document(val, &doc))

  PyObject* doc_obj = PyCapsule_New((void*)doc, KEY_DOCUMENT, doc_dctor);

  return doc_obj;
}

// Method definition object for this extension, these argumens mean:
// ml_name: The name of the method
// ml_meth: Function pointer to the method implementation
// ml_flags: Flags indicating special features of this method, such as
//          accepting arguments, accepting keyword arguments, being a
//          class method, or being a static method of a class.
// ml_doc:  Contents of this method's docstring
static PyMethodDef polodb_methods[] = {
  {
    "open", py_open_database, METH_VARARGS,
    "open a database"
  },
  {
    "close", py_close_database, METH_VARARGS,
    "close a database"
  },
  {
    "start_transaction", py_start_transaction, METH_VARARGS,
    "start a transaction"
  },
  {
    "commit", py_commit, METH_VARARGS,
    "commit a transaction"
  },
  {
    "rollback", py_rollback, METH_VARARGS,
    "rollback a transaction"
  },
  {
    "create_collection", py_create_collection, METH_VARARGS,
    "create a collection"
  },
  {
    "insert", py_insert, METH_VARARGS,
    "insert a document"
  },
  {
    "find", py_find, METH_VARARGS,
    "find documents"
  },
  {
    "handle_step", py_handle_step, METH_VARARGS,
    "step the handle"
  },
  {
    "handle_to_str", py_handle_to_str, METH_VARARGS,
    "handle to string"
  },
  {
    "handle_get_value", py_handle_get, METH_VARARGS,
    "handle get value"
  },
  {
    "update", py_update, METH_VARARGS,
    "update documents"
  },
  {
    "delete", py_delete, METH_VARARGS,
    "delete documents"
  },
  {
    "delete_all", py_delete_all, METH_VARARGS,
    "delete all items in a collection"
  },
  {
    "mk_doc", py_mk_doc, METH_NOARGS,
    "make a document"
  },
  {
    "doc_set", py_doc_set, METH_VARARGS,
    "set document"
  },
  {
    "doc_get", py_doc_get, METH_VARARGS,
    "get document"
  },
  {
    "doc_len", py_doc_len, METH_VARARGS,
    "length of doc"
  },
  {
    "doc_iter", py_doc_iter, METH_VARARGS,
    "get iterator of a doc"
  },
  {
    "doc_iter_next", py_doc_iter_next, METH_VARARGS,
    "next iteration of iterator"
  },
  {
    "doc_to_value", py_doc_to_value, METH_VARARGS,
    "convert document to value"
  },
  {
    "mk_null", py_mk_null, METH_NOARGS,
    "make a null value"
  },
  {
    "mk_str", py_mk_str, METH_NOARGS,
    "make a string value"
  },
  {
    "mk_int", py_mk_int, METH_VARARGS,
    "make a int value"
  },
  {
    "mk_double", py_mk_double, METH_VARARGS,
    "make a double value"
  },
  {
    "mk_bool", py_mk_bool, METH_VARARGS,
    "make a bool value"
  },
  {
    "mk_arr", py_mk_arr, METH_VARARGS,
    "make an array"
  },
  {
    "mk_object_id", py_mk_object_id, METH_VARARGS,
    "make an ObjectId"
  },
  {
    "object_id_to_hex", py_object_id_to_hex, METH_VARARGS,
    "return hex of an ObjectId"
  },
  {
    "arr_to_value", py_arr_to_value, METH_VARARGS,
    "convert array to value"
  },
  {
    "arr_len", py_arr_len, METH_VARARGS,
    "return length of an array"
  },
  {
    "arr_push", py_arr_push, METH_VARARGS,
    "push an item to array"
  },
  {
    "arr_get", py_arr_get, METH_VARARGS,
    "get item by index from array"
  },
  {
    "value_get_type", py_value_get_type, METH_VARARGS,
    "return type of a value"
  },
  {
    "value_get_i64", py_value_get_i64, METH_VARARGS,
    "get i64 from value"
  },
  {
    "value_get_string", py_value_get_string, METH_VARARGS,
    "get string from value"
  },
  {
    "value_get_double", py_value_get_double, METH_VARARGS,
    "get double from value"
  },
  {
    "value_get_bool", py_value_get_bool, METH_VARARGS,
    "get boolean from value"
  },
  {
    "value_get_array", py_value_get_arr, METH_VARARGS,
    "get array from value"
  },
  {
    "value_get_document", py_value_get_doc, METH_VARARGS,
    "get document from value"
  },
  {
    "version", py_version, METH_NOARGS,
    "version of db"
  },
  {NULL, NULL, 0, NULL}
};

// Module definition
// The arguments of this structure tell Python what to call your extension,
// what it's methods are and where to look for it's method definitions
static struct PyModuleDef hello_definition = { 
    PyModuleDef_HEAD_INIT,
    "polodb",
    "PoloDB is a embedded JSON-based database",
    -1, 
    polodb_methods
};

PyMODINIT_FUNC
PyInit_polodb(void)
{
  PyObject *m;
  if (PyType_Ready(&DatabaseObjectType)) {
    return NULL;
  }

  m = PyModule_Create(&hello_definition);
  if (m == NULL) {
    return NULL;
  }

  Py_INCREF(&DatabaseObjectType);
  if (PyModule_AddObject(m, "Database", (PyObject*)&DatabaseObjectType) < 0) {
    Py_DECREF(&DatabaseObjectType);
    Py_DECREF(m);
    return NULL;
  }

  return m;
}
