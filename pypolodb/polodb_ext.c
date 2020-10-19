#include <Python.h>
#include <stdlib.h>
#include <string.h>
#include "./polodb.h"

#define KEY_VALUE    "Value"
#define KEY_DATABASE "Database"
#define KEY_DOCUMENT "DbDocument"
#define KEY_DOC_ITER "DbDocumentIter"
#define KEY_DB_HANDLE "DbHandle"
#define KEY_ARRAY "Array"
#define KEY_OBJECT_ID "ObjectId"

#define DB_HANDLE_STATE_HAS_ROW 2

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

static PyTypeObject ValueObjectType;
static PyTypeObject DocumentObjectType;

typedef struct {
  PyObject_HEAD
  DbDocument* doc;
} DocumentObject;

typedef struct {
  PyObject_HEAD
  DbHandle* handle;
} DbHandleObject;

static PyObject* DbHandleObject_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
  DbHandleObject* self;
  self = (DbHandleObject*)type->tp_alloc(type, 0);
  if (self != NULL) {
    self->handle = NULL;
  }
  return (PyObject*)self;
}

static int DbHandleObject_init(DbHandleObject* self, PyObject *args, PyObject* kwds) {
  PyObject* cap_obj;
  if (!PyArg_ParseTuple(args, "O", &cap_obj)) {
    return -1;
  }

  DbHandle* db_handle = PyCapsule_GetPointer(cap_obj, KEY_DB_HANDLE);
  if (db_handle == NULL) {
    return -1;
  }

  self->handle = db_handle;

  return 0;
}

static void DbHandleObject_dealloc(DbHandleObject* self) {
  if (self->handle != NULL) {
    PLDB_free_handle(self->handle);
    self->handle = NULL;
  }
  Py_TYPE(self)->tp_free(self);
}

static PyObject* DbHandleObject_get(DbHandleObject* self, PyObject* Py_UNUSED(ignored)) {
  DbValue* value;
  PLDB_handle_get(self->handle, &value);

  PyObject* cap = PyCapsule_New(value, KEY_VALUE, NULL);
  
  PyObject* argList = PyTuple_New(1);
  PyTuple_SetItem(argList, 0, cap);

  PyObject* result = PyObject_CallObject((PyObject*)&ValueObjectType, argList);

  Py_DECREF(argList);
  Py_DECREF(cap);

  return result;
}

static PyObject* DbHandleObject_step(DbHandleObject* self, PyObject* Py_UNUSED(ignored)) {
  if (PLDB_handle_step(self->handle) < 0) {
    PyErr_SetString(PyExc_Exception, PLDB_error_msg());
    return NULL;
  }

  Py_RETURN_NONE;
}

static PyObject* DbHandleObject_str(DbHandleObject* self, PyObject* Py_UNUSED(ignored)) {
  char* buffer = malloc(4096);
  memset(buffer, 0, 4096);

  int ec = PLDB_handle_to_str(self->handle, buffer, 4096);
  if (ec < 0) {
    free(buffer);
    PyErr_SetString(PyExc_Exception, PLDB_error_msg());
    return NULL;
  }

  PyObject* result = PyUnicode_FromStringAndSize(buffer, ec);

  free(buffer);

  return result;
}

static PyMethodDef DbHandleObject_methods[] = {
  {"get", (PyCFunction)DbHandleObject_get, METH_NOARGS,
   "get value of handle"
  },
  {"step", (PyCFunction)DbHandleObject_step, METH_NOARGS,
   "step the handle"
  },
  {"str", (PyCFunction)DbHandleObject_str, METH_NOARGS,
   "print the handle"
  },
  {NULL}  /* Sentinel */
};

static PyTypeObject DbHandleObjectType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "polodb.DbHandle",
    .tp_doc = "DbHandle object",
    .tp_basicsize = sizeof(DbHandleObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_new = DbHandleObject_new,
    .tp_init = (initproc) DbHandleObject_init,
    .tp_dealloc = (destructor) DbHandleObject_dealloc,
    .tp_methods = DbHandleObject_methods,
};

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

static PyObject* DatabaseObject_start_transaction(DatabaseObject* self, PyObject* args) {
  int flags = 0;
  if (!PyArg_ParseTuple(args, "i", &flags)) {
    return NULL;
  }

  int ec = PLDB_start_transaction(self->db, flags);
  if (ec < 0) {
    PyErr_SetString(PyExc_Exception, PLDB_error_msg());
    return NULL;
  }

  Py_RETURN_NONE;
}

static PyObject* DatabaseObject_commit(DatabaseObject* self, PyObject* Py_UNUSED(ignored)) {
  int ec = PLDB_commit(self->db);
  if (ec < 0) {
    PyErr_SetString(PyExc_Exception, PLDB_error_msg());
    return NULL;
  }

  Py_RETURN_NONE;
}

static PyObject* DatabaseObject_rollback(DatabaseObject* self, PyObject* Py_UNUSED(ignored)) {
  int ec = PLDB_rollback(self->db);
  if (ec < 0) {
    PyErr_SetString(PyExc_Exception, PLDB_error_msg());
    return NULL;
  }

  Py_RETURN_NONE;
}

static PyObject* DatabaseObject_create_collection(DatabaseObject* self, PyObject* args) {
  if (self->db == NULL) {
    PyErr_SetString(PyExc_Exception, "database is not opened");
    return NULL;
  }

  const char* content;
  if (!PyArg_ParseTuple(args, "s", &content)) {
    return NULL;
  }

  int ec = 0;
  POLO_CALL(PLDB_create_collection(self->db, content));

  Py_RETURN_NONE;
}

static PyObject* DatabaseObject_insert(DatabaseObject* self, PyObject* args) {
  if (self->db == NULL) {
    PyErr_SetString(PyExc_Exception, "database is not opened");
    return NULL;
  }

  const char* col_name;
  PyObject* obj;
  if (!PyArg_ParseTuple(args, "sO", &col_name, &obj)) {
    return NULL;
  }

  if (Py_TYPE(obj) != &DocumentObjectType) {
    PyErr_SetString(PyExc_ValueError, "the second argument should be a document");
    return NULL;
  }

  int ec = 0;
  POLO_CALL(PLDB_insert(self->db, col_name, ((DocumentObject*)obj)->doc));

  Py_RETURN_NONE;
}

static PyObject* DatabaseObject_find(DatabaseObject* self, PyObject* args) {
  if (self->db == NULL) {
    PyErr_SetString(PyExc_Exception, "database is not opened");
    return NULL;
  }

  const char* col_name;
  PyObject* doc_obj;
  if (!PyArg_ParseTuple(args, "sO", &col_name, &doc_obj)) {
    return NULL;
  }
  
  if (Py_TYPE(doc_obj) != &DocumentObjectType) {
    PyErr_SetString(PyExc_ValueError, "the second argument should be a document");
    return NULL;
  }

  DbDocument* doc = ((DocumentObject*)doc_obj)->doc;

  DbHandle* handle = NULL;
  int ec = 0;

  POLO_CALL(PLDB_find(self->db, col_name, doc, &handle))

  PyObject* result = PyList_New(0);

  ec = PLDB_handle_step(handle);
  if (ec < 0) {
    PyErr_SetString(PyExc_Exception, PLDB_error_msg());
    goto handle_err;
  }

  while (PLDB_handle_state(handle) == DB_HANDLE_STATE_HAS_ROW) {
    DbValue* val = NULL;

    PLDB_handle_get(handle, &val);

    PLDB_free_value(val);

    ec = PLDB_handle_step(handle);
    if (ec < 0) {
      PyErr_SetString(PyExc_Exception, PLDB_error_msg());
      goto handle_err;
    }
  }

  goto handle_success;
handle_err:
  PLDB_free_handle(handle);
  Py_DECREF(result);
  return NULL;

handle_success:
  return result;
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
  {"start_transaction", (PyCFunction)DatabaseObject_start_transaction, METH_VARARGS,
   "start a transaction"
  },
  {"commit", (PyCFunction)DatabaseObject_commit, METH_NOARGS,
   "commit"
  },
  {
    "rollback", (PyCFunction)DatabaseObject_rollback, METH_NOARGS,
    "rollback"
  },
  {"create_collection", (PyCFunction)DatabaseObject_create_collection, METH_VARARGS,
   "create a collection"
  },
  {
    "insert", (PyCFunction)DatabaseObject_insert, METH_VARARGS,
    "insert a document"
  },
  {
    "find", (PyCFunction)DatabaseObject_find, METH_VARARGS,
    "find documents"
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

static PyObject* DocumentObject_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
  DocumentObject* self;
  self = (DocumentObject*) type->tp_alloc(type, 0);
  if (self != NULL) {
    self->doc = PLDB_mk_doc();
  }
  return (PyObject*)self;
}

static void DocumentObject_dealloc(DocumentObject* self) {
  PLDB_free_doc(self->doc);
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static int DocumentObject_init(DatabaseObject* self, PyObject *args, PyObject* kwds) {
  return 0;
}

static PyMethodDef DocumentObject_methods[] = {
  {NULL}  /* Sentinel */
};

static PyTypeObject DocumentObjectType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "polodb.Document",
    .tp_doc = "Document object",
    .tp_basicsize = sizeof(DocumentObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_new = DocumentObject_new,
    .tp_init = (initproc) DocumentObject_init,
    .tp_dealloc = (destructor) DocumentObject_dealloc,
    .tp_methods = DocumentObject_methods,
};

typedef struct {
  PyObject_HEAD
  DbObjectId* oid;
} ObjectIdObject;

static PyObject* ObjectIdObject_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
  ObjectIdObject* self;
  self = (ObjectIdObject*) type->tp_alloc(type, 0);
  if (self != NULL) {
    self->oid = NULL;
  }
  return (PyObject*)self;
}

static int ObjectIdObject_init(PyTypeObject* self, PyObject *args, PyObject* kwds) {
  return 0;
}

static void ObjectIdObject_dealloc(ObjectIdObject* self) {
  if (self->oid != NULL) {
    PLDB_free_object_id(self->oid);
    self->oid = NULL;
  }
  Py_TYPE(self)->tp_free(self);
}

static PyObject* ObjectIdObject_to_hex(ObjectIdObject* self, PyObject* Py_UNUSED(ignored)) {
  static char buffer[64];
  memset(buffer, 0, 64);

  int ec = 0;
  POLO_CALL(PLDB_object_id_to_hex(self->oid, buffer, 64));

  return PyUnicode_FromStringAndSize(buffer, ec);
}

static PyMethodDef ObjectIdObject_methods[] = {
  {"to_hex", (PyCFunction)ObjectIdObject_to_hex, METH_NOARGS,
   "return hex of ObjectId"
  },
  {NULL}  /* Sentinel */
};

static PyTypeObject ObjectIdObjectType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "polodb.ObjectId",
    .tp_doc = "ObjectId",
    .tp_basicsize = sizeof(ObjectIdObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_new = ObjectIdObject_new,
    .tp_init = (initproc) ObjectIdObject_init,
    .tp_dealloc = (destructor) ObjectIdObject_dealloc,
    .tp_methods = ObjectIdObject_methods,
};

typedef struct {
  PyObject_HEAD
  DbValue* value;
} ValueObject;

static PyObject* ValueObject_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
  ValueObject* self;
  self = (ValueObject*) type->tp_alloc(type, 0);
  if (self != NULL) {
    self->value = NULL;
  }
  return (PyObject*)self;
}

static int ValueObject_init(ValueObject* self, PyObject *args, PyObject* kwds) {
  PyObject* obj;
  if (!PyArg_ParseTuple(args, "O", &obj)) {
    return -1;
  }

  if (obj == Py_None) {
    self->value = PLDB_mk_null();
  } else if (PyLong_CheckExact(obj)) {
    long long int_value = PyLong_AsLongLong(obj);
    self->value = PLDB_mk_int(int_value);
    return 0;
  } else if (PyBool_Check(obj)) {
    int value = 0;
    if (obj == Py_True) {
      value = 1;
    }
    self->value = PLDB_mk_bool(value);
    return 0;
  } else if (PyFloat_CheckExact(obj)) {
    double float_value = PyFloat_AsDouble(obj);
    self->value = PLDB_mk_double(float_value);
    return 0;
  } else if (PyUnicode_CheckExact(obj)) {
    const char* content = PyUnicode_AsUTF8(obj);
    if (content == NULL) {
      return -1;
    }
    self->value = PLDB_mk_str(content);
    return 0;
  } else if (PyCapsule_CheckExact(obj)) {
    DbValue* value = PyCapsule_GetPointer(obj, KEY_VALUE);
    self->value = value;
    return 0;
  } else if (Py_TYPE(obj) == &DocumentObjectType) {
    DocumentObject* doc = (DocumentObject*)obj;

    self->value = PLDB_doc_to_value(doc->doc);

    return 0;
  } else if (Py_TYPE(obj) == &ObjectIdObjectType) {
    ObjectIdObject* oid_obj = (ObjectIdObject*)obj;

    self->value = PLDB_object_id_to_value(oid_obj->oid);

    return 0;
  }

  PyErr_SetString(PyExc_RuntimeError, "unkown value type");
  return -1;
}

static void ValueObject_dealloc(ValueObject* self) {
  if (self->value != NULL) {
    PLDB_free_value(self->value);
    self->value = NULL;
  }
  Py_TYPE(self)->tp_free(self);
}

static PyObject* ValueObject_type(ValueObject* val, PyObject* Py_UNUSED(ignored)) {
  int ty = PLDB_value_type(val->value);

  return PyLong_FromLong(ty);
}

static PyObject* ValueObject_get_i64(ValueObject* val, PyObject* Py_UNUSED(ignored)) {
  long long result = 0;
  int ec = 0;
  POLO_CALL(PLDB_value_get_i64(val->value, &result));

  return PyLong_FromLongLong(result);
}

static PyObject* ValueObject_get_double(ValueObject* val, PyObject* Py_UNUSED(ignored)) {
  double out = 0;
  int ec = 0;
  POLO_CALL(PLDB_value_get_double(val->value, &out));

  return PyFloat_FromDouble(out);
}

static PyObject* ValueObject_get_string(ValueObject* val, PyObject* Py_UNUSED(ignored)) {
  const char* content = NULL;
  int ec = 0;
  POLO_CALL(PLDB_value_get_string_utf8(val->value, &content));

  return PyUnicode_FromStringAndSize(content, ec);
}

static PyMethodDef ValueObject_methods[] = {
  {"type", (PyCFunction)ValueObject_type, METH_NOARGS,
   "return type from Value"
  },
  {"get_i64", (PyCFunction)ValueObject_get_i64, METH_NOARGS,
   "return i64 from Value"
  },
  {"get_double", (PyCFunction)ValueObject_get_double, METH_NOARGS,
   "return double from Value"
  },
  {"get_string", (PyCFunction)ValueObject_get_string, METH_NOARGS,
   "return string from Value"
  },
  {NULL}  /* Sentinel */
};

static PyTypeObject ValueObjectType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "polodb.Value",
    .tp_doc = "Value object",
    .tp_basicsize = sizeof(ValueObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_new = ValueObject_new,
    .tp_init = (initproc) ValueObject_init,
    .tp_dealloc = (destructor) ValueObject_dealloc,
    .tp_methods = ValueObject_methods,
};

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
    "doc_len", py_doc_len, METH_VARARGS,
    "length of doc"
  },
  {
    "doc_iter", py_doc_iter, METH_VARARGS,
    "get iterator of a doc"
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
    "arr_len", py_arr_len, METH_VARARGS,
    "return length of an array"
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

#define REGISTER_OBJECT(VAR, NAME) \
  Py_INCREF(&VAR); \
  if (PyModule_AddObject(m, NAME, (PyObject*)&VAR) < 0) { \
    Py_DECREF(&VAR); \
    Py_DECREF(m); \
    return NULL; \
  }

PyMODINIT_FUNC
PyInit_polodb(void)
{
  PyObject *m;
  if (PyType_Ready(&DbHandleObjectType) < 0) {
    return NULL;
  }

  if (PyType_Ready(&DatabaseObjectType) < 0) {
    return NULL;
  }

  if (PyType_Ready(&ValueObjectType) < 0) {
    return NULL;
  }

  if (PyType_Ready(&DocumentObjectType) < 0) {
    return NULL;
  }

  if (PyType_Ready(&ObjectIdObjectType) < 0) {
    return NULL;
  }

  m = PyModule_Create(&hello_definition);
  if (m == NULL) {
    return NULL;
  }

  REGISTER_OBJECT(DatabaseObjectType, "Database");
  REGISTER_OBJECT(ValueObjectType, "Value");
  REGISTER_OBJECT(DocumentObjectType, "Document");
  REGISTER_OBJECT(ObjectIdObjectType, "ObjectId");

  return m;
}
