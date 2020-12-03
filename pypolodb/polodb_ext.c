#include <Python.h>
#include <datetime.h>
#include <stdlib.h>
#include <string.h>
#include "include/polodb.h"

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

static PyTypeObject DocumentObjectType;
static PyTypeObject CollectionObjectType;
static DbDocument* PyDictToDbDocument(PyObject* dict);
static DbArray* PyListToDbArray(PyObject* arr);

typedef struct {
  PyObject_HEAD
  DbDocument* doc;
} DocumentObject;

static DbDocument* PyDictToDbDocument(PyObject* dict);
static PyObject* DbValueToPyObject(DbValue*);

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

#define CHECK_DB_OPEND(SELF) \
  if ((SELF)->db == NULL) { \
    PyErr_SetString(PyExc_Exception, "database is not opened"); \
    return NULL; \
  }

static PyObject* DatabaseObject_create_collection(DatabaseObject* self, PyObject* args) {
  CHECK_DB_OPEND(self);

  PyObject* name;
  if (!PyArg_ParseTuple(args, "O", &name)) {
    return NULL;
  }

  const char* content = PyUnicode_AsUTF8(name);

  int ec = 0;
  uint32_t col_id = 0;
  uint32_t meta_version = 0;
  POLO_CALL(PLDB_create_collection(self->db, content, &col_id, &meta_version));

  Py_INCREF(self);
  Py_INCREF(name);

  PyObject* argList = PyTuple_New(4);
  PyTuple_SetItem(argList, 0, (PyObject*)self);
  PyTuple_SetItem(argList, 1, name);
  PyTuple_SetItem(argList, 2, PyLong_FromUnsignedLong(col_id));
  PyTuple_SetItem(argList, 3, PyLong_FromUnsignedLong(meta_version));

  PyObject* result = PyObject_CallObject((PyObject*)&CollectionObjectType, argList);

  Py_DECREF(argList);

  return result;
}

static PyObject* DatabaseObject_collection(DatabaseObject* self, PyObject* args) {
  PyObject* name;
  if (PyArg_ParseTuple(args, "O", &name)) {
    return NULL;
  }

  if (Py_TYPE(name) != &PyUnicode_Type) {
    PyErr_SetString(PyExc_TypeError, "this first argument should be a string");
    return NULL;
  }

  uint32_t col_id = 0;
  uint32_t meta_version = 0;
  const char* name_utf8 = PyUnicode_AsUTF8(name);
  int ec = PLDB_get_collection_meta_by_name(self->db, name_utf8, &col_id, &meta_version);
  if (ec < 0) {
    PyErr_SetString(PyExc_Exception, PLDB_error_msg());
    return NULL;
  }

  Py_INCREF(self);
  Py_INCREF(name);
  
  PyObject* argList = PyTuple_New(4); 
  PyTuple_SetItem(argList, 0, (PyObject*)self);
  PyTuple_SetItem(argList, 1, name);
  PyTuple_SetItem(argList, 2, PyLong_FromUnsignedLong(col_id));
  PyTuple_SetItem(argList, 3, PyLong_FromUnsignedLong(meta_version));

  PyObject* result = PyObject_CallObject((PyObject*)&CollectionObjectType, argList);

  Py_DECREF(argList);

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
  {
    "close", (PyCFunction)DatabaseObject_close, METH_NOARGS,
    "close the database"
  },
  {
    "startTransaction", (PyCFunction)DatabaseObject_start_transaction, METH_VARARGS,
    "start a transaction"
  },
  {
    "commit", (PyCFunction)DatabaseObject_commit, METH_NOARGS,
    "commit"
  },
  {
    "rollback", (PyCFunction)DatabaseObject_rollback, METH_NOARGS,
    "rollback"
  },
  {
    "createCollection", (PyCFunction)DatabaseObject_create_collection, METH_VARARGS,
    "create a collection"
  },
  {
    "collection", (PyCFunction)DatabaseObject_collection, METH_VARARGS,
    "get a collection handle"
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

typedef struct {
  PyObject_HEAD
  DatabaseObject* db_obj;
  char* name;
  uint32_t id;
  uint32_t meta_version;
} CollectionObject;

static PyObject* CollectionObject_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
  CollectionObject* self;
  self = (CollectionObject*)type->tp_alloc(type, 0);
  if (self != NULL) {
    self->db_obj = NULL;
    self->name = NULL;
    self->id = 0;
    self->meta_version = 0;
  }
  return (PyObject*)self;
}

static int CollectionObject_init(CollectionObject* self, PyObject* args, PyObject* kwds) {
  PyObject* db_obj;
  const char* name;
  uint32_t col_id = 0;
  uint32_t meta_version = 0;
  if (!PyArg_ParseTuple(args, "Oskk", &db_obj, &name, &col_id, &meta_version)) {
    return -1;
  }

  if (Py_TYPE(db_obj) != &DatabaseObjectType) {
    PyErr_SetString(PyExc_TypeError, "this first argument should be a DatabaesObject");
    return -1;
  }

  Py_INCREF(db_obj);
  self->db_obj = (DatabaseObject*)db_obj;

  size_t name_len = strlen(name) + 1;
  char* buffer = malloc(name_len);
  memset(buffer, 0, name_len);

  self->name = buffer;
  memcpy(self->name, name, name_len - 1);

  self->id = col_id;
  self->meta_version = meta_version;

  return 0;
}

static void CollectionObject_dealloc(CollectionObject* self) {
  if (self->db_obj != NULL) {
    Py_DECREF(self->db_obj);
    self->db_obj = NULL;
  }
  if (self->name != NULL) {
    free(self->name);
    self->name = NULL;
  }
  Py_TYPE(self)->tp_free(self);
}

static PyObject* CollectionObject_insert(CollectionObject* self, PyObject* args) {
  CHECK_DB_OPEND(self->db_obj);

  PyObject* obj;
  if (!PyArg_ParseTuple(args, "O", &obj)) {
    return NULL;
  }

  if (Py_TYPE(obj) != &PyDict_Type) {
    PyErr_SetString(PyExc_Exception, "the second argument should be a dict");
    return NULL;
  }

  DbDocument* doc = PyDictToDbDocument(obj);
  if (doc == NULL) {
    return NULL;
  }

  int ec = PLDB_insert(self->db_obj->db, self->id, self->meta_version, doc);
  if (ec < 0) {
    PLDB_free_doc(doc);
    PyErr_SetString(PyExc_Exception, PLDB_error_msg());
    return NULL;
  }

  if (ec > 0) {
    DbValue* new_id = NULL;
    int ec2 = PLDB_doc_get(doc, "_id", &new_id);
    if (ec2 < 0) {
      PyErr_SetString(PyExc_Exception, PLDB_error_msg());
      PLDB_free_doc(doc);
      return NULL;
    }
    PyObject* py_id = DbValueToPyObject(new_id);
    if (PyDict_SetItemString(obj, "_id", py_id) < 0) {
      PyErr_SetString(PyExc_RuntimeError, "can not set '_id' for dict");
      PLDB_free_doc(doc);
      return NULL;
    }
  }

  PLDB_free_doc(doc);
  Py_RETURN_NONE;
}


static PyObject* CollectionObject_find(CollectionObject* self, PyObject* args) {
  CHECK_DB_OPEND(self->db_obj);

  PyObject* dict_obj;
  if (!PyArg_ParseTuple(args, "O", &dict_obj)) {
    return NULL;
  }

  DbDocument* doc;

  if (dict_obj == Py_None) {
    doc = NULL;
  } else if (Py_TYPE(dict_obj) == &PyDict_Type) {
    doc = PyDictToDbDocument(dict_obj);
  } else {
    PyErr_SetString(PyExc_ValueError, "the second argument should be a dict");
    return NULL;
  }

  DbHandle* handle = NULL;
  int ec = 0;

  PyObject* result = NULL;

  ec = PLDB_find(self->db_obj->db, self->id, self->meta_version, doc, &handle);
  if (ec < 0) {
    PyErr_SetString(PyExc_Exception, PLDB_error_msg());
    goto handle_err;
  }

  result = PyList_New(0);

  ec = PLDB_handle_step(handle);
  if (ec < 0) {
    PyErr_SetString(PyExc_Exception, PLDB_error_msg());
    goto handle_err;
  }

  while (PLDB_handle_state(handle) == DB_HANDLE_STATE_HAS_ROW) {
    DbValue* val = NULL;

    PLDB_handle_get(handle, &val);

    PyObject* tmp_obj = DbValueToPyObject(val);
    assert(tmp_obj != NULL);

    ec = PyList_Append(result, tmp_obj);
    if (ec < 0) {
      PLDB_free_value(val);
      Py_DECREF(tmp_obj);
      goto handle_err;
    }

    PLDB_free_value(val);
    Py_DECREF(tmp_obj);

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
  PLDB_free_doc(doc);
  return NULL;

handle_success:
  PLDB_free_doc(doc);
  return result;
}

static PyObject* CollectionObject_update(CollectionObject* self, PyObject* args) {
  CHECK_DB_OPEND(self->db_obj);

  PyObject* query_dict_obj;
  PyObject* update_dict_obj;
  if (!PyArg_ParseTuple(args, "OO", &query_dict_obj, &update_dict_obj)) {
    return NULL;
  }

  DbDocument* query = NULL;
  DbDocument* update = NULL;
  PyObject* result = NULL;

  if (query_dict_obj == Py_None) {
    query = NULL;
  } else if (Py_TYPE(query_dict_obj) == &PyDict_Type) {
    query = PyDictToDbDocument(query_dict_obj);
  } else {
    PyErr_SetString(PyExc_Exception, "the second argument should be a dict or None");
    goto result;
  }

  if (Py_TYPE(update_dict_obj) != &PyDict_Type) {
    PyErr_SetString(PyExc_Exception, "the third argument should be a dict");
    goto result;
  }

  update = PyDictToDbDocument(update_dict_obj);

  int64_t count = PLDB_update(self->db_obj->db, self->id, self->meta_version, query, update);
  if (count < 0) {
    PyErr_SetString(PyExc_Exception, PLDB_error_msg());
    goto result;
  }

  result = PyLong_FromLongLong(count);

result:
  if (query != NULL) {
    PLDB_free_doc(query);
    query = NULL;
  }
  if (update != NULL) {
    PLDB_free_doc(update);
    update = NULL;
  }
  return result;
}

static PyObject* CollectionObject_delete(CollectionObject* self, PyObject* args) {
  CHECK_DB_OPEND(self->db_obj);

  PyObject* query_obj;
  if (!PyArg_ParseTuple(args, "O", &query_obj)) {
    return NULL;
  }

  if (Py_TYPE(query_obj) != &PyDict_Type) {
    PyErr_SetString(PyExc_Exception, "the thid argument should be a dict");
    return NULL;
  }

  PyObject* result = NULL;
  DbDocument* doc = PyDictToDbDocument(query_obj);

  int64_t ec = PLDB_delete(self->db_obj->db, self->id, self->meta_version, doc);
  if (ec < 0) {
    PyErr_SetString(PyExc_Exception, PLDB_error_msg());
    goto result;
  }

  result = PyLong_FromLongLong(ec);

result:
  if (doc != NULL) {
    PLDB_free_doc(doc);
    doc = NULL;
  }
  return result;
}

static PyObject* CollectionObject_delete_all(CollectionObject* self, PyObject* args) {
  CHECK_DB_OPEND(self->db_obj);

  int64_t ec = PLDB_delete_all(self->db_obj->db, self->id, self->meta_version);
  if (ec < 0) {
    PyErr_SetString(PyExc_Exception, PLDB_error_msg());
    return NULL;
  }

  return PyLong_FromLongLong(ec);
}

static PyObject* CollectionObject_count(CollectionObject* self, PyObject* args) {
  CHECK_DB_OPEND(self->db_obj);

  int64_t ec = PLDB_count(self->db_obj->db, self->id, self->meta_version);

  if (ec < 0) {
    PyErr_SetString(PyExc_Exception, PLDB_error_msg());
    return NULL;
  }

  return PyLong_FromLongLong(ec);
}

static PyMethodDef CollectionObject_methods[] = {
  {
    "insert", (PyCFunction)CollectionObject_insert, METH_VARARGS,
    "insert a document"
  },
  {
    "update", (PyCFunction)CollectionObject_update, METH_VARARGS,
    "update documents"
  },
  {
    "delete", (PyCFunction)CollectionObject_delete, METH_VARARGS,
    "delete documents"
  },
  {
    "deleteAll", (PyCFunction)CollectionObject_delete_all, METH_NOARGS,
    "delete all documents from a collection",
  },
  {
    "find", (PyCFunction)CollectionObject_find, METH_VARARGS,
    "find documents"
  },
  {
    "count", (PyCFunction)CollectionObject_count, METH_NOARGS,
    "count all documents"
  },
  {NULL}  /* Sentinel */
};

static PyTypeObject CollectionObjectType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "polodb.Collection",
    .tp_doc = "Collection",
    .tp_basicsize = sizeof(CollectionObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_new = CollectionObject_new,
    .tp_init = (initproc) CollectionObject_init,
    .tp_dealloc = (destructor) CollectionObject_dealloc,
    .tp_methods = CollectionObject_methods,
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

static int ObjectIdObject_init(ObjectIdObject* self, PyObject *args, PyObject* kwds) {
  PyObject* arg;
  if (!PyArg_ParseTuple(args, "O", &arg)) {
    return -1;
  }

  if (Py_TYPE(arg) != &PyCapsule_Type) {
    PyErr_SetString(PyExc_Exception, "the first argument should be a capsulute");
    return -1;
  }

  DbObjectId* oid = PyCapsule_GetPointer(arg, KEY_OBJECT_ID);
  self->oid = oid;

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

static PyObject* py_version(PyObject* self, PyObject* args) {
  static char buffer[1024];
  memset(buffer, 0, 1024);

  int ec = 0;
  POLO_CALL(PLDB_version(buffer, 1024));

  return PyUnicode_FromString(buffer);
}

static int PyDictToDbDocument_SetProperty(DbDocument* doc, const char* key, PyObject* value) {
  if (value == Py_None) {
    return PLDB_doc_set_null(doc, key);
  } else if (PyLong_CheckExact(value)) {
    int64_t int_value = PyLong_AsLongLong(value);
    return PLDB_doc_set_int(doc, key, int_value);
  } else if (PyBool_Check(value)) {
    int bl_value = 0;
    if (value == Py_True) {
      bl_value = 1;
    }
    return PLDB_doc_set_bool(doc, key, bl_value);
  } else if (PyFloat_CheckExact(value)) {
    double float_value = PyFloat_AsDouble(value);
    return PLDB_doc_set_double(doc, key, float_value);
  } else if (PyUnicode_CheckExact(value)) {
    const char* content = PyUnicode_AsUTF8(value);
    return PLDB_doc_set_string(doc, key, content);
  } else if (PyCapsule_CheckExact(value)) {
    DbValue* db_value = PyCapsule_GetPointer(value, KEY_VALUE);
    return PLDB_doc_set(doc, key, db_value);
  } else if (Py_TYPE(value) == &PyDict_Type) {
    DbDocument* child_doc = PyDictToDbDocument(value);
    int ec = PLDB_doc_set_doc(doc, key, child_doc);
    PLDB_free_doc(child_doc);
    return ec;
  } else if (Py_TYPE(value) == &PyList_Type) {
    DbArray* child_arr = PyListToDbArray(value);
    int ec = PLDB_doc_set_arr(doc, key, child_arr);
    PLDB_free_arr(child_arr);
    return ec;
  } else if (Py_TYPE(value) == &ObjectIdObjectType) {
    ObjectIdObject* oid = (ObjectIdObject*)value;
    return PLDB_doc_set_object_id(doc, key, oid->oid);
  } else if (Py_TYPE(value) == &DocumentObjectType) {
    DocumentObject* child_doc = (DocumentObject*)value;
    return PLDB_doc_set_doc(doc, key, child_doc->doc);
  } else if (PyDateTime_CheckExact(value)) {
    PyObject* result = PyObject_CallMethod(value, "timestamp", "");
    if (result == NULL) {
      return 1;
    }
    if (!PyFloat_CheckExact(result)) {
      PyErr_SetString(PyExc_TypeError, "return of timestamp should be float");
      Py_DECREF(result);
      return 1;
    }
    double timestamp = PyFloat_AsDouble(result);
    return PLDB_doc_set_UTCDateTime(doc, key, (int64_t)timestamp);
  }
  return 0;
}

static DbDocument* PyDictToDbDocument(PyObject* dict) {
  DbDocument* result = PLDB_mk_doc();
  PyObject* list = PyDict_Items(dict);
  if (list == NULL) {
    return NULL;
  }

  Py_ssize_t list_len = PyList_Size(list);
  for (Py_ssize_t i = 0; i < list_len ; i++) {
    PyObject* item = PyList_GetItem(list, i);
    PyObject* item_key = PyTuple_GetItem(item, 0);
    PyObject* item_value = PyTuple_GetItem(item, 1);

    const char* key_content = PyUnicode_AsUTF8(item_key);

    int ec = PyDictToDbDocument_SetProperty(result, key_content, item_value);
    if (ec < 0) {  // polodb error
      Py_DECREF(list);
      PLDB_free_doc(result);
      PyErr_SetString(PyExc_Exception, PLDB_error_msg());
      return NULL;
    }

    if (ec > 0) {  // python error
      Py_DECREF(list);
      PLDB_free_doc(result);
      return NULL;
    }
  }

  Py_DECREF(list);
  return result;
}

static int PyListToDbArray_SetElement(DbArray* arr, unsigned int index, PyObject* value) {
  if (value == Py_None) {
    return PLDB_arr_set_null(arr, index);
  } else if (PyLong_CheckExact(value)) {
    int64_t int_value = PyLong_AsLongLong(value);
    return PLDB_arr_set_int(arr, index, int_value);
  } else if (PyBool_Check(value)) {
    int bl_value = 0;
    if (value == Py_True) {
      bl_value = 1;
    }
    return PLDB_arr_set_bool(arr, index, bl_value);
  } else if (PyFloat_CheckExact(value)) {
    double float_value = PyFloat_AsDouble(value);
    return PLDB_arr_set_double(arr, index, float_value);
  } else if (PyUnicode_CheckExact(value)) {
    const char* content = PyUnicode_AsUTF8(value);
    return PLDB_arr_set_string(arr, index, content);
  } else if (Py_TYPE(value) == &PyDict_Type) {
    DbDocument* child_doc = PyDictToDbDocument(value);
    int ec = PLDB_arr_set_doc(arr, index, child_doc);
    PLDB_free_doc(child_doc);
    return ec;
  } else if (Py_TYPE(value) == &PyList_Type) {
    DbArray* child_arr = PyListToDbArray(value);
    int ec = PLDB_arr_set_arr(arr, index, child_arr);
    PLDB_free_arr(child_arr);
    return ec;
  } else if (Py_TYPE(value) == &ObjectIdObjectType) {
    ObjectIdObject* oid = (ObjectIdObject*)value;
    return PLDB_arr_set_object_id(arr, index, oid->oid);
  } else if (Py_TYPE(value) == &DocumentObjectType) {
    DocumentObject* child_doc = (DocumentObject*)value;
    return PLDB_arr_set_doc(arr, index, child_doc->doc);
  } else if (PyDateTime_CheckExact(value)) {
    PyObject* result = PyObject_CallMethod(value, "timestamp", "");
    if (result == NULL) {
      return 1;
    }
    if (!PyFloat_CheckExact(result)) {
      PyErr_SetString(PyExc_TypeError, "return of timestamp should be float");
      Py_DECREF(result);
      return 1;
    }
    double timestamp = PyFloat_AsDouble(result);
    return PLDB_arr_set_UTCDateTime(arr, index, (int64_t)timestamp);
  }
  return 0;
}

static DbArray* PyListToDbArray(PyObject* arr) {
  Py_ssize_t len = PyList_Size(arr);

  DbArray* result = PLDB_mk_arr_with_size(len);
  for (Py_ssize_t i = 0; i < len; i++) {
    PyObject* item = PyList_GetItem(arr, i);

    int ec = PyListToDbArray_SetElement(result, i, item);
    if (ec < 0) {  // db error
      PyErr_SetString(PyExc_Exception, PLDB_error_msg());
      PLDB_free_arr(result);
      return NULL;
    }

    if (ec > 0) {  // python error
      PLDB_free_arr(result);
      return NULL;
    }
  }

  return result;
}

static PyObject* DbStringToPyObject(DbValue* value) {
  const char* content = NULL;

  int ec = PLDB_value_get_string_utf8(value, &content);
  if (ec < 0) {
    PyErr_SetString(PyExc_Exception, "DbValue get string error");
    return NULL;
  }

  PyObject* result = PyUnicode_FromStringAndSize(content, ec);
  return result;
}

static PyObject* ArrayTypeValueToPyObject(DbValue* value) {
  DbArray* db_arr = NULL;
  int ec = 0;

  POLO_CALL(PLDB_value_get_array(value, &db_arr));

  unsigned int arr_len = PLDB_arr_len(db_arr);
  PyObject* result = PyList_New(arr_len);

  for (unsigned int i = 0; i < arr_len; i++) {
    DbValue* tmp_val;
    if (PLDB_arr_get(db_arr, i, &tmp_val) < 0) {
      PLDB_free_arr(db_arr);
      PyErr_SetString(PyExc_RuntimeError, "get value from array failed");
      return NULL;
    }

    PyObject* item = DbValueToPyObject(tmp_val);
    if (item == NULL) {
      return NULL;
    }
    if (PyList_SetItem(result, i, item) < 0) {
      PLDB_free_arr(db_arr);
      PyErr_SetString(PyExc_RuntimeError, "set item failed");
      return NULL;
    }

    PLDB_free_value(tmp_val);
  }

  PLDB_free_arr(db_arr);
  return result;
}

static PyObject* DocumentTypeValueToPyObject(DbValue* value) {
  DbDocument* doc = NULL;
  int ec = 0;

  POLO_CALL(PLDB_value_get_document(value, &doc));

  PyObject* result = PyDict_New();

  DbDocumentIter* iter = PLDB_doc_iter(doc);

  static char key_buffer[512];
  memset(key_buffer, 0, 512);

  DbValue *value_tmp;
  while (PLDB_doc_iter_next(iter, key_buffer, 512, &value_tmp)) {
    PyObject* value = DbValueToPyObject(value_tmp);

    if (PyDict_SetItemString(result, key_buffer, value) < 0) {
      Py_DECREF(value);
      Py_DECREF(result);
      result = NULL;
      PLDB_free_value(value_tmp);
      goto result;
    }

    PLDB_free_value(value_tmp);
    memset(key_buffer, 0, 512);
  }

result:
  PLDB_free_doc_iter(iter);
  PLDB_free_doc(doc);
  return result;
}

static PyObject* ObjectIdTypeValueToPyObject(DbValue* value) {
  DbObjectId* oid = NULL;
  if (PLDB_value_get_object_id(value, &oid) < 0) {
    PyErr_SetString(PyExc_Exception, "get ObjectId from value failed");
    return NULL;
  }

  PyObject* cap = PyCapsule_New(oid, KEY_OBJECT_ID, NULL);
  PyObject* argList = PyTuple_New(1);
  PyTuple_SetItem(argList, 0, cap);

  PyObject* result = PyObject_CallObject((PyObject*)&ObjectIdObjectType, argList);

  Py_DECREF(argList);

  return result;
}

static PyObject* UTCDateTimeTypeValueToPyDate(DbValue* value) {
  DbUTCDateTime* date = NULL;
  int ec = 0;
  POLO_CALL(PLDB_value_get_utc_datetime(value, &date))

  int64_t timestamp = PLDB_UTCDateTime_get_timestamp(date);

  PyObject* argList = PyTuple_New(1);
  PyTuple_SetItem(argList, 0, PyLong_FromLongLong(timestamp));

  PyObject* result = NULL;
  result = PyDateTime_FromTimestamp(argList);

  Py_DECREF(argList);
  PLDB_free_UTCDateTime(date);

  return result;
}

static PyObject* DbValueToPyObject(DbValue* value) {
  int ty = PLDB_value_type(value);
  int ec = 0;
  int64_t int_value = 0;
  double float_value;
  switch (ty)
  {
  case PLDB_VAL_NULL:
    Py_RETURN_NONE;

  case PLDB_VAL_DOUBL:
    POLO_CALL(PLDB_value_get_double(value, &float_value));
    return PyFloat_FromDouble(float_value);

  case PLDB_VAL_BOOLEAN:
    ec = PLDB_value_get_bool(value);
    if (ec) {
      Py_RETURN_TRUE;
    } else {
      Py_RETURN_FALSE;
    }

  case PLDB_VAL_INT:
    POLO_CALL(PLDB_value_get_i64(value, &int_value));
    return PyLong_FromLongLong(int_value);

  case PLDB_VAL_STRING:
    return DbStringToPyObject(value);

  case PLDB_VAL_ARRAY:
    return ArrayTypeValueToPyObject(value);

  case PLDB_VAL_DOCUMENT:
    return DocumentTypeValueToPyObject(value);

  case PLDB_VAL_OBJECT_ID:
    return ObjectIdTypeValueToPyObject(value);

  case PLDB_VAL_UTC_DATETIME:
    return UTCDateTimeTypeValueToPyDate(value);
  
  default:
    PyErr_SetString(PyExc_RuntimeError, "unknow DbValue type");
    return NULL;

  }
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
  if (PyType_Ready(&DatabaseObjectType) < 0) {
    return NULL;
  }

  if (PyType_Ready(&CollectionObjectType) < 0) {
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
  REGISTER_OBJECT(CollectionObjectType, "Collection");
  REGISTER_OBJECT(ObjectIdObjectType, "ObjectId");

  return m;
}