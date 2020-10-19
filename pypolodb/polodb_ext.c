#include <Python.h>
#include <datetime.h>
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

  const char* content;
  if (!PyArg_ParseTuple(args, "s", &content)) {
    return NULL;
  }

  int ec = 0;
  POLO_CALL(PLDB_create_collection(self->db, content));

  Py_RETURN_NONE;
}

static PyObject* DatabaseObject_insert(DatabaseObject* self, PyObject* args) {
  CHECK_DB_OPEND(self);

  const char* col_name;
  PyObject* obj;
  if (!PyArg_ParseTuple(args, "sO", &col_name, &obj)) {
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

  if (PLDB_insert(self->db, col_name, doc) < 0) {
    PLDB_free_doc(doc);
    PyErr_SetString(PyExc_Exception, PLDB_error_msg());
    return NULL;
  }

  PLDB_free_doc(doc);
  Py_RETURN_NONE;
}

static PyObject* DatabaseObject_find(DatabaseObject* self, PyObject* args) {
  CHECK_DB_OPEND(self);

  const char* col_name;
  PyObject* dict_obj;
  if (!PyArg_ParseTuple(args, "sO", &col_name, &dict_obj)) {
    return NULL;
  }
  
  if (Py_TYPE(dict_obj) != &PyDict_Type) {
    PyErr_SetString(PyExc_ValueError, "the second argument should be a dict");
    return NULL;
  }

  DbDocument* doc = PyDictToDbDocument(dict_obj);

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

static PyObject* DatabaseObject_update(DatabaseObject* self, PyObject* args) {
  CHECK_DB_OPEND(self);

  const char* col_name;
  PyObject* query_dict_obj;
  PyObject* update_dict_obj;
  if (!PyArg_ParseTuple(args, "sOO", &col_name, &query_dict_obj, &update_dict_obj)) {
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

  long long count = PLDB_update(self->db, col_name, query, update);
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

static PyObject* DatabaseObject_delete(DatabaseObject* self, PyObject* args) {
  CHECK_DB_OPEND(self);

  const char* col_name;
  PyObject* query_obj;
  if (!PyArg_ParseTuple(args, "sO", &col_name, &query_obj)) {
    return NULL;
  }

  if (Py_TYPE(query_obj) != &PyDict_Type) {
    PyErr_SetString(PyExc_Exception, "the thid argument should be a dict");
    return NULL;
  }

  PyObject* result = NULL;
  DbDocument* doc = PyDictToDbDocument(query_obj);

  long long ec = PLDB_delete(self->db, col_name, doc);
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

static PyObject* DatabaseObject_delete_all(DatabaseObject* self, PyObject* args) {
  CHECK_DB_OPEND(self);

  const char* col_name;
  if (!PyArg_ParseTuple(args, "s", &col_name)) {
    return NULL;
  }

  long long ec = PLDB_delete_all(self->db, col_name);
  if (ec < 0) {
    PyErr_SetString(PyExc_Exception, PLDB_error_msg());
    return NULL;
  }

  return PyLong_FromLongLong(ec);
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
    "start_transaction", (PyCFunction)DatabaseObject_start_transaction, METH_VARARGS,
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
    "create_collection", (PyCFunction)DatabaseObject_create_collection, METH_VARARGS,
    "create a collection"
  },
  {
    "insert", (PyCFunction)DatabaseObject_insert, METH_VARARGS,
    "insert a document"
  },
  {
    "update", (PyCFunction)DatabaseObject_update, METH_VARARGS,
    "update documents"
  },
  {
    "delete", (PyCFunction)DatabaseObject_delete, METH_VARARGS,
    "delete documents"
  },
  {
    "delete_all", (PyCFunction)DatabaseObject_delete_all, METH_VARARGS,
    "delete all documents from a collection",
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

static DbValue* PyObjectToDbValue(PyObject* obj) {
  if (obj == Py_None) {
    return PLDB_mk_null();
  } else if (PyLong_CheckExact(obj)) {
    long long int_value = PyLong_AsLongLong(obj);
     return PLDB_mk_int(int_value);
  } else if (PyBool_Check(obj)) {
    int value = 0;
    if (obj == Py_True) {
      value = 1;
    }
    return PLDB_mk_bool(value);
  } else if (PyFloat_CheckExact(obj)) {
    double float_value = PyFloat_AsDouble(obj);
    return PLDB_mk_double(float_value);
  } else if (PyUnicode_CheckExact(obj)) {
    const char* content = PyUnicode_AsUTF8(obj);
    if (content == NULL) {
      return NULL;
    }
    return PLDB_mk_str(content);
  } else if (PyCapsule_CheckExact(obj)) {
    DbValue* value = PyCapsule_GetPointer(obj, KEY_VALUE);
    return value;
  } else if (Py_TYPE(obj) == &PyDict_Type) {
    DbDocument* doc = PyDictToDbDocument(obj);
    DbValue* result = PLDB_doc_to_value(doc);
    PLDB_free_doc(doc);
    return result;
  } else if (Py_TYPE(obj) == &PyList_Type) {
    DbArray* arr = PyListToDbArray(obj);
    DbValue* result = PLDB_arr_into_value(arr);
    PLDB_free_arr(arr);
    return result;
  } else if (Py_TYPE(obj) == &ObjectIdObjectType) {
    ObjectIdObject* oid = (ObjectIdObject*)obj;
    return PLDB_object_id_to_value(oid->oid);
  } else if (Py_TYPE(obj) == &DocumentObjectType) {
    DocumentObject* doc = (DocumentObject*)obj;
    return PLDB_doc_to_value(doc->doc);
  } else if (Py_TYPE(obj) == &ObjectIdObjectType) {
    ObjectIdObject* oid_obj = (ObjectIdObject*)obj;
    return PLDB_object_id_to_value(oid_obj->oid);
  } else if (PyDateTime_CheckExact(obj)) {
    PyObject* result = PyObject_CallMethod(obj, "timestamp", "");
    if (result == NULL) {
      return NULL;
    }
    if (!PyFloat_CheckExact(result)) {
      PyErr_SetString(PyExc_TypeError, "return of timestamp should be float");
      Py_DECREF(result);
      return NULL;
    }
    double timestamp = PyFloat_AsDouble(result);
    Py_DECREF(result);
    DbUTCDateTime* dt = PLDB_mk_UTCDateTime((long long)timestamp);
    DbValue* val = PLDB_UTCDateTime_to_value(dt);
    PLDB_free_UTCDateTime(dt);
    return val;
  }
  return NULL;
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

    DbValue* value = PyObjectToDbValue(item_value);
    if (value == NULL) {
      Py_DECREF(list);
      PLDB_free_doc(result);
      PyErr_SetString(PyExc_Exception, "python object conversion failed");
      return NULL;
    }

    if (PLDB_doc_set(result, key_content, value) < 0) {
      Py_DECREF(list);
      PLDB_free_doc(result);
      PyErr_SetString(PyExc_Exception, PLDB_error_msg());
      PLDB_free_value(value);
      return NULL;
    }

    PLDB_free_value(value);
  }

  Py_DECREF(list);
  return result;
}

static DbArray* PyListToDbArray(PyObject* arr) {
  DbArray* result = PLDB_mk_arr();

  Py_ssize_t len = PyList_Size(arr);
  for (Py_ssize_t i = 0; i < len; i++) {
    PyObject* item = PyList_GetItem(arr, i);
    DbValue* item_value = PyObjectToDbValue(item);
    if (item_value == NULL) {
      PLDB_free_arr(result);
      return NULL;
    }
    PLDB_arr_push(result, item_value);
    PLDB_free_value(item_value);
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

  long long timestamp = PLDB_UTCDateTime_get_timestamp(date);

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
  long long int_value = 0;
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

  if (PyType_Ready(&ObjectIdObjectType) < 0) {
    return NULL;
  }

  m = PyModule_Create(&hello_definition);
  if (m == NULL) {
    return NULL;
  }

  REGISTER_OBJECT(DatabaseObjectType, "Database");
  REGISTER_OBJECT(ValueObjectType, "Value");
  REGISTER_OBJECT(ObjectIdObjectType, "ObjectId");

  return m;
}
