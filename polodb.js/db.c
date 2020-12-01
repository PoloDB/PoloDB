#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <node_api.h>
#include "utils.h"

#define BUFFER_SIZE 512
#define VALUE_NAME_BUFFER_SIZE 64
#define OID_HEX_BUFFER_SIZE 64

#include "./polodb.h"

#define STD_CALL(EXPR) \
  ec = (EXPR); \
  if (ec < 0) { \
    napi_throw_type_error(env, NULL, PLDB_error_msg()); \
    return NULL; \
  }

static napi_ref collection_object_ref;
static napi_ref objectid_ref;

static int check_type(napi_env env, napi_value value, napi_valuetype expected) {
  napi_status status;
  napi_valuetype actual;

  status = napi_typeof(env, value, &actual);
  assert(status == napi_ok);

  return actual == expected;
}

static napi_value db_version(napi_env env, napi_callback_info info) {
  static char buffer[BUFFER_SIZE];
  memset(buffer, 0, BUFFER_SIZE);
  PLDB_version(buffer, BUFFER_SIZE);

  napi_status status;
  napi_value world;
  status = napi_create_string_utf8(env, buffer, strlen(buffer), &world);
  assert(status == napi_ok);
  return world;
}

#define CHECK_STAT2(stat) \
  if ((stat) != napi_ok) { \
    goto err; \
  }

#define DECLARE_NAPI_METHOD(name, func)                                        \
  { name, 0, func, 0, 0, 0, napi_default, 0 }

typedef struct {
  uint32_t  id;
  uint32_t  meta_version;
} InternalCollection;

InternalCollection* NewInternalCollection(Database* db) {
  InternalCollection* collection = (InternalCollection*)malloc(sizeof(InternalCollection));
  memset(collection, 0, sizeof(InternalCollection));

  collection->id = 0;
  collection->meta_version = 0;

  return collection;
}

void InternalCollection_finalizer(napi_env env, void* finalize_data, void* finalize_hint) {
  InternalCollection* internal_collection = (InternalCollection*)finalize_data;
  free(internal_collection);
}

static napi_value Collection_constructor(napi_env env, napi_callback_info info) {
  napi_status status;

  napi_value this_arg;

  size_t argc = 4;
  napi_value args[4];
  status = napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
  CHECK_STAT(status);

  if (!check_type(env, args[0], napi_object)) {
    napi_throw_type_error(env, NULL, "the first arg should be an object");
    return NULL;
  }

  if (!check_type(env, args[1], napi_string)) {
    napi_throw_type_error(env, NULL, "the second arg should be an object");
    return NULL;
  }

  if (!check_type(env, args[2], napi_number)) {
    napi_throw_type_error(env, NULL, "the third arg should be a number");
    return NULL;
  }

  if (!check_type(env, args[3], napi_number)) {
    napi_throw_type_error(env, NULL, "the forth arg should be a number");
    return NULL;
  }

  Database* db = NULL;
  status = napi_unwrap(env, args[0], (void**)&db);
  CHECK_STAT(status);

  napi_property_descriptor db_prop[] = {
    { "__db", 0, 0, 0, 0, args[0], napi_default, 0 },
    { "__name", 0, 0, 0, 0, args[1], napi_default, 0 },
    { "__id", 0, 0, 0, 0, args[2], napi_default, 0 },
    { "__metaVersion", 0, 0, 0, 0, args[3], napi_default, 0 },
    { NULL }
  };

  status = napi_define_properties(env, this_arg, 4, db_prop);
  CHECK_STAT(status);

  InternalCollection* internal_collection = NewInternalCollection(db);
  
  status = napi_wrap(env, this_arg, internal_collection, InternalCollection_finalizer, 0, NULL);
  CHECK_STAT(status);

  status = napi_get_value_uint32(env, args[2], &internal_collection->id);
  CHECK_STAT(status);

  status = napi_get_value_uint32(env, args[3], &internal_collection->meta_version);
  CHECK_STAT(status);

  return this_arg;
}

static DbDocument* JsValueToDbDocument(napi_env env, napi_value value);
static DbArray* JsArrayValueToDbArray(napi_env env, napi_value value);

static napi_status JsArrayValueToDbArray_SetStringElement(napi_env env, DbArray* arr, unsigned int index, napi_value value) {
  napi_status status;

  size_t str_len = 0;
  status = napi_get_value_string_utf8(env, value, NULL, 0, &str_len);
  if (status != napi_ok) {
    return status;
  }

  char* buffer = malloc(str_len + 1);
  memset(buffer, 0, str_len + 1);

  status = napi_get_value_string_utf8(env, value, buffer, str_len + 1, &str_len);
  if (status != napi_ok) {
    return status;
  }

  int ec = PLDB_arr_set_string(arr, index, buffer);
  if (ec < 0) {
    free(buffer);
    napi_throw_error(env, NULL, PLDB_error_msg());
    return napi_generic_failure;
  }

  free(buffer);

  return napi_ok;
}

static napi_status JsArrayValueToDbArray_SetArrayElement(napi_env env, DbArray* arr, unsigned int index, napi_value child_value) {
  DbArray* child_arr = JsArrayValueToDbArray(env, child_value);
  if (arr == napi_ok) {
    return napi_generic_failure;
  }

  napi_status result = napi_ok;

  if (PLDB_arr_set_arr(arr, index, child_arr) < 0) {
    result = napi_throw_error(env, NULL, PLDB_error_msg());
  }

  PLDB_free_arr(child_arr);

  return result;
}

static napi_status JsArrayValueToDbArray_SetUTCDateTime(napi_env env, DbArray* arr, unsigned int index, napi_value value) {
  napi_status status;

  int64_t utc_datetime = 0;
  status = JsGetUTCDateTime(env, value, &utc_datetime);
  CHECK_STAT(status);

  int ec = PLDB_arr_set_UTCDateTime(arr, index, utc_datetime);
  if (ec < 0) {
    napi_throw_error(env, NULL, PLDB_error_msg());
    return napi_generic_failure;
  }

  return napi_ok;
}

static napi_status JsArrayValueToDbArray_SetElement(napi_env env, DbArray* arr, unsigned int index, napi_value value) {
  napi_status status;
  napi_valuetype ty;

  napi_value object_id_instance;
  status = napi_get_reference_value(env, objectid_ref, &object_id_instance);
  CHECK_STAT(status);

  status = napi_typeof(env, value, &ty);

  DbValue* result = NULL;

  int64_t int_value = 0;
  double float_value = 0;
  bool bl_value = false;
  int ec = 0;
  switch (ty) {
    case napi_undefined:
    case napi_null:
      PLDB_arr_set_null(arr, index);
      break;

    case napi_string:
      return JsArrayValueToDbArray_SetStringElement(env, arr, index, value);

    case napi_boolean:
      status = napi_get_value_bool(env, value, &bl_value);
      if (status != napi_ok) {
        return status;
      }
      PLDB_arr_set_bool(arr, index, bl_value ? 1 : 0);
      break;

    case napi_number: {
      ec = JsIsInteger(env, value);
      if (ec < 0) {
        napi_throw_error(env, NULL, PLDB_error_msg());
        return napi_generic_failure;
      } else if (ec) {
        status = napi_get_value_int64(env, value, &int_value);
        if (status != napi_ok) return status;
        PLDB_arr_set_int(arr, index, int_value);
      } else {
        status = napi_get_value_double(env, value, &float_value);
        if (status != napi_ok) return status;
        PLDB_arr_set_double(arr, index, float_value);
      }
      break;
    }

    case napi_object: {
      bool is_array = false;
      bool is_date = false;
      status = JsIsArray(env, value, &is_array);
      CHECK_STAT(status);

      if (is_array) {
        return JsArrayValueToDbArray_SetArrayElement(env, arr, index, value);
      }

      status = napi_is_date(env, value, &is_date);

      if (napi_instanceof(env, value, object_id_instance, &bl_value)) {
        DbObjectId* oid = NULL;  // borrowed
        status = napi_unwrap(env, value, (void**)&oid);
        CHECK_STAT(status);

        if (PLDB_arr_set_object_id(arr, index, oid) < 0) {
          napi_throw_error(env, NULL, PLDB_error_msg());
          return napi_generic_failure;
        }
      } else {
        DbDocument* child_doc = JsValueToDbDocument(env, value);
        if (child_doc == NULL) {
          return napi_generic_failure;
        }

        if (PLDB_arr_set_doc(arr, index, child_doc) < 0) {
          napi_throw_error(env, NULL, PLDB_error_msg());
          return napi_generic_failure;
        }

        PLDB_free_doc(child_doc);
      }
    }
    
    default:
      napi_throw_type_error(env, NULL, "unsupport object type");
      return napi_generic_failure;

  }


  return napi_ok;
}

static DbArray* JsArrayValueToDbArray(napi_env env, napi_value value) {
  napi_status status;
  uint32_t arr_len = 0;

  status = napi_get_array_length(env, value, &arr_len);
  CHECK_STAT2(status);

  DbArray* arr = PLDB_mk_arr_with_size(arr_len);

  napi_value item_value;
  DbValue* item_db_value;
  for (uint32_t i = 0; i < arr_len; i++) {
    status = napi_get_element(env, value, i, &item_value);
    CHECK_STAT2(status);

    status = JsArrayValueToDbArray_SetElement(env, arr, i, item_value);
    CHECK_STAT2(status);
  }

  goto normal;
err:
  PLDB_free_arr(arr);
  return NULL;

normal:
  return arr;
}

static napi_status JsStringValueToDbValue_SetStringProperty(napi_env env, DbDocument* doc, const char* key, napi_value value) {
  napi_status status;

  size_t str_len = 0;
  status = napi_get_value_string_utf8(env, value, NULL, 0, &str_len);
  if (status != napi_ok) {
    return status;
  }

  char* buffer = malloc(str_len + 1);
  memset(buffer, 0, str_len + 1);

  status = napi_get_value_string_utf8(env, value, buffer, str_len + 1, &str_len);
  if (status != napi_ok) {
    return status;
  }

  int ec = PLDB_doc_set_string(doc, key, buffer);
  if (ec < 0) {
    free(buffer);
    napi_throw_error(env, NULL, PLDB_error_msg());
    return napi_generic_failure;
  }

  free(buffer);

  return napi_ok;
}

static napi_status JsStringValueToDbValue_SetArrayProperty(napi_env env, DbDocument* doc, const char* key, napi_value value) {
  DbArray* arr = JsArrayValueToDbArray(env, value);
  if (arr == NULL) {
    return napi_generic_failure;
  }

  napi_status result = napi_ok;

  if (PLDB_doc_set_arr(doc, key, arr) < 0) {
    result = napi_throw_error(env, NULL, PLDB_error_msg());
  }

  PLDB_free_arr(arr);
  return napi_ok;
}

static napi_status JsStringValueToDbValue_SetUTCDateTime(napi_env env, DbDocument* doc, const char* key, napi_value value) {
  napi_status status;

  int64_t utc_datetime = 0;
  status = JsGetUTCDateTime(env, value, &utc_datetime);
  CHECK_STAT(status);

  int ec = PLDB_doc_set_UTCDateTime(doc, key, utc_datetime);
  if (ec < 0) {
    napi_throw_error(env, NULL, PLDB_error_msg());
    return napi_generic_failure;
  }

  return napi_ok;
}

static napi_status JsValueToDbDocument_SetProperty(napi_env env, DbDocument* doc, const char* key, napi_value value) {
  napi_status status;
  napi_valuetype ty;

  napi_value object_id_instance;
  status = napi_get_reference_value(env, objectid_ref, &object_id_instance);
  CHECK_STAT(status);

  status = napi_typeof(env, value, &ty);

  DbValue* result = NULL;

  int64_t int_value = 0;
  double float_value = 0;
  bool bl_value = false;
  int ec = 0;
  switch (ty) {
    case napi_undefined:
    case napi_null:
      PLDB_doc_set_null(doc, key);
      break;

    case napi_string:
      return JsStringValueToDbValue_SetStringProperty(env, doc, key, value);

    case napi_boolean:
      status = napi_get_value_bool(env, value, &bl_value);
      if (status != napi_ok) {
        return status;
      }
      PLDB_doc_set_bool(doc, key, bl_value ? 1 : 0);
      break;

    case napi_number: {
      ec = JsIsInteger(env, value);
      if (ec < 0) {
        napi_throw_error(env, NULL, PLDB_error_msg());
        return napi_generic_failure;
      } else if (ec) {
        status = napi_get_value_int64(env, value, &int_value);
        if (status != napi_ok) return status;
        PLDB_doc_set_int(doc, key, int_value);
      } else {
        status = napi_get_value_double(env, value, &float_value);
        if (status != napi_ok) return status;
        PLDB_doc_set_double(doc, key, float_value);
      }
      break;
    }

    case napi_object: {
      bool is_array = false;
      bool is_date = false;

      status = JsIsArray(env, value, &is_array);
      CHECK_STAT(status);

      if (is_array) {
        return JsStringValueToDbValue_SetArrayProperty(env, doc, key, value);
      }

      status = napi_is_date(env, value, &is_date);
      CHECK_STAT(status);
      if (is_date) {
        return JsStringValueToDbValue_SetUTCDateTime(env, doc, key, value);
      }

      if (napi_instanceof(env, value, object_id_instance, &bl_value)) {
        DbObjectId* oid = NULL;  // borrowed
        status = napi_unwrap(env, value, (void**)&oid);
        CHECK_STAT(status);

        if (PLDB_doc_set_object_id(doc, key, oid) < 0) {
           napi_throw_error(env, NULL, PLDB_error_msg());
           return napi_generic_failure;
        }
      } else {
        DbDocument* child_doc = JsValueToDbDocument(env, value);
        if (child_doc == NULL) {
          return napi_generic_failure;
        }

        if (PLDB_doc_set_doc(doc, key, child_doc) < 0) {
          napi_throw_error(env, NULL, PLDB_error_msg());
          return napi_generic_failure;
        }

        PLDB_free_doc(child_doc);
      }
    }
    
    default:
      napi_throw_type_error(env, NULL, "unsupport object type");
      return napi_generic_failure;

  }

  return napi_ok;
}

static DbDocument* JsValueToDbDocument(napi_env env, napi_value value) {
  napi_status status;
  if (!check_type(env, value, napi_object)) {
    napi_throw_type_error(env, NULL, "object expected");
    return NULL;
  }
  DbDocument* doc = PLDB_mk_doc();

  napi_value names_array;

  status = napi_get_property_names(env, value, &names_array);
  CHECK_STAT2(status);

  uint32_t arr_len = 0;
  status = napi_get_array_length(env, names_array, &arr_len);
  CHECK_STAT2(status);

  char name_buffer[512];

  napi_value element_name;
  napi_value element_value;
  for (uint32_t i = 0; i < arr_len; i++) {
    status = napi_get_element(env, names_array, i, &element_name);
    CHECK_STAT2(status);

    status = napi_get_property(env, value, element_name, &element_value);
    CHECK_STAT2(status);

    memset(name_buffer, 0, 512);

    size_t size = 0;
    status = napi_get_value_string_utf8(env, element_name, name_buffer, 512, &size);
    CHECK_STAT2(status);

    status = JsValueToDbDocument_SetProperty(env, doc, name_buffer, element_value);
    if (status != napi_ok) {
      goto err;
    }
  }

  goto normal;
err:
  PLDB_free_doc(doc);
  return NULL;
normal:
  return doc;
}

static napi_value DbValueToJsValue(napi_env env, DbValue* value);

static napi_value DbDocumentToJsValue(napi_env env, DbDocument* doc) {
  napi_status status;
  napi_value result = 0;

  status = napi_create_object(env, &result);
  CHECK_STAT(status);

  int ec = 0;
  DbDocumentIter* iter = PLDB_doc_iter(doc);

  static char buffer[BUFFER_SIZE];
  memset(buffer, 0, BUFFER_SIZE);

  DbValue* item;
  ec = PLDB_doc_iter_next(iter, buffer, BUFFER_SIZE, &item);

  while (ec) {
    napi_value item_value = DbValueToJsValue(env, item);

    napi_property_descriptor prop = {
      buffer,
      NULL,
      0, 0, 0,
      item_value,
      napi_default | napi_enumerable | napi_writable,
      0
    };
    status = napi_define_properties(env, result, 1, &prop);
    if (status != napi_ok) {
      PLDB_free_value(item);
      goto err;
    }

    memset(buffer, 0, BUFFER_SIZE);
    PLDB_free_value(item);
    item = NULL;

    ec = PLDB_doc_iter_next(iter, buffer, BUFFER_SIZE, &item);
  }

  goto normal;
err:
  if (iter != NULL) {
    PLDB_free_doc_iter(iter);
    iter = NULL;
  }
  return NULL;
normal:
  PLDB_free_doc_iter(iter);
  return result;
}

static napi_value DbArrayToJsValue(napi_env env, DbArray* arr) {
  napi_status status;
  napi_value result = 0;

  uint32_t len = PLDB_arr_len(arr);

  status = napi_create_array_with_length(env, len, &result);
  CHECK_STAT(status);

  DbValue* value_item = NULL;
  int ec = 0;
  for (uint32_t i = 0; i < len; i++) {
    ec = PLDB_arr_get(arr, i, &value_item);
    if (ec < 0) {
      napi_throw_error(env, NULL, "get element error #1");
      return NULL;
    }

    napi_value js_item = DbValueToJsValue(env, value_item);
    if (js_item == NULL) {
      return NULL;
    }

    status = napi_set_element(env, result, i, js_item);
    CHECK_STAT(status);

    PLDB_free_value(value_item);
    value_item = NULL;
  }

  return result;
}

static napi_value DbValueToJsValue(napi_env env, DbValue* value) {
  napi_status status;
  napi_value result = NULL;
  double db_value = 0;
  int ty = PLDB_value_type(value);
  int ec = 0;
  int64_t long_value = 0;
  switch (ty) {
    case PLDB_VAL_NULL:
      status = napi_get_undefined(env, &result);
      CHECK_STAT(status);
      return result;

    case PLDB_VAL_DOUBL:
      ec = PLDB_value_get_double(value, &db_value);
      if (ec < 0) {
        napi_throw_error(env, NULL, PLDB_error_msg());
        return NULL;
      }
      status = napi_create_double(env, db_value, &result);
      CHECK_STAT(status);
      return result;

    case PLDB_VAL_BOOLEAN:
      ec = PLDB_value_get_bool(value);
      if (ec < 0) {
        napi_throw_error(env, NULL, PLDB_error_msg());
        return NULL;
      }
      status = napi_get_boolean(env, ec ? true : false, &result);
      CHECK_STAT(status);
      return result;

    case PLDB_VAL_INT:
      ec = PLDB_value_get_i64(value, &long_value);
      status = napi_create_int64(env, long_value, &result);
      CHECK_STAT(status);
      return result;

    case PLDB_VAL_STRING: {
      if (ec < 0) {
        napi_throw_error(env, NULL, PLDB_error_msg());
        return NULL;
      }
      const char* content = NULL;
      ec = PLDB_value_get_string_utf8(value, &content);

      result = NULL;
      status = napi_create_string_utf8(env, content, ec, &result);

      return result;
    }

    case PLDB_VAL_DOCUMENT: {
      DbDocument* doc = NULL;
      ec = PLDB_value_get_document(value, &doc);
      if (ec < 0) {
        return NULL;
      }

      result = DbDocumentToJsValue(env, doc);

      PLDB_free_doc(doc);

      return result;
    }

    case PLDB_VAL_ARRAY: {
      DbArray* arr = NULL;
      ec = PLDB_value_get_array(value, &arr);
      if (ec < 0) {
        return NULL;
      }

      result = DbArrayToJsValue(env, arr);

      PLDB_free_arr(arr);

      return result;
    }

    case PLDB_VAL_OBJECT_ID: {
      DbObjectId* oid = NULL;
      ec = PLDB_value_get_object_id(value, &oid);
      if (ec < 0) {
        return NULL;
      }

      napi_value objectid_ctor;
      status = napi_get_reference_value(env, objectid_ref, &objectid_ctor);
      CHECK_STAT(status);

      napi_value oid_ext;
      status = napi_create_external(env, oid, NULL, NULL, &oid_ext);
      CHECK_STAT(status);

      size_t argc = 1;
      napi_value args[] = { oid_ext };

      status = napi_new_instance(env, objectid_ctor, argc, args, &result);
      CHECK_STAT(status);
      
      return result;
    }

    case PLDB_VAL_UTC_DATETIME : {
      DbUTCDateTime* dt = NULL;
      ec = PLDB_value_get_utc_datetime(value, &dt);
      if (ec < 0) {
        napi_throw_error(env, NULL, PLDB_error_msg());
        return NULL;
      }

      int64_t utc_datetime = PLDB_UTCDateTime_get_timestamp(dt);
      PLDB_free_UTCDateTime(dt);

      return JsNewDate(env, utc_datetime);
    }
    
    default:
      napi_throw_error(env, NULL, "Unknown DbValue type");
      return NULL;

  }
}

#define CHECK_DB_ALIVE(stat) \
  if ((stat) != napi_ok) { \
    napi_throw_error(env, NULL, "db has been closed"); \
    return NULL; \
  }

static napi_status get_db_from_js_collection(napi_env env, napi_value collection, Database** db) {
  napi_status status;

  napi_value js_db;
  status = napi_get_named_property(env, collection, "__db", &js_db);
  if (status != napi_ok) return status;

  status = napi_unwrap(env, js_db, (void**)db);
  return status;
}

static napi_value Collection_insert(napi_env env, napi_callback_info info) {
  napi_status status;

  napi_value this_arg;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
  CHECK_STAT(status);

  Database* db;
  status = get_db_from_js_collection(env, this_arg, &db);
  CHECK_DB_ALIVE(status);

  InternalCollection* internal_collection;
  status = napi_unwrap(env, this_arg, (void**)&internal_collection);
  CHECK_STAT(status);

  DbDocument* doc = JsValueToDbDocument(env, args[0]); 
  if (doc == NULL) {
    return NULL;
  }

  napi_value result = 0;
  int ec = 0;
  ec = PLDB_insert(db, internal_collection->id, internal_collection->meta_version, doc);
  if (ec < 0) {
    napi_throw_error(env, NULL, PLDB_error_msg());
    goto clean;
  }

clean:
  PLDB_free_doc(doc);
  return result;
}

static napi_value Collection_find(napi_env env, napi_callback_info info) {
  napi_status status;

  napi_value this_arg;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
  CHECK_STAT(status);

  Database* db;
  status = get_db_from_js_collection(env, this_arg, &db);
  CHECK_DB_ALIVE(status);

  InternalCollection* internal_collection;
  status = napi_unwrap(env, this_arg, (void**)&internal_collection);
  CHECK_STAT(status);

  DbDocument* query_doc;

  napi_valuetype arg1_ty;

  status = napi_typeof(env, args[0], &arg1_ty);
  assert(status == napi_ok);

  if (arg1_ty == napi_undefined) {
    query_doc = NULL;
  } else if (arg1_ty == napi_object) {
    query_doc = JsValueToDbDocument(env, args[0]);
    if (query_doc == NULL) {
      return NULL;
    }
  }

  int ec = 0;

  DbHandle* handle = NULL;
  ec = PLDB_find(db,
    internal_collection->id,
    internal_collection->meta_version,
    query_doc,
    &handle
  );

  if (ec < 0) {
    napi_throw_error(env, NULL, PLDB_error_msg());
    return NULL;
  }

  napi_value result;
  status = napi_create_array(env, &result);

  ec = PLDB_handle_step(handle);
  if (ec < 0) {
    napi_throw_error(env, NULL, PLDB_error_msg());
    return NULL;
  }

  uint32_t counter = 0;

  int state = PLDB_handle_state(handle);
  DbValue* item;
  while (state == 2) {
    PLDB_handle_get(handle, &item);
    napi_value js_value = DbValueToJsValue(env, item);
    if (js_value == NULL) {
      PLDB_free_value(item);
      goto err;
    }

    status = napi_set_element(env, result, counter, js_value);
    if (status != napi_ok) {
      PLDB_free_value(item);
      goto err;
    }

    PLDB_free_value(item);
    counter++;

    ec = PLDB_handle_step(handle);
    if (ec < 0) {
      napi_throw_error(env, NULL, PLDB_error_msg());
      goto err;
    }
    state = PLDB_handle_state(handle);
  }

  goto normal;
err:
  PLDB_free_handle(handle);
  return NULL;
normal:
  PLDB_free_handle(handle);
  return result;
}

static napi_value Collection_count(napi_env env, napi_callback_info info) {
  napi_status status;

  napi_value this_arg;

  status = napi_get_cb_info(env, info, NULL, NULL, &this_arg, NULL);
  CHECK_STAT(status);

  Database* db;
  status = get_db_from_js_collection(env, this_arg, &db);
  CHECK_DB_ALIVE(status);

  InternalCollection* internal_collection;
  status = napi_unwrap(env, this_arg, (void**)&internal_collection);
  CHECK_STAT(status);

  int64_t ec = PLDB_count(db,
    internal_collection->id,
    internal_collection->meta_version
  );
  if (ec < 0) {
    napi_throw_error(env, NULL, PLDB_error_msg());
    return NULL;
  }

  napi_value result;

  status = napi_create_int64(env, ec, &result);
  CHECK_STAT(status);

  return result;
}

static napi_value Collection_update(napi_env env, napi_callback_info info) {
  napi_status status;

  napi_value this_arg;

  size_t argc = 2;
  napi_value args[2];
  status = napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
  CHECK_STAT(status);

  if (!check_type(env, args[0], napi_object)) {
    napi_throw_type_error(env, NULL, "the first arg should be an object");
    return NULL;
  }

  if (!check_type(env, args[1], napi_object)) {
    napi_throw_type_error(env, NULL, "the second arg should be an object");
    return NULL;
  }

  Database* db;
  status = get_db_from_js_collection(env, this_arg, &db);
  CHECK_DB_ALIVE(status);

  InternalCollection* internal_collection;
  status = napi_unwrap(env, this_arg, (void**)&internal_collection);
  CHECK_STAT(status);

  DbDocument* query_doc = NULL;
  DbDocument* update_doc = NULL;

  query_doc = JsValueToDbDocument(env, args[0]);
  if (query_doc == 0) {
    goto ret;
  }

  update_doc = JsValueToDbDocument(env, args[1]);
  if (query_doc == 0) {
    goto ret;
  }

  int ec = PLDB_update(db, internal_collection->id, internal_collection->meta_version, query_doc, update_doc);
  if (ec < 0) {
    napi_throw_error(env, NULL, PLDB_error_msg());
    goto ret;
  }

ret:
  if (query_doc != NULL) {
    PLDB_free_doc(query_doc);
    query_doc = NULL;
  }
  if (update_doc == NULL) {
    PLDB_free_doc(update_doc);
    update_doc = NULL;
  }
  return NULL;
}

static napi_value Collection_delete(napi_env env, napi_callback_info info) {
  napi_status status;

  napi_value this_arg;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
  CHECK_STAT(status);

  if (!check_type(env, args[0], napi_object)) {
    napi_throw_type_error(env, NULL, "the first arg should be an object");
    return NULL;
  }

  Database* db;
  status = get_db_from_js_collection(env, this_arg, &db);
  CHECK_DB_ALIVE(status);

  InternalCollection* internal_collection;
  status = napi_unwrap(env, this_arg, (void**)&internal_collection);
  CHECK_STAT(status);

  DbDocument* query_doc = NULL;

  query_doc = JsValueToDbDocument(env, args[0]);
  if (query_doc == 0) {
    goto ret;
  }

  int ec = PLDB_delete(db,
    internal_collection->id,
    internal_collection->meta_version,
    query_doc
  );

  if (ec < 0) {
    napi_throw_error(env, NULL, PLDB_error_msg());
    goto ret;
  }

ret:
  if (query_doc != NULL) {
    PLDB_free_doc(query_doc);
    query_doc = NULL;
  }

  return NULL;
}

static napi_value Collection_delete_all(napi_env env, napi_callback_info info) {
  napi_status status;

  napi_value this_arg;

  status = napi_get_cb_info(env, info, NULL, NULL, &this_arg, NULL);
  CHECK_STAT(status);

  Database* db;
  status = get_db_from_js_collection(env, this_arg, &db);
  CHECK_DB_ALIVE(status);

  InternalCollection* internal_collection;
  status = napi_unwrap(env, this_arg, (void**)&internal_collection);
  CHECK_STAT(status);

  int ec = PLDB_delete_all(db,
    internal_collection->id,
    internal_collection->meta_version
  );
  if (ec < 0) {
    napi_throw_error(env, NULL, PLDB_error_msg());
    return NULL;
  }

  return NULL;
}

static napi_value Collection_drop(napi_env env, napi_callback_info info) {
  napi_status status;
  napi_value this_arg;

  status = napi_get_cb_info(env, info, NULL, NULL, &this_arg, NULL);
  CHECK_STAT(status);

  Database* db;
  status = get_db_from_js_collection(env, this_arg, &db);
  CHECK_DB_ALIVE(status);

  InternalCollection* internal_collection;
  status = napi_unwrap(env, this_arg, (void**)&internal_collection);
  CHECK_STAT(status);

  int ec = PLDB_drop(db,
    internal_collection->id,
    internal_collection->meta_version
  );
  if (ec < 0) {
    napi_throw_error(env, NULL, PLDB_error_msg());
    return NULL;
  }

  return NULL;
}

static napi_value Database_create_collection(napi_env env, napi_callback_info info) {
  napi_status status;

  napi_value this_arg;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
  CHECK_STAT(status);

  Database* db = NULL;
  status = napi_unwrap(env, this_arg, (void*)&db);
  CHECK_STAT(status);

  if (!check_type(env, args[0], napi_string)) {
    napi_throw_type_error(env, NULL, "The first argument should be a string");
    return NULL;
  }

  static char path_buffer[BUFFER_SIZE];
  memset(path_buffer, 0, BUFFER_SIZE);

  size_t written_count = 0;
  status = napi_get_value_string_utf8(env, args[0], path_buffer, BUFFER_SIZE, &written_count);
  assert(status == napi_ok);

  int ec = 0;
  uint32_t col_id = 0;
  uint32_t meta_version = 0;
  STD_CALL(PLDB_create_collection(db, path_buffer, &col_id, &meta_version));

  napi_value collection_ctor;
  status = napi_get_reference_value(env, collection_object_ref, &collection_ctor);
  CHECK_STAT(status);

  napi_value js_col_id;
  napi_value js_meta_version;

  status = napi_create_uint32(env, col_id, &js_col_id);
  CHECK_STAT(status);

  status = napi_create_uint32(env, meta_version, &js_meta_version);
  CHECK_STAT(status);

  size_t arg_size = 4;
  napi_value pass_args[] = { this_arg, args[0], js_col_id, js_meta_version };

  napi_value result = NULL;;
  status = napi_new_instance(env, collection_ctor, arg_size, pass_args, &result);
  if (status == napi_generic_failure) {
    return NULL;
  }
  CHECK_STAT(status);

  return result;
}

static napi_value Database_collection(napi_env env, napi_callback_info info) {
  napi_status status;

  napi_value this_arg;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
  CHECK_STAT(status);

  Database* db = NULL;
  status = napi_unwrap(env, this_arg, (void*)&db);
  CHECK_STAT(status);

  if (!check_type(env, args[0], napi_string)) {
    napi_throw_type_error(env, NULL, "The first argument should be a string");
    return NULL;
  }

  napi_value collection_ctor;
  status = napi_get_reference_value(env, collection_object_ref, &collection_ctor);
  CHECK_STAT(status);

  static char name_buffer[BUFFER_SIZE];
  memset(name_buffer, 0 , BUFFER_SIZE);

  size_t value_len = 0;
  status = napi_get_value_string_utf8(env, args[0], name_buffer, BUFFER_SIZE, &value_len);
  CHECK_STAT(status);

  uint32_t col_id = 0;
  uint32_t meta_version = 0;

  int ec = PLDB_get_collection_meta_by_name(db, name_buffer, &col_id, &meta_version);
  if (ec < 0) {
    napi_throw_error(env, NULL, PLDB_error_msg());
    return NULL;
  }

  napi_value js_col_id;
  napi_value js_meta_version;

  status = napi_create_uint32(env, col_id, &js_col_id);
  CHECK_STAT(status);

  status = napi_create_uint32(env, meta_version, &js_meta_version);
  CHECK_STAT(status);

  size_t arg_size = 4;
  napi_value pass_args[] = { this_arg, args[0], js_col_id, js_meta_version };

  napi_value result = NULL;;
  status = napi_new_instance(env, collection_ctor, arg_size, pass_args, &result);
  if (status == napi_generic_failure) { // an error is thrown
    return NULL;
  } else if (status != napi_ok) {
    napi_throw_error(env, NULL, "new collection failed");
    return NULL;
  }

  return result;
}

static napi_value Database_close(napi_env env, napi_callback_info info) {
  napi_status status;

  napi_value this_arg;

  status = napi_get_cb_info(env, info, NULL, NULL, &this_arg, NULL);
  CHECK_STAT(status);

  Database* db;

  status = napi_remove_wrap(env, this_arg, (void**)&db);
  if (status != napi_ok) {
    napi_throw_error(env, NULL, "database has been closed");
    return NULL;
  }

  PLDB_close(db);

  return NULL;
}

static napi_value Database_start_transaction(napi_env env, napi_callback_info info) {
  napi_status status;

  napi_value this_arg;

  status = napi_get_cb_info(env, info, NULL, NULL, &this_arg, NULL);
  CHECK_STAT(status);

  Database* db = NULL;
  status = napi_unwrap(env, this_arg, (void*)&db);
  CHECK_STAT(status);

  int ec = PLDB_start_transaction(db, PLDB_TRANS_AUTO);
  if (ec < 0) {
    napi_throw_error(env, NULL, PLDB_error_msg());
    return NULL;
  }

  return NULL;
}

static napi_value Database_commit(napi_env env, napi_callback_info info) {
  napi_status status;

  napi_value this_arg;

  status = napi_get_cb_info(env, info, NULL, NULL, &this_arg, NULL);
  CHECK_STAT(status);

  Database* db = NULL;
  status = napi_unwrap(env, this_arg, (void*)&db);
  CHECK_STAT(status);

  int ec = PLDB_commit(db);
  if (ec < 0) {
    napi_throw_error(env, NULL, PLDB_error_msg());
    return NULL;
  }

  return NULL;
}

static napi_value Database_rollback(napi_env env, napi_callback_info info) {
  napi_status status;

  napi_value this_arg;

  status = napi_get_cb_info(env, info, NULL, NULL, &this_arg, NULL);
  CHECK_STAT(status);

  Database* db = NULL;
  status = napi_unwrap(env, this_arg, (void*)&db);
  CHECK_STAT(status);

  int ec = PLDB_rollback(db);

  if (ec < 0) {
    napi_throw_error(env, NULL, PLDB_error_msg());
    return NULL;
  }

  return NULL;
}

static void Database_finalize(napi_env env, void* finalize_data, void* finalize_hint) {
  if (finalize_data == NULL) {
    return;
  }
  PLDB_close((Database*)finalize_data);
}

static napi_value Database_constuctor(napi_env env, napi_callback_info info) {
  napi_status status;

  napi_value this_arg;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
  CHECK_STAT(status);

  if (!check_type(env, args[0], napi_string)) {
    napi_throw_type_error(env, NULL, "The first argument should be a string");
    return NULL;
  }

  static char path_buffer[BUFFER_SIZE];
  memset(path_buffer, 0, BUFFER_SIZE);

  size_t written_count = 0;
  status = napi_get_value_string_utf8(env, args[0], path_buffer, BUFFER_SIZE, &written_count);
  assert(status == napi_ok);

  Database* db = PLDB_open(path_buffer);
  if (db == NULL) {
    napi_throw_type_error(env, NULL, PLDB_error_msg());
    return NULL;
  }

  status = napi_wrap(env, this_arg, db, Database_finalize, 0, NULL);
  CHECK_STAT(status);

  return this_arg;
}

static void ObjectId_finalize(napi_env env, void* finalize_data, void* finalize_hint) {
  PLDB_free_object_id((DbObjectId*)finalize_data);
}

static napi_value ObjectId_constructor(napi_env env, napi_callback_info info) {
  napi_status status;

  napi_value this_arg;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
  CHECK_STAT(status);

  DbObjectId* oid = NULL;

  status = napi_get_value_external(env, args[0], (void**)&oid);

  if (oid == NULL) {
    napi_throw_error(env, NULL, "internal error: data is NULL");
    return NULL;
  }

  status = napi_wrap(env, this_arg, oid, ObjectId_finalize, NULL, NULL);
  CHECK_STAT(status);

  return this_arg;
}

static napi_value ObjectId_toString(napi_env env, napi_callback_info info) {
  napi_status status;

  napi_value this_arg;

  status = napi_get_cb_info(env, info, NULL, NULL, &this_arg, NULL);
  CHECK_STAT(status);

  DbObjectId* oid = NULL;
  status = napi_unwrap(env, this_arg, (void**)&oid);
  CHECK_STAT(status);

  static char buffer[16];
  memset(buffer, 0, 16);

  int ec = PLDB_object_id_to_hex(oid, buffer, 16);
  if (ec < 0) {
    napi_throw_error(env, NULL, PLDB_error_msg());
    return NULL;
  }

  napi_value result;

  status = napi_create_string_utf8(env, buffer, ec, &result);
  CHECK_STAT(status);

  return result;
}

static napi_status SetCallbackProp(napi_env env, napi_value exports, const char* key, napi_callback cb) {
  napi_status status;

  napi_property_descriptor desc = DECLARE_NAPI_METHOD(key, cb);
  status = napi_define_properties(env, exports, 1, &desc);

  return status;
}

static napi_value Database_Init(napi_env env, napi_value exports) {
  napi_status status;
  
  napi_value temp;
  napi_create_int64(env, 100, &temp);

  size_t db_prop_size = 6; 
  napi_property_descriptor db_props[] = {
    DECLARE_NAPI_METHOD("createCollection", Database_create_collection),
    DECLARE_NAPI_METHOD("collection", Database_collection),
    DECLARE_NAPI_METHOD("close", Database_close),
    DECLARE_NAPI_METHOD("startTransaction", Database_start_transaction),
    DECLARE_NAPI_METHOD("commit", Database_commit),
    DECLARE_NAPI_METHOD("rollback", Database_rollback),
    {NULL}
  };

  napi_value db_result;
  status = napi_define_class(
    env,
    "Database",
    NAPI_AUTO_LENGTH,
    Database_constuctor,
    NULL,
    db_prop_size,
    db_props,
    &db_result
  );
  CHECK_STAT(status);

  status = napi_set_named_property(env, exports, "Database", db_result);
  CHECK_STAT(status);

  return exports;
}

static napi_value Collection_Init(napi_env env, napi_value exports) {
  napi_status status;

  size_t collection_prop_size = 7;
  napi_property_descriptor collection_props[] = {
    DECLARE_NAPI_METHOD("insert", Collection_insert),
    DECLARE_NAPI_METHOD("find", Collection_find),
    DECLARE_NAPI_METHOD("count", Collection_count),
    DECLARE_NAPI_METHOD("update", Collection_update),
    DECLARE_NAPI_METHOD("delete", Collection_delete),
    DECLARE_NAPI_METHOD("deleteAll", Collection_delete_all),
    DECLARE_NAPI_METHOD("drop", Collection_drop),
    {NULL}
  };

  napi_value collection_result;
  status = napi_define_class(
    env,
    "Collection",
    NAPI_AUTO_LENGTH,
    Collection_constructor,
    NULL,
    collection_prop_size,
    collection_props,
    &collection_result
  );
  CHECK_STAT(status);

  status = napi_create_reference(
    env,
    collection_result,
    1,
    &collection_object_ref
  );
  CHECK_STAT(status);

  return exports;
}

static napi_value ObjectId_Init(napi_env env, napi_value exports) {
  napi_status status;

  size_t objectid_prop_size = 1;
  napi_property_descriptor objectid_props[] = {
    DECLARE_NAPI_METHOD("toString", ObjectId_toString),
    {NULL}
  };

  napi_value objectid_result;
  status = napi_define_class(
    env,
    "ObjectId",
    NAPI_AUTO_LENGTH,
    ObjectId_constructor,
    NULL,
    objectid_prop_size,
    objectid_props,
    &objectid_result
  );
  CHECK_STAT(status);

  status = napi_create_reference(env, objectid_result, 1, &objectid_ref);
  CHECK_STAT(status);

  status = napi_set_named_property(env, exports, "ObjectId", objectid_result);
  CHECK_STAT(status);

  return exports;
}

static napi_value Init(napi_env env, napi_value exports) {
  napi_status status;

#define REGISTER_CALLBACK(NAME, FUN) \
    status = SetCallbackProp(env, exports, NAME, FUN); \
    assert(status == napi_ok)

  REGISTER_CALLBACK("version", db_version);

  exports = ObjectId_Init(env, exports);
  if (exports == NULL) {
    return NULL;
  }

  exports = Database_Init(env, exports);
  if (exports == NULL) {
    return NULL;
  }

  exports = Collection_Init(env, exports);
  if (exports == NULL) {
    return NULL;
  }

  return exports;
}

NAPI_MODULE(polodb, Init)
