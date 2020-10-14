#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <node_api.h>

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

static void DbValue_finalize(napi_env env, void* finalize_data, void* finalize_hint) {
  DbValue* val = (DbValue*)finalize_data;
  PLDB_free_value(val);
}

static void DbDocument_finalize(napi_env env, void* data, void* hint) {
  PLDB_free_doc((DbDocument*)data);
}

static void DbObjectId_finalize(napi_env env, void* data, void* hint) {
  PLDB_free_object_id((DbObjectId*)data);
}

static void DbArray_finalize(napi_env env, void* data, void* hint) {
  PLDB_free_arr((DbArray*)data);
}

static void DbHandle_finalize(napi_env env, void* data, void* hint) {
  PLDB_free_handle((DbHandle*)data);
}

static void DbDocumentIter_finalize(napi_env env, void* data, void* hint) {
  PLDB_free_doc_iter((DbDocumentIter*)data);
}

static void DbUTDDateTime_finalize(napi_env env, void* data, void* hint) {
  PLDB_free_UTCDateTime((DbUTCDateTime*)data);
}

static int check_type(napi_env env, napi_value value, napi_valuetype expected) {
  napi_status status;
  napi_valuetype actual;

  status = napi_typeof(env, value, &actual);
  assert(status == napi_ok);

  return actual == expected;
}

static napi_value js_mk_null(napi_env env, napi_callback_info info) {
  napi_status status;

  napi_value result;
  DbValue* val = PLDB_mk_null();
  status = napi_create_external(env, (void*)val, DbValue_finalize, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_mk_double(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_number)) {
    napi_throw_type_error(env, NULL, "Wrong arguments");
    return NULL;
  }

  double num = 0;
  status = napi_get_value_double(env, args[0], &num);
  assert(status == napi_ok);

  napi_value result;
  DbValue* val = PLDB_mk_double(num);

  status = napi_create_external(env, (void*)val, DbValue_finalize, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_mk_int(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_number)) {
    napi_throw_type_error(env, NULL, "Wrong arguments");
    return NULL;
  }

  long long num = 0;
  status = napi_get_value_int64(env, args[0], &num);
  assert(status == napi_ok);

  napi_value result;
  DbValue* val = PLDB_mk_int(num);

  status = napi_create_external(env, (void*)val, DbValue_finalize, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_mk_bool(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_boolean)) {
    napi_throw_type_error(env, NULL, "Wrong arguments");
    return NULL;
  }

  bool bl = 0;
  status = napi_get_value_bool(env, args[0], &bl);
  assert(status == napi_ok);

  napi_value result;
  DbValue* val = PLDB_mk_bool((int)bl);

  status = napi_create_external(env, (void*)val, DbValue_finalize, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_mk_str(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_string)) {
    napi_throw_type_error(env, NULL, "Wrong arguments");
    return NULL;
  }

  size_t str_size = 0;
  status = napi_get_value_string_utf8(env, args[0], NULL, 0, &str_size);
  assert(status == napi_ok);

  char* buffer = (char*)malloc(str_size + 1);
  memset(buffer, 0, str_size + 1);

  status = napi_get_value_string_utf8(env, args[0], buffer, str_size + 1, &str_size);
  assert(status == napi_ok);

  napi_value result = NULL;
  DbValue* val = PLDB_mk_str(buffer);
  if (val == NULL) {
    napi_throw_type_error(env, NULL, PLDB_error_msg());
    goto clean;
  }

  status = napi_create_external(env, (void*)val, DbValue_finalize, NULL, &result);
  assert(status == napi_ok);

clean:
  free(buffer);
  return result;
}

static napi_value js_mk_doc_iter(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments");
    return NULL;
  }

  DbDocument* doc = NULL;
  status = napi_get_value_external(env, args[0], (void**)&doc);
  assert(status == napi_ok);

  DbDocumentIter* iter = PLDB_doc_iter(doc);
  napi_value result = NULL;

  status = napi_create_external(env, (void*)iter, DbDocumentIter_finalize, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_doc_iter_next(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments");
    return NULL;
  }

  DbDocumentIter* iter = NULL;
  status = napi_get_value_external(env, args[0], (void**)&iter);
  assert(status == napi_ok);
  
  static char KEY_BUFFER[BUFFER_SIZE];
  memset(KEY_BUFFER, 0, BUFFER_SIZE);

  DbValue* out_val;
  int copied_size = PLDB_doc_iter_next(iter, KEY_BUFFER, BUFFER_SIZE, &out_val);
  if (copied_size < 0) {
    napi_throw_type_error(env, NULL, "buffer not enough");
    return NULL;
  }

  if (copied_size == 0) { // no next
    return NULL;
  }

  napi_value js_key = NULL;
  napi_value js_value = NULL;

  status = napi_create_string_utf8(env, KEY_BUFFER, copied_size, &js_key);
  assert(status == napi_ok);

  status = napi_create_external(env, (void*)out_val, DbValue_finalize, NULL, &js_value);
  assert(status == napi_ok);

  napi_value arr;
  status = napi_create_array(env, &arr);
  assert(status == napi_ok);

  napi_set_element(env, arr, 0, js_key);
  napi_set_element(env, arr, 1, js_value);

  return arr;
}

static napi_value js_mk_utc_datetime(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  napi_valuetype ty;

  status = napi_typeof(env, args[0], &ty);
  assert(status == napi_ok);

  long long ts = 0;
  if (ty == napi_undefined) {
    ts = -1;
  } else if (ty == napi_number) {
    status = napi_get_value_int64(env, args[0], &ts);
    assert(status == napi_ok);
  } else {
    napi_throw_type_error(env, NULL, "Wrong arguments");
    return NULL;
  }

  DbUTCDateTime* dt = PLDB_mk_UTCDateTime(ts);

  napi_value val;

  status = napi_create_external(env, (void*)dt, DbUTDDateTime_finalize, NULL, &val);
  assert(status == napi_ok);

  return val;
}

static napi_value js_utd_datetime_to_value(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "the first argument should be an external object");
    return NULL;
  }

  void* time_raw = NULL;
  status = napi_get_value_external(env, args[0], &time_raw);
  assert(status == napi_ok);

  DbUTCDateTime* dt = (DbUTCDateTime*)time_raw;
  DbValue* val = PLDB_UTCDateTime_to_value(dt);

  napi_value result = NULL;

  status = napi_create_external(env, (void*)val, &DbValue_finalize, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_value_type(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments");
    return NULL;
  }

  DbValue* raw_value;
  status = napi_get_value_external(env, args[0], (void**)&raw_value);
  assert(status == napi_ok);

  int ty = PLDB_value_type(raw_value);

  napi_value result = NULL;
  status = napi_create_int32(env, ty, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_value_get_i64(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments");
    return NULL;
  }

  DbValue *val;
  status = napi_get_value_external(env, args[0], (void**)&val);
  assert(status == napi_ok);

  long long out = 0;
  if (PLDB_value_get_i64(val, &out) != 0) {
    napi_throw_type_error(env, NULL, "DbValue is not an integer");
    return NULL;
  }

  napi_value result;
  status = napi_create_int64(env, out, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_value_get_bool(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments");
    return NULL;
  }

  DbValue *val;
  status = napi_get_value_external(env, args[0], (void**)&val);
  assert(status == napi_ok);

  int result = PLDB_value_get_bool(val);
  if (result < 0) {
    napi_throw_type_error(env, NULL, "value is not a boolean");
    return NULL;
  }

  napi_value num;
  status = napi_create_int32(env ,result, &num);
  assert(status == napi_ok);

  napi_value bl;
  status = napi_coerce_to_bool(env, num, &bl);
  assert(status == napi_ok);

  return bl;
}

static napi_value js_value_get_double(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments");
    return NULL;
  }

  DbValue *val;
  status = napi_get_value_external(env, args[0], (void**)&val);
  assert(status == napi_ok);

  double num = 0;
  status = PLDB_value_get_double(val, &num);
  assert(status == napi_ok);

  napi_value result;
  status = napi_create_double(env, num, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_value_get_array(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments");
    return NULL;
  }

  DbValue *val = NULL;
  status = napi_get_value_external(env, args[0], (void**)&val);
  assert(status == napi_ok);

  DbArray* arr = NULL;
  if (PLDB_value_get_array(val, &arr) < 0) {
    napi_throw_type_error(env, NULL, "value is not an array");
    return NULL;
  }

  napi_value result;
  status = napi_create_external(env, (void*)arr, DbArray_finalize, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_value_get_doc(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments");
    return NULL;
  }

  DbValue *val = NULL;
  status = napi_get_value_external(env, args[0], (void**)&val);
  assert(status == napi_ok);

  DbDocument* doc = NULL;
  if (PLDB_value_get_document(val, &doc) < 0) {
    napi_throw_type_error(env, NULL, "value is not a array");
    return NULL;
  }

  napi_value result;
  status = napi_create_external(env, (void*)doc, DbDocument_finalize, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_value_get_object_id(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments");
    return NULL;
  }

  DbValue *val = NULL;
  status = napi_get_value_external(env, args[0], (void**)&val);
  assert(status == napi_ok);

  DbObjectId* oid = NULL;
  if (PLDB_value_get_object_id(val, &oid) < 0) {
    napi_throw_type_error(env, NULL, "value is not an ObjectId");
    return NULL;
  }

  napi_value result;
  status = napi_create_external(env, (void*)oid, DbObjectId_finalize, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_value_get_string(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments");
    return NULL;
  }

  DbValue *val;
  status = napi_get_value_external(env, args[0], (void**)&val);
  assert(status == napi_ok);

  const char* content = NULL;
  int len = PLDB_value_get_string_utf8(val, &content);
  if (len < 0) {
    napi_throw_type_error(env, NULL, "DbValue is not a string");
    return NULL;
  }

  napi_value result;
  status = napi_create_string_utf8(env, content, len, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_open(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  napi_valuetype valuetype0;

  status = napi_typeof(env, args[0], &valuetype0);
  assert(status == napi_ok);

  if (valuetype0 != napi_string) {
    napi_throw_type_error(env, NULL, "Wrong arguments");
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

  napi_value result;
  status = napi_create_external(env, (void*)db, NULL, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_mk_array(napi_env env, napi_callback_info info) {
  napi_status status;

  DbArray* arr = PLDB_mk_arr();
  if (arr == NULL) {
    napi_throw_type_error(env, NULL, PLDB_error_msg());
    return NULL;
  }

  napi_value result;
  status = napi_create_external(env, (void*)arr, &DbArray_finalize, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_array_len(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "the first argument should be an external object");
    return NULL;
  }

  void* arr_raw = NULL;
  status = napi_get_value_external(env, args[0], &arr_raw);
  assert(status == napi_ok);

  unsigned int size = PLDB_arr_len((DbArray*)arr_raw);

  napi_value result = NULL;
  status = napi_create_uint32(env, size, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_array_get(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 2;
  napi_value args[2];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "the first argument should be an external object");
    return NULL;
  }

  if (!check_type(env, args[1], napi_number)) {
    napi_throw_type_error(env, NULL, "the second argument should be a number");
    return NULL;
  }

  void* arr_raw = NULL;
  status = napi_get_value_external(env, args[0], &arr_raw);
  assert(status == napi_ok);

  unsigned int index = 0;
  status = napi_get_value_uint32(env, args[1], &index);
  assert(status == napi_ok);

  DbValue* out_val = NULL;
  PLDB_arr_get((DbArray*)arr_raw, index, &out_val);

  return NULL;
}

static napi_value js_array_push(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 2;
  napi_value args[2];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "the first argument should be an external object");
    return NULL;
  }

  if (!check_type(env, args[1], napi_external)) {
    napi_throw_type_error(env, NULL, "the second argument should be an external");
    return NULL;
  }

  void* arr_raw = NULL;
  void* val_raw = NULL;

  status = napi_get_value_external(env, args[0], &arr_raw);
  assert(status == napi_ok);

  status = napi_get_value_external(env, args[1], &val_raw);
  assert(status == napi_ok);

  PLDB_arr_push((DbArray*)arr_raw, (DbValue*)val_raw);

  return NULL;
}

static napi_value js_mk_document(napi_env env, napi_callback_info info) {
  napi_status status;

  DbDocument* doc = PLDB_mk_doc();
  if (doc == NULL) {
    napi_throw_type_error(env, NULL, PLDB_error_msg());
    return NULL;
  }

  napi_value result;
  status = napi_create_external(env, (void*)doc, &DbDocument_finalize, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_mk_object_id(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "the first argument should be an external object");
    return NULL;
  }

  void* db_raw = NULL;
  status = napi_get_value_external(env, args[0], &db_raw);
  assert(status == napi_ok);

  napi_value result = NULL;

  DbObjectId* oid = PLDB_mk_object_id((Database*)db_raw);
  if (oid == NULL) {
    goto clean;
  }

  status = napi_create_external(env, (void*)oid, &DbObjectId_finalize, NULL, &result);
  assert(status == napi_ok);

clean:
  return result;
}

static napi_value js_oid2value(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "the first argument should be an external object");
    return NULL;
  }

  void* oid_raw = NULL;
  status = napi_get_value_external(env, args[0], &oid_raw);
  assert(status == napi_ok);

  DbObjectId* oid = (DbObjectId*)oid_raw;
  DbValue* val = PLDB_object_id_to_value(oid);

  napi_value result = NULL;

  status = napi_create_external(env, (void*)val, &DbValue_finalize, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_doc2value(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "the first argument should be an external object");
    return NULL;
  }

  void* oid_raw = NULL;
  status = napi_get_value_external(env, args[0], &oid_raw);
  assert(status == napi_ok);

  DbDocument* oid = (DbDocument*)oid_raw;
  DbValue* val = PLDB_doc_to_value(oid);

  napi_value result = NULL;

  status = napi_create_external(env, (void*)val, &DbValue_finalize, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_oid2hex(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "the first argument should be an external object");
    return NULL;
  }

  void* oid_raw = NULL;
  status = napi_get_value_external(env, args[0], &oid_raw);
  assert(status == napi_ok);

  DbObjectId* oid = (DbObjectId*)oid_raw;
  
  static char buffer[OID_HEX_BUFFER_SIZE];
  memset(buffer, 0, OID_HEX_BUFFER_SIZE);

  PLDB_object_id_to_hex(oid, buffer, OID_HEX_BUFFER_SIZE);

  napi_value result = NULL;

  status = napi_create_string_utf8(env, buffer, strlen(buffer), &result);

  return result;
}

static napi_value js_doc_set(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 3;
  napi_value args[3];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "the first argument should be an external object");
    return NULL;
  }

  if (!check_type(env, args[1], napi_string)) {
    napi_throw_type_error(env, NULL, "the second argument should be a string");
    return NULL;
  }

  if (!check_type(env, args[2], napi_external)) {
    napi_throw_type_error(env, NULL, "the third argument should be an external object");
    return NULL;
  }

  void* raw_doc;
  status = napi_get_value_external(env, args[0], &raw_doc);
  assert(status == napi_ok);

  size_t key_size = 0;
  status = napi_get_value_string_utf8(env, args[1], NULL, 0, &key_size);
  assert(status == napi_ok);

  char* key_buffer = (char*)malloc(sizeof(char) * (key_size + 1));
  memset(key_buffer, 0, key_size + 1);

  status = napi_get_value_string_utf8(env, args[1], key_buffer, key_size + 1, &key_size);
  assert(status == napi_ok);

  void* set_value;
  status = napi_get_value_external(env, args[2], &set_value);
  assert(status == napi_ok);

  PLDB_doc_set((DbDocument*)raw_doc, key_buffer, (DbValue*)set_value);
  
clean:
  free(key_buffer);
  return NULL;
}

static napi_value js_doc_get(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 2;
  napi_value args[2];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "the first argument should be an external object");
    return NULL;
  }

  if (!check_type(env, args[1], napi_string)) {
    napi_throw_type_error(env, NULL, "the second argument should be a string");
    return NULL;
  }

  void* raw_doc;
  status = napi_get_value_external(env, args[0], &raw_doc);
  assert(status == napi_ok);

  size_t key_size = 0;
  status = napi_get_value_string_utf8(env, args[1], NULL, 0, &key_size);
  assert(status == napi_ok);

  char* key_buffer = (char*)malloc(sizeof(char) * (key_size + 1));
  memset(key_buffer, 0, key_size + 1);

  status = napi_get_value_string_utf8(env, args[1], key_buffer, key_size + 1, &key_size);
  assert(status == napi_ok);

  napi_value result = NULL;

  DbValue* out_val = NULL;
  int ec = PLDB_doc_get((DbDocument*)raw_doc, key_buffer, &out_val);
  if (ec < 0) {
    napi_throw_type_error(env, NULL, PLDB_error_msg());
    goto clean;
  }

  // not found
  if (out_val == NULL) {
    goto clean;
  }

  status = napi_create_external(env, (void*)out_val, DbValue_finalize, NULL, &result);
  assert(status == napi_ok);

clean:
  free(key_buffer);
  return result;
}

static napi_value js_doc_len(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments 0");
    return NULL;
  }

  DbDocument* doc = NULL;
  status = napi_get_value_external(env, args[0], (void**)&doc);
  assert(status == napi_ok);

  int len = PLDB_doc_len(doc);

  napi_value result;
  status = napi_create_int32(env, len, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_create_collection(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 2;
  napi_value args[2];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments 0");
    return NULL;
  }

  if (!check_type(env, args[1], napi_string)) {
    napi_throw_type_error(env, NULL, "Wrong arguments 1");
    return NULL;
  }

  Database* db;
  status = napi_get_value_external(env, args[0], (void**)&db);
  assert(status == napi_ok);

  static char name_buffer[BUFFER_SIZE];
  memset(name_buffer, 0, BUFFER_SIZE);

  size_t written_count = 0;
  status = napi_get_value_string_utf8(env, args[1], name_buffer, BUFFER_SIZE, &written_count);
  assert(status == napi_ok);

  int ec = 0;
  STD_CALL(PLDB_create_collection(db, name_buffer));

  return NULL;
}

static napi_value js_start_transaction(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 2;
  napi_value args[2];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments 0");
    return NULL;
  }

  if (!check_type(env, args[1], napi_number)) {
    napi_throw_type_error(env, NULL, "Wrong arguments 1");
    return NULL;
  }

  Database* db;
  status = napi_get_value_external(env, args[0], (void**)&db);
  assert(status == napi_ok);

  int flags = 0;
  status = napi_get_value_int32(env, args[1], &flags);
  assert(status == napi_ok);

  int ec = PLDB_start_transaction(db, flags);
  if (ec != 0) {
    napi_throw_type_error(env, NULL, PLDB_error_msg());
    return NULL;
  }

  return NULL;
}

static napi_value js_commit(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments 0");
    return NULL;
  }

  Database* db;
  status = napi_get_value_external(env, args[0], (void**)&db);
  assert(status == napi_ok);

  int ec = PLDB_commit(db);
  if (ec != 0) {
    napi_throw_type_error(env, NULL, PLDB_error_msg());
    return NULL;
  }

  return NULL;
}

static napi_value js_rollback(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments 0");
    return NULL;
  }

  Database* db;
  status = napi_get_value_external(env, args[0], (void**)&db);
  assert(status == napi_ok);

  int ec = PLDB_rollback(db);
  if (ec != 0) {
    napi_throw_type_error(env, NULL, PLDB_error_msg());
    return NULL;
  }

  return NULL;
}

static napi_value js_insert(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 3;
  napi_value args[3];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  // database
  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments 0");
    return NULL;
  }

  // col name
  if (!check_type(env, args[1], napi_string)) {
    napi_throw_type_error(env, NULL, "Wrong arguments 1");
    return NULL;
  }

  // doc
  if (!check_type(env, args[2], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments 2");
    return NULL;
  }

  Database* db = NULL;
  status = napi_get_value_external(env, args[0], (void**)&db);
  assert(status == napi_ok);

  static char name_buffer[BUFFER_SIZE];
  memset(name_buffer, 0, BUFFER_SIZE);

  size_t count = 0;
  status = napi_get_value_string_utf8(env, args[1], name_buffer, BUFFER_SIZE, &count);
  assert(status == napi_ok);

  DbDocument* doc = NULL;
  status = napi_get_value_external(env ,args[2], (void**)&doc);
  assert(status == napi_ok);

  int ec = 0;
  STD_CALL(PLDB_insert(db, name_buffer, doc));

  return NULL;
}

static napi_value js_find(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 3;
  napi_value args[3];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments 0");
    return NULL;
  }

  if (!check_type(env, args[1], napi_string)) {
    napi_throw_type_error(env, NULL, "Wrong arguments 1");
    return NULL;
  }

  Database* db;
  status = napi_get_value_external(env, args[0], (void**)&db);
  assert(status == napi_ok);

  static char name_buffer[BUFFER_SIZE];
  memset(name_buffer, 0, BUFFER_SIZE);

  size_t written_count = 0;
  status = napi_get_value_string_utf8(env, args[1], name_buffer, BUFFER_SIZE, &written_count);
  assert(status == napi_ok);

  napi_valuetype query_doc_type;

  status = napi_typeof(env, args[2], &query_doc_type);
  assert(status == napi_ok);

  DbDocument* query_doc;

  if (query_doc_type == napi_undefined || query_doc_type == napi_null) {
    query_doc = NULL;
  } else if (query_doc_type == napi_external) {
    status = napi_get_value_external(env, args[2], (void**)&query_doc);
    assert(status == napi_ok);
  } else {
    napi_throw_type_error(env, NULL, "Wrong arguments 2");
    return NULL;
  }

  DbHandle* handle = NULL;
  int ec = 0;
  STD_CALL(PLDB_find(db, name_buffer, query_doc, &handle));

  napi_value result = NULL;
  status = napi_create_external(env, (void*)handle, &DbHandle_finalize, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_update(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 4;
  napi_value args[4];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments 0");
    return NULL;
  }

  Database* db = NULL;
  status = napi_get_value_external(env, args[0], (void**)&db);
  assert(status == napi_ok);

  static char name_buffer[BUFFER_SIZE];
  memset(name_buffer, 0, BUFFER_SIZE);

  size_t written_count = 0;
  status = napi_get_value_string_utf8(env, args[1], name_buffer, BUFFER_SIZE, &written_count);
  assert(status == napi_ok);

  napi_valuetype second_arg_ty;

  status = napi_typeof(env, args[2], &second_arg_ty);
  assert(status == napi_ok);
  
  DbDocument* query;
  if (second_arg_ty == napi_undefined) {
    query = NULL;
  } else if (second_arg_ty == napi_external) {
    status = napi_get_value_external(env, args[2], (void**)&query);
    assert(status == napi_ok);
  } else {
    napi_throw_type_error(env, NULL, "Wrong arguments 2");
    return NULL;
  }

  if (!check_type(env, args[3], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments 3");
    return NULL;
  }

  DbDocument* update;
  status = napi_get_value_external(env, args[3], (void**)&update);
  assert(status == napi_ok);

  long long ec = 0;
  STD_CALL(PLDB_update(db, name_buffer, query, update));

  napi_value result;
  status = napi_create_int64(env, ec, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_delete(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 3;
  napi_value args[3];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments 0");
    return NULL;
  }

  if (!check_type(env, args[1], napi_string)) {
    napi_throw_type_error(env, NULL, "Wrong arguments 1");
    return NULL;
  }

  if (!check_type(env, args[2], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments 2");
    return NULL;
  }

  Database* db;
  status = napi_get_value_external(env, args[0], (void**)&db);
  assert(status == napi_ok);

  static char name_buffer[BUFFER_SIZE];
  memset(name_buffer, 0, BUFFER_SIZE);

  size_t written_count = 0;
  status = napi_get_value_string_utf8(env, args[1], name_buffer, BUFFER_SIZE, &written_count);
  assert(status == napi_ok);

  DbDocument* query_doc;
  status = napi_get_value_external(env, args[2], (void**)&query_doc);
  assert(status == napi_ok);

  long long ec = 0;
  STD_CALL(PLDB_delete(db, name_buffer, query_doc));

  napi_value result;
  status = napi_create_int64(env, ec, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_delete_all(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 2;
  napi_value args[2];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments 0");
    return NULL;
  }

  if (!check_type(env, args[1], napi_string)) {
    napi_throw_type_error(env, NULL, "Wrong arguments 1");
    return NULL;
  }

  Database* db;
  status = napi_get_value_external(env, args[0], (void**)&db);
  assert(status == napi_ok);

  static char name_buffer[BUFFER_SIZE];
  memset(name_buffer, 0, BUFFER_SIZE);

  size_t written_count = 0;
  status = napi_get_value_string_utf8(env, args[1], name_buffer, BUFFER_SIZE, &written_count);
  assert(status == napi_ok);

  long long ec = 0;
  STD_CALL(PLDB_delete_all(db, name_buffer));

  napi_value result;
  status = napi_create_int64(env, ec, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_handle_step(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "The first argument should be Database");
    return NULL;
  }

  DbHandle* handle;
  status = napi_get_value_external(env, args[0], (void**)&handle);
  assert(status == napi_ok);

  int ec = 0;
  STD_CALL(PLDB_handle_step(handle));

  return NULL;
}

static napi_value js_handle_to_str(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "The first argument should be Database");
    return NULL;
  }

  DbHandle* handle;
  status = napi_get_value_external(env, args[0], (void**)&handle);
  assert(status == napi_ok);

  static char buffer[BUFFER_SIZE];
  memset(buffer, 0, BUFFER_SIZE);

  int ec = PLDB_handle_to_str(handle, buffer, BUFFER_SIZE);
  if (ec < 0) {
    napi_throw_type_error(env, NULL, "buffer not enough");
    return NULL;
  }

  napi_value result;
  status = napi_create_string_utf8(env, buffer, ec, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_handle_get(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "The first argument should be Database");
    return NULL;
  }

  DbHandle* handle = NULL;
  status = napi_get_value_external(env, args[0], (void**)&handle);
  assert(status == napi_ok);

  DbValue* value = NULL;
  PLDB_handle_get(handle, &value);

  napi_value result = NULL;

  status = napi_create_external(env, (void*)value, &DbValue_finalize, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value js_close(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "The first argument should be Database");
    return NULL;
  }

  Database* db;
  status = napi_get_value_external(env, args[0], (void**)&db);
  assert(status == napi_ok);

  PLDB_close(db);

  return NULL;
}

static napi_value js_handle_state(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!check_type(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "The first argument should be Database");
    return NULL;
  }

  DbHandle* handle;
  status = napi_get_value_external(env, args[0], (void**)&handle);
  assert(status == napi_ok);

  napi_value result = NULL;

  int state = PLDB_handle_state(handle);
  status = napi_create_int32(env, state, &result);
  assert(status == napi_ok);

  return result;
}

#define DECLARE_NAPI_METHOD(name, func)                                        \
  { name, 0, func, 0, 0, 0, napi_default, 0 }

static napi_status SetCallbackProp(napi_env env, napi_value exports, const char* key, napi_callback cb) {
  napi_status status;

  napi_property_descriptor desc = DECLARE_NAPI_METHOD(key, cb);
  status = napi_define_properties(env, exports, 1, &desc);

  return status;
}

static napi_value Init(napi_env env, napi_value exports) {
  napi_status status;

#define REGISTER_CALLBACK(NAME, FUN) \
    status = SetCallbackProp(env, exports, NAME, FUN); \
    assert(status == napi_ok)

  REGISTER_CALLBACK("open", js_open);
  REGISTER_CALLBACK("close", js_close);
  REGISTER_CALLBACK("makeDocument", js_mk_document);
  REGISTER_CALLBACK("documentSet", js_doc_set);
  REGISTER_CALLBACK("documentGet", js_doc_get);
  REGISTER_CALLBACK("documentLen", js_doc_len);
  REGISTER_CALLBACK("arrayLen", js_array_len);
  REGISTER_CALLBACK("arrayGet", js_array_get);
  REGISTER_CALLBACK("arrayPush", js_array_push);
  REGISTER_CALLBACK("mkNull", js_mk_null);
  REGISTER_CALLBACK("mkInt", js_mk_int);
  REGISTER_CALLBACK("mkBool", js_mk_bool);
  REGISTER_CALLBACK("mkDouble", js_mk_double);
  REGISTER_CALLBACK("mkString", js_mk_str);
  REGISTER_CALLBACK("mkObjectId", js_mk_object_id);
  REGISTER_CALLBACK("mkArray", js_mk_array);
  REGISTER_CALLBACK("mkDocIter", js_mk_doc_iter);
  REGISTER_CALLBACK("mkUTCDateTime", js_mk_utc_datetime);
  REGISTER_CALLBACK("UTCDateTimeToValue", js_utd_datetime_to_value);
  REGISTER_CALLBACK("docIterNext", js_doc_iter_next);
  REGISTER_CALLBACK("docToValue", js_doc2value);
  REGISTER_CALLBACK("objectIdToValue", js_oid2value);
  REGISTER_CALLBACK("objectIdToHex", js_oid2hex);
  REGISTER_CALLBACK("valueType", js_value_type);
  REGISTER_CALLBACK("valueGetNumber", js_value_get_i64);
  REGISTER_CALLBACK("valueGetString", js_value_get_string);
  REGISTER_CALLBACK("valueGetBool", js_value_get_bool);
  REGISTER_CALLBACK("valueGetDouble", js_value_get_double);
  REGISTER_CALLBACK("valueGetArray", js_value_get_array);
  REGISTER_CALLBACK("valueGetDocument", js_value_get_doc);
  REGISTER_CALLBACK("valueGetObjectId", js_value_get_object_id);
  REGISTER_CALLBACK("createCollection", js_create_collection);
  REGISTER_CALLBACK("startTransaction", js_start_transaction);
  REGISTER_CALLBACK("commit", js_commit);
  REGISTER_CALLBACK("rollback", js_rollback);
  REGISTER_CALLBACK("dbInsert", js_insert);
  REGISTER_CALLBACK("dbFind", js_find);
  REGISTER_CALLBACK("dbUpdate", js_update);
  REGISTER_CALLBACK("dbDelete", js_delete);
  REGISTER_CALLBACK("dbDeleteAll", js_delete_all);
  REGISTER_CALLBACK("dbHandleStep", js_handle_step);
  REGISTER_CALLBACK("dbHandleState", js_handle_state);
  REGISTER_CALLBACK("dbHandleGet", js_handle_get);
  REGISTER_CALLBACK("dbHandleToStr", js_handle_to_str);
  REGISTER_CALLBACK("version", db_version);

  return exports;
}

NAPI_MODULE(polodb, Init)
