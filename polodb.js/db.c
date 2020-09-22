#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <node_api.h>

#define BUFFER_SIZE 512
#define VALUE_NAME_BUFFER_SIZE 64

#include "./polodb.h"

static napi_value Version(napi_env env, napi_callback_info info) {
  static char buffer[BUFFER_SIZE];
  memset(buffer, 0, BUFFER_SIZE);
  PLDB_version(buffer, BUFFER_SIZE);

  napi_status status;
  napi_value world;
  status = napi_create_string_utf8(env, buffer, strlen(buffer), &world);
  assert(status == napi_ok);
  return world;
}

static void DbValueFinalize(napi_env env, void* finalize_data, void* finalize_hint) {
  struct DbValue* val = (struct DbValue*)finalize_data;
  PLDB_free_value(val);
}

static void DbDocumentFinalize(napi_env env, void* data, void* hint) {
  PLDB_free_doc((struct DbDocument*)data);
}

static int CheckType(napi_env env, napi_value value, napi_valuetype expected) {
  napi_status status;
  napi_valuetype actual;

  status = napi_typeof(env, value, &actual);
  assert(status == napi_ok);

  return actual == expected;
}

static napi_value MkNull(napi_env env, napi_callback_info info) {
  napi_status status;

  napi_value result;
  struct DbValue* val = PLDB_mk_null();
  status = napi_create_external(env, (void*)val, DbValueFinalize, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value MkDouble(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!CheckType(env, args[0], napi_number)) {
    napi_throw_type_error(env, NULL, "Wrong arguments");
    return NULL;
  }

  double num = 0;
  status = napi_get_value_double(env, args[0], &num);
  assert(status == napi_ok);

  napi_value result;
  struct DbValue* val = PLDB_mk_double(num);

  status = napi_create_external(env, (void*)val, DbValueFinalize, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value MkInt(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!CheckType(env, args[0], napi_number)) {
    napi_throw_type_error(env, NULL, "Wrong arguments");
    return NULL;
  }

  long long num = 0;
  status = napi_get_value_int64(env, args[0], &num);
  assert(status == napi_ok);

  napi_value result;
  struct DbValue* val = PLDB_mk_int(num);

  status = napi_create_external(env, (void*)val, DbValueFinalize, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value MkBool(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!CheckType(env, args[0], napi_boolean)) {
    napi_throw_type_error(env, NULL, "Wrong arguments");
    return NULL;
  }

  bool bl = 0;
  status = napi_get_value_bool(env, args[0], &bl);
  assert(status == napi_ok);

  napi_value result;
  struct DbValue* val = PLDB_mk_bool((int)bl);

  status = napi_create_external(env, (void*)val, DbValueFinalize, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value MkStr(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!CheckType(env, args[0], napi_string)) {
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
  struct DbValue* val = PLDB_mk_str(buffer);
  if (val == NULL) {
    napi_throw_type_error(env, NULL, PLDB_error_msg());
    goto clean;
  }

  status = napi_create_external(env, (void*)val, DbValueFinalize, NULL, &result);
  assert(status == napi_ok);

clean:
  free(buffer);
  return result;
}

static napi_value ValueTypeName(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!CheckType(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments");
    return NULL;
  }

  void* raw_value;
  status = napi_get_value_external(env, args[0], &raw_value);
  assert(status == napi_ok);

  static char buffer[VALUE_NAME_BUFFER_SIZE];
  memset(buffer, 0, VALUE_NAME_BUFFER_SIZE);

  int size = PLDB_value_type_name((struct DbValue*)raw_value, buffer, VALUE_NAME_BUFFER_SIZE);

  napi_value result = NULL;
  status = napi_create_string_utf8(env, buffer, size, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value Open(napi_env env, napi_callback_info info) {
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

  struct Database* db = PLDB_open(path_buffer);
  if (db == NULL) {
    napi_throw_type_error(env, NULL, PLDB_error_msg());
    return NULL;
  }

  napi_value result;
  status = napi_create_external(env, (void*)db, NULL, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value MkDocument(napi_env env, napi_callback_info info) {
  napi_status status;

  struct DbDocument* doc = PLDB_mk_doc();
  if (doc == NULL) {
    napi_throw_type_error(env, NULL, PLDB_error_msg());
    return NULL;
  }

  napi_value result;
  status = napi_create_external(env, (void*)doc, &DbDocumentFinalize, NULL, &result);
  assert(status == napi_ok);

  return result;
}

static napi_value DocSet(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 3;
  napi_value args[3];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!CheckType(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "the first argument should be an external object");
    return NULL;
  }

  if (!CheckType(env, args[1], napi_string)) {
    napi_throw_type_error(env, NULL, "the second argument should be a string");
    return NULL;
  }

  if (!CheckType(env, args[2], napi_external)) {
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

  PLDB_doc_set((struct DbDocument*)raw_doc, key_buffer, (struct DbValue*)set_value);
  
clean:
  free(key_buffer);
  return NULL;
}

static napi_value DocGet(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 2;
  napi_value args[2];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!CheckType(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "the first argument should be an external object");
    return NULL;
  }

  if (!CheckType(env, args[1], napi_string)) {
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

  struct DbValue* out_val = NULL;
  int ec = PLDB_doc_get((struct DbDocument*)raw_doc, key_buffer, &out_val);
  if (ec < 0) {
    napi_throw_type_error(env, NULL, PLDB_error_msg());
    goto clean;
  }

  // not found
  if (out_val == NULL) {
    goto clean;
  }

  status = napi_create_external(env, (void*)out_val, DbValueFinalize, NULL, &result);
  assert(status == napi_ok);

clean:
  free(key_buffer);
  return result;
}

static napi_value CreateCollection(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 2;
  napi_value args[2];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!CheckType(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "Wrong arguments 0");
    return NULL;
  }

  if (!CheckType(env, args[1], napi_string)) {
    napi_throw_type_error(env, NULL, "Wrong arguments 1");
    return NULL;
  }

  void* db_raw;
  status = napi_get_value_external(env, args[0], &db_raw);
  assert(status == napi_ok);

  static char name_buffer[BUFFER_SIZE];
  memset(name_buffer, 0, BUFFER_SIZE);

  size_t written_count = 0;
  status = napi_get_value_string_utf8(env, args[0], name_buffer, BUFFER_SIZE, &written_count);
  assert(status == napi_ok);

  struct Database* db = (struct Database*)db_raw;
  int ec = PLDB_create_collection(db, name_buffer);
  if (ec != 0) {
    napi_throw_type_error(env, NULL, PLDB_error_msg());
    return NULL;
  }

  return NULL;
}

static napi_value Close(napi_env env, napi_callback_info info) {
  napi_status status;

  size_t argc = 1;
  napi_value args[1];
  status = napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  assert(status == napi_ok);

  if (!CheckType(env, args[0], napi_external)) {
    napi_throw_type_error(env, NULL, "The first argument should be Database");
    return NULL;
  }

  void* db;
  status = napi_get_value_external(env, args[0], &db);
  assert(status == napi_ok);


  PLDB_close((struct Database*)db);

  return NULL;
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

  REGISTER_CALLBACK("open", Open);
  REGISTER_CALLBACK("close", Close);
  REGISTER_CALLBACK("makeDocument", MkDocument);
  REGISTER_CALLBACK("documentSet", DocSet);
  REGISTER_CALLBACK("documentGet", DocGet);
  REGISTER_CALLBACK("mkNull", MkNull);
  REGISTER_CALLBACK("mkInt", MkInt);
  REGISTER_CALLBACK("mkBool", MkBool);
  REGISTER_CALLBACK("mkDouble", MkDouble);
  REGISTER_CALLBACK("mkString", MkStr);
  REGISTER_CALLBACK("valueTypeName", ValueTypeName);
  REGISTER_CALLBACK("createCollection", CreateCollection);
  REGISTER_CALLBACK("version", Version);

  return exports;
}

NAPI_MODULE(polodb, Init)
