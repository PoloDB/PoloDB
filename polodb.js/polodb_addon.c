#include <node_api.h>
#include <stdlib.h>
#include "headers/polodb.h"

#define NAPI_CALL(env, call)                                      \
  do {                                                            \
    napi_status status = (call);                                  \
    if (status != napi_ok) {                                      \
      const napi_extended_error_info* error_info = NULL;          \
      napi_get_last_error_info((env), &error_info);               \
      const char* err_message = error_info->error_message;        \
      bool is_pending;                                            \
      napi_is_exception_pending((env), &is_pending);              \
      if (!is_pending) {                                          \
        const char* message = (err_message == NULL)               \
            ? "empty error message"                               \
            : err_message;                                        \
        napi_throw_error((env), NULL, message);                   \
        return NULL;                                              \
      }                                                           \
    }                                                             \
  } while(0)

static void db_finalize(napi_env env, void* finalize_data, void* finalize_hint) {
  Database* db = (Database*)finalize_data;
  PLDB_close(db);
}

static napi_value
open_file(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  if (st != napi_ok) {
    return NULL;
  }

  size_t str_len = 0;
  napi_value result = NULL;
  char* buf = NULL;
  Database* db = NULL;
  PLDBError* db_err = NULL;

  st = napi_get_value_string_utf8(env, argv[0], NULL, 0, &str_len);
  if (st != napi_ok) {
    goto clean;
  }

  buf = malloc(str_len);

  st = napi_get_value_string_utf8(env, argv[0], buf, str_len, &str_len);
  if (st != napi_ok) {
    goto clean;
  }

  db_err = PLDB_open(buf, &db);
  if (db_err != NULL) {
    napi_throw_error(env, NULL, db_err->message);
    // TODO: throw error
    goto clean;
  }

  st = napi_create_external(env, db, db_finalize, NULL, &result);
  if (st != napi_ok) {
    goto clean;
  }

clean:
  if (buf != NULL) {
    free(buf);
    buf = NULL;
  }
  if (db_err != NULL) {
    PLDB_free_error(db_err);
  }
  // Do something useful.
  return result;
}


static napi_value
handle_message(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  if (st != napi_ok) {
    return NULL;
  }

  Database* db = NULL;
  PLDBError* db_err = NULL;
  st = napi_get_value_external(env, argv[0], (void**)&db);
  if (st != napi_ok) {
    return NULL;
  }

  void* data = NULL;
  size_t data_len = 0;

  st = napi_get_buffer_info(env, argv[1], NULL, &data_len);
  if (st != napi_ok) {
    goto clean;
  }

  data = malloc(data_len);
  st = napi_get_buffer_info(env, argv[1], &data, &data_len);
  if (st != napi_ok) {
    goto clean;
  }

  unsigned char* result_data = NULL;
  uint64_t result_size;
  napi_value result = NULL;
  db_err = PLDB_handle_message(db, (unsigned char*)data, data_len, &result_data, &result_size);

  if (db_err != NULL) {
    napi_throw_error(env, NULL, db_err->message);
    // TODO: throw error
    goto clean;
  }

  st = napi_create_buffer_copy(env, result_size, result_data, NULL, &result);
  if (st != napi_ok) {
    goto clean;
  }

clean:
  if (data != NULL) {
    free(data);
    data = NULL;
  }
  if (db_err != NULL) {
    PLDB_free_error(db_err);
  }
  if (result_data != NULL) {
    PLDB_free_result(result_data);
  }

  return result;
}

napi_value create_addon(napi_env env) {
  napi_value result;
  NAPI_CALL(env, napi_create_object(env, &result));

  napi_value exported_function;
  NAPI_CALL(env, napi_create_function(env,
                                      "openFile",
                                      NAPI_AUTO_LENGTH,
                                      open_file,
                                      NULL,
                                      &exported_function));

  NAPI_CALL(env, napi_create_function(env,
                                      "handleMessage",
                                      NAPI_AUTO_LENGTH,
                                      handle_message,
                                      NULL,
                                      &exported_function));

  NAPI_CALL(env, napi_set_named_property(env,
                                         result,
                                         "doSomethingUseful",
                                         exported_function));

  return result;
}

NAPI_MODULE_INIT() {
  // This function body is expected to return a `napi_value`.
  // The variables `napi_env env` and `napi_value exports` may be used within
  // the body, as they are provided by the definition of `NAPI_MODULE_INIT()`.
  return create_addon(env);
}
