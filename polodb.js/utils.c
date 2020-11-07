#include "utils.h"

int JsIsInteger(napi_env env, napi_value value) {
  napi_status status;
  napi_value global;

  status = napi_get_global(env, &global);
  CHECK_STAT(status);

  napi_value number_instance;
  status = napi_get_named_property(env, global, "Number", &number_instance);
  CHECK_STAT(status);

  napi_value is_int_fun;
  status = napi_get_named_property(env, number_instance, "isInteger", &is_int_fun);
  CHECK_STAT(status);

  size_t argc = 1;
  napi_value argv[] = { value };

  napi_value result;
  status = napi_call_function(env, number_instance, is_int_fun, argc, argv, &result);
  CHECK_STAT(status);

  bool bl_result = false;

  status = napi_get_value_bool(env, result, &bl_result);
  CHECK_STAT(status);

  return bl_result ? 1 : 0;
}

int JsIsArray(napi_env env, napi_value value) {
  napi_status status;
  napi_value global;

  status = napi_get_global(env, &global);
  if (status != napi_ok) {
    return -1;
  }

  napi_value array_str;
  status = napi_create_string_utf8(env, "Array", NAPI_AUTO_LENGTH, &array_str);
  if (status != napi_ok) {
    return -1;
  }

  napi_value is_array_str;
  status = napi_create_string_utf8(env, "isArray", NAPI_AUTO_LENGTH, &is_array_str);
  if (status != napi_ok) {
    return -1;
  }

  size_t argc = 1;
  napi_value argv[] = { value };

  napi_value result;
  status = napi_call_function(env, array_str, is_array_str, argc, argv, &result);
  if (status != napi_ok) {
    return -1;
  }

  bool bl_result = false;

  status = napi_get_value_bool(env, result, &bl_result);
  if (status != napi_ok) {
    return -1;
  }

  return bl_result ? 1 : 0;
}
