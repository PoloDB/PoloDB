#include <node_api.h>
#include <stdio.h>
#include <stdlib.h>

#define CHECK_STAT(stat) \
  if ((stat) != napi_ok) { \
    printf("PoloDB addon abortion: %d, status: %d\n", __LINE__, (stat)); \
    abort(); \
  }

int JsIsInteger(napi_env env, napi_value value);

napi_status JsIsArray(napi_env env, napi_value value, bool* result);

napi_status JsGetUTCDateTime(napi_env env, napi_value value, int64_t* utc_datetime);
