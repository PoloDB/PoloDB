#include <node_api.h>
#include <stdio.h>
#include <stdlib.h>

#define CHECK_STAT(stat) \
  if ((stat) != napi_ok) { \
    printf("PoloDB addon abortion: %d\n", __LINE__); \
    abort(); \
  }

int JsIsInteger(napi_env env, napi_value value);

int JsIsArray(napi_env env, napi_value value);
