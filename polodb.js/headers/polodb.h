#ifndef POLODB_H
#define POLODB_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct Database;
struct DbHandle;
struct DbDocument;
struct DbDocumentIter;
struct DbArray;
struct DbObjectId;

typedef struct DbDocument DbDocument;
typedef struct DbDocumentIter DbDocumentIter;
typedef struct DbArray DbArray;
typedef struct Database Database;
typedef struct DbHandle DbHandle;
typedef struct DbObjectId DbObjectId;

#define PLDB_TRANS_AUTO 0
#define PLDB_TRANS_READ 1
#define PLDB_TRANS_WRITE 2

#define PLDB_ERR_NOT_A_VALID_DB -46

typedef struct PLDBError
{
  int code;
  char* message;
} PLDBError;

void PLDB_free_error(PLDBError* err);

enum PLDB_ERR_TYPE {
  PLDB_ERR_COLLECTION_NOT_FOUND = -24,
};

PLDBError* PLDB_open(const char* path, Database** result);

PLDBError* PLDB_handle_message(Database* db, const unsigned char *msg, uint64_t msg_size,
  unsigned char** result, uint64_t* result_size);

void PLDB_free_result(unsigned char* result);

int PLDB_version(char* buffer, unsigned int buffer_size);

void PLDB_close(Database* db);

#ifdef __cplusplus
}
#endif

#endif
