#ifndef POLODB_H
#define POLODB_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct Database;
struct DatabaseV2;
struct DbHandle;
struct DbDocument;
struct DbDocumentIter;
struct DbArray;
struct DbObjectId;

typedef struct DbDocument DbDocument;
typedef struct DbDocumentIter DbDocumentIter;
typedef struct DbArray DbArray;
typedef struct Database Database;
typedef struct DatabaseV2 DatabaseV2;
typedef struct DbHandle DbHandle;
typedef struct DbObjectId DbObjectId;

#define PLDB_TRANS_AUTO 0
#define PLDB_TRANS_READ 1
#define PLDB_TRANS_WRITE 2

#define PLDB_ERR_NOT_A_VALID_DB -46

typedef enum PLDB_VALUE_TYPE {
  PLDB_VAL_NULL = 0x0A,
  PLDB_VAL_DOUBL = 0x01,
  PLDB_VAL_BOOLEAN = 0x08,
  PLDB_VAL_INT = 0x16,
  PLDB_VAL_STRING = 0x02,
  PLDB_VAL_OBJECT_ID = 0x07,
  PLDB_VAL_ARRAY = 0x17,
  PLDB_VAL_DOCUMENT = 0x13,
  PLDB_VAL_BINARY = 0x05,
  PLDB_VAL_UTC_DATETIME = 0x09,
} PLDB_VALUE_TYPE;

typedef struct PLDBValue {
    PLDB_VALUE_TYPE tag: 8;
    union {
        int64_t     int_value;
        double      double_value;
        int         bool_value;
        const char* str;
        DbObjectId* oid;
        DbArray*    arr;
        DbDocument* doc;
        uint64_t    utc;
    } v;
} PLDBValue;

#define PLDB_NULL { PLDB_VAL_NULL, { .int_value = 0 } }
#define PLDB_INT(x) { PLDB_VAL_INT, { .int_value = (x) } }
#define PLDB_DOUBLE(x) { PLDB_VAL_DOUBL, { .double_value = (x) } }
#define PLDB_BOOL(x) { PLDB_VAL_BOOLEAN, { .bool_value = !!(x) ? 1 : 0 } }

enum PLDB_ERR_TYPE {
  PLDB_ERR_COLLECTION_NOT_FOUND = -24,
};

// Database {
Database* PLDB_open(const char* path);

int PLDB_error_code();

int PLDB_start_transaction(Database*db, int flags);

int PLDB_commit(Database* db);

int PLDB_rollback(Database* db);

int PLDB_create_collection(Database* db, const char* name, uint32_t* col_id, uint32_t* meta_verison);

int PLDB_get_collection_meta_by_name(Database* db, const char* name, uint32_t* id, uint32_t* version);

int64_t PLDB_count(Database* db, uint32_t col_id, uint32_t meta_version);

int PLDB_insert(Database* db, uint32_t col_id, uint32_t meta_version, const char* doc);

// <query> is nullable
int PLDB_find(Database* db, uint32_t col_id, uint32_t meta_version, const char* query, DbHandle** out_handle);

// <query> is nullable
int64_t PLDB_update(Database* db, uint32_t col_id, uint32_t meta_version, const char* query, const char* update);

int64_t PLDB_delete(Database* db, uint32_t col_id, uint32_t meta_version, const DbDocument* query);

int64_t PLDB_delete_all(Database* db, uint32_t col_id, uint32_t meta_version);

int PLDB_drop(Database* db, uint32_t col_id, uint32_t meta_version);

const char* PLDB_error_msg();

int PLDB_version(char* buffer, unsigned int buffer_size);

void PLDB_close(Database* db);
// }

// DbHandle {
int PLDB_step(DbHandle* handle);

int PLDB_handle_state(DbHandle* handle);

void PLDB_handle_get(DbHandle* handle, PLDBValue* out_val);

int PLDB_handle_to_str(DbHandle* handle, char* buffer, unsigned int buffer_size);

void PLDB_close_and_free_handle(DbHandle* handle);

void PLDB_free_handle(DbHandle* handle);
// }

DatabaseV2* PLDB_v2_open(const char* path);
void PLDB_v2_close(DatabaseV2* db);
unsigned char* PLDB_v2_request(DatabaseV2* db, const unsigned char* buffer);
void PLDB_v2_free(unsigned char* buffer);

#ifdef __cplusplus
}
#endif

#endif
