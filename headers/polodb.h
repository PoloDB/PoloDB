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

int PLDB_insert(Database* db, uint32_t col_id, uint32_t meta_version, DbDocument* doc);

// <query> is nullable
int PLDB_find(Database* db, uint32_t col_id, uint32_t meta_version, const DbDocument* query, DbHandle** out_handle);

// <query> is nullable
int64_t PLDB_update(Database* db, uint32_t col_id, uint32_t meta_version, const DbDocument* query, const DbDocument* update);

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

// DbArray {
DbArray* PLDB_mk_arr();

DbArray* PLDB_mk_arr_with_size(unsigned int size);

void PLDB_free_arr(DbArray* arr);

unsigned int PLDB_arr_len(DbArray* arr);

void PLDB_arr_push(DbArray* arr, PLDBValue value);

int PLDB_arr_set(DbArray* doc, uint32_t index, PLDBValue val);

int PLDB_arr_get(DbArray* arr, unsigned int index, PLDBValue* out_val);
// }

// DbDocument {
DbDocument* PLDB_mk_doc();

void PLDB_free_doc(DbDocument* doc);

int PLDB_doc_set(DbDocument* doc, const char* key, PLDBValue val);

int PLDB_doc_get(DbDocument* doc, const char* key, PLDBValue* out_val);

int PLDB_doc_len(DbDocument* doc);

DbDocumentIter* PLDB_doc_iter(DbDocument* doc);

int PLDB_doc_iter_next(DbDocumentIter* iter,
  char* key_buffer, unsigned int key_buffer_size, PLDBValue* out_val);

void PLDB_free_doc_iter(DbDocumentIter* iter);

// }

// DbValue {

PLDBValue PLDB_mk_binary_value(const char* bytes, uint32_t size);
PLDBValue PLDB_dup_value(PLDBValue val);
void PLDB_free_value(PLDBValue val);
// }

// DbObjectId {
DbObjectId* PLDB_mk_object_id(Database* db);

DbObjectId* PLDB_dup_object_id(const DbObjectId* that);

DbObjectId* PLDB_mk_object_id_from_bytes(const char* bytes);

void PLDB_object_id_to_bytes(const DbObjectId* oid, char* bytes);

void PLDB_free_object_id(DbObjectId*);

int PLDB_object_id_to_hex(const DbObjectId* oid, char* buffer, unsigned int size);
// }

// DbUTCDateTime {
// -1 to make current date

uint64_t  PLDB_mk_UTCDateTime();

// }

#ifdef __cplusplus
}
#endif

#endif
