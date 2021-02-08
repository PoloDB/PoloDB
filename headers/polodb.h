#ifndef POLODB_H
#define POLODB_H

#include <stdint.h>

struct Database;
struct DbHandle;
struct DbDocument;
struct DbDocumentIter;
struct DbArray;
struct DbObjectId;
struct DbValue;
struct DbUTCDateTime;

typedef struct DbDocument DbDocument;
typedef struct DbDocumentIter DbDocumentIter;
typedef struct DbArray DbArray;
typedef struct Database Database;
typedef struct DbHandle DbHandle;
typedef struct DbValue DbValue;
typedef struct DbObjectId DbObjectId;
typedef struct DbUTCDateTime DbUTCDateTime;

#define PLDB_TRANS_AUTO 0
#define PLDB_TRANS_READ 1
#define PLDB_TRANS_WRITE 2

#ifdef __cplusplus
extern "C" {
#endif

enum PLDB_VALUE_TYPE {
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
};

struct PLDBValue {
    PLDB_VALUE_TYPE tag: 8,
    union {
        int64_t int_value,
        double double_value,
        int bool_value,
        const char* str,
        DbObjectId* oid,
    } v;
};

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

void PLDB_handle_get(DbHandle* handle, DbValue** out_val);

int PLDB_handle_to_str(DbHandle* handle, char* buffer, unsigned int buffer_size);

void PLDB_close_and_free_handle(DbHandle* handle);

void PLDB_free_handle(DbHandle* handle);
// }

// DbArray {
DbArray* PLDB_mk_arr();

DbArray* PLDB_mk_arr_with_size(unsigned int size);

void PLDB_free_arr(DbArray* arr);

unsigned int PLDB_arr_len(DbArray* arr);

void PLDB_arr_push(DbArray* arr, DbValue* value);

int PLDB_arr_set_null(DbArray* arr, unsigned int index);

int PLDB_arr_set_int(DbArray* arr, unsigned int index, int64_t value);

int PLDB_arr_set_bool(DbArray* arr, unsigned int index, int value);

int PLDB_arr_set_double(DbArray* arr, unsigned int index, double value);

int PLDB_arr_set_string(DbArray* arr, unsigned int index, const char* value);

int PLDB_arr_set_binary(DbArray* arr, unsigned int index, const unsigned char* data, unsigned int data_size);

int PLDB_arr_set_arr(DbArray* arr, unsigned int index, DbArray* value);

int PLDB_arr_set_doc(DbArray* arr, unsigned int index, DbDocument* value);

int PLDB_arr_set_object_id(DbArray* arr, unsigned int index, DbObjectId* value);

int PLDB_arr_set_UTCDateTime(DbArray* arr, unsigned int index, int64_t ts);

int PLDB_arr_get(DbArray* arr, unsigned int index, DbValue** out_val);
// }

// DbDocument {
DbDocument* PLDB_mk_doc();

void PLDB_free_doc(DbDocument* doc);

int PLDB_doc_set(DbDocument* doc, const char* key, const PLDBValue* val);

int PLDB_arr_set(DbDocument* doc, uint32_t index, const PLDBValue* val);

int PLDB_doc_set_string(DbDocument* doc, const char* key, const char* value);

int PLDB_doc_set_null(DbDocument* doc, const char* key);

int PLDB_doc_set_int(DbDocument* doc, const char* key, int64_t value);

int PLDB_doc_set_bool(DbDocument* doc, const char* key, int value);

int PLDB_doc_set_double(DbDocument* doc, const char* key, double value);

int PLDB_doc_set_doc(DbDocument* doc, const char* key, DbDocument* value);

int PLDB_doc_set_arr(DbDocument* doc, const char* key, DbArray* value);

int PLDB_doc_set_object_id(DbDocument* doc, const char* key, DbObjectId* value);

int PLDB_doc_set_UTCDateTime(DbDocument* doc, const char* key, int64_t ts);

int PLDB_doc_get(DbDocument* doc, const char* key, DbValue** out_val);

int PLDB_doc_len(DbDocument* doc);

DbDocumentIter* PLDB_doc_iter(DbDocument* doc);

int PLDB_doc_iter_next(DbDocumentIter* iter,
  char* key_buffer, unsigned int key_buffer_size, DbValue** out_val);

void PLDB_free_doc_iter(DbDocumentIter* iter);

// }

// DbValue {
int PLDB_value_type(const DbValue* value);

int PLDB_value_get_i64(const DbValue* value, int64_t* out_value);

int PLDB_value_get_string_utf8(const DbValue* value, const char** content);

int PLDB_value_get_bool(const DbValue* value);

int PLDB_value_get_double(const DbValue* value, double* out);

int PLDB_value_get_array(const DbValue* value, DbArray** arr);

int PLDB_value_get_object_id(const DbValue* value, DbObjectId** oid);

int PLDB_value_get_document(const DbValue* value, DbDocument** doc);

int PLDB_value_get_utc_datetime(const DbValue* value, DbUTCDateTime** time);

void PLDB_free_value(DbValue* val);
// }

// DbObjectId {
DbObjectId* PLDB_mk_object_id(Database* db);

DbObjectId* PLDB_mk_object_id_from_bytes(const char* bytes);

void PLDB_object_id_to_bytes(const DbObjectId* oid, char* bytes);

void PLDB_free_object_id(DbObjectId*);

int PLDB_object_id_to_hex(const DbObjectId* oid, char* buffer, unsigned int size);
// }

// DbUTCDateTime {
// -1 to make current date
DbUTCDateTime*  PLDB_mk_UTCDateTime(int64_t time);

int64_t PLDB_UTCDateTime_get_timestamp(const DbUTCDateTime* dt);

void PLDB_free_UTCDateTime(DbUTCDateTime* dt);
// }

#ifdef __cplusplus
}
#endif

#endif
