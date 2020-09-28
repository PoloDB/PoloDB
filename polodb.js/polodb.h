/*
 * Copyright (c) 2020 Vincent Chan
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation; either version 3, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 */

struct Database;
struct DbHandle;
struct DbDocument;
struct DbDocumentIter;
struct DbArray;
struct DbObjectId;
struct DbValue;

typedef struct DbDocument DbDocument;
typedef struct DbDocumentIter DbDocumentIter;
typedef struct DbArray DbArray;
typedef struct Database Database;
typedef struct DbHandle DbHandle;
typedef struct DbValue DbValue;
typedef struct DbObjectId DbObjectId;

#ifdef __cplusplus
extern "C" {
#endif

// Database {
Database* PLDB_open(const char* path);

int PLDB_error_code();

int PLDB_start_transaction(Database*db, int flags);

int PLDB_commit(Database* db);

int PLDB_rollback(Database* db);

int PLDB_create_collection(Database* db, const char* name);

int PLDB_insert(Database* db, const char* col_name, const DbDocument* doc);

// <query> is nullable
int PLDB_find(Database* db, const char* col_name, const DbDocument* query, DbHandle** out_handle);

const char* PLDB_error_msg();

int PLDB_version(char* buffer, unsigned int buffer_size);

void PLDB_close(Database* db);
// }

// DbHandle {
int PLDB_handle_step(DbHandle* handle);

int PLDB_handle_state(DbHandle* handle);

void PLDB_handle_get(DbHandle* handle, DbValue** out_val);

int PLDB_handle_to_str(DbHandle* handle, char* buffer, unsigned int buffer_size);

void PLDB_free_handle(DbHandle* handle);
// }

// DbArray {
DbArray* PLDB_mk_arr();

void PLDB_free_arr(DbArray* arr);

unsigned int PLDB_arr_len(DbArray* arr);

DbValue* PLDB_arr_into_value(DbArray* arr);

void PLDB_arr_push(DbArray* arr, DbValue* value);

int PLDB_arr_get(DbArray* arr, unsigned int index, DbValue** out_val);
// }

// DbDocument {
DbDocument* PLDB_mk_doc();

void PLDB_free_doc(DbDocument* doc);

int PLDB_doc_set(DbDocument* doc, const char* key, DbValue* val);

int PLDB_doc_get(DbDocument* doc, const char* key, DbValue** out_val);

int PLDB_doc_len(DbDocument* doc);

DbDocumentIter* PLDB_doc_iter(DbDocument* doc);

int PLDB_doc_iter_next(DbDocumentIter* iter,
  char* key_buffer, unsigned int key_buffer_size, DbValue** out_val);

void PLDB_free_doc_iter(DbDocumentIter* iter);

// }

// DbValue {
DbValue* PLDB_doc_into_value(DbDocument* db);

DbValue* PLDB_mk_null();

DbValue* PLDB_mk_double(double value);

DbValue* PLDB_mk_bool(int bl);

DbValue* PLDB_mk_int(long long value);

DbValue* PLDB_mk_str(const char* content);

DbValue* PLDB_mk_binary(unsigned char* content, unsigned int size);

int PLDB_value_type(const DbValue* value);

int PLDB_value_get_i64(const DbValue* value, long long* out_value);

int PLDB_value_get_string_utf8(const DbValue* value, const char** content);

int PLDB_value_get_bool(const DbValue* value);

int PLDB_value_get_double(const DbValue* value, double* out);

int PLDB_value_get_array(const DbValue* value, DbArray** arr);

int PLDB_value_get_object_id(const DbValue* value, DbObjectId** oid);

int PLDB_value_get_document(const DbValue* value, DbDocument** doc);

void PLDB_free_value(DbValue* val);
// }

// DbObjectId {
DbObjectId* PLDB_mk_object_id(Database* db);

void PLDB_free_object_id(DbObjectId*);

int PLDB_object_id_to_hex(const DbObjectId* oid, char* buffer, unsigned int size);

DbValue* PLDB_object_id_into_value(const DbObjectId* oid);
// }

#ifdef __cplusplus
}
#endif
