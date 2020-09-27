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
struct DbArray;
struct DbObjectId;
struct DbValue;

typedef struct DbDocument DbDocument;
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

int PLDB_exec(Database* db, unsigned char* bytes, unsigned int size);

int PLDB_create_collection(Database* db, const char* name);

const char* PLDB_error_msg();

int PLDB_version(char* buffer, unsigned int buffer_size);

void PLDB_close(Database* db);
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

int PLDB_doc_get(DbDocument* dc, const char* key, DbValue** out_val);
// }

// DbValue {
DbValue* PLDB_doc_into_value(DbDocument* db);

DbValue* PLDB_mk_null();

DbValue* PLDB_mk_double(double value);

DbValue* PLDB_mk_bool(int bl);

DbValue* PLDB_mk_int(long long value);

DbValue* PLDB_mk_str(const char* content);

DbValue* PLDB_mk_binary(unsigned char* content, unsigned int size);

int PLDB_value_type_name(DbValue* value, char* buffer, unsigned int size);

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
