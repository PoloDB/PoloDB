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
struct ByteCodeBuilder;
struct DbDocument;
struct DbValue;

#ifdef __cplusplus
extern "C" {
#endif

struct Database* PLDB_open(const char* path);

int PLDB_error_code();

int PLDB_exec(struct Database* db, unsigned char* bytes, unsigned int size);

int PLDB_create_collection(struct Database* db, const char* name);

const char* PLDB_error_msg();

int PLDB_version(char* buffer, unsigned int buffer_size);

void PLDB_close(struct Database* db);

struct ByteCodeBuilder* PLDB_new_bytecode_builder();

void PLDB_free_byte_code_builder(struct ByteCodeBuilder* builder);

struct DbDocument* PLDB_mk_doc();

void PLDB_free_doc(struct DbDocument* doc);

int PLDB_doc_set(struct DbDocument* doc, const char* key, struct DbValue* val);

int PLDB_doc_get(struct DbDocument* dc, const char* key, struct DbValue** out_val);

struct DbValue* PLDB_doc_into_value(struct DbDocument* db);

struct DbValue* PLDB_mk_null();

struct DbValue* PLDB_mk_double(double value);

struct DbValue* PLDB_mk_bool(int bl);

struct DbValue* PLDB_mk_int(long long value);

struct DbValue* PLDB_mk_str(char* content);

struct DbValue* PLDB_mk_binary(unsigned char* content, unsigned int size);

struct DbValue* PLDB_mk_object_id(unsigned char* bytes);

void PLDB_free_value(struct DbValue* val);

#ifdef __cplusplus
}
#endif
