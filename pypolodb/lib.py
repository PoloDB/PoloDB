#
# Copyright (c) 2020 Vincent Chan
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License as published by the Free Software
# Foundation; either version 3, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License along with
# this program.  If not, see <http://www.gnu.org/licenses/>.
#
from cffi import FFI

ffi = FFI()
ffi.cdef("""
    struct Database;
    struct ByteCodeBuilder;
    struct DbValue;
    
    struct Database* PLDB_open(const char* path);
    
    int PLDB_error_code();
    
    int PLDB_exec(struct Database* db, unsigned char* bytes, unsigned int size);
    
    const char* PLDB_error_msg();
    
    int PLDB_version(char* buffer, unsigned int buffer_size);
    
    void PLDB_close(struct Database* db);
    
    struct ByteCodeBuilder* PLDB_new_bytecode_builder();
    
    void PLDB_free_byte_code_builder(struct ByteCodeBuilder* builder);
    
    struct DbValue* PLDB_mk_null();
    
    struct DbValue* PLDB_mk_double(double value);
    
    struct DbValue* PLDB_mk_bool(int bool);
    
    struct DbValue* PLDB_mk_int(int value);
    
    struct DbValue* PLDB_mk_str(char* content);
    
    struct DbValue* PLDB_mk_binary(unsigned char* content, unsigned int size);
    
    struct DbValue* PLDB_mk_object_id(unsigned char* bytes);
    
    void PLDB_free_value(struct DbValue* val);
""")

C = ffi.dlopen("./../target/debug/libpolodb_clib.dylib")


def polodb_version():
    buffer = ffi.new("char[]", 500)
    C.PLDB_version(buffer, 500)
    return ffi.string(buffer).decode('utf-8')


class DbValue:

    def __init__(self, value):
        if value is None:
            self.val = C.PLDB_mk_null()
        elif type(value) is int:
            self.val = C.PLDB_mk_int(value)
        elif type(value) is str:
            tmp = ffi.new("char[]", bytes(value, 'utf-8'))
            self.val = C.PLDB_mk_str(tmp)
        elif type(value) is bool:
            tmp = int(value)
            self.val = C.PLDB_mk_bool(tmp)
        elif type(value) is float:
            self.val = C.PLDB_mk_double(value)
        else:
            raise RuntimeError("unknown type: " + type(value))

    def __del__(self):
        C.PLDB_free_value(self.val)


class PoloDB:

    def __init__(self, path):
        db_path = ffi.new("char[]", bytes(path, 'utf-8'))
        self.db = C.PLDB_open(db_path)
        if self.db == ffi.NULL:
            error_msg = ffi.string(C.PLDB_error_msg())
            raise RuntimeError("open db error: " + error_msg)

    def close(self):
        if self.db == ffi.NULL:
            return
        C.PLDB_close(self.db)
        self.db = ffi.NULL


class BytecodeBuilder:

    def __init__(self):
        self.builder = C.PLDB_new_bytecode_builder()
        if self.builder == ffi.NULL:
            error_msg = ffi.string(C.PLDB_error_msg())
            raise RuntimeError("create builder failed: " + error_msg)

    def __del__(self):
        if self.builder == ffi.NULL:
            return
        C.PLDB_free_byte_code_builder(self.builder)
        self.builder = ffi.NULL


print(polodb_version())
db = PoloDB("/tmp/test-python.db")
