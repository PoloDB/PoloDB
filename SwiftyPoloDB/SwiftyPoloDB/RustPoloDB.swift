//
//  RustPoloDB.swift
//  SwiftyPoloDB
//
//  Created by Duzhong Chen on 2020/11/28.
//

import Foundation

enum DbWrapperError: Error {
    case DbError(content: String)
    case UnknownAnyType
    case NullPointer
}

class PoloDB {
    private let db: OpaquePointer;

    init(path: String) throws {
        let db: OpaquePointer? = PLDB_open(path)

        guard let unwrapDb = db else {
            let errorMsg = String(cString: PLDB_error_msg())
            throw DbWrapperError.DbError(content: errorMsg)
        }

        self.db = unwrapDb
    }

    func createCollection(name: String) throws -> Collection {
        var colId: UInt32 = 0;
        var metaVersion: UInt32 = 0;
        let ec = PLDB_create_collection(db, name, &colId, &metaVersion)
        if ec < 0 {
            let errorMsg = String(cString: PLDB_error_msg())
            throw DbWrapperError.DbError(content: errorMsg)
        }
        return Collection.init(db: self, id: colId, metaVersion: metaVersion)
    }

    func collection(name: String) throws -> Collection {
        var colId: UInt32 = 0
        var metaVersion: UInt32 = 0
        let ec = PLDB_get_collection_meta_by_name(db, name, &colId, &metaVersion)
        if ec < 0 {
            let errorMsg = String(cString: PLDB_error_msg())
            throw DbWrapperError.DbError(content: errorMsg)
        }
        return Collection.init(db: self, id: colId, metaVersion: metaVersion)
    }

    func raw() -> OpaquePointer {
        return self.db
    }

    deinit {
        PLDB_close(db)
    }

}

class Collection {

    private let db: PoloDB
    private let id: UInt32
    private let metaVersion: UInt32

    init(db: PoloDB, id: UInt32, metaVersion: UInt32) {
        self.db = db
        self.id = id
        self.metaVersion = metaVersion
    }
    
    func count() throws -> UInt {
        let ec = PLDB_count(self.db.raw(), self.id, self.metaVersion)
        if ec < 0 {
            let errorMsg = String(cString: PLDB_error_msg())
            throw DbWrapperError.DbError(content: errorMsg)
        }
        return UInt(ec)
    }
    
    func deleteAll() throws {
        let ec = PLDB_delete_all(self.db.raw(), self.id, self.metaVersion)
        if ec < 0 {
            let errorMsg = String(cString: PLDB_error_msg())
            throw DbWrapperError.DbError(content: errorMsg)
        }
    }

    func drop() throws {
        let ec = PLDB_drop(self.db.raw(), self.id, self.metaVersion)
        if ec < 0 {
            let errorMsg = String(cString: PLDB_error_msg())
            throw DbWrapperError.DbError(content: errorMsg)
        }
    }

}

class DbValueWrapper {
    private let value: OpaquePointer
    
    init(a: Any) throws {
        switch a {
        case let someInt as Int:
            self.value = PLDB_mk_int(Int64(someInt))
            
        case let someUInt as UInt:
            self.value = PLDB_mk_int(Int64(someUInt))
            
        case let someI64 as Int64:
            self.value = PLDB_mk_int(someI64)
            
        case let someFloat as Float32:
            self.value = PLDB_mk_double(Double(someFloat))
            
        case let someDouble as Double:
            self.value = PLDB_mk_double(someDouble)
            
        case let someString as String:
            self.value = PLDB_mk_str(someString)
            
        case let someDict as [String: Any]:
            let wrapper = try DbDocumentWrapper.init(dict: someDict)
            self.value = PLDB_doc_to_value(wrapper.raw())
        
        default:
            throw DbWrapperError.UnknownAnyType
        }
    }
    
    func raw() -> OpaquePointer {
        return self.value
    }
    
    deinit {
        PLDB_free_value(self.value)
    }
    
}

class DbDocumentWrapper {
    private let value: OpaquePointer
    
    init(dict: [String: Any]) throws {
        let unsafeResult = PLDB_mk_doc()
        guard let result = unsafeResult else {
            throw DbWrapperError.NullPointer
        }
        self.value = result
        for (key, value) in dict {
            let wrapValue = try DbValueWrapper.init(a: value)
            PLDB_doc_set(self.value, key, wrapValue.raw())
        }
    }
    
    func raw() -> OpaquePointer {
        return self.value
    }
    
    deinit {
        PLDB_free_doc(self.value)
    }
}
