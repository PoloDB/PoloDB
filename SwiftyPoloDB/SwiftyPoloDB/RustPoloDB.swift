//
//  RustPoloDB.swift
//  SwiftyPoloDB
//
//  Created by Duzhong Chen on 2020/11/28.
//

import Foundation

enum DbWrapperError: Error {
    case DbError(content: String)
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

    func drop() throws {
        let ec = PLDB_drop(self.db.raw(), self.id, self.metaVersion)
        if ec < 0 {
            let errorMsg = String(cString: PLDB_error_msg())
            throw DbWrapperError.DbError(content: errorMsg)
        }
    }

}
