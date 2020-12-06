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
    case UnknownDbValueType
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
    
    func insert(doc: inout [String: Any]) throws {
        let wrapper = try DbDocumentWrapper.init(dict: doc)
        let ec = PLDB_insert(self.db.raw(), self.id, self.metaVersion, wrapper.raw())
        if ec < 0 {
            let errorMsg = String(cString: PLDB_error_msg())
            throw DbWrapperError.DbError(content: errorMsg)
        }
        if ec > 0 {
            var dbValue: OpaquePointer? = nil;
            let ec2 = PLDB_doc_get(self.db.raw(), "_id", &dbValue)
            if ec2 < 0 {
                let errorMsg = String(cString: PLDB_error_msg())
                throw DbWrapperError.DbError(content: errorMsg)
            }
            let swiftValue: Any
            do {
                swiftValue = try dbValueToAny(dbValue: dbValue!)
            } catch {
                PLDB_free_value(dbValue!)
                throw error
            }
            doc["_id"] = swiftValue
            PLDB_free_value(dbValue!)
        }
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

private func dbDocumentToDict(dbDocument: OpaquePointer) throws -> [String: Any] {
    var result: [String: Any] = [:]
    let iter = PLDB_doc_iter(dbDocument)
    
    let buffer = UnsafeMutablePointer<Int8>.allocate(capacity: 512)
    buffer.initialize(repeating: 0, count: 512)
    
    var tmpValue: OpaquePointer? = nil
    var ec = PLDB_doc_iter_next(iter, buffer, 512, &tmpValue)
    
    while ec != 0 {
        let key = String(cString: buffer)
        let value: Any
        do {
            value = try dbValueToAny(dbValue: tmpValue!)
        } catch {
            PLDB_free_doc_iter(iter)
            throw error
        }
        
        result[key] = value
        
        PLDB_free_value(tmpValue)
        
        buffer.assign(repeating: 0, count: 512)
        ec = PLDB_doc_iter_next(iter, buffer, 512, &tmpValue)
    }
    
    PLDB_free_doc_iter(iter)
    return result
}

private func dbArrayToArray(dbArray: OpaquePointer) throws -> [Any] {
    var result: [Any] = []
    
    let len = PLDB_arr_len(dbArray)
    
    for i in 0..<len {
        var dbValue: OpaquePointer?
        let ec = PLDB_arr_get(dbArray, i, &dbValue)
        if ec < 0 {
            let errorMsg = String(cString: PLDB_error_msg())
            throw DbWrapperError.DbError(content: errorMsg)
        }
        let item: Any
        do {
            item = try dbValueToAny(dbValue: dbValue!)
        } catch {
            PLDB_free_value(dbValue)
            throw error
        }
        PLDB_free_value(dbValue)
        result.append(item)
    }
    
    return result
}

private func dbValueToAny(dbValue: OpaquePointer) throws -> Any {
    let ty = PLDB_VALUE_TYPE(UInt32(PLDB_value_type(dbValue)))
    switch ty {
    case PLDB_VAL_DOUBL:
        var outDouble: Double = 0.0
        let ec = PLDB_value_get_double(dbValue, &outDouble)
        if ec < 0 {
            let errorMsg = String(cString: PLDB_error_msg())
            throw DbWrapperError.DbError(content: errorMsg)
        }
        return outDouble
        
    case PLDB_VAL_BOOLEAN:
        let ec = PLDB_value_get_bool(dbValue)
        if ec < 0 {
            let errorMsg = String(cString: PLDB_error_msg())
            throw DbWrapperError.DbError(content: errorMsg)
        }
        return ec != 0
        
    case PLDB_VAL_INT:
        var outInt: Int64 = 0
        let ec = PLDB_value_get_i64(dbValue, &outInt)
        if ec < 0 {
            let errorMsg = String(cString: PLDB_error_msg())
            throw DbWrapperError.DbError(content: errorMsg)
        }
        return outInt
        
    case PLDB_VAL_STRING:
        var outStr: UnsafePointer<Int8>!
        let ec = PLDB_value_get_string_utf8(dbValue, &outStr)
        if ec < 0 {
            let errorMsg = String(cString: PLDB_error_msg())
            throw DbWrapperError.DbError(content: errorMsg)
        }
        return String(cString: outStr)
        
    case PLDB_VAL_DOCUMENT:
        var outDoc: OpaquePointer? = nil
        let ec = PLDB_value_get_document(dbValue, &outDoc)
        if ec < 0 {
            let errorMsg = String(cString: PLDB_error_msg())
            throw DbWrapperError.DbError(content: errorMsg)
        }
        do {
            let dict = try dbDocumentToDict(dbDocument: outDoc!)
            PLDB_free_doc(outDoc!)
            return dict
        } catch {
            PLDB_free_doc(outDoc!)
            throw error
        }
        
    case PLDB_VAL_ARRAY:
        var outArr: OpaquePointer? = nil
        let ec = PLDB_value_get_array(dbValue, &outArr)
        if ec < 0 {
            let errorMsg = String(cString: PLDB_error_msg())
            throw DbWrapperError.DbError(content: errorMsg)
        }
        do {
            let dict = try dbArrayToArray(dbArray: outArr!)
            PLDB_free_arr(outArr!)
            return dict
        } catch {
            PLDB_free_arr(outArr!)
            throw error
        }
        
    case PLDB_VAL_OBJECT_ID:
        var outObjectId: OpaquePointer? = nil
        let ec = PLDB_value_get_object_id(dbValue, &outObjectId)
        if ec < 0 {
            let errorMsg = String(cString: PLDB_error_msg())
            throw DbWrapperError.DbError(content: errorMsg)
        }
        return ObjectIdWrapper.init(value: outObjectId!)
        
    case PLDB_VAL_UTC_DATETIME:
        var dateTimePtr: OpaquePointer? = nil
        let ec = PLDB_value_get_utc_datetime(dbValue, &dateTimePtr)
        if ec < 0 {
            let errorMsg = String(cString: PLDB_error_msg())
            throw DbWrapperError.DbError(content: errorMsg)
        }
        let outTimestamp = PLDB_UTCDateTime_get_timestamp(dateTimePtr!)
        PLDB_free_UTCDateTime(dateTimePtr!)
        return Date(timeIntervalSince1970: TimeInterval(outTimestamp) / 1000)
        
    default:
        throw DbWrapperError.UnknownDbValueType
    }
}

private func docSetKV(doc: OpaquePointer, key: String, value: Any) throws {
    switch value {
    case let someInt as Int:
        PLDB_doc_set_int(doc, key, Int64(someInt))
        
    case let someUInt as UInt:
        PLDB_doc_set_int(doc, key, Int64(someUInt))
        
    case let someI64 as Int64:
        PLDB_doc_set_int(doc, key, someI64)
        
    case let someFloat as Float32:
        PLDB_doc_set_double(doc, key, Double(someFloat))
        
    case let someDouble as Double:
        PLDB_doc_set_double(doc, key, someDouble)
        
    case let someString as String:
        PLDB_doc_set_string(doc, key, someString)
        
    case let someDict as [String: Any]:
        let wrapper = try DbDocumentWrapper.init(dict: someDict)
        PLDB_doc_set_doc(doc, key, wrapper.raw())
        
    case let someArr as [Any]:
        let wrapper = try DbArrayWrapper.init(arr: someArr)
        PLDB_doc_set_arr(doc, key, wrapper.raw())
        
    case let someOid as ObjectIdWrapper:
        PLDB_doc_set_object_id(doc, key, someOid.raw())
        
    case let someDate as Date:
        let ts = Int64((someDate.timeIntervalSince1970 * 1000.0).rounded())
        PLDB_doc_set_UTCDateTime(doc, key, ts)
    
    default:
        throw DbWrapperError.UnknownAnyType
    }
}

private func arrSetKV(arr: OpaquePointer, index: UInt32, value: Any) throws {
    switch value {
    case let someInt as Int:
        PLDB_arr_set_int(arr, index, Int64(someInt))
        
    case let someUInt as UInt:
        PLDB_arr_set_int(arr, index, Int64(someUInt))
        
    case let someI64 as Int64:
        PLDB_arr_set_int(arr, index, someI64)
        
    case let someFloat as Float32:
        PLDB_arr_set_double(arr, index, Double(someFloat))
        
    case let someDouble as Double:
        PLDB_arr_set_double(arr, index, someDouble)
        
    case let someString as String:
        PLDB_arr_set_string(arr, index, someString)
        
    case let someDict as [String: Any]:
        let wrapper = try DbDocumentWrapper.init(dict: someDict)
        PLDB_arr_set_doc(arr, index, wrapper.raw())
        
    case let someArr as [Any]:
        let wrapper = try DbArrayWrapper.init(arr: someArr)
        PLDB_arr_set_arr(arr, index, wrapper.raw())
        
    case let someOid as ObjectIdWrapper:
        PLDB_arr_set_arr(arr, index, someOid.raw())
        
    case let someDate as Date:
        let ts = Int64((someDate.timeIntervalSince1970 * 1000.0).rounded())
        PLDB_arr_set_UTCDateTime(arr, index, ts)
    
    default:
        throw DbWrapperError.UnknownAnyType
    }
}

class DbDocumentWrapper {
    private let value: OpaquePointer
    
    init(dict: [String: Any]) throws {
        self.value = PLDB_mk_doc()
        for (key, value) in dict {
            try docSetKV(doc: self.value, key: key, value: value)
        }
    }
    
    func raw() -> OpaquePointer {
        return self.value
    }
    
    deinit {
        PLDB_free_doc(self.value)
    }
}

class DbArrayWrapper {
    private let value: OpaquePointer
    
    init(arr: [Any]) throws {
        self.value = PLDB_mk_arr()
    }
    
    func raw() -> OpaquePointer {
        return self.value
    }
    
    deinit {
        PLDB_free_arr(self.value)
    }
    
}

class ObjectIdWrapper {
    private let value: OpaquePointer
    
    init(value: OpaquePointer) {
        self.value = value
    }
    
    func raw() -> OpaquePointer {
        return self.value
    }
    
    func hex() -> String {
        let buffer = UnsafeMutablePointer<Int8>.allocate(capacity: 64)
        buffer.assign(repeating: 0, count: 64)
        PLDB_object_id_to_hex(self.value, buffer, 64)
        return String(cString: buffer)
    }
    
    deinit {
        PLDB_free_object_id(self.value)
    }
    
}
