#[no_mangle]
pub extern "C" fn Database_open_path(path: &std::ffi::c_char) -> *mut polodb_core::Database {
    unsafe {
        let path = std::ffi::CStr::from_ptr(path).to_str().unwrap();
        let db = polodb_core::Database::open_path(path).unwrap();
        Box::into_raw(Box::new(db))
    }
}

#[no_mangle]
pub extern "C" fn Database_destroy(ptr: *mut polodb_core::Database) {
    unsafe {
        if !ptr.is_null() {
            std::mem::drop(Box::from_raw(ptr));
        }
    }
}

#[no_mangle]
pub extern "C" fn Database_collection(ptr: *mut polodb_core::Database, name: &std::ffi::c_char) -> *mut polodb_core::Collection<polodb_core::bson::Document> {
    unsafe {
        let db = &*ptr;
        let name = std::ffi::CStr::from_ptr(name).to_str().unwrap();
        let collection = db.collection(name);
        Box::into_raw(Box::new(collection))
    }
}

#[no_mangle]
pub extern "C" fn Collection_destroy(ptr: *mut polodb_core::Collection<polodb_core::bson::Document>) {
    unsafe {
        if !ptr.is_null() {
            std::mem::drop(Box::from_raw(ptr));
        }
    }
}

#[no_mangle]
pub extern "C" fn Collection_insert_many(ptr: *mut polodb_core::Collection<polodb_core::bson::Document>, documents: &std::ffi::c_char) -> u32 {
    use polodb_core::CollectionT;
    unsafe {
        let collection = &*ptr;
        let documents = std::ffi::CStr::from_ptr(documents).to_str().unwrap();
        let documents: serde_json::Value = serde_json::from_str(documents).unwrap();
        let documents = polodb_core::bson::to_bson(&documents).unwrap();
        let documents: Vec<_> = documents.as_array().unwrap().iter().map(|doc| doc.as_document().unwrap()).collect();
        collection.insert_many(documents).unwrap().inserted_ids.len() as u32
    }
}

#[no_mangle]
pub extern "C" fn Collection_find(ptr: *mut polodb_core::Collection<polodb_core::bson::Document>, filter: &std::ffi::c_char) -> *mut polodb_core::action::Find<polodb_core::bson::Document> {
    use polodb_core::CollectionT;
    unsafe {
        let collection = &*ptr;
        let filter = std::ffi::CStr::from_ptr(filter).to_str().unwrap();
        let filter: serde_json::Value = serde_json::from_str(filter).unwrap();
        let filter = polodb_core::bson::to_bson(&filter).unwrap();
        let filter = filter.as_document().unwrap().clone();
        let cursor = collection.find(filter);
        Box::into_raw(Box::new(cursor))
    }
}

#[no_mangle]
pub extern "C" fn Find_run(ptr: *mut polodb_core::action::Find<polodb_core::bson::Document>) -> *mut *mut std::ffi::c_void {
    unsafe {
        let find = Box::from_raw(ptr);
        let cursor = find.run().unwrap();
        let mut results = Vec::new();
        for doc in cursor {
            let doc = doc.unwrap();
            let doc = serde_json::to_string(&doc).unwrap();
            let doc = std::ffi::CString::new(doc).unwrap();
            results.push(doc.into_raw() as *mut std::ffi::c_void);
        }
        results.push(std::ptr::null_mut());
        let layout = std::alloc::Layout::array::<*mut std::ffi::c_void>(results.len()).unwrap();
        let vector = std::alloc::alloc(layout) as *mut *mut std::ffi::c_void;
        std::ptr::copy_nonoverlapping(results.as_ptr(), vector, results.len());
        vector
    }
}

#[no_mangle]
pub extern "C" fn Vector_destroy(mut ptr: *mut *mut std::ffi::c_void) {
    unsafe {
        if !ptr.is_null() {
            let length = {
                let mut length = 0;
                while !(*ptr).is_null() {
                    length += 1;
                    ptr = ptr.add(1);
                }
                length
            };
            let layout = std::alloc::Layout::array::<*mut std::ffi::c_void>(length).unwrap();
            std::alloc::dealloc(ptr as *mut u8, layout)
        }
    }
}

#[no_mangle]
pub extern "C" fn String_destroy(ptr: *mut std::ffi::c_char) {
    unsafe {
        if !ptr.is_null() {
            std::mem::drop(std::ffi::CString::from_raw(ptr));
        }
    }
}