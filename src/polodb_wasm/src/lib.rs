use std::rc::Rc;
use std::cell::RefCell;
use wasm_bindgen::prelude::*;
use polodb_core::{bson, DatabaseServer};
#[cfg(target_arch = "wasm32")]
use polodb_core::lsm::IndexeddbBackend;
#[cfg(target_arch = "wasm32")]
use polodb_core::Database;

#[wasm_bindgen(js_name = Database)]
pub struct DatabaseWrapper {
    db:        Rc<RefCell<Option<DatabaseServer>>>,
    onsuccess: Option<js_sys::Function>,
    onerror:   Option<js_sys::Function>,
}

#[wasm_bindgen(js_class = Database)]
impl DatabaseWrapper {

    #[wasm_bindgen(constructor)]
    pub fn new() -> DatabaseWrapper {
        DatabaseWrapper {
            db: Rc::new(RefCell::new(None)),
            onsuccess: None,
            onerror: None,
        }
    }

    /// If a name is provided, the data will be synced to IndexedDB
    #[wasm_bindgen]
    #[cfg(target_arch = "wasm32")]
    pub async fn open(&mut self, name: Option<String>) -> Result<(), JsError> {
        match name {
            Some(name) => {
                let init_data = IndexeddbBackend::load_snapshot(&name).await;
                let db = Database::open_indexeddb(init_data)?;
                let mut db_ref = self.db.as_ref().borrow_mut();
                *db_ref = Some(DatabaseServer::new(db));
            },
            None => {
                let db = Database::open_memory()?;
                let mut db_ref = self.db.as_ref().borrow_mut();
                *db_ref = Some(DatabaseServer::new(db));
            },
        };
        Ok(())
    }

    #[wasm_bindgen(js_name = handleMessage)]
    pub fn handle_message(&self, buf: &[u8]) -> Result<Vec<u8>, JsError> {
        let mut db_ref = self.db.as_ref().borrow_mut();
        let db = db_ref.as_mut().unwrap();
        let bson = bson::from_slice(buf)?;
        let result = db.handle_request_doc(bson)?;
        let result_vec = bson::to_vec(&result.value)?;
        Ok(result_vec)
    }

    #[wasm_bindgen(getter)]
    pub fn onsuccess(&self) -> Option<js_sys::Function> {
        self.onsuccess.clone()
    }

    #[wasm_bindgen(setter)]
    pub fn set_onsuccess(&mut self, fun: js_sys::Function) {
        self.onsuccess = Some(fun);
    }

    #[wasm_bindgen(getter)]
    pub fn onerror(&self) -> Option<js_sys::Function> {
        self.onerror.clone()
    }

    #[wasm_bindgen(setter)]
    pub fn set_onerror(&mut self, fun: js_sys::Function) {
        self.onerror = Some(fun);
    }
}
