use wasm_bindgen::prelude::*;
use polodb_core::Database;
use polodb_core::bson;

#[wasm_bindgen(js_name = Database)]
pub struct DatabaseWrapper(Database);

#[wasm_bindgen(js_class = Database)]
impl DatabaseWrapper {

    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<DatabaseWrapper, JsError> {
        let db = Database::open_memory()?;
        Ok(DatabaseWrapper(db))
    }

    #[wasm_bindgen]
    pub fn handle_message(&self, buf: &[u8]) -> Result<Vec<u8>, JsError> {
        let bson = bson::from_slice(buf)?;
        let result = self.0.handle_request_doc(bson)?;
        let result_vec = bson::to_vec(&result.value)?;
        Ok(result_vec)
    }

}
