
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod file_lock;

pub(crate) mod vli;
pub(crate) mod bson;
