use pyo3::prelude::*;
use pyo3::exceptions::PyOSError;
use polodb_core::Database;
use std::path::Path;

/// Formats the sum of two numbers as string.
// #[pyfunction]
// fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
//     Ok((a + b).to_string())
// }

#[pyclass]
struct PyDatabase {
    inner: Database,
}

#[pymethods]
impl PyDatabase {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let db_path = Path::new(path);
        match Database::open_path(db_path) {
            Ok(db) => Ok(PyDatabase { inner: db }),
            Err(e) => Err(PyOSError::new_err(e.to_string())),
        }
    }

    #[staticmethod]
    fn open_path(path: &str) -> PyResult<PyDatabase> {
        let db_path = Path::new(path);
        Database::open_path(db_path)
            .map(|db| PyDatabase { inner: db })
            .map_err(|e| PyOSError::new_err(e.to_string()))
    }

    // You can add methods here to interact with the Database
}
// #[pyfunction]
// fn open_path(path: &str) -> PyResult<Database> {
//     let db_path = Path::new(path);
//     match Database::open_path(db_path) {
//         Ok(db) => Ok(db),
//         Err(e) => Err(PyOSError::new_err(e.to_string())),
//     }
// }

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn rust_polodb(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // m.add_function(wrap_pyfunction!(sum_as_string, m)?);
    m.add_class::<PyDatabase>()?;
    Ok(())

}