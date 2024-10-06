use pyo3::prelude::*;

mod doc_bson_to_py_translator;
mod py_database;

use py_database::PyCollection;
use py_database::PyDatabase;

#[pymodule]
fn rust_polodb(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // m.add_function(wrap_pyfunction!(sum_as_string, m)?);
    m.add_class::<PyDatabase>()?;
    m.add_class::<PyCollection>()?;

    Ok(())
}
