use polodb_core::bson::{Bson, Document};
use polodb_core::results;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::types::{PyAny, PyBool, PyBytes, PyFloat, PyList, PyString};

pub fn convert_py_list_to_vec_document<'a>(py_list_obj: &'a Py<PyAny>) -> Vec<Document> {
    Python::with_gil(|py| {
        // Try to downcast the PyAny to a PyList
        if let Ok(py_list) = py_list_obj.downcast_bound::<PyList>(py) {
            // If downcast is successful, return an iterator over the list's items
            let iter = py_list.iter().map(|item| {
                let py_obj: Py<PyAny> = item.to_object(item.py());
                // Convert each item (expected to be a dictionary) into a BSON document

                convert_py_obj_to_document(&py_obj).unwrap()
            });
            Vec::from_iter(iter)
        } else {
            Vec::from_iter(std::iter::empty())
        }
    })
}

pub fn convert_py_obj_to_document(py_obj: &Py<PyAny>) -> PyResult<Document> {
    Python::with_gil(|py| {
        // Try to extract as a String and convert to BSON
        //    let mut doc: Document = Document::new();
        if let Ok(dict) = py_obj.downcast_bound::<PyDict>(py) {
            // let dict_ref = dict.borrow(); // Convert Py<PyDict> to &PyDict
            let mut doc = Document::new();
            for (key, value) in dict.iter() {
                // Use `iter()` on the `PyDict`
                let key: String = key.extract()?; // Extract the key as a string
                let bson_value = convert_py_obj_to_bson(value.to_object(py).as_any())?; // Convert value to BSON
                doc.insert(key, bson_value);
            }
            Ok(doc)
        }
        // If the type is not supported, return an error
        else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Unsupported Python type for BSON conversion",
            ))
        }
    })
}

pub fn convert_py_obj_to_bson(py_obj: &Py<PyAny>) -> PyResult<Bson> {
    Python::with_gil(|py| {
        // Try to extract as a String and convert to BSON
        if let Ok(rust_string) = py_obj.extract::<String>(py) {
            Ok(Bson::String(rust_string))
        }
        // Try to extract as a bool and convert to BSON
        else if let Ok(rust_bool) = py_obj.extract::<bool>(py) {
            Ok(Bson::Boolean(rust_bool))
        }
        // Try to extract as an int (i64) and convert to BSON
        else if let Ok(rust_int) = py_obj.extract::<i64>(py) {
            Ok(Bson::Int64(rust_int))
        }
        // Try to extract as a float and convert to BSON double
        else if let Ok(rust_float) = py_obj.extract::<f64>(py) {
            Ok(Bson::Double(rust_float))
        }
        // Try to extract as a dictionary and convert to BSON document
        else if let Ok(dict) = py_obj.downcast_bound::<PyDict>(py) {
            let mut bson_doc = Document::new();
            for (key, value) in dict.iter() {
                let key_str: String = key.extract::<String>()?;

                let bson_value = convert_py_obj_to_bson(value.to_object(py).as_any())?;
                bson_doc.insert(key_str, bson_value);
            }
            Ok(Bson::Document(bson_doc))
        }
        // Try to extract as a list and convert to BSON array
        else if let Ok(list) = py_obj.downcast_bound::<PyList>(py) {
            let mut bson_array = Vec::new();
            for item in list.iter() {
                let bson_item = convert_py_obj_to_bson(item.to_object(py).as_any())?;
                bson_array.push(bson_item);
            }
            Ok(Bson::Array(bson_array))
        }
        // If the type is not supported, return an error
        else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Unsupported Python type for BSON conversion",
            ))
        }
    })
}

pub fn delete_result_to_pydict(
    py: Python,
    delete_result: results::DeleteResult,
) -> PyResult<Py<PyDict>> {
    let py_dict = PyDict::new_bound(py);

    // Insert matched_count and modified_count into the PyDict
    py_dict.set_item("deleted_count", delete_result.deleted_count as i64)?;

    Ok(py_dict.into())
}

pub fn update_result_to_pydict(
    py: Python,
    update_result: results::UpdateResult,
) -> PyResult<Py<PyDict>> {
    let py_dict = PyDict::new_bound(py);

    // Insert matched_count and modified_count into the PyDict
    py_dict.set_item("matched_count", update_result.matched_count as i64)?;
    py_dict.set_item("modified_count", update_result.modified_count as i64)?;

    Ok(py_dict.into())
}
pub fn document_to_pydict(py: Python, doc: Document) -> PyResult<Py<PyDict>> {
    let py_dict = PyDict::new_bound(py);
    for (key, value) in doc {
        let py_value = bson_to_py_obj(py, &value);
        py_dict.set_item(key, py_value)?;
    }
    Ok(py_dict.into())
}

pub fn bson_to_py_obj(py: Python, bson: &Bson) -> PyObject {
    match bson {
        Bson::Null => py.None(),
        Bson::Int32(i) => i.into_py(py),
        Bson::Int64(i) => i.into_py(py),
        Bson::Double(f) => PyFloat::new_bound(py, *f).into_py(py),
        Bson::String(s) => PyString::new_bound(py, s).into_py(py),
        Bson::Boolean(b) => PyBool::new_bound(py, *b).into_py(py),
        Bson::Array(arr) => {
            // Create an empty PyList without specifying a slice
            let py_list = PyList::empty_bound(py); // Use empty method instead of new_bound
            for item in arr {
                py_list.append(bson_to_py_obj(py, item)).unwrap();
            }
            py_list.into_py(py)
        }
        Bson::Document(doc) => {
            let py_dict = PyDict::new_bound(py);
            for (key, value) in doc.iter() {
                py_dict.set_item(key, bson_to_py_obj(py, value)).unwrap();
            }
            py_dict.into_py(py)
        }
        Bson::RegularExpression(regex) => {
            let re_module = py.import_bound("re").unwrap();
            re_module
                .call_method1("compile", (regex.pattern.as_str(),))
                .unwrap()
                .to_object(py)
        }
        // Handle JavaScript code
        Bson::JavaScriptCode(code) => PyString::new_bound(py, code).into_py(py),
        Bson::Timestamp(ts) => (ts.time, ts.increment).into_py(py),
        Bson::Binary(bin) => PyBytes::new_bound(py, &bin.bytes).into_py(py),
        Bson::ObjectId(oid) => PyString::new_bound(py, &oid.to_hex()).into_py(py),
        Bson::DateTime(dt) => {
            let timestamp = dt.timestamp_millis() / 1000;
            let datetime = py
                .import_bound("datetime")
                .unwrap()
                .getattr("datetime")
                .unwrap();
            datetime.call1((timestamp,)).unwrap().to_object(py)
        }
        Bson::Symbol(s) => PyString::new_bound(py, s).into_py(py),

        // Handle undefined value (deprecated)
        Bson::Undefined => py.None(),

        // Handle MaxKey (convert to None)
        Bson::MaxKey => py.None(),

        // Handle MinKey (convert to None)
        Bson::MinKey => py.None(),

        _ => py.None(), // Handle other BSON types as needed
    }
}
