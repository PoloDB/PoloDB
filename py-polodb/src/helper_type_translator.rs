use polodb_core::bson::{Bson, Document};
use polodb_core::results;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::types::{PyAny, PyBool, PyBytes, PyFloat, PyList, PyString};
use pyo3::IntoPyObjectExt;

pub fn convert_py_list_to_vec_document(py_list: &Bound<'_, PyList>) -> PyResult<Vec<Document>> {
    py_list
        .iter()
        .map(|item| convert_py_obj_to_document(item.as_any()))
        .collect()
}

pub fn convert_py_obj_to_document(py_obj: &Bound<'_, PyAny>) -> PyResult<Document> {
    if let Ok(dict) = py_obj.cast::<PyDict>() {
        let mut doc = Document::new();
        for (key, value) in dict.iter() {
            let key: String = key.extract()?;
            let bson_value = convert_py_obj_to_bson(&value)?;
            doc.insert(key, bson_value);
        }
        Ok(doc)
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Unsupported Python type for BSON conversion",
        ))
    }
}

pub fn convert_py_obj_to_bson(py_obj: &Bound<'_, PyAny>) -> PyResult<Bson> {
    if let Ok(rust_string) = py_obj.extract::<String>() {
        Ok(Bson::String(rust_string))
    } else if let Ok(rust_bool) = py_obj.extract::<bool>() {
        Ok(Bson::Boolean(rust_bool))
    } else if let Ok(rust_int) = py_obj.extract::<i64>() {
        Ok(Bson::Int64(rust_int))
    } else if let Ok(rust_float) = py_obj.extract::<f64>() {
        Ok(Bson::Double(rust_float))
    } else if let Ok(dict) = py_obj.cast::<PyDict>() {
        let mut bson_doc = Document::new();
        for (key, value) in dict.iter() {
            let key_str: String = key.extract::<String>()?;
            let bson_value = convert_py_obj_to_bson(&value)?;
            bson_doc.insert(key_str, bson_value);
        }
        Ok(Bson::Document(bson_doc))
    } else if let Ok(list) = py_obj.cast::<PyList>() {
        let mut bson_array = Vec::new();
        for item in list.iter() {
            bson_array.push(convert_py_obj_to_bson(&item)?);
        }
        Ok(Bson::Array(bson_array))
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Unsupported Python type for BSON conversion",
        ))
    }
}

pub fn delete_result_to_pydict(
    py: Python<'_>,
    delete_result: results::DeleteResult,
) -> PyResult<Py<PyDict>> {
    let py_dict = PyDict::new(py);

    // Insert matched_count and modified_count into the PyDict
    py_dict.set_item("deleted_count", delete_result.deleted_count as i64)?;

    Ok(py_dict.into())
}

pub fn update_result_to_pydict(
    py: Python<'_>,
    update_result: results::UpdateResult,
) -> PyResult<Py<PyDict>> {
    let py_dict = PyDict::new(py);

    // Insert matched_count and modified_count into the PyDict
    py_dict.set_item("matched_count", update_result.matched_count as i64)?;
    py_dict.set_item("modified_count", update_result.modified_count as i64)?;

    Ok(py_dict.into())
}
pub fn document_to_pydict(py: Python<'_>, doc: Document) -> PyResult<Py<PyDict>> {
    let py_dict = PyDict::new(py);
    for (key, value) in doc {
        let py_value = bson_to_py_obj(py, &value);
        py_dict.set_item(key, py_value)?;
    }
    Ok(py_dict.into())
}

pub fn bson_to_py_obj(py: Python<'_>, bson: &Bson) -> Py<PyAny> {
    match bson {
        Bson::Null => py.None(),
        Bson::Int32(i) => i.into_py_any(py).unwrap(),
        Bson::Int64(i) => i.into_py_any(py).unwrap(),
        Bson::Double(f) => PyFloat::new(py, *f).into_any().unbind(),
        Bson::String(s) => PyString::new(py, s).into_any().unbind(),
        Bson::Boolean(b) => PyBool::new(py, *b).to_owned().into_any().unbind(),
        Bson::Array(arr) => {
            // Create an empty PyList without specifying a slice
            let py_list = PyList::empty(py);
            for item in arr {
                py_list.append(bson_to_py_obj(py, item)).unwrap();
            }
            py_list.into_any().unbind()
        }
        Bson::Document(doc) => {
            let py_dict = PyDict::new(py);
            for (key, value) in doc.iter() {
                py_dict.set_item(key, bson_to_py_obj(py, value)).unwrap();
            }
            py_dict.into_any().unbind()
        }
        Bson::RegularExpression(regex) => {
            let re_module = py.import("re").unwrap();
            re_module
                .call_method1("compile", (regex.pattern.as_str(),))
                .unwrap()
                .into_any()
                .unbind()
        }
        // Handle JavaScript code
        Bson::JavaScriptCode(code) => PyString::new(py, code).into_any().unbind(),
        Bson::Timestamp(ts) => (ts.time, ts.increment).into_py_any(py).unwrap(),
        Bson::Binary(bin) => PyBytes::new(py, &bin.bytes).into_any().unbind(),
        Bson::ObjectId(oid) => PyString::new(py, &oid.to_hex()).into_any().unbind(),
        Bson::DateTime(dt) => {
            let timestamp = dt.timestamp_millis() / 1000;
            let datetime = py.import("datetime").unwrap().getattr("datetime").unwrap();
            datetime.call1((timestamp,)).unwrap().into_any().unbind()
        }
        Bson::Symbol(s) => PyString::new(py, s).into_any().unbind(),

        // Handle undefined value (deprecated)
        Bson::Undefined => py.None(),

        // Handle MaxKey (convert to None)
        Bson::MaxKey => py.None(),

        // Handle MinKey (convert to None)
        Bson::MinKey => py.None(),

        _ => py.None(), // Handle other BSON types as needed
    }
}
