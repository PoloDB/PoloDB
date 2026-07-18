use crate::helper_type_translator::{
    bson_to_py_obj, convert_py_list_to_vec_document, convert_py_obj_to_document,
    delete_result_to_pydict, document_to_pydict, update_result_to_pydict,
};
use polodb_core::bson::Document;
use polodb_core::{Collection, CollectionT, Database};
use pyo3::exceptions::PyOSError;
use pyo3::exceptions::PyRuntimeError; // Import PyRuntimeError for error handling
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::types::{PyDict, PyList};
use pyo3::IntoPyObjectExt;
use std::path::Path;
use std::sync::{Arc, Mutex};

#[pyclass]
pub struct PyCollection {
    inner: Arc<Collection<Document>>, // Use Arc for thread-safe shared ownership
}

#[pymethods]
impl PyCollection {
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    pub fn update_one(
        &self,
        py: Python<'_>,
        filter: Py<PyDict>,
        update: Py<PyDict>,
    ) -> PyResult<Option<Py<PyAny>>> {
        // Convert PyDict to BSON Document
        let filter_doc = convert_py_obj_to_document(filter.bind(py).as_any())?;
        let update_doc = convert_py_obj_to_document(update.bind(py).as_any())?;

        // Call the Rust method `find_one`
        match self.inner.update_one(filter_doc, update_doc) {
            Ok(update_result) => {
                // Convert BSON Document to Python Dict
                let py_result = update_result_to_pydict(py, update_result).unwrap();
                Ok(Some(py_result.into_any()))
            }
            Err(err) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Update one error: {}",
                err
            ))),
        }
    }
    pub fn update_many(
        &self,
        py: Python<'_>,
        filter: Py<PyDict>,
        update: Py<PyDict>,
    ) -> PyResult<Option<Py<PyAny>>> {
        // Convert PyDict to BSON Document
        let filter_doc = convert_py_obj_to_document(filter.bind(py).as_any())?;
        let update_doc = convert_py_obj_to_document(update.bind(py).as_any())?;

        // Call the Rust method `find_one`
        match self.inner.update_many(filter_doc, update_doc) {
            Ok(update_result) => {
                // Convert BSON Document to Python Dict
                let py_result = update_result_to_pydict(py, update_result).unwrap();
                Ok(Some(py_result.into_any()))
            }
            Err(err) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Update many error: {}",
                err
            ))),
        }
    }
    pub fn insert_many(&self, py: Python<'_>, doc: Py<PyList>) -> PyResult<Py<PyAny>> {
        let bson_vec_docs = convert_py_list_to_vec_document(doc.bind(py))?;
        match self.inner.insert_many(bson_vec_docs) {
            Ok(result) => {
                let dict = PyDict::new(py);
                for (key, value) in &result.inserted_ids {
                    dict.set_item(key, bson_to_py_obj(py, value))?;
                }
                Ok(dict.into_any().unbind())
            }
            Err(e) => Err(PyRuntimeError::new_err(format!("Insert many error: {}", e))),
        }
    }

    pub fn count_documents(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.inner.count_documents() {
            Ok(result) => result.into_py_any(py),
            Err(e) => Err(PyRuntimeError::new_err(format!(
                "Count documents error: {}",
                e
            ))),
        }
    }

    pub fn insert_one(&self, py: Python<'_>, doc: Py<PyDict>) -> PyResult<Py<PyAny>> {
        let bson_doc: Document = match convert_py_obj_to_document(doc.bind(py).as_any()) {
            Ok(d) => d,
            Err(e) => return Err(PyRuntimeError::new_err(format!("Insert many error: {}", e))),
        };
        match self.inner.insert_one(bson_doc) {
            Ok(result) => {
                let py_inserted_id = bson_to_py_obj(py, &result.inserted_id);
                let dict = PyDict::new(py);
                dict.set_item("inserted_id", py_inserted_id)?;
                Ok(dict.into_any().unbind())
            }
            Err(e) => Err(PyRuntimeError::new_err(format!("Insert error: {}", e))),
        }
    }

    pub fn delete_one(&self, py: Python<'_>, filter: Py<PyDict>) -> PyResult<Py<PyAny>> {
        let bson_doc: Document = match convert_py_obj_to_document(filter.bind(py).as_any()) {
            Ok(d) => d,
            Err(e) => return Err(PyRuntimeError::new_err(format!("Delete one : {}", e))),
        };
        match self.inner.delete_one(bson_doc) {
            Ok(delete_result) => {
                let py_result = delete_result_to_pydict(py, delete_result)?;
                Ok(py_result.into_any())
            }
            Err(e) => Err(PyRuntimeError::new_err(format!("Delete one error: {}", e))),
        }
    }

    pub fn delete_many(&self, py: Python<'_>, filter: Py<PyDict>) -> PyResult<Py<PyAny>> {
        let bson_doc: Document = match convert_py_obj_to_document(filter.bind(py).as_any()) {
            Ok(d) => d,
            Err(e) => return Err(PyRuntimeError::new_err(format!("Delete many : {}", e))),
        };

        match self.inner.delete_many(bson_doc) {
            Ok(delete_result) => {
                let py_result = delete_result_to_pydict(py, delete_result)?;
                Ok(py_result.into_any())
            }
            Err(e) => Err(PyRuntimeError::new_err(format!("Delete one error: {}", e))),
        }
    }

    fn aggregate(&self, py: Python<'_>, pipeline: Py<PyList>) -> PyResult<Py<PyAny>> {
        let pipeline_documents = convert_py_list_to_vec_document(pipeline.bind(py))?;
        match self.inner.aggregate(pipeline_documents).run() {
            Ok(agg_cursor) => {
                let vec_res: Vec<Py<PyDict>> = agg_cursor
                    .map(|x| document_to_pydict(py, x.unwrap()).unwrap())
                    .collect();
                vec_res.into_py_any(py)
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Error in Aggregate {}",
                e
            ))),
        }
    }

    pub fn find_one(&self, py: Python<'_>, filter: Py<PyDict>) -> PyResult<Option<Py<PyAny>>> {
        // Convert PyDict to BSON Document
        let filter_doc = convert_py_obj_to_document(filter.bind(py).as_any())?;
        // Call the Rust method `find_one`
        match self.inner.find_one(filter_doc) {
            Ok(Some(result_doc)) => {
                // Convert BSON Document to Python Dict
                let py_result = document_to_pydict(py, result_doc).unwrap();
                Ok(Some(py_result.into_any()))
            }
            Ok(None) => Ok(None), // Return None if no document is found
            Err(err) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Find one error: {}",
                err
            ))),
        }
    }
    pub fn find(&self, py: Python<'_>, filter: Py<PyDict>) -> PyResult<Option<Py<PyAny>>> {
        // Convert PyDict to BSON Document
        let filter_doc = convert_py_obj_to_document(filter.bind(py).as_any())?;

        // Call the Rust method `find_one`
        match self.inner.find(filter_doc).run() {
            Ok(result_doc) => {
                // Convert BSON Document to Python Dict
                let py_result: Vec<Py<PyDict>> = result_doc
                    .map(|x| document_to_pydict(py, x.unwrap()).unwrap())
                    .collect();
                // let py_result = document_to_pydict(py, result_doc).unwrap();
                Ok(Some(py_result.into_py_any(py)?))
            }
            // Ok(None) => Ok(None), // Return None if no document is found
            Err(err) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Find one error: {}",
                err
            ))),
        }
    }
}
impl From<Collection<Document>> for PyCollection {
    fn from(collection: Collection<Document>) -> PyCollection {
        PyCollection {
            inner: Arc::new(collection),
        }
    }
}

#[pyclass]
pub struct PyDatabase {
    inner: Arc<Mutex<Database>>,
}

#[pymethods]
impl PyDatabase {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let db_path = Path::new(path);
        match Database::open_path(db_path) {
            Ok(db) => Ok(PyDatabase {
                inner: Arc::new(Mutex::new(db)),
            }),
            Err(e) => Err(PyOSError::new_err(e.to_string())),
        }
    }

    #[staticmethod]
    fn open_path(path: &str) -> PyResult<PyDatabase> {
        let db_path = Path::new(path);
        Database::open_path(db_path)
            .map(|db| PyDatabase {
                inner: Arc::new(Mutex::new(db)),
            })
            .map_err(|e| PyOSError::new_err(e.to_string()))
    }

    pub fn create_collection(&self, name: &str) -> PyResult<()> {
        let _ = self.inner.lock().unwrap().create_collection(name);
        Ok(())
    }

    fn collection(&self, name: &str) -> PyResult<PyCollection> {
        // Attempt to acquire the lock and fetch/create the collection
        let guard = self
            .inner
            .lock()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to lock: {}", e)))?;
        let rust_collection = guard.collection::<Document>(name); // Assume this returns a Rust Collection

        //Convert a Rust Collection to a PyCollection
        let py_collection: PyCollection = PyCollection::from(rust_collection);
        Ok(py_collection)
    }

    pub fn list_collection_names(&self) -> PyResult<Vec<String>> {
        let collections_names = self.inner.lock().unwrap().list_collection_names();
        match collections_names {
            Ok(collection_names) => Ok(collection_names),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Error listing collection names: {}",
                e
            ))),
        }
    }

    // You can add methods here to interact with the Database
}
