use automerge::Automerge;
use pyo3::prelude::*;
mod frompython;
mod tojson;
mod topython;
use pyo3::exceptions::PyTypeError;
use pyo3::types::PyDict;
use automerge as am;
use automerge::transaction::Transactable;
use pyo3::exceptions::{PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::*;
#[pyclass]
struct Doc {
    inner: Vec<u8>,
}

#[pymethods]
impl Doc {
    #[new]
    fn new(py: Python, obj: PyObject) -> PyResult<Self> {
        if let Ok(bytes) = obj.extract::<Vec<u8>>(py) {
            Ok(Doc { inner: bytes })
        } else {
            if let Ok(dict) = obj.extract::<&PyDict>(py) {
                frompython::initialize_from_python(py, dict).map(|mut am| Doc { inner: am.save() })
            } else {
                Err(PyTypeError::new_err("Only bytes or dictionary"))
            }
        }
    }

    fn merge(&self, value: Vec<u8>) -> Self {
        let mut am = Automerge::load(&self.inner).unwrap();
        let mut am2 = Automerge::load(&value).unwrap();
        am.merge(&mut am2).unwrap();
        Doc { inner: am.save() }
    }

    fn obj(&self, py: Python) -> PyObject {
        return crate::topython::topython(py, self.inner.clone());
    }

    fn json(&self) -> String {
        return crate::tojson::tojson(self.inner.clone());
    }
}

#[pymodule]
#[pyo3(name = "automerge")]
fn automerge_py(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Doc>().unwrap();
    Ok(())
}
