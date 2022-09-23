use automerge as am;
use automerge::transaction::Transactable;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::*;

#[derive(FromPyObject)]
enum ScalarTypes {
    Bool(bool),
    Int(i64),
    String(String),
}

pub fn initialize_from_python(py: Python, dict: &PyDict) -> PyResult<am::AutoCommit> {
    let mut doc = am::AutoCommit::new();
    import_dict(&mut doc, &am::ObjId::Root, &dict)?;
    Ok(doc)
}

fn me(e: am::AutomergeError) -> PyErr {
    PyRuntimeError::new_err(format!("automerge error : {}", e.to_string()))
}

fn import_dict(doc: &mut am::AutoCommit, obj: &am::ObjId, dict: &PyDict) -> PyResult<()> {
    for (key, value) in dict {
        if let Ok(key) = key.extract::<String>() {
            if let Ok(value) = value.extract::<ScalarTypes>() {
                match value {
                    ScalarTypes::Int(value) => doc.put(obj, key, value).map_err(me)?,
                    ScalarTypes::String(value) => doc.put(obj, key, value).map_err(me)?,
                    ScalarTypes::Bool(value) => doc.put(obj, key, value).map_err(me)?,
                }
            } else {
                if let Ok(valuedict) = value.extract::<&PyDict>() {
                    let id = doc.put_object(obj, key, am::ObjType::Map).map_err(me)?;
                    import_dict(doc, &id, valuedict)?;
                } else {
                    if let Ok(valuelist) = value.extract::<&PyList>() {
                        let id = doc.put_object(obj, key, am::ObjType::List).map_err(me)?;
                        import_list(doc, &id, valuelist)?;
                    } else {
                        return Err(PyTypeError::new_err(format!(
                            "value type unsupport : {}",
                            value.get_type()
                        )));
                    }
                }
            }
        } else {
            return Err(PyTypeError::new_err(
                "only string dictionary keys are supported by automerge",
            ));
        }
    }
    Ok(())
}

fn import_list(doc: &mut am::AutoCommit, obj: &am::ObjId, list: &PyList) -> PyResult<()> {
    for (i, value) in list.iter().enumerate() {
        if let Ok(value) = value.extract::<ScalarTypes>() {
            match value {
                ScalarTypes::Int(value) => doc.insert(obj, i, value).map_err(me)?,
                ScalarTypes::String(value) => doc.insert(obj, i, value).map_err(me)?,
                ScalarTypes::Bool(value) => doc.insert(obj, i, value).map_err(me)?,
            }
        } else {
            if let Ok(valuedict) = value.extract::<&PyDict>() {
                let id = doc.insert_object(obj, i, am::ObjType::Map).map_err(me)?;
                import_dict(doc, &id, valuedict)?;
            } else {
                if let Ok(valuelist) = value.extract::<&PyList>() {
                    let id = doc.insert_object(obj, i, am::ObjType::List).map_err(me)?;
                    import_list(doc, &id, valuelist)?;
                } else {
                    return Err(PyTypeError::new_err(format!(
                        "value type unsupport : {}",
                        value.get_type()
                    )));
                }
            }
        }
    }
    Ok(())
}
