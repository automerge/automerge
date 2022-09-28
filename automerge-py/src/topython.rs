use automerge as am;
use pyo3::prelude::*;
use pyo3::types::*;

fn pymap(py: Python, doc: &am::Automerge, obj: &am::ObjId) -> PyObject {
    let keys = doc.keys(obj);
    let dict = PyDict::new(py);
    for k in keys {
        let val = doc.get(obj, &k);
        match val {
            Ok(Some((am::Value::Object(o), exid)))
                if o == am::ObjType::Map || o == am::ObjType::Table =>
            {
                dict.set_item(k.to_owned(), pymap(py, doc, &exid)).unwrap();
            }
            Ok(Some((am::Value::Object(_), exid))) => {
                dict.set_item(k.to_owned(), pylist(py, doc, &exid)).unwrap();
            }
            Ok(Some((am::Value::Scalar(v), _))) => {
                dict.set_item(k.to_owned(), pyscalar(py, &v)).unwrap();
            }
            _ => (),
        };
    }
    dict.into_py(py)
}

fn pylist(py: Python, doc: &am::Automerge, obj: &am::ObjId) -> PyObject {
    let len = doc.length(obj);
    let list = PyList::empty(py);
    for i in 0..len {
        let val = doc.get(obj, i as usize);
        match val {
            Ok(Some((am::Value::Object(o), exid)))
                if o == am::ObjType::Map || o == am::ObjType::Table =>
            {
                list.append(pymap(py, doc, &exid)).unwrap();
            }
            Ok(Some((am::Value::Object(_), exid))) => {
                list.append(pylist(py, doc, &exid)).unwrap();
            }
            Ok(Some((am::Value::Scalar(v), _))) => {
                list.append(pyscalar(py, &v)).unwrap();
            }
            _ => (),
        };
    }
    list.into_py(py)
}

fn pyscalar(py: Python, val: &am::ScalarValue) -> PyObject {
    match val {
        am::ScalarValue::Str(s) => PyString::new(py, &s).into_py(py),
        am::ScalarValue::Bytes(b) | am::ScalarValue::Unknown { bytes: b, .. } => {
            b.clone().into_py(py)
        }
        am::ScalarValue::Int(n) => n.into_py(py),
        am::ScalarValue::Uint(n) => n.into_py(py),
        am::ScalarValue::F64(n) => n.into_py(py),
        am::ScalarValue::Counter(c) => i64::from(c).into_py(py),
        am::ScalarValue::Timestamp(n) => (*n).into_py(py),
        am::ScalarValue::Boolean(b) => b.into_py(py),
        am::ScalarValue::Null => py.None(),
    }
}

pub fn topython(py: Python, input_data: Vec<u8>) -> PyObject {
    let doc = am::Automerge::load(&input_data).unwrap();
    pymap(py, &doc, &am::ObjId::Root)
}
