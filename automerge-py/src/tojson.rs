use automerge as am;

fn map_to_json(doc: &am::Automerge, obj: &am::ObjId) -> serde_json::Value {
    let keys = doc.keys(obj);
    let mut map = serde_json::Map::new();
    for k in keys {
        let val = doc.get(obj, &k);
        match val {
            Ok(Some((am::Value::Object(o), exid)))
                if o == am::ObjType::Map || o == am::ObjType::Table =>
            {
                map.insert(k.to_owned(), map_to_json(doc, &exid));
            }
            Ok(Some((am::Value::Object(_), exid))) => {
                map.insert(k.to_owned(), list_to_json(doc, &exid));
            }
            Ok(Some((am::Value::Scalar(v), _))) => {
                map.insert(k.to_owned(), scalar_to_json(&v));
            }
            _ => (),
        };
    }
    serde_json::Value::Object(map)
}

fn list_to_json(doc: &am::Automerge, obj: &am::ObjId) -> serde_json::Value {
    let len = doc.length(obj);
    let mut array = Vec::new();
    for i in 0..len {
        let val = doc.get(obj, i as usize);
        match val {
            Ok(Some((am::Value::Object(o), exid)))
                if o == am::ObjType::Map || o == am::ObjType::Table =>
            {
                array.push(map_to_json(doc, &exid));
            }
            Ok(Some((am::Value::Object(_), exid))) => {
                array.push(list_to_json(doc, &exid));
            }
            Ok(Some((am::Value::Scalar(v), _))) => {
                array.push(scalar_to_json(&v));
            }
            _ => (),
        };
    }
    serde_json::Value::Array(array)
}

fn scalar_to_json(val: &am::ScalarValue) -> serde_json::Value {
    match val {
        am::ScalarValue::Str(s) => serde_json::Value::String(s.to_string()),
        am::ScalarValue::Bytes(b) | am::ScalarValue::Unknown { bytes: b, .. } => {
            serde_json::Value::Array(
                b.iter()
                    .map(|byte| serde_json::Value::Number((*byte).into()))
                    .collect(),
            )
        }
        am::ScalarValue::Int(n) => serde_json::Value::Number((*n).into()),
        am::ScalarValue::Uint(n) => serde_json::Value::Number((*n).into()),
        am::ScalarValue::F64(n) => serde_json::Number::from_f64(*n)
            .unwrap_or_else(|| 0_i64.into())
            .into(),
        am::ScalarValue::Counter(c) => serde_json::Value::Number(i64::from(c).into()),
        am::ScalarValue::Timestamp(n) => serde_json::Value::Number((*n).into()),
        am::ScalarValue::Boolean(b) => serde_json::Value::Bool(*b),
        am::ScalarValue::Null => serde_json::Value::Null,
    }
}

pub fn tojson(input_data: Vec<u8>) -> String {
    let doc = am::Automerge::load(&input_data).unwrap();
    let json = map_to_json(&doc, &am::ObjId::Root);
    serde_json::to_string_pretty(&json).unwrap()
}
