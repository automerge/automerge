use automerge as am;
use automerge::transaction::Transactable;

pub(crate) fn initialize_from_json(
    json_value: &serde_json::Value,
) -> Result<am::AutoCommit, am::AutomergeError> {
    let mut doc = am::AutoCommit::new();
    match json_value {
        serde_json::Value::Object(m) => {
            import_map(&mut doc, &am::ObjId::Root, m)?;
            Ok(doc)
        }
        _ => Err(am::AutomergeError::Decoding),
    }
}

fn import_map(
    doc: &mut am::AutoCommit,
    obj: &am::ObjId,
    map: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), am::AutomergeError> {
    for (key, value) in map {
        match value {
            serde_json::Value::Null => {
                doc.set(obj, key, ())?;
            }
            serde_json::Value::Bool(b) => {
                doc.set(obj, key, *b)?;
            }
            serde_json::Value::String(s) => {
                doc.set(obj, key, s.as_ref())?;
            }
            serde_json::Value::Array(vec) => {
                let id = doc.set(obj, key, am::Value::list())?.unwrap();
                import_list(doc, &id, vec)?;
            }
            serde_json::Value::Number(n) => {
                if let Some(m) = n.as_i64() {
                    doc.set(obj, key, m)?;
                } else if let Some(m) = n.as_u64() {
                    doc.set(obj, key, m)?;
                } else if let Some(m) = n.as_f64() {
                    doc.set(obj, key, m)?;
                } else {
                    return Err(am::AutomergeError::Decoding);
                }
            }
            serde_json::Value::Object(map) => {
                let id = doc.set(obj, key, am::Value::map())?.unwrap();
                import_map(doc, &id, map)?;
            }
        }
    }
    Ok(())
}

fn import_list(
    doc: &mut am::AutoCommit,
    obj: &am::ObjId,
    list: &[serde_json::Value],
) -> Result<(), am::AutomergeError> {
    for (i, value) in list.iter().enumerate() {
        match value {
            serde_json::Value::Null => {
                doc.insert(obj, i, ())?;
            }
            serde_json::Value::Bool(b) => {
                doc.insert(obj, i, *b)?;
            }
            serde_json::Value::String(s) => {
                doc.insert(obj, i, s.as_ref())?;
            }
            serde_json::Value::Array(vec) => {
                let id = doc.insert(obj, i, am::Value::list())?.unwrap();
                import_list(doc, &id, vec)?;
            }
            serde_json::Value::Number(n) => {
                if let Some(m) = n.as_i64() {
                    doc.insert(obj, i, m)?;
                } else if let Some(m) = n.as_u64() {
                    doc.insert(obj, i, m)?;
                } else if let Some(m) = n.as_f64() {
                    doc.insert(obj, i, m)?;
                } else {
                    return Err(am::AutomergeError::Decoding);
                }
            }
            serde_json::Value::Object(map) => {
                let id = doc.insert(obj, i, am::Value::map())?.unwrap();
                import_map(doc, &id, map)?;
            }
        }
    }
    Ok(())
}

pub fn import_json(
    mut reader: impl std::io::Read,
    mut writer: impl std::io::Write,
) -> anyhow::Result<()> {
    let mut buffer = String::new();
    reader.read_to_string(&mut buffer)?;

    let json_value: serde_json::Value = serde_json::from_str(&buffer)?;
    let mut doc = initialize_from_json(&json_value)?;
    writer.write_all(&doc.save())?;
    Ok(())
}
