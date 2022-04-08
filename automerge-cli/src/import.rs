use automerge as am;
use automerge::transaction::Transactable;

pub(crate) fn initialize_from_json(
    json_value: &serde_json::Value,
) -> anyhow::Result<am::AutoCommit> {
    let mut doc = am::AutoCommit::new();
    match json_value {
        serde_json::Value::Object(m) => {
            import_map(&mut doc, &am::ObjId::Root, m)?;
            Ok(doc)
        }
        _ => anyhow::bail!("expected an object"),
    }
}

fn import_map(
    doc: &mut am::AutoCommit,
    obj: &am::ObjId,
    map: &serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<()> {
    for (key, value) in map {
        match value {
            serde_json::Value::Null => {
                doc.put(obj, key, ())?;
            }
            serde_json::Value::Bool(b) => {
                doc.put(obj, key, *b)?;
            }
            serde_json::Value::String(s) => {
                doc.put(obj, key, s)?;
            }
            serde_json::Value::Array(vec) => {
                let id = doc.put_object(obj, key, am::ObjType::List)?;
                import_list(doc, &id, vec)?;
            }
            serde_json::Value::Number(n) => {
                if let Some(m) = n.as_i64() {
                    doc.put(obj, key, m)?;
                } else if let Some(m) = n.as_u64() {
                    doc.put(obj, key, m)?;
                } else if let Some(m) = n.as_f64() {
                    doc.put(obj, key, m)?;
                } else {
                    anyhow::bail!("not a number");
                }
            }
            serde_json::Value::Object(map) => {
                let id = doc.put_object(obj, key, am::ObjType::Map)?;
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
) -> anyhow::Result<()> {
    for (i, value) in list.iter().enumerate() {
        match value {
            serde_json::Value::Null => {
                doc.insert(obj, i, ())?;
            }
            serde_json::Value::Bool(b) => {
                doc.insert(obj, i, *b)?;
            }
            serde_json::Value::String(s) => {
                doc.insert(obj, i, s)?;
            }
            serde_json::Value::Array(vec) => {
                let id = doc.insert_object(obj, i, am::ObjType::List)?;
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
                    anyhow::bail!("not a number");
                }
            }
            serde_json::Value::Object(map) => {
                let id = doc.insert_object(obj, i, am::ObjType::Map)?;
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
