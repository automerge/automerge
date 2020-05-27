use anyhow::Result;
use automerge_backend::Backend;
use automerge_frontend::{Frontend, Value};

fn initialize_from_json(json_value: &serde_json::Value) -> Result<Vec<u8>> {
    let value: Value = Value::from_json(&json_value);

    let (_, initial_change) = Frontend::new_with_initial_state(value)?;
    let mut backend = Backend::init();
    backend.apply_local_change(initial_change)?;

    Ok(backend.save()?)
}

pub fn import_json(mut reader: impl std::io::Read, mut writer: impl std::io::Write) -> Result<()> {
    let mut buffer = String::new();
    reader.read_to_string(&mut buffer)?;

    let json_value: serde_json::Value = serde_json::from_str(&buffer)?;
    let changes_bytes = initialize_from_json(&json_value)?;
    writer.write_all(&changes_bytes)?;
    Ok(())
}
