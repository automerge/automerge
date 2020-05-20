use anyhow::Result;
use automerge_backend::Backend;
use automerge_frontend::{Frontend, Value};
use std::io::{Read, Stdin};

pub fn import_json(mut stdin: Stdin) -> Result<Vec<u8>> {
    let mut buffer = String::new();
    stdin.read_to_string(&mut buffer)?;

    let json_value: serde_json::Value = serde_json::from_str(&buffer)?;
    let value: Value = Value::from_json(&json_value);

    let (_, initial_change) = Frontend::new_with_initial_state(value)?;
    let mut backend = Backend::init();
    backend.apply_local_change(initial_change)?;

    Ok(backend.save()?)
}
