use anyhow::Result;
use automerge_backend::AutomergeError;
use automerge_protocol::Patch;
use serde_json::Value;
use std::fs::read;
use std::path::Path;

fn get_patch(changes_bytes: Vec<u8>) -> Result<Patch, AutomergeError> {
    let mut backend = automerge_backend::Backend::init();
    backend.apply_changes_binary(vec![changes_bytes])
}

fn get_state_json(changes_file: &Path) -> Result<Value> {
    let input_data = read(changes_file)?;

    let patch = get_patch(input_data)?;
    let mut frontend = automerge_frontend::Frontend::new();
    frontend.apply_patch(patch).unwrap();

    Ok(frontend.state().to_json())
}

pub fn export_json(changes_file: &Path) -> Result<()> {
    let state_json = get_state_json(changes_file)?;
    println!("{}", serde_json::to_string_pretty(&state_json).unwrap());
    Ok(())
}
