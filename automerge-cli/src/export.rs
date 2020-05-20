use anyhow::Result;
use std::fs::read;
use std::path::Path;

fn get_state_json(input_data: Vec<u8>) -> Result<serde_json::Value> {
    let mut backend = automerge_backend::Backend::init();
    let patch = backend.apply_changes_binary(vec![input_data])?;

    let mut frontend = automerge_frontend::Frontend::new();
    frontend.apply_patch(patch).unwrap();

    Ok(frontend.state().to_json())
}

pub fn export_json(changes_file: &Path, mut writer: impl std::io::Write) -> Result<()> {
    let input_data = read(changes_file)?;

    let state_json = get_state_json(input_data)?;
    writeln!(writer, "{}", serde_json::to_string_pretty(&state_json).unwrap())?;
    Ok(())
}
