use crate::error::AutomergeCliError;
use automerge_backend::AutomergeError;
use automerge_frontend::Value;
use automerge_protocol::Patch;
use std::fs::File;
use std::path::Path;
use std::{io, io::Read};

fn get_changes(changes_file: &Path) -> io::Result<Vec<u8>> {
    let mut input_data = Vec::new();
    let mut input_file = File::open(changes_file)?;
    input_file.read_to_end(&mut input_data)?;

    Ok(input_data)
}

fn get_patch(changes_bytes: Vec<u8>) -> Result<Patch, AutomergeError> {
    let mut backend = automerge_backend::Backend::init();
    backend.load_changes_binary(vec![changes_bytes]).unwrap();

    backend.get_patch()
}

fn get_state(changes_file: &Path) -> Result<Value, AutomergeCliError> {
    let input_data =
        get_changes(changes_file).map_err(|_| AutomergeCliError::InvalidChangesFile)?;

    let patch = get_patch(input_data).map_err(|_| AutomergeCliError::BackendError)?;
    let mut frontend = automerge_frontend::Frontend::new();
    frontend.apply_patch(patch).unwrap();

    Ok(frontend.state().clone())
}

pub fn export_json(changes_file: &Path) -> Result<(), AutomergeCliError> {
    let state = get_state(changes_file)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&state.to_json()).unwrap()
    );
    Ok(())
}
