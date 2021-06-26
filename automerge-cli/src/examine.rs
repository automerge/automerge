use automerge_backend as amb;
use automerge_protocol as amp;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExamineError {
    #[error("Error reading change file: {:?}", source)]
    ReadingChanges {
        #[source]
        source: std::io::Error,
    },
    #[error("Error loading changes: {:?}", source)]
    ApplyingInitialChanges {
        #[source]
        source: amb::AutomergeError,
    },
    #[error("Error writing to output: {:?}", source)]
    WritingToOutput {
        #[source]
        source: std::io::Error,
    },
}

pub fn examine(
    mut input: impl std::io::Read,
    mut output: impl std::io::Write,
    is_tty: bool,
) -> Result<(), ExamineError> {
    let mut buf: Vec<u8> = Vec::new();
    input
        .read_to_end(&mut buf)
        .map_err(|e| ExamineError::ReadingChanges { source: e })?;
    let changes = amb::Change::load_document(&buf)
        .map_err(|e| ExamineError::ApplyingInitialChanges { source: e })?;
    let uncompressed_changes: Vec<amp::Change> = changes.iter().map(|c| c.decode()).collect();
    if is_tty {
        let json_changes = serde_json::to_value(uncompressed_changes).unwrap();
        colored_json::write_colored_json(&json_changes, &mut output).unwrap();
    } else {
        let json_changes = serde_json::to_string_pretty(&uncompressed_changes).unwrap();
        output
            .write_all(&json_changes.into_bytes())
            .map_err(|e| ExamineError::WritingToOutput { source: e })?;
    }
    Ok(())
}
