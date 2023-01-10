use automerge::sync::ReadMessageError;

use crate::color_json::print_colored_json;

#[derive(Debug, thiserror::Error)]
pub enum ExamineSyncError {
    #[error("Error reading message: {0}")]
    ReadMessage(#[source] std::io::Error),

    #[error("error writing message: {0}")]
    WriteMessage(#[source] std::io::Error),

    #[error("error writing json to output: {0}")]
    WriteJson(#[source] serde_json::Error),

    #[error("Error parsing message: {0}")]
    ParseMessage(#[from] ReadMessageError),
}

pub(crate) fn examine_sync<W: std::io::Write>(
    mut input: Box<dyn std::io::Read>,
    output: W,
    is_tty: bool,
) -> Result<(), ExamineSyncError> {
    let mut buf: Vec<u8> = Vec::new();
    input
        .read_to_end(&mut buf)
        .map_err(ExamineSyncError::ReadMessage)?;

    let message = automerge::sync::Message::decode(&buf)?;
    let json = serde_json::to_value(message).unwrap();
    if is_tty {
        print_colored_json(&json).map_err(ExamineSyncError::WriteMessage)?;
    } else {
        serde_json::to_writer(output, &json).map_err(ExamineSyncError::WriteJson)?;
    }
    Ok(())
}
