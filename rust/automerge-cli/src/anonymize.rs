use std::collections::BTreeSet;
use std::io::Read;

use anyhow::{Context, Result};

pub(crate) struct AnonymizedDocument {
    pub(crate) bytes: Vec<u8>,
    pub(crate) change_count: usize,
    pub(crate) operation_count: usize,
    pub(crate) actor_count: usize,
}

pub(crate) fn anonymize(mut input: impl Read) -> Result<AnonymizedDocument> {
    let mut bytes = Vec::new();
    input
        .read_to_end(&mut bytes)
        .context("failed to read Automerge document")?;
    let document =
        automerge::Automerge::load(&bytes).context("failed to load Automerge document")?;
    let changes = document
        .get_changes(&[])
        .context("failed to read the document's changes")?;
    let change_count = changes.len();
    let operation_count = changes.iter().map(automerge::Change::len).sum();
    let actor_count = changes
        .iter()
        .flat_map(automerge::Change::actors)
        .collect::<BTreeSet<_>>()
        .len();
    let bytes = document
        .anonymize()
        .context("failed to anonymize Automerge document")?
        .save();

    Ok(AnonymizedDocument {
        bytes,
        change_count,
        operation_count,
        actor_count,
    })
}
