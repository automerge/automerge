use crate::{
    storage::{self, load::Error as LoadError, parse, Bundle, ChunkType},
    types::ObjMeta,
    Automerge, AutomergeError, Change, LoadOptions, OnPartialLoad, StringMigration,
};

mod collector_cell;
mod load_doc_chunk;
use load_doc_chunk::LoadingDocChunk;

#[derive(Debug)]
pub enum StepResult<S, O> {
    Loading(S),
    Ready(O),
}

#[derive(Debug)]
pub struct LoadState<'a, 'b> {
    options: LoadOptions<'b>,
    first_chunk_type: Option<ChunkType>,
    changes: Vec<Change>,
    doc: Option<Automerge>,
    phase: Phase<'a>,
}

enum Phase<'a> {
    /// Looking at the byte stream for the next chunk header.
    NextChunk { data: parse::Input<'a> },
    /// Currently iterating ops within a document chunk. Boxed because the
    /// loader is significantly larger than the other variants.
    LoadingDoc(Box<LoadingDocPhase<'a>>),
    /// Done parsing chunks; ready to assemble the document.
    Finishing,
}

struct LoadingDocPhase<'a> {
    loader: LoadingDocChunk<'a>,
    remaining: parse::Input<'a>,
}

impl<'a> std::fmt::Debug for Phase<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NextChunk { .. } => write!(f, "NextChunk"),
            Self::LoadingDoc(_) => write!(f, "LoadingDoc"),
            Self::Finishing => write!(f, "Finishing"),
        }
    }
}

impl<'a, 'b> LoadState<'a, 'b> {
    pub(crate) fn new(options: LoadOptions<'b>, data: &'a [u8]) -> Self {
        Self {
            options,
            first_chunk_type: None,
            changes: Vec::new(),
            doc: None,
            phase: Phase::NextChunk {
                data: parse::Input::new(data),
            },
        }
    }

    pub fn step(mut self) -> Result<StepResult<Self, Automerge>, AutomergeError> {
        match std::mem::replace(&mut self.phase, Phase::Finishing) {
            Phase::Finishing => Ok(StepResult::Ready(self.finish()?)),
            Phase::LoadingDoc(state) => self.step_loading_doc(state.loader, state.remaining),
            Phase::NextChunk { data } => self.step_next_chunk(data),
        }
    }

    fn step_next_chunk(
        mut self,
        data: parse::Input<'a>,
    ) -> Result<StepResult<Self, Automerge>, AutomergeError> {
        if data.is_empty() {
            self.phase = Phase::Finishing;
            return Ok(StepResult::Loading(self));
        }

        let is_first_chunk = self.first_chunk_type.is_none();
        let (remaining, chunk) = match storage::Chunk::parse(data) {
            Ok(c) => c,
            Err(e) => {
                if is_first_chunk || self.options.on_partial_load == OnPartialLoad::Error {
                    return Err(LoadError::Parse(Box::new(e)).into());
                }
                self.phase = Phase::Finishing;
                return Ok(StepResult::Loading(self));
            }
        };

        if !chunk.checksum_valid() {
            return Err(LoadError::BadChecksum.into());
        }

        let chunk_type = chunk.chunk_type();
        if self.first_chunk_type.is_none() {
            self.first_chunk_type = Some(chunk_type);
        }
        let remaining = remaining.reset();

        let on_partial_load = self.options.on_partial_load;
        match self.handle_chunk(chunk, is_first_chunk, remaining) {
            Ok(()) => Ok(StepResult::Loading(self)),
            Err(e) => {
                if is_first_chunk || on_partial_load == OnPartialLoad::Error {
                    Err(e.into())
                } else {
                    self.phase = Phase::Finishing;
                    Ok(StepResult::Loading(self))
                }
            }
        }
    }

    /// Process a freshly-parsed chunk header. For document chunks this kicks
    /// off an interruptible sub-load; other chunk types are decoded eagerly
    /// (they are small and unlikely to be a bottleneck).
    fn handle_chunk(
        &mut self,
        chunk: storage::Chunk<'a>,
        is_first_chunk: bool,
        remaining: parse::Input<'a>,
    ) -> Result<(), LoadError> {
        match chunk {
            storage::Chunk::Document(d) => {
                tracing::trace!("loading document chunk");
                if is_first_chunk {
                    let loader = LoadingDocChunk::new(
                        d,
                        self.options.text_encoding,
                        self.options.verification_mode,
                    );
                    self.phase = Phase::LoadingDoc(Box::new(LoadingDocPhase { loader, remaining }));
                } else {
                    if !self.change_graph_has_heads(d.heads()) {
                        let new_changes = d
                            .reconstruct_changes(self.options.text_encoding)
                            .map_err(|e| LoadError::InflateDocument(Box::new(e)))?;
                        self.changes.extend(new_changes);
                    }
                    self.phase = Phase::NextChunk { data: remaining };
                }
            }
            storage::Chunk::Change(stored) => {
                tracing::trace!("loading change chunk");
                let change = Change::new_from_unverified(stored.into_owned(), None)
                    .map_err(|e| LoadError::InvalidChangeColumns(Box::new(e)))?;
                self.changes.push(change);
                self.phase = Phase::NextChunk { data: remaining };
            }
            storage::Chunk::CompressedChange(stored, compressed) => {
                tracing::trace!("loading compressed change chunk");
                let change =
                    Change::new_from_unverified(stored.into_owned(), Some(compressed.into_owned()))
                        .map_err(|e| LoadError::InvalidChangeColumns(Box::new(e)))?;
                self.changes.push(change);
                self.phase = Phase::NextChunk { data: remaining };
            }
            storage::Chunk::Bundle(bundle) => {
                tracing::trace!("loading bundle chunk");
                let bundle = Bundle::new_from_unverified(bundle.into_owned())
                    .map_err(|e| LoadError::InvalidBundleColumn(Box::new(e)))?;
                let bundle_changes = bundle
                    .into_changes()
                    .map_err(|e| LoadError::InvalidBundleChange(Box::new(e)))?;
                self.changes.extend(bundle_changes);
                self.phase = Phase::NextChunk { data: remaining };
            }
        }
        Ok(())
    }

    fn step_loading_doc(
        mut self,
        loader: LoadingDocChunk<'a>,
        remaining: parse::Input<'a>,
    ) -> Result<StepResult<Self, Automerge>, AutomergeError> {
        match loader.step() {
            Ok(StepResult::Loading(loader)) => {
                self.phase = Phase::LoadingDoc(Box::new(LoadingDocPhase { loader, remaining }));
                Ok(StepResult::Loading(self))
            }
            Ok(StepResult::Ready(doc)) => {
                self.doc = Some(doc);
                self.phase = Phase::NextChunk { data: remaining };
                Ok(StepResult::Loading(self))
            }
            Err(e) => {
                // We're inside the first chunk; partial loads aren't allowed
                // here because there's no usable document yet.
                Err(e.into())
            }
        }
    }

    fn change_graph_has_heads(&self, heads: &[crate::types::ChangeHash]) -> bool {
        match &self.doc {
            Some(doc) => heads.iter().all(|h| doc.change_graph.has_change(h)),
            None => false,
        }
    }

    fn finish(self) -> Result<Automerge, AutomergeError> {
        let LoadState {
            options,
            first_chunk_type,
            changes,
            doc,
            ..
        } = self;
        let mut doc = doc.unwrap_or_else(Automerge::new);
        doc.apply_changes(changes)?;

        if !doc.queue.is_empty()
            && first_chunk_type != Some(ChunkType::Document)
            && options.on_partial_load == OnPartialLoad::Error
        {
            return Err(AutomergeError::MissingDeps);
        }

        if let StringMigration::ConvertToText = options.string_migration {
            doc.convert_scalar_strings_to_text()?;
        }

        if let Some(patch_log) = options.patch_log {
            if patch_log.is_active() {
                doc.log_current_state(ObjMeta::root(), patch_log, true);
            }
        }

        Ok(doc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{transaction::Transactable, AutoCommit, ObjType, ROOT};

    fn build_big_doc() -> AutoCommit {
        let mut doc = AutoCommit::new();
        let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
        for i in 0..2000 {
            doc.insert(&list, i, i as i64).unwrap();
        }
        let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
        for i in 0..5 {
            doc.splice_text(&text, 0, 0, &format!("chunk-{i}-"))
                .unwrap();
        }
        doc
    }

    #[test]
    fn step_loads_document_in_multiple_steps() {
        let mut doc = build_big_doc();
        let bytes = doc.save();

        let mut state = LoadState::new(LoadOptions::new(), &bytes);
        let mut steps = 0;
        let loaded = loop {
            match state.step().unwrap() {
                StepResult::Loading(next) => {
                    state = next;
                    steps += 1;
                }
                StepResult::Ready(doc) => break doc,
            }
        };

        // The document was built to have many more ops than the test
        // `BATCH_SIZE`, so the doc-chunk loader has to suspend and resume the
        // op iterator several times before finishing.
        assert!(
            steps > 8,
            "expected mid-iteration suspends, only got {steps} steps"
        );

        let expected = Automerge::load(&bytes).unwrap();
        assert_eq!(loaded.get_heads(), expected.get_heads());
        assert_eq!(loaded.save(), expected.save());
    }

    #[test]
    fn empty_input_produces_empty_doc() {
        let mut state = LoadState::new(LoadOptions::new(), &[]);
        let doc = loop {
            match state.step().unwrap() {
                StepResult::Loading(next) => state = next,
                StepResult::Ready(doc) => break doc,
            }
        };
        assert!(doc.get_heads().is_empty());
    }
}
