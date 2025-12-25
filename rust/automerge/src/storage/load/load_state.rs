use crate::{
    storage::{self, load::Error as LoadError, parse, ChunkType},
    Automerge, AutomergeError, Change, LoadOptions, OnPartialLoad, StringMigration,
};

mod load_chunk;
use load_chunk::{LoadChunkResult, LoadingChunk};
mod load_doc_chunk;
use load_doc_chunk::LoadingDocChunk;

#[derive(Debug)]
pub enum StepResult<S, O> {
    Loading(S),
    Ready(O),
}

#[derive(Debug)]
pub struct LoadState<'a, 'b> {
    data: parse::Input<'a>,
    options: LoadOptions<'b>,
    first_chunk_type: Option<ChunkType>,
    changes: Vec<Change>,
    doc: Automerge,
    phase: LoadPhase<'a>,
}

enum LoadPhase<'a> {
    Starting,
    NextChunk,
    LoadingChunk {
        state: Box<LoadingChunk<'a>>,
        is_first_chunk: bool,
    },
}

impl<'a> std::fmt::Debug for LoadPhase<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Starting => write!(f, "Starting"),
            Self::NextChunk => write!(f, "NextChunk"),
            Self::LoadingChunk {
                state: _,
                is_first_chunk,
            } => f
                .debug_struct("LoadingChunk")
                .field("is_first_chunk", is_first_chunk)
                .finish(),
        }
    }
}

impl<'a, 'b> LoadState<'a, 'b> {
    pub(crate) fn new(options: LoadOptions<'b>, data: &'a [u8]) -> Self {
        Self {
            data: parse::Input::new(data),
            options,
            first_chunk_type: None,
            changes: Vec::new(),
            doc: Automerge::new(),
            phase: LoadPhase::Starting,
        }
    }

    pub fn step(mut self) -> Result<StepResult<Self, Automerge>, AutomergeError> {
        match self.phase {
            LoadPhase::Starting => {
                if self.data.is_empty() {
                    tracing::trace!("no data, initializing empty document");
                    return Ok(StepResult::Ready(Automerge::new()));
                }
                self.phase = LoadPhase::NextChunk;
                Ok(StepResult::Loading(self))
            }
            LoadPhase::NextChunk => {
                let is_first_chunk = self.first_chunk_type.is_none();
                if self.data.is_empty() {
                    return Ok(StepResult::Ready(finish(
                        self.options,
                        self.doc,
                        self.changes,
                        self.first_chunk_type,
                    )?));
                }
                let (remaining, chunk) = match storage::Chunk::parse(self.data) {
                    Ok(chunk) => chunk,
                    Err(e) => {
                        if is_first_chunk || self.options.on_partial_load == OnPartialLoad::Error {
                            return Err(LoadError::Parse(Box::new(e)).into());
                        } else {
                            return Ok(StepResult::Ready(finish(
                                self.options,
                                self.doc,
                                self.changes,
                                self.first_chunk_type,
                            )?));
                        }
                    }
                };
                tracing::trace!(chunk_type=?chunk.chunk_type(), "loading chunk");
                if !chunk.checksum_valid() {
                    return Err(LoadError::BadChecksum.into());
                }
                if self.first_chunk_type.is_none() {
                    self.first_chunk_type = Some(chunk.chunk_type());
                }
                self.data = remaining.reset();
                self.phase = LoadPhase::LoadingChunk {
                    state: Box::new(LoadingChunk::new(
                        chunk,
                        self.options.text_encoding,
                        self.options.verification_mode,
                    )),
                    is_first_chunk,
                };
                Ok(StepResult::Loading(self))
            }
            LoadPhase::LoadingChunk {
                state: loading_chunk,
                is_first_chunk,
            } => match loading_chunk.step() {
                Ok(StepResult::Loading(loading_chunk)) => {
                    self.phase = LoadPhase::LoadingChunk {
                        state: loading_chunk,
                        is_first_chunk,
                    };
                    Ok(StepResult::Loading(self))
                }
                Ok(StepResult::Ready(loaded_chunk)) => match loaded_chunk {
                    LoadChunkResult::Document(recon_op_set) => {
                        if is_first_chunk {
                            self.doc = Automerge::from_recon_opset(*recon_op_set);
                        } else {
                            self.changes.extend(recon_op_set.changes);
                        }
                        self.phase = LoadPhase::NextChunk;
                        Ok(StepResult::Loading(self))
                    }
                    LoadChunkResult::Change(change) => {
                        self.changes.push(*change);
                        self.phase = LoadPhase::NextChunk;
                        Ok(StepResult::Loading(self))
                    }
                    LoadChunkResult::Bundle(changes) => {
                        self.changes.extend(changes);
                        self.phase = LoadPhase::NextChunk;
                        Ok(StepResult::Loading(self))
                    }
                },
                Err(e) => {
                    if is_first_chunk || self.options.on_partial_load == OnPartialLoad::Error {
                        return Err(e.into());
                    }
                    // If this error occurred after the first change chunk and
                    // we are ignoring partial loads then load what we have and
                    // return that
                    Ok(StepResult::Ready(finish(
                        self.options,
                        self.doc,
                        self.changes,
                        self.first_chunk_type,
                    )?))
                }
            },
        }
    }
}

fn finish<'a>(
    options: LoadOptions<'a>,
    mut doc: Automerge,
    changes: Vec<Change>,
    first_chunk_type: Option<ChunkType>,
) -> Result<Automerge, AutomergeError> {
    doc.apply_changes(changes)
        .map_err(|e| LoadError::InflateDocument(Box::new(e)))?;
    // Only allow missing deps if the first chunk was a document chunk
    // See https://github.com/automerge/automerge/pull/599#issuecomment-1549667472
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
            //TODO: make this interruptible
            doc.log_current_state(patch_log);
        }
    }

    Ok(doc)
}
