use crate::{
    storage::{
        bundle::LoadingBundleChanges,
        change::{Change as StoredChange, Unverified},
        load::{load_state::LoadingDocChunk, Error as LoadError, ReconOpSet},
        Chunk,
    },
    Change, StepResult, TextEncoding, VerificationMode,
};

pub(crate) struct LoadingChunk<'a> {
    inner: LoadingChunkInner<'a>,
}

impl<'a> LoadingChunk<'a> {
    pub(crate) fn new(
        chunk: Chunk<'a>,
        encoding: TextEncoding,
        verification_mode: VerificationMode,
    ) -> Self {
        let inner = match chunk {
            Chunk::Document(document) => {
                LoadingChunkInner::Doc(LoadingDocChunk::new(document, encoding, verification_mode))
            }
            Chunk::Change(change) | Chunk::CompressedChange(change, _) => {
                LoadingChunkInner::Change(Box::new(change))
            }
            Chunk::Bundle(bundle_storage) => {
                LoadingChunkInner::Bundle(bundle_storage.begin_loading_changes())
            }
        };
        Self { inner }
    }
    pub(crate) fn step(
        mut self: Box<Self>,
    ) -> Result<StepResult<Box<Self>, LoadChunkResult>, LoadError> {
        match self.inner.step()? {
            StepResult::Loading(l) => {
                self.inner = l;
                Ok(StepResult::Loading(self))
            }
            StepResult::Ready(loaded_chunk) => Ok(StepResult::Ready(loaded_chunk)),
        }
    }
}

pub(crate) enum LoadChunkResult {
    Document(Box<ReconOpSet>),
    Change(Box<Change>),
    Bundle(Vec<Change>),
}

enum LoadingChunkInner<'a> {
    Doc(LoadingDocChunk<'a>),
    Bundle(LoadingBundleChanges<'a>),
    Change(Box<StoredChange<'a, Unverified>>),
}

impl<'a> LoadingChunkInner<'a> {
    pub(crate) fn step(mut self) -> Result<StepResult<Self, LoadChunkResult>, LoadError> {
        match self {
            LoadingChunkInner::Doc(loading_doc_chunk) => match loading_doc_chunk.step()? {
                StepResult::Loading(l) => {
                    self = LoadingChunkInner::Doc(l);
                    Ok(StepResult::Loading(self))
                }
                StepResult::Ready(d) => {
                    Ok(StepResult::Ready(LoadChunkResult::Document(Box::new(d))))
                }
            },
            LoadingChunkInner::Bundle(loading_bundle) => {
                match loading_bundle
                    .step()
                    .map_err(|e| LoadError::InvalidBundleChange(Box::new(e)))?
                {
                    StepResult::Loading(l) => {
                        self = LoadingChunkInner::Bundle(l);
                        Ok(StepResult::Loading(self))
                    }
                    StepResult::Ready(changes) => {
                        Ok(StepResult::Ready(LoadChunkResult::Bundle(changes)))
                    }
                }
            }
            LoadingChunkInner::Change(change) => {
                let change = Change::new_from_unverified(change.into_owned(), None)
                    .map_err(|e| LoadError::InvalidChangeColumns(Box::new(e)))?;
                Ok(StepResult::Ready(LoadChunkResult::Change(Box::new(change))))
            }
        }
    }
}
