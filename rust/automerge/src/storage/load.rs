use crate::{change::Change, storage::parse};

pub(crate) mod change_collector;
pub(crate) use crate::storage::chunk::group::{ChunkGroupReader, InitialLoad};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum VerificationMode {
    Check,
    DontCheck,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MarkOrderValidation {
    Validate,
    AllowInvalid,
}

impl MarkOrderValidation {
    pub(crate) fn allows_invalid(self) -> bool {
        matches!(self, Self::AllowInvalid)
    }
}

#[derive(Debug, thiserror::Error)]
#[allow(unreachable_pub)]
pub enum Error {
    #[error("unable to parse chunk: {0}")]
    Parse(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("invalid change columns: {0}")]
    InvalidChangeColumns(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("invalid ops columns: {0}")]
    InvalidOpsColumns(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("a chunk contained leftover data")]
    LeftoverData,
    #[error("a bundle contained an invalid column")]
    InvalidBundleColumn(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("a bundle contained an invalid change")]
    InvalidBundleChange(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("a chunk contained invalid signatures")]
    InvalidSignatures(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("error inflating document chunk ops: {0}")]
    InflateDocument(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("bad checksum")]
    BadChecksum,
    #[error("a signatures chunk has no preceding change, bundle, or document")]
    OrphanSignatures,
    #[error("a change, bundle, or document must only have one attached signatures chunk")]
    DuplicateSignatures,
}

pub(crate) enum LoadedChanges<'a> {
    /// All the data was succesfully loaded into a list of changes
    Complete(Vec<Change>),
    /// We only managed to load _some_ changes.
    Partial {
        /// The succesfully loaded changes
        loaded: Vec<Change>,
        /// The data which we were unable to parse
        #[allow(dead_code)]
        remaining: parse::Input<'a>,
        /// The error encountered whilst trying to parse `remaining`
        error: Error,
    },
}

#[cfg(test)]
mod tests {
    use super::{ChunkGroupReader, Error, LoadedChanges, MarkOrderValidation};
    use crate::actor::ActorTable;
    use crate::change_graph::ChangeGraph;
    use crate::storage::parse::Input;
    use crate::storage::{Chunk, ChunkType, Header};
    use crate::transaction::Transactable;
    use crate::{
        ActorId, Automerge, AutomergeError, Change, LoadOptions, OnPartialLoad, ReadDoc,
        TextEncoding, ROOT,
    };

    fn change(actor: u8) -> Change {
        let mut doc = Automerge::new().with_actor(ActorId::from(vec![actor]));
        let mut tx = doc.transaction();
        tx.put(ROOT, "a", i64::from(actor)).unwrap();
        let hash = tx.commit().0.unwrap();
        doc.get_change_by_hash(&hash).unwrap()
    }

    fn framed(chunk_type: ChunkType, body: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        Header::new(chunk_type, body).write(&mut bytes);
        bytes.extend_from_slice(body);
        bytes
    }

    fn load_stream(bytes: &[u8]) -> LoadedChanges<'_> {
        let actors = ActorTable::new();
        let current = ChangeGraph::new(&actors);
        ChunkGroupReader::new(Input::new(bytes)).load_changes(
            TextEncoding::platform_default(),
            &current,
            MarkOrderValidation::Validate,
        )
    }

    #[test]
    fn initial_document_with_signatures_preserves_snapshot_and_following_changes() {
        let mut doc = Automerge::new().with_actor(ActorId::from(vec![1]));
        let mut tx = doc.transaction();
        tx.put(ROOT, "snapshot", 1).unwrap();
        tx.commit();
        let snapshot = doc.save();
        assert!(matches!(
            Chunk::parse(Input::new(&snapshot)).unwrap().1,
            Chunk::Document(_)
        ));

        let mut tx = doc.transaction();
        tx.put(ROOT, "later", 2).unwrap();
        let head = tx.commit().0.unwrap();
        let later = doc.get_change_by_hash(&head).unwrap();
        let signatures = framed(ChunkType::Signatures, &[0]);
        let stream = [
            snapshot.as_slice(),
            signatures.as_slice(),
            later.raw_bytes(),
        ]
        .concat();
        let loaded = Automerge::load(&stream).expect("snapshot and paired signatures load");
        assert_eq!(loaded.get_heads(), vec![head]);
        assert_eq!(loaded.get_changes(&[]).len(), 2);
        assert_eq!(
            loaded.get(ROOT, "snapshot").unwrap().unwrap().0.as_i64(),
            Some(1)
        );
        assert_eq!(
            loaded.get(ROOT, "later").unwrap().unwrap().0.as_i64(),
            Some(2)
        );
    }

    #[test]
    fn bundle_with_signatures_loads_through_initial_and_incremental_paths() {
        let first = change(1);
        let second = change(2);
        let mut doc = Automerge::new();
        doc.apply_changes([first.clone(), second.clone()]).unwrap();
        let bundle = doc.bundle([first.hash(), second.hash()]).unwrap();
        let bytes = bundle.bytes();
        assert!(matches!(
            Chunk::parse(Input::new(bytes)).unwrap().1,
            Chunk::Bundle(_)
        ));
        let stream = [bytes, framed(ChunkType::Signatures, &[0]).as_slice()].concat();
        let LoadedChanges::Complete(changes) = load_stream(&stream) else {
            panic!("bundle and signatures must load");
        };
        let mut hashes: Vec<_> = changes.iter().map(Change::hash).collect();
        hashes.sort();
        let mut expected = vec![first.hash(), second.hash()];
        expected.sort();
        assert_eq!(hashes, expected);
        assert_eq!(Automerge::load(&stream).unwrap().get_changes(&[]).len(), 2);

        let existing = change(3);
        let mut target = Automerge::new();
        target.apply_changes([existing.clone()]).unwrap();
        target.load_incremental(&stream).unwrap();
        for change in [first, second, existing] {
            assert_eq!(
                target
                    .get_change_by_hash(&change.hash())
                    .unwrap()
                    .raw_bytes(),
                change.raw_bytes()
            );
        }
    }

    #[test]
    fn compressed_change_with_signatures_preserves_change_bytes() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        tx.put(ROOT, "text", "compressible".repeat(1000)).unwrap();
        let hash = tx.commit().0.unwrap();
        let mut change = doc.get_change_by_hash(&hash).unwrap();
        let compressed = change.bytes().into_owned();
        assert!(matches!(
            Chunk::parse(Input::new(&compressed)).unwrap().1,
            Chunk::CompressedChange(_, _)
        ));
        let stream = [
            compressed.as_slice(),
            framed(ChunkType::Signatures, &[0]).as_slice(),
        ]
        .concat();
        let LoadedChanges::Complete(mut changes) = load_stream(&stream) else {
            panic!("compressed change and signatures must load");
        };
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].raw_bytes(), change.raw_bytes());
        assert_eq!(changes[0].bytes().as_ref(), compressed.as_slice());
        let loaded = Automerge::load(&stream).unwrap();
        assert_eq!(loaded.get_heads(), vec![hash]);
        assert_eq!(
            loaded.get_change_by_hash(&hash).unwrap().raw_bytes(),
            change.raw_bytes()
        );
    }

    #[test]
    fn invalid_magic_tail_respects_partial_load_policy_after_snapshot() {
        let mut doc = Automerge::new();
        doc.apply_changes([change(1)]).unwrap();
        for signed in [false, true] {
            let mut stream = doc.save();
            if signed {
                stream.extend(framed(ChunkType::Signatures, &[0]));
            }
            stream.extend_from_slice(&[1, 2, 3, 4]);
            let error = Automerge::load_with_options(
                &stream,
                LoadOptions::new().on_partial_load(OnPartialLoad::Error),
            )
            .unwrap_err();
            assert!(matches!(error, AutomergeError::Load(Error::Parse(_))));
            let recovered = Automerge::load_with_options(
                &stream,
                LoadOptions::new().on_partial_load(OnPartialLoad::Ignore),
            )
            .expect("ignore policy recovers complete snapshot");
            assert_eq!(recovered.get_heads(), doc.get_heads());
            assert_eq!(
                recovered.get(ROOT, "a").unwrap().unwrap().0.as_i64(),
                Some(1)
            );
        }
    }

    #[test]
    fn malformed_snapshot_signatures_are_not_ignored_by_partial_load_policy() {
        let doc = Automerge::new();
        let stream = [
            doc.save().as_slice(),
            framed(ChunkType::Signatures, &[1]).as_slice(),
        ]
        .concat();
        for policy in [OnPartialLoad::Error, OnPartialLoad::Ignore] {
            let error =
                Automerge::load_with_options(&stream, LoadOptions::new().on_partial_load(policy))
                    .unwrap_err();
            assert!(matches!(error, AutomergeError::Load(Error::Parse(_))));
        }
    }

    #[test]
    fn load_changes_accepts_a_change_with_trailing_signatures() {
        let change = change(1);
        let signatures = framed(ChunkType::Signatures, &[0]);
        let stream = [change.raw_bytes(), signatures.as_slice()].concat();
        let LoadedChanges::Complete(changes) = load_stream(&stream) else {
            panic!("expected complete paired group");
        };
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].raw_bytes(), change.raw_bytes());
    }

    #[test]
    fn load_changes_preserves_progress_before_a_malformed_body() {
        let change = change(1);
        let malformed = framed(ChunkType::Change, &[]);
        let stream = [change.raw_bytes(), malformed.as_slice()].concat();
        let LoadedChanges::Partial {
            loaded,
            remaining,
            error,
        } = load_stream(&stream)
        else {
            panic!("expected partial load");
        };
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].raw_bytes(), change.raw_bytes());
        assert_eq!(remaining.unconsumed_bytes(), malformed.as_slice());
        assert!(
            matches!(error, Error::Parse(_)),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn load_changes_withholds_the_body_of_malformed_signatures() {
        let first = change(1);
        let second = change(2);
        let signatures = framed(ChunkType::Signatures, &[1]); // Entry count, but no entry.
        let failed_group = [second.raw_bytes(), signatures.as_slice()].concat();
        let stream = [first.raw_bytes(), failed_group.as_slice()].concat();
        let LoadedChanges::Partial {
            loaded,
            remaining,
            error,
        } = load_stream(&stream)
        else {
            panic!("expected partial load");
        };
        assert_eq!(
            loaded.len(),
            1,
            "the second body must not escape its failed group"
        );
        assert_eq!(loaded[0].raw_bytes(), first.raw_bytes());
        assert_eq!(remaining.unconsumed_bytes(), failed_group.as_slice());
        assert!(
            matches!(error, Error::Parse(_)),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn load_changes_conversion_failure_reports_the_failed_group_start() {
        // This historical document has valid framing and columns, but invalid
        // mark ordering is detected when reconstructing its changes.
        let document = include_bytes!("../../tests/fixtures/broken_zero_width_mark.automerge");
        let (rest, chunk) = Chunk::parse(Input::new(document)).unwrap();
        assert!(rest.is_empty());
        assert!(matches!(chunk, Chunk::Document(_)));
        assert!(chunk.checksum_valid());

        let first = change(1);
        let following = change(2);
        let failed_and_following = [document.as_slice(), following.raw_bytes()].concat();
        let stream = [first.raw_bytes(), failed_and_following.as_slice()].concat();
        let LoadedChanges::Partial {
            loaded,
            remaining,
            error,
        } = load_stream(&stream)
        else {
            panic!("expected document reconstruction failure");
        };
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].raw_bytes(), first.raw_bytes());
        // Lookahead may already have read the following body. Neither that
        // cursor nor the end of the failed document is the retry boundary.
        assert_eq!(
            remaining.unconsumed_bytes(),
            failed_and_following.as_slice()
        );
        assert!(
            matches!(error, Error::InflateDocument(_)),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn trailing_signatures_after_change_load_successfully() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        tx.put(ROOT, "a", 1).unwrap();
        let hash = tx.commit().0.unwrap();
        let change = doc.get_change_by_hash(&hash).unwrap();
        let mut bytes = change.raw_bytes().to_vec();

        let body = [0]; // A valid empty signatures chunk paired with the change.
        Header::new(ChunkType::Signatures, &body).write(&mut bytes);
        bytes.extend_from_slice(&body);

        let loaded = Automerge::load(&bytes).expect("paired signatures must load");
        assert_eq!(loaded.get_heads(), vec![hash]);
        assert_eq!(loaded.get_changes(&[]).len(), 1);
        assert_eq!(loaded.get(ROOT, "a").unwrap().unwrap().0.as_i64(), Some(1));
        assert_eq!(
            loaded.get_change_by_hash(&hash).unwrap().raw_bytes(),
            change.raw_bytes()
        );
    }

    #[test]
    fn signatures_chunk_with_no_preceding_group_is_an_error() {
        // A well-formed empty signatures chunk isolates the placement rule
        // from payload validation and checksum errors.
        let body = [0];
        let mut bytes = Vec::new();
        Header::new(ChunkType::Signatures, &body).write(&mut bytes);
        bytes.extend_from_slice(&body);

        let error = Automerge::load(&bytes).expect_err("signatures require a preceding group");
        assert!(
            matches!(error, AutomergeError::Load(Error::OrphanSignatures)),
            "expected orphan signatures error, got {error:?}",
        );
    }
}
