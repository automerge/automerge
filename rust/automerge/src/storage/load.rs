use crate::{
    change::Change,
    storage::{parse, signatures::HashEntries},
};

pub(crate) mod change_collector;
pub(crate) use crate::storage::chunk::group::{
    ChunkBody, ChunkGroupReader, FailureKind, InitialLoad, SingleGroupError,
};

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
    #[error("attached signatures require loading in signing mode")]
    SignedContent,
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

/// Content of a decoded chunk group prior to authentication or staging.
///
/// A snapshot keeps its storage representation so that required-mode loading
/// can reconstruct and validate its history before adopting it.
#[derive(Clone, Debug)]
pub(crate) enum LoadedGroupBody {
    Document(Box<super::Document<'static>>),
    Changes(Vec<Change>),
}

/// A decoded change or bundle chunk together with its own optional trailing
/// signatures chunk.
///
/// Convert a [`Change`] into an unsigned group, or parse one encoded group with
/// [`TryFrom<&[u8]>`] or [`TryFrom<Vec<u8>>`]. Use [`crate::Automerge::apply_changes`]
/// to apply groups without decoding them again. Parsing checks framing and
/// checksums; cryptographic proofs are checked when applying in signing mode.
#[derive(Clone, Debug)]
pub struct LoadedGroup {
    pub(crate) body: LoadedGroupBody,
    pub(crate) signatures: Option<HashEntries>,
}

impl LoadedGroup {
    /// Borrow the decoded changes without discarding their attached proof.
    pub fn changes(&self) -> &[Change] {
        match &self.body {
            LoadedGroupBody::Changes(changes) => changes,
            // Snapshot groups are only constructed by the internal stream loader.
            LoadedGroupBody::Document(_) => &[],
        }
    }
}

impl From<Change> for LoadedGroup {
    fn from(change: Change) -> Self {
        Self {
            body: LoadedGroupBody::Changes(vec![change]),
            signatures: None,
        }
    }
}

impl From<crate::change::ParsedChange> for LoadedGroup {
    fn from(parsed: crate::change::ParsedChange) -> Self {
        Self {
            body: LoadedGroupBody::Changes(vec![parsed.change]),
            signatures: parsed.signatures,
        }
    }
}

impl TryFrom<&[u8]> for LoadedGroup {
    type Error = crate::AutomergeError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let group =
            ChunkGroupReader::read_single(parse::Input::new(bytes)).map_err(
                |error| match error {
                    SingleGroupError::LeftoverData => Error::LeftoverData,
                    SingleGroupError::Load(error) => error,
                    SingleGroupError::Empty => Error::Parse(Box::new(error)),
                },
            )?;
        let signatures = group
            .signatures
            .map(super::signatures::SignaturesChunk::into_entries);
        let changes = match *group.body {
            ChunkBody::Change(change) | ChunkBody::CompressedChange(change) => vec![change],
            ChunkBody::Bundle(bundle) => bundle
                .to_changes()
                .map_err(|error| Error::InvalidBundleChange(Box::new(error)))?,
            ChunkBody::Document(_) => return Err(crate::LoadChangeError::WrongChunkType.into()),
        };
        Ok(Self {
            body: LoadedGroupBody::Changes(changes),
            signatures,
        })
    }
}

impl TryFrom<Vec<u8>> for LoadedGroup {
    type Error = crate::AutomergeError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(bytes.as_slice())
    }
}

/// Result of reading chunk groups without pooling or discarding signatures.
pub(crate) enum LoadedGroups<'a> {
    Complete(Vec<LoadedGroup>),
    Partial {
        loaded: Vec<LoadedGroup>,
        #[allow(dead_code)]
        remaining: parse::Input<'a>,
        error: Error,
        kind: FailureKind,
    },
}

#[cfg(test)]
mod tests {
    use super::{
        ChunkGroupReader, Error, FailureKind, LoadedChanges, LoadedGroupBody, LoadedGroups,
        MarkOrderValidation,
    };
    use crate::actor::ActorTable;
    use crate::change_graph::ChangeGraph;
    use crate::signature::{Signature, SignatureBytes, SignatureTag};
    use crate::storage::parse::Input;
    use crate::storage::signatures::{HashEntries, SignaturesChunk};
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
    fn unsigned_load_rejects_a_signed_snapshot_instead_of_discarding_its_proof() {
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
        for policy in [OnPartialLoad::Error, OnPartialLoad::Ignore] {
            let error =
                Automerge::load_with_options(&stream, LoadOptions::new().on_partial_load(policy))
                    .unwrap_err();
            assert!(
                matches!(error, AutomergeError::Load(Error::SignedContent)),
                "unexpected error: {error:?}"
            );
        }
        let mut incremental = Automerge::new();
        incremental.apply_changes([change(3)]).unwrap();
        let before = incremental.get_heads();
        assert!(matches!(
            incremental.load_incremental(&stream),
            Err(AutomergeError::Load(Error::SignedContent))
        ));
        assert_eq!(incremental.get_heads(), before);
        // Plain unsigned bytes still load as before.
        let loaded = Automerge::load(&[snapshot.as_slice(), later.raw_bytes()].concat()).unwrap();
        assert_eq!(loaded.get_heads(), vec![head]);
    }

    #[test]
    fn signed_bundle_and_compressed_change_are_decoded_as_groups_but_not_loaded_unsigned() {
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
        // Pure codec decoding keeps the group and its proof.
        let LoadedGroups::Complete(groups) =
            ChunkGroupReader::new(Input::new(&stream)).load_groups()
        else {
            panic!("bundle and signatures must decode");
        };
        assert_eq!(groups.len(), 1);
        assert!(groups[0].signatures.is_some());
        let mut hashes: Vec<_> = groups[0].changes().iter().map(Change::hash).collect();
        hashes.sort();
        let mut expected = vec![first.hash(), second.hash()];
        expected.sort();
        assert_eq!(hashes, expected);
        // Unsigned loading refuses to strip the proof.
        let LoadedChanges::Partial { loaded, error, .. } = load_stream(&stream) else {
            panic!("signed group must not load unsigned");
        };
        assert!(loaded.is_empty());
        assert!(matches!(error, Error::SignedContent));
        assert!(matches!(
            Automerge::load(&stream),
            Err(AutomergeError::Load(Error::SignedContent))
        ));

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
        let LoadedGroups::Complete(groups) =
            ChunkGroupReader::new(Input::new(&stream)).load_groups()
        else {
            panic!("compressed change and signatures must decode");
        };
        assert_eq!(groups[0].changes()[0].raw_bytes(), change.raw_bytes());
        assert!(matches!(
            Automerge::load(&stream),
            Err(AutomergeError::Load(Error::SignedContent))
        ));
    }

    #[test]
    fn invalid_magic_tail_respects_partial_load_policy_after_snapshot() {
        let mut doc = Automerge::new();
        doc.apply_changes([change(1)]).unwrap();
        let mut stream = doc.save();
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

        // A signed snapshot with an invalid tail is rejected under both
        // policies by the unsigned loader: the proof is not stripped.
        let mut signed = doc.save();
        signed.extend(framed(ChunkType::Signatures, &[0]));
        signed.extend_from_slice(&[1, 2, 3, 4]);
        for policy in [OnPartialLoad::Error, OnPartialLoad::Ignore] {
            assert!(matches!(
                Automerge::load_with_options(&signed, LoadOptions::new().on_partial_load(policy)),
                Err(AutomergeError::Load(Error::SignedContent))
            ));
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
    fn load_changes_rejects_a_change_with_trailing_signatures_at_its_start() {
        let first = change(1);
        let signed = change(2);
        let signatures = framed(ChunkType::Signatures, &[0]);
        let signed_group = [signed.raw_bytes(), signatures.as_slice()].concat();
        let stream = [first.raw_bytes(), signed_group.as_slice()].concat();
        let LoadedChanges::Partial {
            loaded,
            remaining,
            error,
        } = load_stream(&stream)
        else {
            panic!("signed group must not load unsigned");
        };
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].raw_bytes(), first.raw_bytes());
        assert_eq!(remaining.unconsumed_bytes(), signed_group.as_slice());
        assert!(matches!(error, Error::SignedContent));
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
    fn trailing_signatures_after_change_are_rejected_by_unsigned_initial_load() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        tx.put(ROOT, "a", 1).unwrap();
        let hash = tx.commit().0.unwrap();
        let change = doc.get_change_by_hash(&hash).unwrap();
        let mut bytes = change.raw_bytes().to_vec();

        let body = [0]; // A valid empty signatures chunk paired with the change.
        Header::new(ChunkType::Signatures, &body).write(&mut bytes);
        bytes.extend_from_slice(&body);

        assert!(matches!(
            Automerge::load(&bytes),
            Err(AutomergeError::Load(Error::SignedContent))
        ));
        // The same change without its proof chunk loads normally.
        let loaded = Automerge::load(change.raw_bytes()).unwrap();
        assert_eq!(loaded.get_heads(), vec![hash]);
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

    #[test]
    fn load_groups_preserves_each_groups_own_attached_signatures() {
        let first = change(1);
        let second = change(2);
        let third = change(3);
        let sig_a = Signature::new(
            SignatureTag::new(7),
            SignatureBytes::new(vec![1, 2, 3]).unwrap(),
        );
        let sig_c = Signature::new(
            SignatureTag::new(9),
            SignatureBytes::new(vec![4, 5]).unwrap(),
        );
        let first_sigs = SignaturesChunk::encode(
            HashEntries::canonicalize([(first.hash(), sig_a.clone())]).unwrap(),
        );
        let third_sigs = SignaturesChunk::encode(
            HashEntries::canonicalize([(third.hash(), sig_c.clone())]).unwrap(),
        );
        let stream = [
            first.raw_bytes(),
            first_sigs.bytes(),
            second.raw_bytes(),
            third.raw_bytes(),
            third_sigs.bytes(),
        ]
        .concat();

        let LoadedGroups::Complete(groups) =
            ChunkGroupReader::new(Input::new(&stream)).load_groups()
        else {
            panic!("expected complete group stream");
        };
        assert_eq!(groups.len(), 3);
        let LoadedGroupBody::Changes(ref c0) = groups[0].body else {
            panic!("expected change group");
        };
        assert_eq!(c0.len(), 1);
        assert_eq!(c0[0].hash(), first.hash());
        assert_eq!(
            groups[0]
                .signatures
                .as_ref()
                .unwrap()
                .for_hash(&first.hash())
                .collect::<Vec<_>>(),
            vec![&sig_a],
        );
        assert!(groups[1].signatures.is_none());
        assert_eq!(
            groups[2]
                .signatures
                .as_ref()
                .unwrap()
                .for_hash(&third.hash())
                .collect::<Vec<_>>(),
            vec![&sig_c],
        );
    }

    #[test]
    fn load_groups_classifies_signature_incomplete_and_body_boundaries() {
        let first = change(1);
        let second = change(2);
        let first_sigs = framed(ChunkType::Signatures, &[0]);

        let malformed_sigs = [
            first.raw_bytes(),
            first_sigs.as_slice(),
            second.raw_bytes(),
            framed(ChunkType::Signatures, &[1]).as_slice(),
        ]
        .concat();
        let LoadedGroups::Partial {
            loaded,
            remaining,
            error,
            kind,
        } = ChunkGroupReader::new(Input::new(&malformed_sigs)).load_groups()
        else {
            panic!("expected partial load");
        };
        assert!(matches!(error, Error::Parse(_)));
        assert_eq!(loaded.len(), 1);
        assert!(loaded[0].signatures.is_some());
        assert_eq!(kind, FailureKind::Signatures);
        assert!(remaining.unconsumed_bytes().starts_with(second.raw_bytes()));

        let truncated_prefix = [
            first.raw_bytes(),
            first_sigs.as_slice(),
            &second.raw_bytes()[..4],
        ]
        .concat();
        let LoadedGroups::Partial {
            loaded,
            remaining,
            kind,
            ..
        } = ChunkGroupReader::new(Input::new(&truncated_prefix)).load_groups()
        else {
            panic!("expected incomplete prefix");
        };
        assert_eq!(loaded.len(), 1);
        assert_eq!(kind, FailureKind::IncompletePrefix);
        assert_eq!(remaining.unconsumed_bytes(), &second.raw_bytes()[..4]);

        let malformed_body = [
            first.raw_bytes(),
            first_sigs.as_slice(),
            framed(ChunkType::Change, &[]).as_slice(),
        ]
        .concat();
        let LoadedGroups::Partial { loaded, kind, .. } =
            ChunkGroupReader::new(Input::new(&malformed_body)).load_groups()
        else {
            panic!("expected malformed body");
        };
        assert_eq!(loaded.len(), 1);
        assert_eq!(kind, FailureKind::Body);
    }
}
