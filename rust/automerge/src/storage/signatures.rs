use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use crate::storage::parse::{Input, ParseResult};
use crate::storage::ChunkType;
use crate::{ChangeHash, Signature, SignatureBytes, SignatureBytesError, SignatureTag};

use super::{parse, Header};

pub(crate) use error::HashEntries as EntriesError;
pub(super) use error::Signatures as ParseError;

const MAX_SIGNATURES_PER_HASH: usize = 16;

/// The storage representation of a domain signature.
///
/// The associated change hash is stored separately by [`HashEntries`].
/// Entries are ordered lexicographically by `(tag, bytes)`. Entries with
/// identical tags and bytes are duplicates within the same hash.
///
/// All scheme tags and signature lengths from 0 through 1024 bytes are
/// accepted. Ed25519-sized values are classified by the domain constructor,
/// while unknown schemes and malformed Ed25519 lengths remain opaque.
/// Construction does not establish cryptographic validity.
///
/// # Encoding
///
/// One scheme-tag byte, without a length prefix or LEB128 encoding, followed
/// by an unsigned LEB128 signature length and exactly that many signature bytes.
/// The declared length is checked before reading or copying the payload.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct SignatureEntry(Signature);

impl SignatureEntry {
    fn parse<'a>(input: Input<'a>) -> ParseResult<'a, Self, error::Signatures> {
        let (i, tag) = parse::take1(input)?;
        let (i, size) = parse::leb128_u64(i)?;
        // Validate the wire length before narrowing it or allocating a copy.
        if size > SignatureBytes::MAX_SIZE as u64 {
            return Err(parse::ParseError::Error(
                SignatureBytesError::ExceededSize { actual: size }.into(),
            ));
        }
        let (i, bytes) = parse::take_n(size as usize, i)?;
        let bytes =
            SignatureBytes::new(bytes.to_vec()).map_err(|e| parse::ParseError::Error(e.into()))?;
        ParseResult::Ok((i, Self(Signature::new(SignatureTag::new(tag), bytes))))
    }

    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.push(self.0.tag().inner_tag());
        self.0.with_bytes(|bytes| {
            leb128::write::unsigned(buffer, bytes.len() as u64).unwrap();
            buffer.extend_from_slice(bytes);
        });
    }
}

impl PartialOrd for SignatureEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SignatureEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.0.tag().cmp(&other.0.tag()) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        self.0
            .with_bytes(|left| other.0.with_bytes(|right| left.cmp(right)))
    }
}

/// Ordered, unique signature entries grouped by change hash.
///
/// Each hash has at most 16 distinct signatures across all tags. The total
/// entry count counts signatures rather than hashes. Construct from domain
/// signatures with [`HashEntries::canonicalize`] before encoding a chunk.
///
/// In-memory construction sorts and deduplicates the supplied entries. Parsing
/// instead requires strictly increasing `(hash, tag, bytes)` wire order and
/// rejects duplicate or descending entries rather than normalizing them.
///
/// # Encoding
///
/// An unsigned LEB128 total entry count followed by the entries in order.
/// Each entry repeats its 32-byte change hash before its signature scheme
/// tag, signature length, and signature bytes.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct HashEntries {
    inner: BTreeMap<ChangeHash, BTreeSet<SignatureEntry>>,
    total: usize,
}

impl HashEntries {
    fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
            total: 0,
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.total
    }

    /// Take ownership of domain signatures, sorting and discarding exact duplicates.
    ///
    /// Signature bytes are already size-checked. This does not verify signatures
    /// or select proofs. Unknown schemes and malformed Ed25519 lengths are kept.
    /// Empty input produces an empty collection.
    ///
    /// # Errors
    ///
    /// Rejects more than 16 distinct signatures for any hash, counting all schemes
    /// together. Duplicates do not count toward the limit, even at capacity.
    pub(crate) fn canonicalize(
        entries: impl IntoIterator<Item = (ChangeHash, Signature)>,
    ) -> Result<Self, EntriesError> {
        entries
            .into_iter()
            .try_fold(Self::new(), |mut entries, (hash, signature)| {
                entries.insert_bounded(hash, SignatureEntry(signature))?;
                Ok(entries)
            })
    }

    /// Append an entry in strictly increasing `(hash, tag, bytes)` order.
    ///
    /// Leaves the collection unchanged on error.
    ///
    /// # Errors
    ///
    /// Rejects an entry that does not sort after the greatest stored entry.
    /// Also rejects an entry that would exceed 16 signatures for its hash,
    /// counting all tags together.
    fn push_ordered(
        &mut self,
        hash: ChangeHash,
        entry: SignatureEntry,
    ) -> Result<(), error::HashEntries> {
        use std::cmp::Ordering;

        if let Some((last_hash, last_entries)) = self.inner.last_key_value() {
            // Every stored hash has at least one entry.
            let last_entry = last_entries.last().expect("nonempty hash group");

            match (&hash, &entry).cmp(&(last_hash, last_entry)) {
                Ordering::Less => return Err(error::HashEntries::Unsorted),
                Ordering::Equal => return Err(error::HashEntries::Duplicate),
                Ordering::Greater => {}
            }
        }

        self.insert_bounded(hash, entry)
    }

    /// Insert a distinct entry within the per-hash limit.
    ///
    /// Exact duplicates are accepted without changing the collection,
    /// including when the hash already has 16 entries.
    ///
    /// # Errors
    ///
    /// Rejects a new entry if its hash already has 16 distinct entries.
    /// Leaves the collection unchanged on error.
    fn insert_bounded(
        &mut self,
        hash: ChangeHash,
        entry: SignatureEntry,
    ) -> Result<(), error::HashEntries> {
        if let Some(entries) = self.inner.get(&hash) {
            // Check this first: a duplicate is allowed even at capacity.
            if entries.contains(&entry) {
                return Ok(());
            }
            if entries.len() >= MAX_SIGNATURES_PER_HASH {
                return Err(error::HashEntries::TooManyForHash(hash));
            }
        }

        self.inner.entry(hash).or_default().insert(entry);
        self.total += 1;
        Ok(())
    }

    fn encode(&self, buffer: &mut Vec<u8>) {
        leb128::write::unsigned(buffer, self.total as u64).unwrap();
        for (hash, entry) in self
            .inner
            .iter()
            .flat_map(|(hash, entries)| entries.iter().map(move |e| (hash, e)))
        {
            buffer.extend_from_slice(hash.as_bytes());
            entry.encode(buffer);
        }
    }
}

/// A storage chunk containing signature entries keyed by change hash.
///
/// Stores the decoded entries and the complete encoded chunk.
/// Parsing checks structural limits only. Signature verification requires the
/// change hash's author binding and is performed separately.
///
/// # Encoding
///
/// A standard chunk header with type tag 4
/// precedes the body.
///
/// The body begins with an unsigned LEB128 entry count, followed by exactly
/// that many entries. Each entry contains these fields in order:
///
/// | Field                | Encoding                             |
/// | -------------------- | ------------------------------------ |
/// | Change hash          | 32 bytes                             |
/// | Signature scheme tag | One byte                             |
/// | Signature length     | Unsigned LEB128 byte count           |
/// | Signature            | Exactly the declared number of bytes |
///
/// Entries are sorted lexicographically by `(hash, tag, bytes)`, with no
/// duplicates. An empty entry list is encoded as a body containing one
/// zero byte.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SignaturesChunk<'a> {
    /// The decoded entries for inspection.
    entries: HashEntries,
    /// The completed encoded chunk, including its header.
    bytes: Cow<'a, [u8]>,
    /// The framing metadata and checksum information.
    header: Header,
}

impl SignaturesChunk<'static> {
    /// Encode a validated collection as a complete chunk in canonical order.
    ///
    /// Consumes the collection and retains it for inspection through
    /// [`SignaturesChunk::iter`]. The bytes include the header and checksum.
    /// Encoding does not verify or discard any of the supplied signatures.
    pub(crate) fn encode(entries: HashEntries) -> Self {
        let mut buffer = Vec::new();
        entries.encode(&mut buffer);
        let header = Header::new(ChunkType::Signatures, &buffer);
        let mut bytes = Vec::with_capacity(header.len() + buffer.len());
        header.write(&mut bytes);
        bytes.extend_from_slice(&buffer);
        Self {
            entries,
            bytes: Cow::Owned(bytes),
            header,
        }
    }
}

impl<'a> SignaturesChunk<'a> {
    /// Borrow domain signatures in canonical `(hash, tag, bytes)` order.
    ///
    /// Hashes are copied and signatures are borrowed. Works identically for
    /// parsed and newly encoded chunks, without verifying their signatures.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (ChangeHash, &Signature)> {
        self.entries
            .inner
            .iter()
            .flat_map(|(hash, entries)| entries.iter().map(move |entry| (*hash, &entry.0)))
    }

    /// Borrow the complete encoded chunk, including its header and checksum.
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(super) fn checksum_valid(&self) -> bool {
        self.header.checksum_valid()
    }

    /// Parse signature entries after their chunk header.
    ///
    /// The input must be positioned at the body and bounded to the complete
    /// body length declared by `header`. Its backing bytes must include the
    /// header so the parsed chunk can retain its original encoded bytes.
    ///
    /// Returns the parsed chunk and any unconsumed body bytes. The caller is
    /// responsible for rejecting leftover bytes and checking the checksum.
    ///
    /// # Errors
    ///
    /// Rejects malformed lengths, oversized signatures, duplicate or unsorted
    /// entries, and groups exceeding the per-hash signature limit. Missing
    /// fields within the declared body are reported as truncation rather than
    /// a request for more stream input.
    pub(super) fn parse_following_header(
        input: Input<'a>,
        header: Header,
    ) -> ParseResult<'a, Self, error::Signatures> {
        let bytes = Cow::Borrowed(input.bytes());
        let result = (|| {
            let (mut remaining, count) = parse::leb128_u64::<error::Signatures>(input)?;
            let mut entries = HashEntries::new();
            for _ in 0..count {
                let (next, (hash, entry)) = Self::parse_hash_entry(remaining)?;
                entries
                    .push_ordered(hash, entry)
                    .map_err(|e| parse::ParseError::Error(e.into()))?;
                remaining = next;
            }
            Ok((
                remaining,
                Self {
                    entries,
                    bytes,
                    header,
                },
            ))
        })();

        // Chunk::parse has already bounded the input to the complete declared
        // body. Running out inside it is malformed, not a need for more stream data.
        result.map_err(|error| match error {
            parse::ParseError::Incomplete(_) => {
                parse::ParseError::Error(error::Signatures::Truncated)
            }
            error => error,
        })
    }

    fn parse_hash_entry(
        input: Input<'a>,
    ) -> ParseResult<'a, (ChangeHash, SignatureEntry), error::Signatures> {
        let (i, hash) = parse::change_hash::<error::Signatures>(input)?;
        let (i, entry) = SignatureEntry::parse(i)?;
        ParseResult::Ok((i, (hash, entry)))
    }
}

pub(super) mod error {
    use crate::storage::parse::leb128;
    use crate::{ChangeHash, SignatureBytesError};

    /// A failure to parse or structurally validate a signatures chunk body.
    ///
    /// Truncation means a field is incomplete within the declared body.
    /// It does not indicate that more bytes are needed from the outer stream.
    #[derive(Debug, thiserror::Error)]
    pub(crate) enum Signatures {
        #[error(transparent)]
        Leb128(#[from] leb128::Error),
        #[error(transparent)]
        Signature(#[from] SignatureBytesError),
        #[error(transparent)]
        Entries(#[from] HashEntries),
        #[error("signatures chunk body is truncated")]
        Truncated,
    }

    /// A violation of signature entry ordering, uniqueness, or the per-hash limit.
    #[derive(Debug, thiserror::Error)]
    pub(crate) enum HashEntries {
        #[error("hash signature entries are expected to be in sorted order")]
        Unsorted,
        #[error("hash signature entries are expected to be unique")]
        Duplicate,
        #[error("found too many signatures for the hash '{0}'")]
        TooManyForHash(ChangeHash),
    }
}

#[cfg(test)]
mod tests {
    mod domain;
    mod properties;

    use super::{error, HashEntries, SignatureEntry, SignaturesChunk};
    use crate::storage::chunk::error::Chunk as ChunkError;
    use crate::storage::parse::{Input, ParseError};
    use crate::storage::{Chunk, ChunkType, Header};
    use crate::{ChangeHash, Signature, SignatureBytes, SignatureBytesError, SignatureTag};

    fn flattened_entries(chunk: &SignaturesChunk<'_>) -> Vec<(ChangeHash, SignatureEntry)> {
        chunk
            .iter()
            .map(|(hash, signature)| (hash, SignatureEntry(signature.clone())))
            .collect()
    }

    // Preserve input order and duplicates: malformed fixtures must bypass encode.
    fn raw_body(entries: &[(ChangeHash, SignatureEntry)]) -> Vec<u8> {
        let mut body = Vec::new();
        leb128::write::unsigned(&mut body, entries.len() as u64).unwrap();
        for (hash, entry) in entries {
            body.extend_from_slice(hash.as_bytes());
            body.push(entry.0.tag().inner_tag());
            let bytes = entry.0.to_bytes();
            leb128::write::unsigned(&mut body, bytes.as_bytes().len() as u64).unwrap();
            body.extend_from_slice(bytes.as_bytes());
        }
        body
    }

    fn frame_body(body: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        Header::new(ChunkType::Signatures, body).write(&mut bytes);
        bytes.extend_from_slice(body);
        bytes
    }

    fn entry(hash: u8, tag: u8, bytes: &[u8]) -> (ChangeHash, SignatureEntry) {
        (ChangeHash([hash; 32]), signature(tag, bytes.to_vec()))
    }

    fn signature(tag: u8, bytes: Vec<u8>) -> SignatureEntry {
        SignatureEntry(Signature::new(
            SignatureTag::new(tag),
            SignatureBytes::new(bytes).expect("size-bounded test signature"),
        ))
    }

    fn signature_error(bytes: &[u8]) -> error::Signatures {
        match Chunk::parse(Input::new(bytes)) {
            Err(ParseError::Error(ChunkError::Signatures(error))) => error,
            Err(error) => panic!("expected a signatures error, got {error:?}"),
            Ok(_) => panic!("expected a signatures error, but parsing succeeded"),
        }
    }

    #[test]
    fn parsing_nonempty_entries_preserves_bytes_and_the_next_chunk() {
        let entries = vec![
            entry(1, 0, &[]),
            entry(1, 0, &[7]),
            entry(1, 2, &[3]),
            entry(2, 0, &[9]),
        ];
        let bytes = frame_body(&raw_body(&entries));
        let next = frame_body(&[0]);
        let stream = [bytes.as_slice(), next.as_slice()].concat();
        let (rest, chunk) = Chunk::parse(Input::new(&stream)).unwrap();
        assert_eq!(rest.unconsumed_bytes(), next.as_slice());
        assert!(chunk.checksum_valid());
        let Chunk::Signatures(signatures) = chunk else {
            panic!("expected signatures chunk");
        };
        assert_eq!(flattened_entries(&signatures), entries);
        assert_eq!(signatures.bytes(), bytes.as_slice());
    }

    #[test]
    fn parsing_rejects_descending_hashes() {
        signature_error(&frame_body(&raw_body(&[
            entry(2, 0, &[]),
            entry(1, 0, &[]),
        ])));
    }

    #[test]
    fn parsing_rejects_descending_tags_for_one_hash() {
        signature_error(&frame_body(&raw_body(&[
            entry(1, 2, &[]),
            entry(1, 1, &[]),
        ])));
    }

    #[test]
    fn parsing_rejects_descending_bytes_for_one_hash_and_tag() {
        signature_error(&frame_body(&raw_body(&[
            entry(1, 1, &[8]),
            entry(1, 1, &[7]),
        ])));
    }

    #[test]
    fn parsing_rejects_duplicate_entries() {
        let entry = entry(1, 1, &[7]);
        let error = signature_error(&frame_body(&raw_body(&[entry.clone(), entry])));
        assert!(matches!(
            error,
            error::Signatures::Entries(error::HashEntries::Duplicate)
        ));
    }

    #[test]
    fn parsing_accepts_signature_and_per_hash_limits() {
        let entries: Vec<_> = [1, 2]
            .into_iter()
            .flat_map(|hash| (0u8..16).map(move |tag| entry(hash, tag, &vec![7; 1024])))
            .collect();
        let bytes = frame_body(&raw_body(&entries));
        let (rest, chunk) = Chunk::parse(Input::new(&bytes)).unwrap();
        assert!(rest.is_empty());
        let Chunk::Signatures(signatures) = chunk else {
            panic!("expected signatures chunk");
        };
        assert_eq!(flattened_entries(&signatures), entries);
    }

    #[test]
    fn parsing_rejects_oversized_length_before_reading_signature_bytes() {
        let mut body = vec![1];
        body.extend_from_slice(&[1; 32]);
        body.push(1);
        leb128::write::unsigned(&mut body, 1025).unwrap();
        // No payload: length validation must precede the attempt to read it.
        let error = signature_error(&frame_body(&body));
        assert!(matches!(
            error,
            error::Signatures::Signature(SignatureBytesError::ExceededSize { actual: 1025 })
        ));
    }

    #[test]
    fn parsing_rejects_large_u64_lengths_before_usize_conversion() {
        for size in [1u64 << 32, u64::MAX] {
            let mut body = vec![1];
            body.extend_from_slice(&[1; 32]);
            body.push(1);
            leb128::write::unsigned(&mut body, size).unwrap();
            let error = signature_error(&frame_body(&body));
            assert!(matches!(error,
                error::Signatures::Signature(SignatureBytesError::ExceededSize { actual })
                if actual == size
            ));
        }
    }

    #[test]
    fn hash_entries_rejected_insertions_preserve_contents_and_count() {
        let hash = ChangeHash([1; 32]);
        let mut entries = HashEntries::new();
        for byte in 0u8..16 {
            entries
                .push_ordered(hash, signature(1, vec![byte]))
                .unwrap();
        }
        let before = entries.clone();
        entries.insert_bounded(hash, signature(1, vec![0])).unwrap();
        assert_eq!(entries.len(), 16);
        assert_eq!(entries, before);

        assert!(matches!(
            entries.insert_bounded(hash, signature(1, vec![16])),
            Err(error::HashEntries::TooManyForHash(_))
        ));
        assert_eq!(entries, before);
        assert!(matches!(
            entries.push_ordered(hash, signature(1, vec![16])),
            Err(error::HashEntries::TooManyForHash(_))
        ));
        assert_eq!(entries, before);
        assert!(matches!(
            entries.push_ordered(hash, signature(1, vec![15])),
            Err(error::HashEntries::Duplicate)
        ));
        assert_eq!(entries, before);
        assert!(matches!(
            entries.push_ordered(hash, signature(1, vec![0])),
            Err(error::HashEntries::Unsorted)
        ));
        assert_eq!(entries, before);
    }

    #[test]
    fn parsing_rejects_malformed_leb128() {
        let error = signature_error(&frame_body(&[0x80, 0]));
        assert!(matches!(error, error::Signatures::Leb128(_)));
    }

    #[test]
    fn parsing_rejects_17_signatures_for_one_hash_and_tag() {
        let entries: Vec<_> = (0u8..17).map(|byte| entry(1, 1, &[byte])).collect();
        let error = signature_error(&frame_body(&raw_body(&entries)));
        assert!(
            matches!(error, error::Signatures::Entries(error::HashEntries::TooManyForHash(hash)) if hash == ChangeHash([1; 32]))
        );
    }

    #[test]
    fn parsing_rejects_17_signatures_for_one_hash_across_tags() {
        let entries: Vec<_> = (0u8..17).map(|tag| entry(1, tag, &[7])).collect();
        let error = signature_error(&frame_body(&raw_body(&entries)));
        assert!(
            matches!(error, error::Signatures::Entries(error::HashEntries::TooManyForHash(hash)) if hash == ChangeHash([1; 32]))
        );
    }

    #[test]
    fn parsing_rejects_truncated_fields_in_a_complete_chunk() {
        let body = raw_body(&[entry(1, 1, &[7, 8, 9])]);
        for end in 0..body.len() {
            // Reframe each prefix: the entire declared body is present, but
            // its contents end inside the count, hash, tag, length or payload.
            // This is malformed, not a request for more stream input.
            let bytes = frame_body(&body[..end]);
            assert!(
                matches!(
                    Chunk::parse(Input::new(&bytes)),
                    Err(ParseError::Error(ChunkError::Signatures(
                        error::Signatures::Truncated
                    )))
                ),
                "body prefix of {end} bytes must produce a signatures error",
            );
        }
    }

    #[test]
    fn parsing_incomplete_outer_chunk_requests_more_input() {
        let bytes = frame_body(&raw_body(&[entry(1, 1, &[7, 8, 9])]));
        // Keep the original header's length: the stream really is incomplete.
        assert!(matches!(
            Chunk::parse(Input::new(&bytes[..bytes.len() - 1])),
            Err(ParseError::Incomplete(_))
        ));
    }

    // Literal limits pin the format contract independently of implementation constants.
    #[test]
    fn encoding_accepts_a_1024_byte_signature() {
        let hash = ChangeHash([1; 32]);
        let signature = signature(1, vec![7; 1024]).0;
        let chunk = SignaturesChunk::encode(
            HashEntries::canonicalize([(hash, signature.clone())]).unwrap(),
        );
        assert_eq!(chunk.iter().collect::<Vec<_>>(), vec![(hash, &signature)]);
        assert!(chunk.checksum_valid());
    }

    #[test]
    fn encoding_accepts_16_entries_for_each_of_two_hashes() {
        let entries: Vec<_> = [ChangeHash([1; 32]), ChangeHash([2; 32])]
            .into_iter()
            .flat_map(|hash| (0u8..16).map(move |value| (hash, signature(1, vec![value]))))
            .collect();
        let chunk = SignaturesChunk::encode(
            HashEntries::canonicalize(entries.iter().map(|(hash, entry)| (*hash, entry.0.clone())))
                .unwrap(),
        );
        assert_eq!(flattened_entries(&chunk), entries);
        assert!(chunk.checksum_valid());
    }

    #[test]
    fn canonicalization_rejects_17_entries_for_one_hash_and_tag() {
        let hash = ChangeHash([1; 32]);
        let result =
            HashEntries::canonicalize((0u8..17).map(|value| (hash, signature(1, vec![value]).0)));
        assert!(
            matches!(result, Err(error::HashEntries::TooManyForHash(actual)) if actual == hash)
        );
    }

    #[test]
    fn canonicalization_rejects_17_entries_for_one_hash_across_tags() {
        let hash = ChangeHash([1; 32]);
        let result =
            HashEntries::canonicalize((0u8..17).map(|tag| (hash, signature(tag, vec![7]).0)));
        assert!(
            matches!(result, Err(error::HashEntries::TooManyForHash(actual)) if actual == hash)
        );
    }

    #[test]
    fn canonicalization_counts_the_per_hash_limit_after_deduplication() {
        let entries: Vec<_> = (0u8..16)
            .map(|value| (ChangeHash([1; 32]), signature(1, vec![value])))
            .collect();
        // 32 inputs, but only 16 distinct entries; reverse order also exercises sorting.
        let duplicated = entries
            .iter()
            .rev()
            .chain(entries.iter())
            .map(|(hash, entry)| (*hash, entry.0.clone()));
        let chunk = SignaturesChunk::encode(HashEntries::canonicalize(duplicated).unwrap());
        let canonical = SignaturesChunk::encode(
            HashEntries::canonicalize(entries.iter().map(|(hash, entry)| (*hash, entry.0.clone())))
                .unwrap(),
        );
        assert_eq!(flattened_entries(&chunk), entries);
        assert_eq!(chunk.bytes(), canonical.bytes());
    }

    #[test]
    fn encoding_keeps_flat_wire_entries_in_canonical_order() {
        let a = ChangeHash([1; 32]);
        let b = ChangeHash([2; 32]);
        let entry = |tag, byte| signature(tag, vec![byte]).0;
        let chunk = SignaturesChunk::encode(
            HashEntries::canonicalize([
                (b, entry(0, 9)),
                (a, entry(2, 3)),
                (a, entry(1, 8)),
                (a, entry(1, 7)),
                (a, entry(1, 7)),
            ])
            .unwrap(),
        );

        // Four distinct entries, not two hash groups or five inputs. Hashes
        // repeat on the wire; the nested collections are only an in-memory model.
        let mut body = vec![4];
        for (hash, tag, byte) in [(a, 1, 7), (a, 1, 8), (a, 2, 3), (b, 0, 9)] {
            body.extend_from_slice(hash.as_bytes());
            body.extend_from_slice(&[tag, 1, byte]);
        }
        let mut expected = Vec::new();
        Header::new(ChunkType::Signatures, &body).write(&mut expected);
        expected.extend_from_slice(&body);
        assert_eq!(chunk.bytes(), expected.as_slice());
    }

    #[test]
    fn empty_signatures_encode_a_zero_entry_count() {
        let chunk = SignaturesChunk::encode(HashEntries::canonicalize([]).unwrap());

        // The body is a single unsigned LEB128 zero (the entry count).
        let body = [0];
        let mut expected = Vec::new();
        Header::new(ChunkType::Signatures, &body).write(&mut expected);
        expected.extend_from_slice(&body);

        assert_eq!(chunk.iter().count(), 0);
        assert_eq!(chunk.bytes(), expected.as_slice());
        assert!(chunk.checksum_valid());
    }

    #[test]
    fn parsing_rejects_trailing_body_bytes() {
        let bytes = frame_body(&[0, 9]);
        assert!(matches!(
            Chunk::parse(Input::new(&bytes)),
            Err(ParseError::Error(ChunkError::LeftoverData))
        ));
    }
}
