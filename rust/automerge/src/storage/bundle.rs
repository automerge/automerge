use crate::change_graph::ChangeGraph;
use crate::op_set2::change::{length_prefixed_bytes, ActorMapper, BuildChangeMetadata};
use crate::op_set2::OpSet;
use crate::storage::change::{Unverified, Verified};
use crate::storage::{parse, ChunkType, Header};
use crate::types::{ActorId, ChangeHash};
use crate::{AutomergeError, Change, ChangeId};

use std::borrow::Cow;
use std::num::NonZeroU64;

mod builder;
mod error;
mod meta;
mod storage;

pub use builder::BundleChangeIter;

pub(crate) use builder::{
    frag_prepass, BundleBuilder, BundleChangeIterUnverified, BundleOp, BundleOpWriter, FragMeta,
    FragOp, FragOps, OpIter, OpIterUnverified,
};
pub(crate) use error::ParseError;
pub(crate) use meta::{BundleMetadata, DepRef};
pub(crate) use storage::BundleStorage;

/// EXPERIMENTAL: A set of changes in compressed columnar form, plus the
/// fragment metadata a fragments-mode document needs to apply them.
///
/// The carried changes name their external dependencies by hash alone,
/// which a document in the fragment-hashes state cannot resolve to nodes
/// (only fragment heads have known hashes there). So a bundle chunk
/// prefixes the changes with:
///
/// * the fragment's **head** hash, paired with its member index
/// * the **checkpoint** hashes (interior fragment-level hashes), each
///   paired with its member index
/// * the fragment's **boundary** hashes, each paired with its
///   [`ChangeId`]
/// * for every external dep of the carried changes (in the same
///   order), its [`ChangeId`] — together with the dep hash list this
///   gives the full `(change id, hash)` pairing, so deps resolve
///   structurally with no hash lookup
///
/// The wire layout after the chunk header is:
///
/// ```text
/// actors      uleb count, then length-prefixed actor ids (only the
///             actors the prefix itself references)
/// head        32-byte hash + uleb member index
/// checkpoints uleb count, then per entry: uleb member index + 32-byte hash
/// boundary    uleb count, then per entry: 32-byte hash + uleb actor + uleb seq
/// deps        uleb count, then per entry: uleb actor + uleb seq
/// changes     a complete legacy bundle chunk, header and all — the
///             carried changes in columnar form
/// ```
///
/// Member indexes refer to the carried changes' list, which is in
/// topological order.
///
/// The inner chunk is still written and read in the legacy
/// ([`ChunkType::BundleV0`]) encoding so that a reader which only knows
/// that format can find the changes; nothing but the prefix distinguishes
/// the two.
///
/// This is experimental, the format may still change — do not use it
/// in systems where you expect data to stick around.
#[derive(Debug)]
pub struct Bundle {
    pub(crate) head: ChangeHash,
    pub(crate) head_index: usize,
    /// `(member index, hash)`
    pub(crate) checkpoints: Vec<(usize, ChangeHash)>,
    /// each boundary hash paired with the change it names
    pub(crate) boundary: Vec<(ChangeHash, ChangeId)>,
    /// the change id of each external dep, aligned with [`Self::deps`]
    pub(crate) dep_ids: Vec<ChangeId>,
    /// Each member change's sequence number, validated non-zero at parse
    /// time and aligned with [`Self::iter_changes`].
    pub(crate) member_seqs: Vec<NonZeroU64>,
    /// The carried changes, in the columnar bundle encoding.
    pub(crate) storage: BundleStorage<'static, Verified>,
}

impl Bundle {
    pub(crate) fn new(
        head: ChangeHash,
        head_index: usize,
        checkpoints: Vec<(usize, ChangeHash)>,
        boundary: Vec<(ChangeHash, ChangeId)>,
        dep_ids: Vec<ChangeId>,
        member_seqs: Vec<NonZeroU64>,
        storage: BundleStorage<'static, Verified>,
    ) -> Self {
        Self {
            head,
            head_index,
            checkpoints,
            boundary,
            dep_ids,
            member_seqs,
            storage,
        }
    }

    /// The hash of the change this bundle delivers — the one member
    /// nothing else in the bundle depends on.
    pub fn head(&self) -> ChangeHash {
        self.head
    }

    /// The changes carried by the bundle, in topological order.
    pub fn to_changes(&self) -> Result<Vec<Change>, AutomergeError> {
        self.storage
            .to_changes()
            .map_err(|e| AutomergeError::Unbundle(Box::new(e)))
    }

    /// Metadata for each carried change, without decoding its ops.
    pub fn iter_changes(&self) -> BundleChangeIter<'_> {
        self.storage.iter_change_meta()
    }

    /// The hashes this bundle depends on but does not carry.
    pub fn deps(&self) -> &[ChangeHash] {
        self.storage.deps()
    }

    pub(crate) fn actors(&self) -> &[ActorId] {
        &self.storage.actors
    }

    /// Build the carried-change storage from a set of change hashes.
    /// No hint column: hints need a clock, which only the node-keyed
    /// path has.
    pub(crate) fn storage_for_hashes<I>(
        op_set: &OpSet,
        change_graph: &ChangeGraph,
        hashes: I,
    ) -> Result<BundleStorage<'static, Verified>, AutomergeError>
    where
        I: IntoIterator<Item = ChangeHash>,
    {
        let changes = change_graph
            .get_bundle_metadata(hashes)
            .collect::<Result<_, _>>()?;
        Ok(Self::storage_from_meta(op_set, changes, None))
    }

    /// Build the carried-change storage from member nodes. Only the
    /// boundary (external dep) hashes need to be known, so this works in
    /// the fragment-hashes state. `nodes` must be sorted ascending.
    pub(crate) fn storage_for_nodes(
        op_set: &OpSet,
        change_graph: &ChangeGraph,
        nodes: Vec<crate::change_graph::NodeIdx>,
    ) -> Result<BundleStorage<'static, Verified>, AutomergeError> {
        let clock = change_graph.clock_for_nodes(nodes.clone());
        let changes = change_graph
            .bundle_metadata_for_nodes(nodes)
            .collect::<Result<_, _>>()?;
        Ok(Self::storage_from_meta(op_set, changes, Some(&clock)))
    }

    fn storage_from_meta(
        op_set: &OpSet,
        changes: Vec<BundleMetadata<'_>>,
        clock: Option<&crate::clock::Clock>,
    ) -> BundleStorage<'static, Verified> {
        let min = changes
            .iter()
            .map(|c| c.start_op as usize)
            .min()
            .unwrap_or(0);
        let max = changes.iter().map(|c| c.max_op as usize).max().unwrap_or(0) + 1;

        let mapper = ActorMapper::new(&op_set.actors);

        let mut collector = BundleBuilder::from_change_meta(changes, mapper);

        for op in op_set.iter_ctr_range(min..max) {
            let op_id = op.id;
            let op_succ: Vec<_> = op.succ().collect();
            collector.process_op(op, &op_succ);

            for id in op_succ {
                collector.process_succ(op_id, id);
            }
        }

        // the hint ranks: for every covered seq target the member ops
        // reference, its rank among the dep-covered rows (in document
        // order) — a receiver-independent position floor. One id-column
        // walk with early exit; members do not count (a receiver about
        // to apply the fragment does not have them yet)
        let mut ranks = std::collections::HashMap::new();
        if let Some(clock) = clock {
            let needed = collector.hint_targets();
            if !needed.is_empty() {
                let mut remaining = needed.len();
                let mut rank = 0u64;
                for id in op_set.id_iter() {
                    if collector.is_member(id) {
                        continue;
                    }
                    if needed.contains(&id) {
                        ranks.insert(id, rank);
                        remaining -= 1;
                        if remaining == 0 {
                            break;
                        }
                    }
                    if clock.covers(&id) {
                        rank += 1;
                    }
                }
                debug_assert_eq!(remaining, 0, "hint target missing from doc");
            }
        }

        collector.finish_with_ranks(&ranks)
    }

    /// The inner chunk: the carried changes on their own, with per-column
    /// DEFLATE applied where each column is large enough to benefit.
    /// Falls back to the uncompressed buffer for storage built before the
    /// per-column compression pass (or parsed from input with no
    /// compressed columns).
    fn inner_bytes(&self) -> &[u8] {
        match &self.storage.compressed_bytes {
            Some(c) => c,
            None => &self.storage.bytes,
        }
    }

    /// The chunk's bytes: the metadata prefix plus the embedded
    /// bundle's on-disk form.
    pub fn bytes(&self) -> Vec<u8> {
        // dedup the actors the prefix references
        fn actor_idx<'x>(actors: &mut Vec<&'x ActorId>, a: &'x ActorId) -> u64 {
            match actors.iter().position(|x| *x == a) {
                Some(i) => i as u64,
                None => {
                    actors.push(a);
                    (actors.len() - 1) as u64
                }
            }
        }
        let mut actors: Vec<&ActorId> = Vec::new();
        let boundary: Vec<(ChangeHash, u64, u64)> = self
            .boundary
            .iter()
            .map(|(h, id)| (*h, actor_idx(&mut actors, id.actor()), id.seq()))
            .collect();
        let deps: Vec<(u64, u64)> = self
            .dep_ids
            .iter()
            .map(|id| (actor_idx(&mut actors, id.actor()), id.seq()))
            .collect();

        let mut data = Vec::new();
        leb128::write::unsigned(&mut data, actors.len() as u64).unwrap();
        for a in &actors {
            length_prefixed_bytes(a.to_bytes(), &mut data);
        }
        data.extend_from_slice(self.head.as_bytes());
        leb128::write::unsigned(&mut data, self.head_index as u64).unwrap();
        leb128::write::unsigned(&mut data, self.checkpoints.len() as u64).unwrap();
        for (i, h) in &self.checkpoints {
            leb128::write::unsigned(&mut data, *i as u64).unwrap();
            data.extend_from_slice(h.as_bytes());
        }
        leb128::write::unsigned(&mut data, boundary.len() as u64).unwrap();
        for (h, a, s) in &boundary {
            data.extend_from_slice(h.as_bytes());
            leb128::write::unsigned(&mut data, *a).unwrap();
            leb128::write::unsigned(&mut data, *s).unwrap();
        }
        leb128::write::unsigned(&mut data, deps.len() as u64).unwrap();
        for (a, s) in &deps {
            leb128::write::unsigned(&mut data, *a).unwrap();
            leb128::write::unsigned(&mut data, *s).unwrap();
        }
        data.extend_from_slice(self.inner_bytes());

        let header = Header::new(ChunkType::Bundle, &data);
        let mut out = Vec::with_capacity(header.len() + data.len());
        header.write(&mut out);
        out.extend(data);
        out
    }

    /// Parse the metadata prefix, returning the remaining input (the
    /// carried changes' chunk).
    ///
    /// Every count is a wire-supplied length prefix, so none of them may
    /// be trusted to size an allocation: a 5-byte varint can claim more
    /// entries than the machine has memory. Each is capped by what the
    /// remaining input could possibly hold ([`entry_capacity`]) before it
    /// reserves anything — a claim beyond that just falls out as the
    /// incomplete-input error the entry parse would have produced anyway.
    pub(crate) fn parse_prefix(
        i: parse::Input<'_>,
    ) -> parse::ParseResult<'_, ParsedPrefix, parse::leb128::Error> {
        let (i, actors) = parse::length_prefixed(parse::actor_id)(i)?;
        let (i, head) = parse::change_hash(i)?;
        let (i, head_index) = parse::leb128_u64(i)?;

        // a checkpoint is a uleb index plus a 32-byte hash
        let (mut i, n_checkpoints) = parse::leb128_u64(i)?;
        let mut checkpoints = Vec::with_capacity(entry_capacity(&i, n_checkpoints, 33));
        for _ in 0..n_checkpoints {
            let (j, idx) = parse::leb128_u64(i)?;
            let (j, h) = parse::change_hash(j)?;
            checkpoints.push((idx as usize, h));
            i = j;
        }

        // a boundary entry is a 32-byte hash plus two ulebs
        let (i, n_boundary) = parse::leb128_u64(i)?;
        let mut i = i;
        let mut boundary = Vec::with_capacity(entry_capacity(&i, n_boundary, 34));
        for _ in 0..n_boundary {
            let (j, h) = parse::change_hash(i)?;
            let (j, a) = parse::leb128_u64(j)?;
            let (j, s) = parse::leb128_u64(j)?;
            boundary.push((h, a, s));
            i = j;
        }

        // a dep id is two ulebs
        let (i, n_deps) = parse::leb128_u64(i)?;
        let mut i = i;
        let mut deps = Vec::with_capacity(entry_capacity(&i, n_deps, 2));
        for _ in 0..n_deps {
            let (j, a) = parse::leb128_u64(i)?;
            let (j, s) = parse::leb128_u64(j)?;
            deps.push((a, s));
            i = j;
        }

        Ok((
            i,
            ParsedPrefix {
                actors,
                head,
                head_index: head_index as usize,
                checkpoints,
                boundary,
                deps,
            },
        ))
    }
}

/// How many entries it is safe to reserve for a wire-supplied `count`:
/// the claim, clamped to what the unconsumed input could hold at
/// `min_entry_bytes` each. Reserving beyond that could never be filled,
/// so clamping loses nothing and keeps a hostile count from turning into
/// an allocation the size it asked for.
fn entry_capacity(i: &parse::Input<'_>, count: u64, min_entry_bytes: usize) -> usize {
    let ceiling = i.unconsumed_bytes().len() / min_entry_bytes;
    usize::try_from(count).unwrap_or(usize::MAX).min(ceiling)
}

/// The decoded metadata prefix of a [`Bundle`] chunk, with actors
/// still in index form.
#[derive(Debug)]
pub(crate) struct ParsedPrefix {
    actors: Vec<ActorId>,
    head: ChangeHash,
    head_index: usize,
    checkpoints: Vec<(usize, ChangeHash)>,
    boundary: Vec<(ChangeHash, u64, u64)>,
    deps: Vec<(u64, u64)>,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid bundle: {0}")]
pub struct InvalidBundle(pub(crate) String);

/// Parse the inner (carried-changes) chunk of a bundle.
fn parse_inner_chunk(bytes: &[u8]) -> Result<BundleStorage<'static, Verified>, String> {
    let input = parse::Input::new(bytes);
    let (i, header) = Header::parse::<crate::storage::chunk::error::Header>(input)
        .map_err(|e| format!("invalid header: {}", e))?;
    let (_i, stored) = BundleStorage::parse_following_header(i, header)
        .map_err(|e| format!("invalid contents: {}", e))?;
    let verified = stored
        .verify()
        .map_err(|e| format!("unable to verify ops: {}", e))?;
    Ok(verified.into_owned())
}

/// Build the verified storage from a chunk the loader already split out.
pub(crate) fn verify_inner(
    stored: BundleStorage<'static, Unverified>,
) -> Result<BundleStorage<'static, Verified>, ParseError> {
    stored.verify()
}

/// Metadata for one change carried by a bundle.
#[derive(Clone, Debug)]
pub struct BundleChange<'a> {
    pub actor: usize,
    pub author: Option<usize>,
    pub seq: u64,
    pub start_op: u64,
    pub max_op: u64,
    pub timestamp: i64,
    pub message: Option<Cow<'a, str>>,
    pub deps: Vec<u64>,
    pub extra: Cow<'a, [u8]>,
}

impl<'a> From<BundleChange<'a>> for BuildChangeMetadata<'a> {
    fn from(bundle: BundleChange<'a>) -> Self {
        BuildChangeMetadata {
            actor: bundle.actor,
            seq: bundle.seq,
            start_op: bundle.start_op,
            max_op: bundle.max_op,
            timestamp: bundle.timestamp,
            message: bundle.message,
            deps: bundle.deps,
            extra: bundle.extra,
            builder: 0,
        }
    }
}

impl TryFrom<&[u8]> for Bundle {
    type Error = InvalidBundle;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let bad = |s: &str| InvalidBundle(s.to_string());
        let input = parse::Input::new(bytes);
        let (i, header) = Header::parse::<crate::storage::chunk::error::Header>(input)
            .map_err(|e| InvalidBundle(format!("invalid header: {}", e)))?;
        if header.chunk_type() != ChunkType::Bundle {
            return Err(bad("not a bundle chunk"));
        }

        let (i, prefix) =
            Self::parse_prefix(i).map_err(|e| InvalidBundle(format!("invalid prefix: {}", e)))?;

        let resolve = |a: u64| -> Result<ActorId, InvalidBundle> {
            prefix
                .actors
                .get(a as usize)
                .cloned()
                .ok_or_else(|| bad("bad actor index"))
        };
        // change sequence numbers are 1-based everywhere; rejecting zero
        // here is what lets `ChangeId` take a `NonZeroU64` and stay total
        let seq = |s: u64| NonZeroU64::new(s).ok_or_else(|| bad("change sequence number is zero"));
        let change_id = |a: u64, s: u64| Ok(ChangeId::new(seq(s)?, resolve(a)?, 0));
        let boundary = prefix
            .boundary
            .iter()
            .map(|(h, a, s)| Ok((*h, change_id(*a, *s)?)))
            .collect::<Result<Vec<_>, InvalidBundle>>()?;
        let dep_ids = prefix
            .deps
            .iter()
            .map(|(a, s)| change_id(*a, *s))
            .collect::<Result<Vec<_>, InvalidBundle>>()?;

        let storage = parse_inner_chunk(i.unconsumed_bytes())
            .map_err(|e| InvalidBundle(format!("invalid carried changes: {}", e)))?;

        // all shape errors are caught here, at parse time — a Bundle
        // that exists is well formed and appliers need no bounds checks
        if dep_ids.len() != storage.deps().len() {
            return Err(bad("dep ids do not match the carried changes' deps"));
        }
        let num_actors = storage.actors.len();
        let member_seqs = storage
            .iter_change_meta()
            .map(|c| {
                if c.actor >= num_actors {
                    return Err(bad("bad member actor index"));
                }
                // `num_ops` is `1 + max_op - start_op`, so an inverted
                // range would underflow in the applier
                if c.max_op < c.start_op {
                    return Err(bad("member max_op precedes its start_op"));
                }
                seq(c.seq)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let num_members = member_seqs.len();
        if prefix.head_index >= num_members {
            return Err(bad("head index out of range"));
        }
        if prefix.checkpoints.iter().any(|(i, _)| *i >= num_members) {
            return Err(bad("checkpoint index out of range"));
        }

        Ok(Bundle {
            head: prefix.head,
            head_index: prefix.head_index,
            checkpoints: prefix.checkpoints,
            boundary,
            dep_ids,
            member_seqs,
            storage,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MAGIC_BYTES;

    fn leb(mut n: u64, out: &mut Vec<u8>) {
        loop {
            let mut b = (n & 0x7f) as u8;
            n >>= 7;
            if n != 0 {
                b |= 0x80;
            }
            out.push(b);
            if n == 0 {
                break;
            }
        }
    }

    /// A chunk header around `data`. The checksum is never verified by
    /// `Bundle::try_from`, so it is left blank — the point of these
    /// tests is what the parser does with hostile *content*.
    fn chunk(data: Vec<u8>) -> Vec<u8> {
        let mut out = MAGIC_BYTES.to_vec();
        out.extend([0u8; 4]);
        out.push(u8::from(ChunkType::Bundle));
        leb(data.len() as u64, &mut out);
        out.extend(data);
        out
    }

    /// A prefix whose counts are wire-supplied lies. Each of the three
    /// counted sections claims more entries than exist; none of them may
    /// turn into an allocation of the size it asked for.
    #[test]
    fn absurd_counts_error_instead_of_allocating() {
        for section in 0..3 {
            let mut data = Vec::new();
            leb(0, &mut data); // no actors
            data.extend([0u8; 32]); // head hash
            leb(0, &mut data); // head index
            for s in 0..3 {
                // the section under test claims u64::MAX/64 entries; the
                // ones before it are empty so parsing reaches it
                leb(if s == section { u64::MAX / 64 } else { 0 }, &mut data);
                if s == section {
                    break;
                }
            }
            let err = Bundle::try_from(&chunk(data)[..]);
            assert!(err.is_err(), "section {section} should not parse");
        }
    }

    /// The counts are only trusted as far as the remaining bytes could
    /// possibly satisfy them.
    #[test]
    fn entry_capacity_is_clamped_to_the_input() {
        let bytes = [0u8; 100];
        let i = parse::Input::new(&bytes);
        assert_eq!(entry_capacity(&i, 3, 33), 3, "an honest count is kept");
        assert_eq!(entry_capacity(&i, u64::MAX, 33), 3, "a lie is clamped");
        assert_eq!(entry_capacity(&i, u64::MAX, 2), 50);
    }

    /// Change sequence numbers are 1-based, so a zero on the wire is a
    /// parse error — not a panic deep in `ChangeId`.
    #[test]
    fn zero_sequence_numbers_are_rejected() {
        use crate::transaction::Transactable;
        use crate::{Automerge, ROOT};

        let mut src = Automerge::new();
        src.enable_audit_mode().unwrap();
        for i in 0..6 {
            let mut tx = src.transaction();
            tx.put(ROOT, "k", i).unwrap();
            tx.commit();
        }
        let frags = src.fragments(0..=0).unwrap();
        // the second fragment has a boundary entry (its dep on the first)
        let mut bytes = src.bundle_fragment(&frags[1]).unwrap().bytes();
        assert!(Bundle::try_from(&bytes[..]).is_ok(), "baseline parses");

        // walk the prefix to the first boundary entry's seq and zero it
        let mut at = MAGIC_BYTES.len() + 4 + 1;
        let read = |b: &[u8], at: &mut usize| -> u64 {
            let mut r = 0u64;
            let mut shift = 0;
            loop {
                let x = b[*at];
                *at += 1;
                r |= u64::from(x & 0x7f) << shift;
                if x & 0x80 == 0 {
                    return r;
                }
                shift += 7;
            }
        };
        read(&bytes, &mut at); // chunk length
        let n_actors = read(&bytes, &mut at);
        for _ in 0..n_actors {
            let n = read(&bytes, &mut at) as usize;
            at += n;
        }
        at += 32; // head hash
        read(&bytes, &mut at); // head index
        let n_checkpoints = read(&bytes, &mut at);
        for _ in 0..n_checkpoints {
            read(&bytes, &mut at);
            at += 32;
        }
        let n_boundary = read(&bytes, &mut at);
        assert!(n_boundary > 0, "fixture needs a boundary entry");
        at += 32; // boundary hash
        read(&bytes, &mut at); // boundary actor
        let seq_at = at;
        let seq = read(&bytes, &mut at);
        assert_eq!(at, seq_at + 1, "fixture seq is a single byte");
        assert_ne!(seq, 0);

        bytes[seq_at] = 0;
        let err = Bundle::try_from(&bytes[..]).expect_err("zero seq must be rejected");
        assert!(err.0.contains("sequence number"), "got {err}");
    }
}
