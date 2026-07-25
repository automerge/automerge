use crate::op_set2::change::length_prefixed_bytes;
use crate::storage::{parse, ChunkType, Header};
use crate::types::{ActorId, ChangeHash};

use super::Bundle;

use std::num::NonZeroU64;

/// EXPERIMENTAL: A bundle plus the fragment metadata a fragments-mode
/// document needs to apply it.
///
/// A v1 [`Bundle`] identifies its external dependencies by hash only,
/// which a document in the fragment-hashes state cannot resolve to
/// nodes (only fragment heads have known hashes there). A `BundleV2`
/// chunk wraps a complete, unmodified v1 bundle and prefixes it with:
///
/// * the fragment's **head** hash, paired with its member index
/// * the **checkpoint** hashes (interior fragment-level hashes), each
///   paired with its member index
/// * the fragment's **boundary** hashes, each paired with its
///   `(actor, seq)` change id
/// * for every external dep of the embedded bundle (in the same
///   order), its `(actor, seq)` change id — together with the
///   bundle's dep hash list this gives the full `(actor, seq, hash)`
///   triple, and deps resolve structurally with no hash lookup
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
/// bundle      a complete v1 bundle chunk, header and all
/// ```
///
/// Member indexes refer to the embedded bundle's change list (which is
/// in topological order).
///
/// This is experimental, the format may still change — do not use it
/// in systems where you expect data to stick around.
#[derive(Debug)]
pub struct BundleV2 {
    pub(crate) head: ChangeHash,
    pub(crate) head_index: usize,
    /// `(member index, hash)`
    pub(crate) checkpoints: Vec<(usize, ChangeHash)>,
    /// `(hash, actor, seq)`
    pub(crate) boundary: Vec<(ChangeHash, ActorId, NonZeroU64)>,
    /// `(actor, seq)` of each of the embedded bundle's external deps,
    /// aligned with `bundle.deps()`
    pub(crate) dep_ids: Vec<(ActorId, NonZeroU64)>,
    /// Each member change's sequence number, validated non-zero at parse
    /// time and aligned with `bundle.iter_changes()`.
    pub(crate) member_seqs: Vec<NonZeroU64>,
    pub(crate) bundle: Bundle,
}

impl BundleV2 {
    pub(crate) fn new(
        head: ChangeHash,
        head_index: usize,
        checkpoints: Vec<(usize, ChangeHash)>,
        boundary: Vec<(ChangeHash, ActorId, NonZeroU64)>,
        dep_ids: Vec<(ActorId, NonZeroU64)>,
        member_seqs: Vec<NonZeroU64>,
        bundle: Bundle,
    ) -> Self {
        Self {
            head,
            head_index,
            checkpoints,
            boundary,
            dep_ids,
            member_seqs,
            bundle,
        }
    }

    pub fn head(&self) -> ChangeHash {
        self.head
    }

    pub fn bundle(&self) -> &Bundle {
        &self.bundle
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
            .map(|(h, a, s)| (*h, actor_idx(&mut actors, a), s.get()))
            .collect();
        let deps: Vec<(u64, u64)> = self
            .dep_ids
            .iter()
            .map(|(a, s)| (actor_idx(&mut actors, a), s.get()))
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
        data.extend_from_slice(self.bundle.bytes());

        let header = Header::new(ChunkType::BundleV2, &data);
        let mut out = Vec::with_capacity(header.len() + data.len());
        header.write(&mut out);
        out.extend(data);
        out
    }

    /// Parse the metadata prefix, returning the remaining input (the
    /// embedded v1 bundle chunk).
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

/// The decoded metadata prefix of a [`BundleV2`] chunk, with actors
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
#[error("invalid bundle v2: {0}")]
pub struct InvalidBundleV2(pub(crate) String);

impl TryFrom<&[u8]> for BundleV2 {
    type Error = InvalidBundleV2;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let bad = |s: &str| InvalidBundleV2(s.to_string());
        let input = parse::Input::new(bytes);
        let (i, header) = Header::parse::<crate::storage::chunk::error::Header>(input)
            .map_err(|e| InvalidBundleV2(format!("invalid header: {}", e)))?;
        if header.chunk_type() != ChunkType::BundleV2 {
            return Err(bad("not a bundle v2 chunk"));
        }

        let (i, prefix) =
            Self::parse_prefix(i).map_err(|e| InvalidBundleV2(format!("invalid prefix: {}", e)))?;

        let resolve = |a: u64| -> Result<ActorId, InvalidBundleV2> {
            prefix
                .actors
                .get(a as usize)
                .cloned()
                .ok_or_else(|| bad("bad actor index"))
        };
        // change sequence numbers are 1-based everywhere; rejecting zero
        // here is what lets `ChangeId` take a `NonZeroU64` and stay total
        let seq = |s: u64| NonZeroU64::new(s).ok_or_else(|| bad("change sequence number is zero"));
        let boundary = prefix
            .boundary
            .iter()
            .map(|(h, a, s)| Ok((*h, resolve(*a)?, seq(*s)?)))
            .collect::<Result<Vec<_>, _>>()?;
        let dep_ids = prefix
            .deps
            .iter()
            .map(|(a, s)| Ok((resolve(*a)?, seq(*s)?)))
            .collect::<Result<Vec<_>, _>>()?;

        let bundle = Bundle::try_from(i.unconsumed_bytes())
            .map_err(|e| InvalidBundleV2(format!("invalid embedded bundle: {}", e)))?;

        // all shape errors are caught here, at parse time — a BundleV2
        // that exists is well formed and appliers need no bounds checks
        if dep_ids.len() != bundle.deps().len() {
            return Err(bad("dep ids do not match the embedded bundle's deps"));
        }
        let num_actors = bundle.actors().len();
        let member_seqs = bundle
            .iter_changes()
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

        Ok(BundleV2 {
            head: prefix.head,
            head_index: prefix.head_index,
            checkpoints: prefix.checkpoints,
            boundary,
            dep_ids,
            member_seqs,
            bundle,
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
    /// `BundleV2::try_from`, so it is left blank — the point of these
    /// tests is what the parser does with hostile *content*.
    fn chunk(data: Vec<u8>) -> Vec<u8> {
        let mut out = MAGIC_BYTES.to_vec();
        out.extend([0u8; 4]);
        out.push(u8::from(ChunkType::BundleV2));
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
            let err = BundleV2::try_from(&chunk(data)[..]);
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
        let mut bytes = src.bundle_fragment_v2(&frags[1]).unwrap().bytes();
        assert!(BundleV2::try_from(&bytes[..]).is_ok(), "baseline parses");

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
        let err = BundleV2::try_from(&bytes[..]).expect_err("zero seq must be rejected");
        assert!(err.0.contains("sequence number"), "got {err}");
    }
}
