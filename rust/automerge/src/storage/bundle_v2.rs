use crate::op_set2::change::length_prefixed_bytes;
use crate::storage::{parse, ChunkType, Header};
use crate::types::{ActorId, ChangeHash};

use super::Bundle;

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
    pub(crate) boundary: Vec<(ChangeHash, ActorId, u64)>,
    /// `(actor, seq)` of each of the embedded bundle's external deps,
    /// aligned with `bundle.deps()`
    pub(crate) dep_ids: Vec<(ActorId, u64)>,
    pub(crate) bundle: Bundle,
}

impl BundleV2 {
    pub(crate) fn new(
        head: ChangeHash,
        head_index: usize,
        checkpoints: Vec<(usize, ChangeHash)>,
        boundary: Vec<(ChangeHash, ActorId, u64)>,
        dep_ids: Vec<(ActorId, u64)>,
        bundle: Bundle,
    ) -> Self {
        Self {
            head,
            head_index,
            checkpoints,
            boundary,
            dep_ids,
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
            .map(|(h, a, s)| (*h, actor_idx(&mut actors, a), *s))
            .collect();
        let deps: Vec<(u64, u64)> = self
            .dep_ids
            .iter()
            .map(|(a, s)| (actor_idx(&mut actors, a), *s))
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
    pub(crate) fn parse_prefix(
        i: parse::Input<'_>,
    ) -> parse::ParseResult<'_, ParsedPrefix, parse::leb128::Error> {
        let (i, actors) = parse::length_prefixed(parse::actor_id)(i)?;
        let (i, head) = parse::change_hash(i)?;
        let (i, head_index) = parse::leb128_u64(i)?;

        let (mut i, n_checkpoints) = parse::leb128_u64(i)?;
        let mut checkpoints = Vec::with_capacity(n_checkpoints as usize);
        for _ in 0..n_checkpoints {
            let (j, idx) = parse::leb128_u64(i)?;
            let (j, h) = parse::change_hash(j)?;
            checkpoints.push((idx as usize, h));
            i = j;
        }

        let (i, n_boundary) = parse::leb128_u64(i)?;
        let mut i = i;
        let mut boundary = Vec::with_capacity(n_boundary as usize);
        for _ in 0..n_boundary {
            let (j, h) = parse::change_hash(i)?;
            let (j, a) = parse::leb128_u64(j)?;
            let (j, s) = parse::leb128_u64(j)?;
            boundary.push((h, a, s));
            i = j;
        }

        let (i, n_deps) = parse::leb128_u64(i)?;
        let mut i = i;
        let mut deps = Vec::with_capacity(n_deps as usize);
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
        let boundary = prefix
            .boundary
            .iter()
            .map(|(h, a, s)| Ok((*h, resolve(*a)?, *s)))
            .collect::<Result<Vec<_>, _>>()?;
        let dep_ids = prefix
            .deps
            .iter()
            .map(|(a, s)| Ok((resolve(*a)?, *s)))
            .collect::<Result<Vec<_>, _>>()?;

        let bundle = Bundle::try_from(i.unconsumed_bytes())
            .map_err(|e| InvalidBundleV2(format!("invalid embedded bundle: {}", e)))?;

        // all shape errors are caught here, at parse time — a BundleV2
        // that exists is well formed and appliers need no bounds checks
        if dep_ids.len() != bundle.deps().len() {
            return Err(bad("dep ids do not match the embedded bundle's deps"));
        }
        let num_actors = bundle.actors().len();
        let num_members = bundle
            .iter_changes()
            .map(|c| {
                if c.actor >= num_actors {
                    Err(bad("bad member actor index"))
                } else {
                    Ok(())
                }
            })
            .collect::<Result<Vec<_>, _>>()?
            .len();
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
            bundle,
        })
    }
}
