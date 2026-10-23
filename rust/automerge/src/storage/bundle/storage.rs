use std::borrow::Cow;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::ops::Range;
use std::sync::OnceLock;

use crate::actor::ActorTable;
use crate::op_set2::change::ChangeCollector;
use crate::op_set2::types::ActorIdx;
use crate::op_set2::OpSet;
use crate::storage::change::{OpReadState, Unverified, Verified, DEFLATE_MIN_SIZE};
use crate::storage::columns::compression;

use crate::storage::{parse, RawColumns};
use crate::types::{ActorId, ChangeHash};
use crate::Change;

use super::builder::ExternalPreds;
use super::{BundleChange, BundleChangeCols, BundleChangeIterUnverified, ParseError};

#[derive(Clone, Debug)]
pub(crate) struct BundleStorage<'a, OpReadState> {
    /// Uncompressed in-memory form. Iterators index into this.
    pub(crate) bytes: Cow<'a, [u8]>,
    pub(crate) compressed: Compressed<'a>,
    pub(crate) deps: Vec<ChangeHash>,
    pub(crate) actors: Vec<ActorId>,
    pub(crate) ops_meta: RawColumns<compression::Uncompressed>,
    pub(crate) ops_data: Range<usize>,
    pub(crate) changes_meta: RawColumns<compression::Uncompressed>,
    pub(crate) changes_data: Range<usize>,
    /// Decoded on demand; allocates per member, so the apply path reads
    /// [`Self::change_cols`] instead.
    pub(crate) changes: OnceLock<Vec<BundleChange<'static>>>,
    /// The op columns in bundle actor space, loaded at parse time.
    /// Empty once taken, or if built locally; reloaded from `bytes` then.
    pub(crate) bundle_ops: OnceLock<OpSet>,
    pub(crate) _phantom: PhantomData<OpReadState>,
}

/// The column bytes a bundle is written with.
#[derive(Clone, Debug)]
pub(crate) enum Compressed<'a> {
    /// As received, with some columns DEFLATE-compressed.
    Stored(Cow<'a, [u8]>),
    /// Received uncompressed: written as received.
    AsIs,
    /// Built here: large columns are compressed when written.
    Lazy,
}

impl<O: OpReadState> BundleStorage<'_, O> {
    /// The columns as written: large ones DEFLATE-compressed, unless they
    /// arrived uncompressed.
    pub(crate) fn wire_columns(&self) -> Cow<'_, [u8]> {
        match &self.compressed {
            Compressed::Stored(c) => Cow::Borrowed(c),
            Compressed::AsIs => Cow::Borrowed(&self.bytes),
            Compressed::Lazy => Cow::Owned(self.compress_columns()),
        }
    }

    fn compress_columns(&self) -> Vec<u8> {
        let meta_len = self.changes_meta.write(&mut Vec::new());
        let prefix_end = self.changes_data.start - meta_len;
        let mut out = self.bytes[..prefix_end].to_vec();
        let mut cols = Vec::new();
        for (meta, range) in [
            (&self.changes_meta, &self.changes_data),
            (&self.ops_meta, &self.ops_data),
        ] {
            cols.clear();
            meta.compress(&self.bytes[range.clone()], &mut cols, DEFLATE_MIN_SIZE)
                .write(&mut out);
            out.extend_from_slice(&cols);
        }
        out
    }

    pub(crate) fn into_owned(self) -> BundleStorage<'static, O> {
        BundleStorage {
            bytes: Cow::Owned(self.bytes.into_owned()),
            compressed: match self.compressed {
                Compressed::Stored(c) => Compressed::Stored(Cow::Owned(c.into_owned())),
                Compressed::AsIs => Compressed::AsIs,
                Compressed::Lazy => Compressed::Lazy,
            },
            deps: self.deps,
            actors: self.actors,
            ops_meta: self.ops_meta,
            ops_data: self.ops_data,
            changes_meta: self.changes_meta,
            changes_data: self.changes_data,
            changes: self.changes,
            bundle_ops: self.bundle_ops,
            _phantom: self._phantom,
        }
    }

    pub(crate) fn change_cols(&self) -> Result<BundleChangeCols<'_>, ParseError> {
        BundleChangeCols::try_new(&self.changes_meta, &self.bytes[self.changes_data.clone()])
    }

    /// Each member's actor and sequence number, checking that actors are
    /// in range and sequence numbers non-zero. No other column is validated.
    pub(crate) fn member_ids(&self) -> Result<(Vec<ActorIdx>, Vec<NonZeroU64>), ParseError> {
        let bad = ParseError::InvalidChangeMetadata;
        let cols = self.change_cols()?;
        let num_actors = self.actors.len();
        let actors: Vec<ActorIdx> = cols.actors().map(|a| a.unwrap_or_default()).collect();
        let len = actors.len();
        if actors.iter().any(|a| usize::from(*a) >= num_actors) {
            return Err(bad("bad member actor index"));
        }
        let mut seqs = Vec::with_capacity(len);
        for s in cols.seqs().take(len) {
            let s = s
                .filter(|s| *s > 0)
                .and_then(|s| NonZeroU64::new(s as u64))
                .ok_or(bad("change sequence number is zero"))?;
            seqs.push(s);
        }
        if seqs.len() != len {
            return Err(bad("short member seq column"));
        }
        Ok((actors, seqs))
    }

    /// Each member's metadata, decoded afresh rather than cached.
    pub(crate) fn iter_change_meta(&self) -> Result<BundleChangeIterUnverified<'_>, ParseError> {
        BundleChangeIterUnverified::try_new(
            &self.changes_meta,
            &self.bytes[self.changes_data.clone()],
        )
    }

    fn collector<'c>(&'c self, actors: &'c ActorTable) -> Result<ChangeCollector<'c>, ParseError> {
        let meta = self
            .iter_change_meta()?
            .map(|c| c.map(Into::into))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ChangeCollector::from_change_meta(meta, actors))
    }

    pub(crate) fn changes(&self) -> Result<&[BundleChange<'static>], ParseError> {
        if self.changes.get().is_none() {
            let decoded =
                decode_change_meta(&self.changes_meta, &self.bytes[self.changes_data.clone()])?;
            // a racing caller may have won; either vector is equivalent
            let _ = self.changes.set(decoded);
        }
        Ok(self
            .changes
            .get()
            .expect("change metadata was just decoded"))
    }

    /// Reloads from `bytes` if the op columns were already taken.
    pub(crate) fn take_bundle_ops(&mut self) -> Result<OpSet, ParseError> {
        if let Some(ops) = self.bundle_ops.take() {
            return Ok(ops);
        }
        self.load_bundle_ops()
    }

    fn load_bundle_ops(&self) -> Result<OpSet, ParseError> {
        OpSet::load_bundle_cols(
            &self.ops_meta,
            &self.bytes[self.ops_data.clone()],
            self.actors.len(),
        )
        .map_err(|e| ParseError::InvalidColumns(Box::new(e)))
    }
}

fn decode_change_meta(
    changes_meta: &RawColumns<compression::Uncompressed>,
    data: &[u8],
) -> Result<Vec<BundleChange<'static>>, ParseError> {
    BundleChangeIterUnverified::try_new(changes_meta, data)?
        .map(|c| c.map(BundleChange::into_owned))
        .collect()
}

fn primed<T>(value: T) -> OnceLock<T> {
    let cell = OnceLock::new();
    let _ = cell.set(value);
    cell
}

impl<'a> BundleStorage<'a, Unverified> {
    pub(crate) fn parse_columns(
        input: parse::Input<'a>,
    ) -> parse::ParseResult<'a, BundleStorage<'a, Unverified>, ParseError> {
        let full_bytes = input.bytes();

        // Parse the leading deps + actors, capturing the byte range so we
        // know where the change-column metadata begins.
        let (i, prefix_r) = parse::range_of(
            |i| -> parse::ParseResult<'_, _, ParseError> {
                let (i, deps) = parse::length_prefixed(parse::change_hash)(i)?;
                let (i, actors) = parse::length_prefixed(parse::actor_id)(i)?;
                Ok((i, (deps, actors)))
            },
            input,
        )?;
        let (deps, actors) = prefix_r.value;
        let prefix_end = prefix_r.range.end;

        // Change column metadata + data.
        let (i, changes_meta_raw) = RawColumns::parse(i)?;
        let (i, changes) =
            parse::range_of(|i| parse::take_n(changes_meta_raw.total_column_len(), i), i)?;
        let changes_data_range = changes.range.clone();

        // Op column metadata + data.
        let (i, ops_meta_raw) = RawColumns::parse(i)?;
        let (_, ops) = parse::range_of(|i| parse::take_n(ops_meta_raw.total_column_len(), i), i)?;
        let ops_data_range = ops.range.clone();

        // Fast path: nothing is compressed — keep input bytes as-is.
        if let (Some(changes_meta), Some(ops_meta)) =
            (changes_meta_raw.uncompressed(), ops_meta_raw.uncompressed())
        {
            let bundle_ops = OpSet::load_bundle_cols(&ops_meta, ops.value, actors.len())
                .and_then(|o| {
                    o.validate_foreign_bundle(&ops_meta, ops.value, !deps.is_empty())?;
                    Ok(o)
                })
                .map_err(|e| parse::ParseError::Error(ParseError::InvalidColumns(Box::new(e))))?;
            return Ok((
                parse::Input::empty(),
                BundleStorage {
                    bytes: full_bytes.into(),
                    compressed: Compressed::AsIs,
                    deps,
                    actors,
                    ops_meta,
                    ops_data: ops_data_range,
                    changes_meta,
                    changes_data: changes_data_range,
                    changes: OnceLock::new(),
                    bundle_ops: primed(bundle_ops),
                    _phantom: PhantomData,
                },
            ));
        }

        // Slow path: at least one column is DEFLATE-encoded. Reconstruct a
        // fully-uncompressed buffer with the same section layout:
        //   deps | actors | change_meta' | change_data' | ops_meta' | ops_data'
        // where the primed sections use uncompressed column specs and
        // inflated data. We keep the compressed input around for
        // re-emission.
        let mut out = Vec::with_capacity(full_bytes.len());
        out.extend_from_slice(&full_bytes[..prefix_end]);

        let mut changes_data_buf = Vec::new();
        let changes_meta = changes_meta_raw
            .uncompress(
                &full_bytes[changes_data_range.clone()],
                &mut changes_data_buf,
            )
            .map_err(|_| parse::ParseError::Error(ParseError::CompressedChangeCols))?;
        changes_meta.write(&mut out);
        let new_changes_start = out.len();
        out.extend_from_slice(&changes_data_buf);
        let new_changes_end = out.len();

        let mut ops_data_buf = Vec::new();
        let ops_meta = ops_meta_raw
            .uncompress(&full_bytes[ops_data_range.clone()], &mut ops_data_buf)
            .map_err(|_| parse::ParseError::Error(ParseError::CompressedOpCols))?;
        ops_meta.write(&mut out);
        let new_ops_start = out.len();
        out.extend_from_slice(&ops_data_buf);
        let new_ops_end = out.len();

        let ops_data = &out[new_ops_start..new_ops_end];
        let bundle_ops = OpSet::load_bundle_cols(&ops_meta, ops_data, actors.len())
            .and_then(|o| {
                o.validate_foreign_bundle(&ops_meta, ops_data, !deps.is_empty())?;
                Ok(o)
            })
            .map_err(|e| parse::ParseError::Error(ParseError::InvalidColumns(Box::new(e))))?;

        Ok((
            parse::Input::empty(),
            BundleStorage {
                bytes: Cow::Owned(out),
                compressed: Compressed::Stored(full_bytes.into()),
                deps,
                actors,
                ops_meta,
                ops_data: new_ops_start..new_ops_end,
                changes_meta,
                changes_data: new_changes_start..new_changes_end,
                changes: OnceLock::new(),
                bundle_ops: primed(bundle_ops),
                _phantom: PhantomData,
            },
        ))
    }

    /// `parse_columns` has already validated the op columns. Rows malformed
    /// in ways a column decode cannot see (a map op with a null key, say)
    /// are only caught by [`Self::to_changes`].
    pub(crate) fn verify(self) -> Result<BundleStorage<'a, Verified>, ParseError> {
        self.change_cols()?.validate()?;
        for change in self.iter_change_meta()? {
            change?;
        }
        Ok(BundleStorage {
            bytes: self.bytes,
            compressed: self.compressed,
            deps: self.deps,
            actors: self.actors,
            ops_meta: self.ops_meta,
            ops_data: self.ops_data,
            changes_meta: self.changes_meta,
            changes_data: self.changes_data,
            changes: self.changes,
            bundle_ops: self.bundle_ops,
            _phantom: PhantomData,
        })
    }
}

impl BundleStorage<'_, Verified> {
    /// Rebuilds the member [`Change`]s, including the deletes that have no
    /// row of their own.
    pub(crate) fn to_changes(&self) -> Result<Vec<Change>, ParseError> {
        let actors = ActorTable::from_sorted(self.actors.clone())?;
        let ops = self.ops()?;
        let mut collector = self.collector(&actors)?;
        self.feed(&ops, &mut collector)?;
        collector
            .decode_bundle(&actors, &self.deps)
            .map_err(|e| ParseError::DecodeBundle(Box::new(e)))
    }

    /// Every member's hash, hashing each as soon as its ops are in, as a
    /// document load does.
    pub(crate) fn hash_members(&self) -> Result<Vec<ChangeHash>, ParseError> {
        let actors = ActorTable::from_sorted(self.actors.clone())?;
        let ops = self.ops()?;
        let mut collector = self.collector(&actors)?.hashing_bundle(&self.deps);
        self.feed(&ops, &mut collector)?;
        let collected = collector
            .collect_hashes()
            .map_err(|e| ParseError::DecodeBundle(Box::new(e)))?;
        Ok(collected.hashes)
    }

    fn ops(&self) -> Result<Cow<'_, OpSet>, ParseError> {
        match self.bundle_ops.get() {
            Some(ops) => Ok(Cow::Borrowed(ops)),
            None => self.load_bundle_ops().map(Cow::Owned),
        }
    }

    fn feed<'c>(
        &self,
        ops: &'c OpSet,
        collector: &mut ChangeCollector<'c>,
    ) -> Result<(), ParseError> {
        let mut preds = ExternalPreds::new(&self.ops_meta, &self.bytes[self.ops_data.clone()]);
        collector.process_all_ops_with(ops, |out| preds.next_row(out))
    }

    pub(crate) fn deps(&self) -> &[ChangeHash] {
        &self.deps
    }
}
