use std::borrow::Cow;
use std::marker::PhantomData;
use std::ops::Range;

use crate::op_set2::change::ChangeCollector;
use crate::storage::change::{OpReadState, Unverified, Verified};
use crate::storage::columns::compression;
use crate::storage::{parse, Header, RawColumns};
use crate::types::{ActorId, ChangeHash};
use crate::Change;

use super::{BundleChangeIter, BundleChangeIterUnverified, OpIter, OpIterUnverified, ParseError};

#[derive(Clone, Debug)]
pub(crate) struct BundleStorage<'a, OpReadState> {
    pub(crate) bytes: Cow<'a, [u8]>,
    pub(crate) header: Header,
    pub(crate) deps: Vec<ChangeHash>,
    pub(crate) actors: Vec<ActorId>,
    pub(crate) ops_meta: RawColumns<compression::Uncompressed>,
    pub(crate) ops_data: Range<usize>,
    pub(crate) changes_meta: RawColumns<compression::Uncompressed>,
    pub(crate) changes_data: Range<usize>,
    pub(crate) _phantom: PhantomData<OpReadState>,
}

impl<O: OpReadState> BundleStorage<'_, O> {
    pub(crate) fn into_owned(self) -> BundleStorage<'static, O> {
        BundleStorage {
            bytes: Cow::Owned(self.bytes.into_owned()),
            header: self.header,
            deps: self.deps,
            actors: self.actors,
            ops_meta: self.ops_meta,
            ops_data: self.ops_data,
            changes_meta: self.changes_meta,
            changes_data: self.changes_data,
            _phantom: self._phantom,
        }
    }

    pub(crate) fn checksum_valid(&self) -> bool {
        self.header.checksum_valid()
    }
}

impl<'a> BundleStorage<'a, Unverified> {
    pub(crate) fn parse_following_header(
        input: parse::Input<'a>,
        header: Header,
    ) -> parse::ParseResult<'a, BundleStorage<'a, Unverified>, ParseError> {
        let (i, deps) = parse::length_prefixed(parse::change_hash)(input)?;
        let (i, actors) = parse::length_prefixed(parse::actor_id)(i)?;

        let (i, changes_meta) = RawColumns::parse(i)?;
        let (i, changes) =
            parse::range_of(|i| parse::take_n(changes_meta.total_column_len(), i), i)?;
        let changes_meta = changes_meta
            .uncompressed()
            .ok_or(parse::ParseError::Error(ParseError::CompressedChangeCols))?;
        // this validates that the iterator can be created
        BundleChangeIterUnverified::try_new(&changes_meta, changes.value)
            .map_err(|e| parse::ParseError::Error(ParseError::InvalidColumns(Box::new(e))))?;

        let (i, ops_meta) = RawColumns::parse(i)?;
        let ops_meta = ops_meta
            .uncompressed()
            .ok_or(parse::ParseError::Error(ParseError::CompressedOpCols))?;
        let (_, ops) = parse::range_of(|i| parse::take_n(ops_meta.total_column_len(), i), i)?;
        OpIterUnverified::try_new(&ops_meta, ops.value)
            .map_err(|e| parse::ParseError::Error(ParseError::InvalidColumns(Box::new(e))))?;

        Ok((
            parse::Input::empty(),
            BundleStorage {
                bytes: input.bytes().into(),
                header,
                deps,
                actors,
                ops_meta,
                ops_data: ops.range,
                changes_meta,
                changes_data: changes.range,
                _phantom: PhantomData,
            },
        ))
    }

    pub(crate) fn verify(self) -> Result<BundleStorage<'a, Verified>, ParseError> {
        for c in self.iter_change_meta() {
            let _ = c?;
        }
        for o in self.iter_ops() {
            let _ = o?;
        }
        Ok(BundleStorage {
            bytes: self.bytes,
            header: self.header,
            deps: self.deps,
            actors: self.actors,
            ops_meta: self.ops_meta,
            ops_data: self.ops_data,
            changes_meta: self.changes_meta,
            changes_data: self.changes_data,
            _phantom: PhantomData,
        })
    }

    pub(crate) fn iter_ops(&self) -> OpIterUnverified<'_> {
        let bytes = &self.bytes[self.ops_data.clone()];
        OpIterUnverified::new(&self.ops_meta, bytes)
    }

    fn iter_change_meta(&self) -> BundleChangeIterUnverified<'_> {
        let change_data = &self.bytes[self.changes_data.clone()];
        BundleChangeIterUnverified::new(&self.changes_meta, change_data)
    }
}

impl BundleStorage<'_, Verified> {
    pub(crate) fn to_changes(&self) -> Result<Vec<Change>, ParseError> {
        let change_meta = self.iter_change_meta().collect();
        let mut collector = ChangeCollector::from_bundle_changes(change_meta);
        for op in self.iter_ops() {
            collector.add(op);
        }
        let bundle = collector
            .unbundle(&self.actors, &self.deps)
            .map_err(|e| ParseError::Unbundle(Box::new(e)))?;
        Ok(bundle)
    }

    pub(crate) fn iter_ops(&self) -> OpIter<'_> {
        let bytes = &self.bytes[self.ops_data.clone()];
        OpIter::new(&self.ops_meta, bytes)
    }

    pub(crate) fn iter_change_meta(&self) -> BundleChangeIter<'_> {
        let change_data = &self.bytes[self.changes_data.clone()];
        BundleChangeIter::new_from_verified(&self.changes_meta, change_data)
    }

    pub(crate) fn deps(&self) -> &[ChangeHash] {
        &self.deps
    }
}
