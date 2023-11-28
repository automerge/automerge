use std::{borrow::Cow, num::NonZeroU64};

use crate::{
    columnar::Key as StoredKey,
    storage::{
        change::{Unverified, Verified},
        parse, Change as StoredChange, ChangeOp, Chunk, Compressed, ReadChangeOpError,
    },
    types::{ActorId, ChangeHash, ElemId},
};

#[derive(Clone, Debug, PartialEq)]
pub struct Change {
    stored: StoredChange<'static, Verified>,
    compression: CompressionState,
    len: usize,
}

impl Change {
    pub(crate) fn new(stored: StoredChange<'static, Verified>) -> Self {
        let len = stored.len();
        Self {
            stored,
            len,
            compression: CompressionState::NotCompressed,
        }
    }

    pub(crate) fn new_from_unverified(
        stored: StoredChange<'static, Unverified>,
        compressed: Option<Compressed<'static>>,
    ) -> Result<Self, ReadChangeOpError> {
        let mut len = 0;
        let stored = stored.verify_ops(|_| len += 1)?;
        let compression = if let Some(c) = compressed {
            CompressionState::Compressed(c)
        } else {
            CompressionState::NotCompressed
        };
        Ok(Self {
            stored,
            len,
            compression,
        })
    }

    pub fn actor_id(&self) -> &ActorId {
        self.stored.actor()
    }

    pub fn other_actor_ids(&self) -> &[ActorId] {
        self.stored.other_actors()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn max_op(&self) -> u64 {
        self.stored.start_op().get() + (self.len as u64) - 1
    }

    pub fn start_op(&self) -> NonZeroU64 {
        self.stored.start_op()
    }

    pub fn message(&self) -> Option<&String> {
        self.stored.message().as_ref()
    }

    pub fn deps(&self) -> &[ChangeHash] {
        self.stored.dependencies()
    }

    pub fn hash(&self) -> ChangeHash {
        self.stored.hash()
    }

    pub fn seq(&self) -> u64 {
        self.stored.seq()
    }

    pub fn timestamp(&self) -> i64 {
        self.stored.timestamp()
    }

    pub fn bytes(&mut self) -> Cow<'_, [u8]> {
        if let CompressionState::NotCompressed = self.compression {
            if let Some(compressed) = self.stored.compress() {
                self.compression = CompressionState::Compressed(compressed);
            } else {
                self.compression = CompressionState::TooSmallToCompress;
            }
        };
        match &self.compression {
            // SAFETY: We just checked this case above
            CompressionState::NotCompressed => unreachable!(),
            CompressionState::TooSmallToCompress => Cow::Borrowed(self.stored.bytes()),
            CompressionState::Compressed(c) => c.bytes(),
        }
    }

    pub fn raw_bytes(&self) -> &[u8] {
        self.stored.bytes()
    }

    pub(crate) fn iter_ops(&self) -> impl Iterator<Item = ChangeOp> + '_ {
        self.stored.iter_ops()
    }

    pub fn extra_bytes(&self) -> &[u8] {
        self.stored.extra_bytes()
    }

    // TODO replace all uses of this with TryFrom<&[u8]>
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, LoadError> {
        Self::try_from(&bytes[..])
    }

    pub fn decode(&self) -> crate::ExpandedChange {
        crate::ExpandedChange::from(self)
    }
}

#[derive(Clone, Debug, PartialEq)]
enum CompressionState {
    /// We haven't tried to compress this change
    NotCompressed,
    /// We have compressed this change
    Compressed(Compressed<'static>),
    /// We tried to compress this change but it wasn't big enough to be worth it
    TooSmallToCompress,
}

impl AsRef<StoredChange<'static, Verified>> for Change {
    fn as_ref(&self) -> &StoredChange<'static, Verified> {
        &self.stored
    }
}

impl From<Change> for StoredChange<'static, Verified> {
    fn from(c: Change) -> Self {
        c.stored
    }
}

#[derive(thiserror::Error, Debug)]
pub enum LoadError {
    #[error("unable to parse change: {0}")]
    Parse(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("leftover data after parsing")]
    LeftoverData,
    #[error("wrong chunk type")]
    WrongChunkType,
}

impl<'a> TryFrom<&'a [u8]> for Change {
    type Error = LoadError;

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let input = parse::Input::new(value);
        let (remaining, chunk) = Chunk::parse(input).map_err(|e| LoadError::Parse(Box::new(e)))?;
        if !remaining.is_empty() {
            return Err(LoadError::LeftoverData);
        }
        match chunk {
            Chunk::Change(c) => Self::new_from_unverified(c.into_owned(), None)
                .map_err(|e| LoadError::Parse(Box::new(e))),
            Chunk::CompressedChange(c, compressed) => {
                Self::new_from_unverified(c.into_owned(), Some(compressed.into_owned()))
                    .map_err(|e| LoadError::Parse(Box::new(e)))
            }
            _ => Err(LoadError::WrongChunkType),
        }
    }
}

impl<'a> TryFrom<StoredChange<'a, Unverified>> for Change {
    type Error = ReadChangeOpError;

    fn try_from(c: StoredChange<'a, Unverified>) -> Result<Self, Self::Error> {
        Self::new_from_unverified(c.into_owned(), None)
    }
}

impl From<crate::ExpandedChange> for Change {
    fn from(e: crate::ExpandedChange) -> Self {
        let stored = StoredChange::builder()
            .with_actor(e.actor_id)
            .with_extra_bytes(e.extra_bytes)
            .with_seq(e.seq)
            .with_dependencies(e.deps)
            .with_timestamp(e.time)
            .with_start_op(e.start_op)
            .with_message(e.message)
            .build(e.operations.iter());
        match stored {
            Ok(c) => Change::new(c),
            Err(crate::storage::change::PredOutOfOrder) => {
                // Should never happen because we use `SortedVec` in legacy::Op::pred
                panic!("preds out of order");
            }
        }
    }
}

mod convert_expanded {
    use std::borrow::Cow;

    use crate::{convert, legacy, storage::AsChangeOp, types::ActorId, ScalarValue};

    impl<'a> AsChangeOp<'a> for &'a legacy::Op {
        type ActorId = &'a ActorId;
        type OpId = &'a legacy::OpId;
        type PredIter = std::slice::Iter<'a, legacy::OpId>;

        fn action(&self) -> u64 {
            self.action.action_index()
        }

        fn insert(&self) -> bool {
            self.insert
        }

        fn pred(&self) -> Self::PredIter {
            self.pred.iter()
        }

        fn key(&self) -> convert::Key<'a, Self::OpId> {
            match &self.key {
                legacy::Key::Map(s) => convert::Key::Prop(Cow::Borrowed(s)),
                legacy::Key::Seq(legacy::ElementId::Head) => {
                    convert::Key::Elem(convert::ElemId::Head)
                }
                legacy::Key::Seq(legacy::ElementId::Id(o)) => {
                    convert::Key::Elem(convert::ElemId::Op(o))
                }
            }
        }

        fn obj(&self) -> convert::ObjId<Self::OpId> {
            match &self.obj {
                legacy::ObjectId::Root => convert::ObjId::Root,
                legacy::ObjectId::Id(o) => convert::ObjId::Op(o),
            }
        }

        fn val(&self) -> Cow<'a, crate::ScalarValue> {
            match self.primitive_value() {
                Some(v) => Cow::Owned(v),
                None => Cow::Owned(ScalarValue::Null),
            }
        }

        fn expand(&self) -> bool {
            self.action.expand()
        }

        fn mark_name(&self) -> Option<Cow<'a, smol_str::SmolStr>> {
            if let legacy::OpType::MarkBegin(legacy::MarkData { name, .. }) = &self.action {
                Some(Cow::Borrowed(name))
            } else {
                None
            }
        }
    }

    impl<'a> convert::OpId<&'a ActorId> for &'a legacy::OpId {
        fn counter(&self) -> u64 {
            legacy::OpId::counter(self)
        }

        fn actor(&self) -> &'a ActorId {
            &self.1
        }
    }
}

impl From<&Change> for crate::ExpandedChange {
    fn from(c: &Change) -> Self {
        let actors = std::iter::once(c.actor_id())
            .chain(c.other_actor_ids().iter())
            .cloned()
            .enumerate()
            .collect::<std::collections::HashMap<_, _>>();
        let operations = c
            .iter_ops()
            .map(|o| crate::legacy::Op {
                action: crate::legacy::OpType::from_parts(crate::legacy::OpTypeParts {
                    action: o.action,
                    value: o.val,
                    expand: o.expand,
                    mark_name: o.mark_name,
                }),
                insert: o.insert,
                key: match o.key {
                    StoredKey::Elem(e) if e.is_head() => {
                        crate::legacy::Key::Seq(crate::legacy::ElementId::Head)
                    }
                    StoredKey::Elem(ElemId(o)) => {
                        crate::legacy::Key::Seq(crate::legacy::ElementId::Id(
                            crate::legacy::OpId::new(o.counter(), actors.get(&o.actor()).unwrap()),
                        ))
                    }
                    StoredKey::Prop(p) => crate::legacy::Key::Map(p),
                },
                obj: if o.obj.is_root() {
                    crate::legacy::ObjectId::Root
                } else {
                    crate::legacy::ObjectId::Id(crate::legacy::OpId::new(
                        o.obj.opid().counter(),
                        actors.get(&o.obj.opid().actor()).unwrap(),
                    ))
                },
                pred: o
                    .pred
                    .into_iter()
                    .map(|p| crate::legacy::OpId::new(p.counter(), actors.get(&p.actor()).unwrap()))
                    .collect(),
            })
            .collect::<Vec<_>>();
        crate::ExpandedChange {
            operations,
            actor_id: actors.get(&0).unwrap().clone(),
            hash: Some(c.hash()),
            time: c.timestamp(),
            deps: c.deps().to_vec(),
            seq: c.seq(),
            start_op: c.start_op(),
            extra_bytes: c.extra_bytes().to_vec(),
            message: c.message().cloned(),
        }
    }
}

#[cfg(test)]
pub(crate) mod gen {
    use super::Change;
    use crate::{
        op_set::{OpIdx, OpSetData},
        storage::{change::ChangeBuilder, convert::op_as_actor_id},
        types::{
            gen::{gen_hash, gen_op},
            ObjId, OpId,
        },
        ActorId,
    };
    use proptest::prelude::*;

    fn gen_actor() -> impl Strategy<Value = ActorId> {
        proptest::array::uniform32(proptest::bits::u8::ANY).prop_map(ActorId::from)
    }

    prop_compose! {
        fn gen_actors()(this_actor in gen_actor(), other_actors in proptest::collection::vec(gen_actor(), 0..10)) -> (ActorId, Vec<ActorId>) {
            (this_actor, other_actors)
        }
    }

    fn gen_ops(
        this_actor: ActorId,
        other_actors: Vec<ActorId>,
    ) -> impl Strategy<Value = (Vec<(ObjId, OpIdx)>, OpSetData)> {
        let mut all_actors = vec![this_actor];
        all_actors.extend(other_actors);
        let mut osd = OpSetData::from_actors(all_actors);
        osd.props.cache("someprop".to_string());
        let root_id = ObjId::root();
        (0_u64..10)
            .prop_map(|num_ops| {
                (0..num_ops)
                    .map(|counter| OpId::new(counter, 0))
                    .collect::<Vec<_>>()
            })
            .prop_flat_map(move |opids| {
                let mut strat = Just(Vec::new()).boxed();
                for opid in opids {
                    strat = (gen_op(opid, vec![0]), strat)
                        .prop_map(move |(op, ops)| {
                            let mut result = Vec::with_capacity(ops.len() + 1);
                            result.extend(ops);
                            result.push((root_id, op));
                            result
                        })
                        .boxed();
                }
                strat
            })
            .prop_map(move |ops| {
                let mut osd = osd.clone();
                let ops = ops
                    .into_iter()
                    .map(|(obj, op)| (obj, osd.push(obj, op)))
                    .collect();
                (ops, osd)
            })
    }

    prop_compose! {
        pub(crate) fn gen_change()((this_actor, other_actors) in gen_actors())(
                (ops, osd) in gen_ops(this_actor.clone(), other_actors),
                start_op in 1_u64..200000,
                seq in 0_u64..200000,
                timestamp in 0..i64::MAX,
                deps in proptest::collection::vec(gen_hash(), 0..100),
                message in proptest::option::of("[a-z]{200}"),
                this_actor in Just(this_actor),
            ) -> Change {
            let ops = ops.iter().map(|(_, op)| op_as_actor_id(op.as_op(&osd)));
            Change::new(ChangeBuilder::new()
                .with_dependencies(deps)
                .with_start_op(start_op.try_into().unwrap())
                .with_message(message)
                .with_actor(this_actor)
                .with_seq(seq)
                .with_timestamp(timestamp)
                .build(ops.into_iter())
                .unwrap())
        }

    }
}
