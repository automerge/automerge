use crate::storage::columns::ColumnId;
use crate::storage::ColumnType;
use crate::storage::{columns::compression, ColumnSpec, Document, RawColumn, RawColumns};
use crate::types::{ActorId, ElemId};

use super::columns::{ColumnData, ColumnDataIter, RawReader};
use super::rle::{ActionCursor, ActorCursor};
use super::types::ActorIdx;
use super::{
    BooleanCursor, Column, DeltaCursor, GroupCursor, IntCursor, Key, MetaCursor, Packable,
    RawCursor, Slab, StrCursor, ValueMeta,
};

use std::collections::BTreeMap;
use std::sync::Arc;

mod iter;
pub(crate) use iter::OpIter;

// Stick all of the column ID initialization in a module so we can turn off
// rustfmt for the whole thing
#[rustfmt::skip]
mod ids {
    use crate::storage::{columns::ColumnId, ColumnSpec};

    pub(super) const OBJ_COL_ID:                ColumnId = ColumnId::new(0);
    pub(super) const KEY_COL_ID:                ColumnId = ColumnId::new(1);
    pub(super) const ID_COL_ID:                 ColumnId = ColumnId::new(2);
    pub(super) const INSERT_COL_ID:             ColumnId = ColumnId::new(3);
    pub(in crate::op_set2) const ACTION_COL_ID: ColumnId = ColumnId::new(4);
    pub(super) const VAL_COL_ID:                ColumnId = ColumnId::new(5);
    pub(super) const SUCC_COL_ID:               ColumnId = ColumnId::new(8);
    pub(super) const EXPAND_COL_ID:             ColumnId = ColumnId::new(9);
    pub(super) const MARK_NAME_COL_ID:          ColumnId = ColumnId::new(10);

    pub(super) const ID_ACTOR_COL_SPEC:       ColumnSpec = ColumnSpec::new_actor(ID_COL_ID);
    pub(super) const ID_COUNTER_COL_SPEC:     ColumnSpec = ColumnSpec::new_delta(ID_COL_ID);
    pub(super) const OBJ_ID_ACTOR_COL_SPEC:   ColumnSpec = ColumnSpec::new_actor(OBJ_COL_ID);
    pub(super) const OBJ_ID_COUNTER_COL_SPEC: ColumnSpec = ColumnSpec::new_integer(OBJ_COL_ID);
    pub(super) const KEY_ACTOR_COL_SPEC:      ColumnSpec = ColumnSpec::new_actor(KEY_COL_ID);
    pub(super) const KEY_COUNTER_COL_SPEC:    ColumnSpec = ColumnSpec::new_delta(KEY_COL_ID);
    pub(super) const KEY_STR_COL_SPEC:        ColumnSpec = ColumnSpec::new_string(KEY_COL_ID);
    pub(super) const SUCC_COUNT_COL_SPEC:     ColumnSpec = ColumnSpec::new_group(SUCC_COL_ID);
    pub(super) const SUCC_ACTOR_COL_SPEC:     ColumnSpec = ColumnSpec::new_actor(SUCC_COL_ID);
    pub(super) const SUCC_COUNTER_COL_SPEC:   ColumnSpec = ColumnSpec::new_delta(SUCC_COL_ID);
    pub(super) const INSERT_COL_SPEC:         ColumnSpec = ColumnSpec::new_boolean(INSERT_COL_ID);
    pub(super) const ACTION_COL_SPEC:         ColumnSpec = ColumnSpec::new_integer(ACTION_COL_ID);
    pub(super) const VALUE_META_COL_SPEC:     ColumnSpec = ColumnSpec::new_value_metadata(VAL_COL_ID);
    pub(super) const VALUE_COL_SPEC:          ColumnSpec = ColumnSpec::new_value(VAL_COL_ID);
    pub(super) const MARK_NAME_COL_SPEC:      ColumnSpec = ColumnSpec::new_string(MARK_NAME_COL_ID);
    pub(super) const EXPAND_COL_SPEC:         ColumnSpec = ColumnSpec::new_boolean(EXPAND_COL_ID);

    pub(super) const ALL_COLUMN_SPECS: [ColumnSpec; 16] = [
        ID_ACTOR_COL_SPEC,
        ID_COUNTER_COL_SPEC,
        OBJ_ID_ACTOR_COL_SPEC,
        OBJ_ID_COUNTER_COL_SPEC,
        KEY_ACTOR_COL_SPEC,
        KEY_COUNTER_COL_SPEC,
        KEY_STR_COL_SPEC,
        SUCC_COUNT_COL_SPEC,
        SUCC_ACTOR_COL_SPEC,
        SUCC_COUNTER_COL_SPEC,
        INSERT_COL_SPEC,
        ACTION_COL_SPEC,
        VALUE_META_COL_SPEC,
        VALUE_COL_SPEC,
        MARK_NAME_COL_SPEC,
        EXPAND_COL_SPEC,
    ];
}
pub(super) use ids::*;

#[derive(Debug, Default, Clone)]
pub(crate) struct OpSet {
    len: usize,
    actors: Vec<ActorId>,
    cols: Columns,
}

impl OpSet {
    pub(crate) fn new(doc: &Document<'_>) -> Self {
        // FIXME - shouldn't need to clone bytes here (eventually)
        let data = Arc::new(doc.op_raw_bytes().to_vec());
        let actors = doc.actors().to_vec();
        Self::from_parts(doc.op_metadata.raw_columns(), data, actors)
    }

    pub(crate) fn from_doc_ops<
        'a,
        I: Iterator<Item = super::op::Op<'a>> + ExactSizeIterator + Clone,
    >(
        actors: Vec<ActorId>,
        ops: I,
    ) -> Self {
        let len = ops.len();
        let cols = Columns::new(ops);
        OpSet { actors, cols, len }
    }

    fn from_parts(
        cols: RawColumns<compression::Uncompressed>,
        data: Arc<Vec<u8>>,
        actors: Vec<ActorId>,
    ) -> Self {
        let cols = Columns(
            cols.iter()
                .map(|c| {
                    (
                        c.spec(),
                        Column::external(c.spec(), data.clone(), c.data()).unwrap(),
                    )
                })
                .collect(),
        );
        let len = cols.len();
        let op_set = OpSet { actors, cols, len };
        op_set
    }

    fn export(&self) -> (RawColumns<compression::Uncompressed>, Vec<u8>) {
        let mut data = vec![]; // should be able to do with_capacity here
        let mut raw = vec![];
        for (spec, c) in &self.cols {
            let range = c.write(&mut data);
            if !range.is_empty() {
                raw.push(RawColumn::new(*spec, range));
            }
        }
        (raw.into_iter().collect(), data)
    }

    fn iter(&self) -> OpIter<'_, iter::Unverified> {
        OpIter {
            index: 0,
            id_actor: self.cols.get_actor(ID_ACTOR_COL_SPEC),
            id_counter: self.cols.get_delta_integer(ID_COUNTER_COL_SPEC),
            obj_id_actor: self.cols.get_actor(OBJ_ID_ACTOR_COL_SPEC),
            obj_id_counter: self.cols.get_integer(OBJ_ID_COUNTER_COL_SPEC),
            key_actor: self.cols.get_actor(KEY_ACTOR_COL_SPEC),
            key_counter: self.cols.get_delta_integer(KEY_COUNTER_COL_SPEC),
            key_str: self.cols.get_str(KEY_STR_COL_SPEC),
            succ_count: self.cols.get_group(SUCC_COUNT_COL_SPEC),
            succ_actor: self.cols.get_actor(SUCC_ACTOR_COL_SPEC),
            succ_counter: self.cols.get_delta_integer(SUCC_COUNTER_COL_SPEC),
            insert: self.cols.get_boolean(INSERT_COL_SPEC),
            action: self.cols.get_action(ACTION_COL_SPEC),
            value_meta: self.cols.get_value_meta(VALUE_META_COL_SPEC),
            value: self.cols.get_value(VALUE_COL_SPEC),
            mark_name: self.cols.get_str(MARK_NAME_COL_SPEC),
            expand: self.cols.get_boolean(EXPAND_COL_SPEC),
            _phantom: std::marker::PhantomData,
        }
    }

    // iter ops

    // better error handling
    // export bytes
    // insert op
    // seek nth (read)
    // seek nth (insert)
    // seek prop
    // seek opid
    // seek mark

    // split slabs at some point

    // slab in-place edits
    // slab index vec<cursor>

    // ugly api stuff
    //
    // * boolean packable has unused pack/unpack - maybe we want two traits
    //    one for Rle<> and one for Cursor<> that overlap?
    // * columns that don't handle nulls still take Option<Item> and the
    //    iterator still returns Option<item> - could be nice to more cleanly
    //    handle columns that can't take nulls - currently hide this with
    //    MaybePackable allowing you to pass in Item or Option<Item> to splice
    // * maybe do something with types to make scan required to get
    //    validated bytes
}

#[derive(Debug, Default, Clone)]
struct Columns(BTreeMap<ColumnSpec, Column>);

impl Columns {
    fn new<'a, I: Iterator<Item = super::op::Op<'a>> + Clone>(ops: I) -> Self {
        let mut columns = BTreeMap::new();

        let mut id_actor = ColumnData::<ActorCursor>::new();
        id_actor.splice(
            0,
            ops.clone()
                .map(|op| ActorIdx::from(op.id.actor()))
                .collect::<Vec<_>>(),
        );
        columns.insert(ID_ACTOR_COL_SPEC, Column::Actor(id_actor));

        let mut id_counter = ColumnData::<DeltaCursor>::new();
        id_counter.splice(
            0,
            ops.clone()
                .map(|op| op.id.counter() as i64)
                .collect::<Vec<_>>(),
        );
        columns.insert(ID_COUNTER_COL_SPEC, Column::Delta(id_counter));

        let mut obj_actor_col = ColumnData::<ActorCursor>::new();
        obj_actor_col.splice(
            0,
            ops.clone()
                .map(|op| ActorIdx::from(op.obj.0.actor()))
                .collect::<Vec<_>>(),
        );
        columns.insert(OBJ_ID_ACTOR_COL_SPEC, Column::Actor(obj_actor_col));

        let mut obj_counter_col = ColumnData::<IntCursor>::new();
        obj_counter_col.splice(
            0,
            ops.clone().map(|op| op.obj.0.counter()).collect::<Vec<_>>(),
        );
        columns.insert(OBJ_ID_COUNTER_COL_SPEC, Column::Integer(obj_counter_col));

        let mut key_actor = ColumnData::<ActorCursor>::new();
        key_actor.splice(
            0,
            ops.clone()
                .map(|op| match op.key {
                    Key::Map(_) => None,
                    Key::Seq(e) => {
                        if e.is_head() {
                            None
                        } else {
                            Some(ActorIdx::from(e.0.actor()))
                        }
                    }
                })
                .collect::<Vec<_>>(),
        );
        columns.insert(KEY_ACTOR_COL_SPEC, Column::Actor(key_actor));

        let mut key_counter = ColumnData::<DeltaCursor>::new();
        key_counter.splice(
            0,
            ops.clone()
                .map(|op| match op.key {
                    Key::Map(_) => None,
                    Key::Seq(e) => {
                        if e.is_head() {
                            None
                        } else {
                            Some(e.0.counter() as i64)
                        }
                    }
                })
                .collect::<Vec<_>>(),
        );
        columns.insert(KEY_COUNTER_COL_SPEC, Column::Delta(key_counter));

        let mut key_str = ColumnData::<StrCursor>::new();
        key_str.splice(
            0,
            ops.clone()
                .map(|op| match op.key {
                    Key::Map(s) => Some(s),
                    Key::Seq(_) => None,
                })
                .collect::<Vec<_>>(),
        );
        columns.insert(KEY_STR_COL_SPEC, Column::Str(key_str));

        let mut succ_count = ColumnData::<GroupCursor>::new();
        succ_count.splice(
            0,
            ops.clone()
                .map(|op| op.succ().len() as u64)
                .collect::<Vec<_>>(),
        );
        columns.insert(SUCC_COUNT_COL_SPEC, Column::Group(succ_count));

        let mut succ_actor = ColumnData::<ActorCursor>::new();
        succ_actor.splice(
            0,
            ops.clone()
                .flat_map(|op| op.succ().map(|n| ActorIdx::from(n.actor())))
                .collect::<Vec<_>>(),
        );
        columns.insert(SUCC_ACTOR_COL_SPEC, Column::Actor(succ_actor));

        let mut succ_counter = ColumnData::<DeltaCursor>::new();
        succ_counter.splice(
            0,
            ops.clone()
                .flat_map(|op| op.succ().map(|n| n.counter() as i64))
                .collect::<Vec<_>>(),
        );
        columns.insert(SUCC_COUNTER_COL_SPEC, Column::Delta(succ_counter));

        let mut insert = ColumnData::<BooleanCursor>::new();
        insert.splice(0, ops.clone().map(|op| op.insert).collect::<Vec<_>>());
        columns.insert(INSERT_COL_SPEC, Column::Bool(insert));

        let mut action = ColumnData::<ActionCursor>::new();
        action.splice(0, ops.clone().map(|op| op.action).collect::<Vec<_>>());
        columns.insert(ACTION_COL_SPEC, Column::Action(action));

        let mut value_meta = ColumnData::<MetaCursor>::new();
        value_meta.splice(
            0,
            ops.clone()
                .map(|op| ValueMeta::from(&op.value))
                .collect::<Vec<_>>(),
        );
        columns.insert(VALUE_META_COL_SPEC, Column::ValueMeta(value_meta));

        let mut value = ColumnData::<RawCursor>::new();
        let values = ops
            .clone()
            .filter_map(|op| op.value.to_raw())
            .collect::<Vec<_>>();
        value.splice(0, values);
        columns.insert(VALUE_COL_SPEC, Column::Value(value));

        let mut mark_name = ColumnData::<StrCursor>::new();
        mark_name.splice(
            0,
            ops.clone()
                .map(|op| op.mark_name.clone())
                .collect::<Vec<_>>(),
        );
        columns.insert(MARK_NAME_COL_SPEC, Column::Str(mark_name));

        let mut expand = ColumnData::<BooleanCursor>::new();
        expand.splice(0, ops.clone().map(|op| op.expand).collect::<Vec<_>>());
        columns.insert(EXPAND_COL_SPEC, Column::Bool(expand));

        Columns(columns)
    }

    fn len(&self) -> usize {
        self.0.get(&ID_ACTOR_COL_SPEC).map(|c| c.len()).unwrap_or(0)
    }

    fn get_actor_coldata(&self, spec: ColumnSpec) -> &Column {
        self.0.get(&spec).unwrap()
    }

    fn get_actor(&self, spec: ColumnSpec) -> ColumnDataIter<'_, ActorCursor> {
        match self.0.get(&spec) {
            Some(Column::Actor(c)) => c.iter(),
            _ => ColumnDataIter::empty(),
        }
    }

    fn get_coldata(&self, spec: ColumnSpec) -> &[Slab] {
        self.0.get(&spec).unwrap().slabs()
    }

    fn get_integer(&self, spec: ColumnSpec) -> ColumnDataIter<'_, IntCursor> {
        match self.0.get(&spec) {
            Some(Column::Integer(c)) => c.iter(),
            _ => ColumnDataIter::empty(),
        }
    }

    fn get_action(&self, spec: ColumnSpec) -> ColumnDataIter<'_, ActionCursor> {
        match self.0.get(&spec) {
            Some(Column::Action(c)) => c.iter(),
            _ => ColumnDataIter::empty(),
        }
    }

    fn get_delta_integer(&self, spec: ColumnSpec) -> ColumnDataIter<'_, DeltaCursor> {
        match self.0.get(&spec) {
            Some(Column::Delta(c)) => c.iter(),
            _ => ColumnDataIter::empty(),
        }
    }

    fn get_str(&self, spec: ColumnSpec) -> ColumnDataIter<'_, StrCursor> {
        match self.0.get(&spec) {
            Some(Column::Str(c)) => c.iter(),
            _ => ColumnDataIter::empty(),
        }
    }

    fn get_boolean(&self, spec: ColumnSpec) -> ColumnDataIter<'_, BooleanCursor> {
        match self.0.get(&spec) {
            Some(Column::Bool(c)) => c.iter(),
            _ => ColumnDataIter::empty(),
        }
    }

    fn get_value_meta(&self, spec: ColumnSpec) -> ColumnDataIter<'_, MetaCursor> {
        match self.0.get(&spec) {
            Some(Column::ValueMeta(c)) => c.iter(),
            _ => ColumnDataIter::empty(),
        }
    }

    fn get_value(&self, spec: ColumnSpec) -> RawReader<'_> {
        match self.0.get(&spec) {
            Some(Column::Value(c)) => c.raw_reader(),
            _ => RawReader::empty(),
        }
    }

    fn get_group(&self, spec: ColumnSpec) -> ColumnDataIter<'_, GroupCursor> {
        match self.0.get(&spec) {
            Some(Column::Group(c)) => c.iter(),
            _ => ColumnDataIter::empty(),
        }
    }
}

impl<'a> Iterator for &'a Columns {
    type Item = (&'a ColumnSpec, &'a Column);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.iter().next()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use proptest::{
        arbitrary::any,
        prop_compose, prop_oneof,
        strategy::{Just, Strategy},
    };

    use crate::{
        indexed_cache::IndexedCache,
        op_set2::{
            columns::ColumnData,
            op::SuccCursors,
            rle::ActorCursor,
            types::{Action, ActorIdx, ScalarValue},
            ColumnCursor, DeltaCursor, GroupCursor, Key, Slab, WritableSlab,
        },
        storage::Document,
        transaction::Transactable,
        types::{ObjId, OpBuilder, OpId},
        ActorId, AutoCommit, ObjType, OpType,
    };

    use super::OpSet;

    #[test]
    fn basic_iteration() {
        let mut doc = AutoCommit::new();
        let text = doc.put_object(crate::ROOT, "text", ObjType::Text).unwrap();
        doc.splice_text(&text, 0, 0, "hello").unwrap();
        doc.put(crate::ROOT, "key", "value").unwrap();
        doc.put(crate::ROOT, "key2", "value2").unwrap();
        doc.delete(crate::ROOT, "key2").unwrap();
        let saved = doc.save();
        let doc_chunk = load_document_chunk(&saved);
        let opset = super::OpSet::new(&doc_chunk);
        let ops = opset.iter().collect::<Result<Vec<_>, _>>().unwrap();
        let actual_ops = doc
            .doc
            .ops()
            .iter()
            .map(|(_, _, op)| op)
            .collect::<Vec<_>>();
        if ops != actual_ops {
            for (i, (a, b)) in actual_ops.iter().zip(ops.iter()).enumerate() {
                if b != a {
                    println!("op {} mismatch", i);
                    println!("expected: {:?}", a);
                    println!("actual: {:?}", b);
                }
            }
        }
        assert_eq!(ops, actual_ops);
    }

    fn load_document_chunk(data: &[u8]) -> Document<'_> {
        let input = crate::storage::parse::Input::new(data);
        let (_i, chunk) = crate::storage::Chunk::parse(input).unwrap();
        let crate::storage::Chunk::Document(doc) = chunk else {
            panic!("expected document chunk");
        };
        doc
    }

    #[derive(Debug)]
    struct TestOp {
        id: OpId,
        obj: ObjId,
        action: Action,
        value: ScalarValue<'static>,
        key: Key<'static>,
        insert: bool,
        succs: Vec<OpId>,
        expand: bool,
        mark_name: Option<&'static str>,
    }

    impl<'a> PartialEq<super::super::op::Op<'a>> for TestOp {
        fn eq(&self, other: &super::super::op::Op<'a>) -> bool {
            self.id == other.id
                && self.obj == other.obj
                && self.action == other.action
                && self.value == other.value
                && self.key == other.key
                && self.insert == other.insert
                && self.succs == other.succ().collect::<Vec<_>>()
                && self.expand == other.expand
                && self.mark_name == other.mark_name
        }
    }

    fn with_test_ops<F>(actors: Vec<ActorId>, test_ops: &[TestOp], f: F)
    where
        F: FnOnce(super::OpSet),
    {
        let mut ops = Vec::new();

        let mut group_data = ColumnData::<GroupCursor>::new();
        let mut succ_actor_data = ColumnData::<ActorCursor>::new();
        let mut succ_counter_data = ColumnData::<DeltaCursor>::new();
        group_data.splice(
            0,
            test_ops
                .iter()
                .map(|o| o.succs.len() as u64)
                .collect::<Vec<_>>(),
        );
        succ_actor_data.splice(
            0,
            test_ops
                .iter()
                .flat_map(|o| o.succs.iter().map(|s| ActorIdx::from(s.actor())))
                .collect::<Vec<_>>(),
        );
        succ_counter_data.splice(
            0,
            test_ops
                .iter()
                .flat_map(|o| o.succs.iter().map(|s| s.counter() as i64))
                .collect::<Vec<_>>(),
        );

        let mut group_iter = group_data.iter();
        let mut actor_iter = succ_actor_data.iter();
        let mut counter_iter = succ_counter_data.iter();

        // first encode the succs
        for test_op in test_ops {
            let group_count = group_iter.next().unwrap().unwrap();
            let op = super::super::op::Op {
                id: test_op.id,
                obj: test_op.obj,
                action: test_op.action,
                value: test_op.value.clone(),
                key: test_op.key.clone(),
                insert: test_op.insert,
                expand: test_op.expand,
                mark_name: test_op.mark_name,
                succ_cursors: SuccCursors {
                    len: group_count as usize,
                    succ_counter: counter_iter.clone(),
                    succ_actor: actor_iter.clone(),
                },
            };
            for _ in 0..group_count {
                counter_iter.next();
                actor_iter.next();
            }
            ops.push(op);
        }
        let op_set = OpSet::from_doc_ops(actors, ops.iter().cloned());
        f(op_set);
    }

    #[test]
    fn seek_to_obj() {
        let actors = vec![crate::ActorId::random()];

        let ops = vec![
            TestOp {
                id: OpId::new(1, 1),
                obj: ObjId::root(),
                action: Action::MakeMap,
                value: ScalarValue::Null,
                key: Key::Map("key"),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(2, 1),
                obj: ObjId::root(),
                action: Action::Set,
                value: ScalarValue::Str("value"),
                key: Key::Map("key"),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(2, 1),
                obj: ObjId::root(),
                action: Action::Set,
                value: ScalarValue::Str("value"),
                key: Key::Map("key"),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(2, 1),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::MakeMap,
                value: ScalarValue::Null,
                key: Key::Map("inner_key"),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
        ];

        with_test_ops(actors, &ops, |opset| {
            let mut iter = opset.iter();
            iter.seek_to_obj(&ObjId(OpId::new(1, 1)));
            let op = iter.next().unwrap().unwrap();
            assert_eq!(ops[3], op);
        });
    }

    proptest::proptest! {
        #[test]
        fn same_as_old_encoding(Scenario{opset, actors, keys} in arbitrary_opset()) {

            // encode with old encoders
            let actor_lookup = actors
                .iter()
                .enumerate()
                .map(|(i, _)| i)
                .collect::<Vec<_>>();
            let objs_and_ops = opset
                .iter_objs()
                .flat_map(|(_, ops)| ops.map(move |op| op))
                .collect::<Vec<_>>();
            let doc_ops = objs_and_ops
                .iter()
                .map(|op_idx| {
                    let op = op_idx.as_op(&opset.osd);
                    crate::storage::convert::op_as_docop(
                        &actor_lookup,
                        &keys,
                        op,
                    )
                });
            let mut old_encoding = Vec::new();
            let ops_meta = crate::storage::document::DocOpColumns::encode(doc_ops, &mut old_encoding);

            // decode with new decoders
            let op_set = super::OpSet::from_parts(
                ops_meta.raw_columns(),
                Arc::new(old_encoding),
                actors.clone()
            );

            let actual_ops = objs_and_ops.iter().map(|op_idx| op_idx.as_op(&opset.osd)).collect::<Vec<_>>();
            let ops = op_set.iter().collect::<Result<Vec<_>, _>>().unwrap();
            if !(ops == actual_ops) {
                for (i, (a, b)) in actual_ops.iter().zip(ops.iter()).enumerate() {
                    if b != a {
                        println!("first mismatch: {}", i);
                        println!("expected: {:?}", a);
                        println!("actual: {:?}", b);
                        println!("expected successors: {:?}", a.succ().map(|n| *n.id()).collect::<Vec<_>>());
                        println!("actual successors: {:?}", b.succ().collect::<Vec<_>>());
                        break;
                    }
                }
                panic!("ops mismatch");
            }
        }
    }

    struct Scenario {
        opset: crate::op_set::OpSetInternal,
        actors: Vec<crate::ActorId>,
        keys: IndexedCache<String>,
    }

    prop_compose! {
        fn arbitrary_opset()(
            actors in proptest::collection::vec(arbitrary_actor(), 1..10),
            keys in arbitrary_keys(),
        )(
            obj in arbitrary_objid(&actors),
            ops in proptest::collection::vec(arbitrary_op(&actors, &keys), 0..100),
            actors in Just(actors),
            keys in Just(keys)
        ) -> Scenario {
            let mut opset = crate::op_set::OpSetInternal::new();
            opset.osd.props = keys.clone();
            opset.osd.actors = actors.clone().into_iter().collect();
            for ArbOp{op, succs} in ops {
                let op_idx = opset.load(obj, op);
                opset.insert(0, &obj, op_idx);
                for succ in succs {
                    let succ_idx = opset.load(obj, succ);
                    opset.osd.add_dep(op_idx, succ_idx);
                }
            }
            Scenario{
                opset,
                actors,
                keys
            }
        }
    }

    impl std::fmt::Debug for Scenario {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let ops_desc = self
                .opset
                .iter_objs()
                .flat_map(|(_, ops)| {
                    ops.map(|op| {
                        let op = op.as_op(&self.opset.osd);
                        let succs = op.succ().map(|n| *n.id()).collect::<Vec<_>>();
                        format!("{:?}, succs: {:?}", op, succs)
                    })
                })
                .collect::<Vec<_>>();
            f.debug_struct("Scenario")
                .field("ops", &ops_desc)
                .field("actors", &self.actors)
                .field("keys", &self.keys)
                .finish()
        }
    }

    fn arbitrary_action() -> impl Strategy<Value = OpType> {
        prop_oneof![
            prop_oneof![Just(ObjType::Text), Just(ObjType::Map), Just(ObjType::List)]
                .prop_map(|t| OpType::Make(t)),
            Just(OpType::Delete),
            (-1000_i64..1000).prop_map(|i| OpType::Increment(i)),
            arbitrary_value().prop_map(|v| OpType::Put(v)),
            (any::<String>(), arbitrary_value(), any::<bool>()).prop_map(|(k, v, expand)| {
                OpType::MarkBegin(
                    expand,
                    crate::marks::MarkData {
                        name: k.into(),
                        value: v,
                    },
                )
            }),
        ]
    }

    fn arbitrary_value() -> impl Strategy<Value = crate::ScalarValue> {
        prop_oneof![
            Just(crate::ScalarValue::Null),
            any::<i64>().prop_map(crate::ScalarValue::Int),
            any::<u64>().prop_map(crate::ScalarValue::Uint),
            any::<i64>().prop_map(|c| crate::ScalarValue::Counter(crate::value::Counter::from(c))),
            any::<f64>().prop_map(crate::ScalarValue::F64),
            any::<i64>().prop_map(crate::ScalarValue::Timestamp),
            any::<bool>().prop_map(crate::ScalarValue::Boolean),
            any::<String>().prop_map(|s| crate::ScalarValue::Str(s.into())),
            proptest::collection::vec(any::<u8>(), 0..100)
                .prop_map(|v| crate::ScalarValue::Bytes(v.into())),
        ]
    }

    fn arbitrary_key(
        actors: &[crate::ActorId],
        keys: &crate::indexed_cache::IndexedCache<String>,
    ) -> impl Strategy<Value = crate::types::Key> {
        prop_oneof![
            (0..keys.len()).prop_map(|i| crate::types::Key::Map(i)),
            prop_oneof![
                Just(crate::types::ElemId::head()),
                arbitrary_opid(actors).prop_map(crate::types::ElemId)
            ]
            .prop_map(crate::types::Key::Seq)
        ]
    }

    fn arbitrary_opid(actors: &[crate::ActorId]) -> impl Strategy<Value = crate::types::OpId> {
        (0..actors.len()).prop_flat_map(move |actor_idx| {
            (1_u64..1000).prop_map(move |counter| crate::types::OpId::new(counter, actor_idx))
        })
    }

    fn arbitrary_actor() -> impl Strategy<Value = crate::ActorId> {
        proptest::collection::vec(any::<u8>(), 32).prop_map(|v| crate::ActorId::from(&v))
    }

    #[derive(Debug)]
    struct ArbOp {
        op: OpBuilder,
        succs: Vec<OpBuilder>,
    }

    prop_compose! {
        fn arbitrary_op_builder(actors: &[crate::ActorId], keys: &crate::indexed_cache::IndexedCache<String>)
        (
            action in arbitrary_action(),
            key in arbitrary_key(&actors, &keys),
            id in arbitrary_opid(&actors),
            insert in any::<bool>(),
        )-> OpBuilder {
            OpBuilder {
                id,
                action,
                key,
                insert,
            }
        }
    }

    prop_compose! {
        fn arbitrary_op(actors: &[crate::ActorId], keys: &IndexedCache<String>)
        (
            op in arbitrary_op_builder(&actors, &keys),
            succs in proptest::collection::vec(arbitrary_op_builder(&actors, &keys), 0..10),
        )-> ArbOp {
            ArbOp{
                op,
                succs,
            }
        }
    }

    fn arbitrary_objid(actors: &[crate::ActorId]) -> impl Strategy<Value = crate::types::ObjId> {
        prop_oneof![
            Just(crate::types::ObjId::root()),
            arbitrary_opid(actors).prop_map(crate::types::ObjId)
        ]
    }

    fn arbitrary_keys() -> impl Strategy<Value = IndexedCache<String>> {
        proptest::collection::vec(proptest::string::string_regex("[a-zA-Z]*").unwrap(), 1..10)
            .prop_map(|v| v.into_iter().collect())
    }
}
