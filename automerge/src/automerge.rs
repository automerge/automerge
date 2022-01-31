use std::collections::{HashMap, HashSet, VecDeque};
use unicode_segmentation::UnicodeSegmentation;

use crate::change::{encode_document, export_change};
use crate::exid::ExId;
use crate::op_set::OpSet;
use crate::types::{
    ActorId, ChangeHash, Clock, ElemId, Export, Exportable, Key, ObjId, Op, OpId, OpType, Patch,
    ScalarValue, Value,
};
use crate::{legacy, query, types, ObjType};
use crate::{AutomergeError, Change, Prop};

#[derive(Debug, Clone)]
pub struct Automerge {
    queue: Vec<Change>,
    history: Vec<Change>,
    history_index: HashMap<ChangeHash, usize>,
    states: HashMap<usize, Vec<usize>>,
    deps: HashSet<ChangeHash>,
    saved: Vec<ChangeHash>,
    ops: OpSet,
    actor: Option<usize>,
    max_op: u64,
    transaction: Option<Transaction>,
}

impl Automerge {
    pub fn new() -> Self {
        Automerge {
            queue: vec![],
            history: vec![],
            history_index: HashMap::new(),
            states: HashMap::new(),
            ops: Default::default(),
            deps: Default::default(),
            saved: Default::default(),
            actor: None,
            max_op: 0,
            transaction: None,
        }
    }

    pub fn set_actor(&mut self, actor: ActorId) {
        self.ensure_transaction_closed();
        self.actor = Some(self.ops.m.actors.cache(actor))
    }

    fn random_actor(&mut self) -> ActorId {
        let actor = ActorId::from(uuid::Uuid::new_v4().as_bytes().to_vec());
        self.actor = Some(self.ops.m.actors.cache(actor.clone()));
        actor
    }

    pub fn get_actor(&mut self) -> ActorId {
        if let Some(actor) = self.actor {
            self.ops.m.actors[actor].clone()
        } else {
            self.random_actor()
        }
    }

    pub fn maybe_get_actor(&self) -> Option<ActorId> {
        self.actor.map(|i| self.ops.m.actors[i].clone())
    }

    fn get_actor_index(&mut self) -> usize {
        if let Some(actor) = self.actor {
            actor
        } else {
            self.random_actor();
            self.actor.unwrap() // random_actor always sets actor to is_some()
        }
    }

    pub fn new_with_actor_id(actor: ActorId) -> Self {
        let mut am = Automerge {
            queue: vec![],
            history: vec![],
            history_index: HashMap::new(),
            states: HashMap::new(),
            ops: Default::default(),
            deps: Default::default(),
            saved: Default::default(),
            actor: None,
            max_op: 0,
            transaction: None,
        };
        am.actor = Some(am.ops.m.actors.cache(actor));
        am
    }

    pub fn pending_ops(&self) -> u64 {
        self.transaction
            .as_ref()
            .map(|t| t.operations.len() as u64)
            .unwrap_or(0)
    }

    fn tx(&mut self) -> &mut Transaction {
        if self.transaction.is_none() {
            let actor = self.get_actor_index();

            let seq = self.states.entry(actor).or_default().len() as u64 + 1;
            let mut deps = self.get_heads();
            if seq > 1 {
                let last_hash = self.get_hash(actor, seq - 1).unwrap();
                if !deps.contains(&last_hash) {
                    deps.push(last_hash);
                }
            }

            self.transaction = Some(Transaction {
                actor,
                seq,
                start_op: self.max_op + 1,
                time: 0,
                message: None,
                extra_bytes: Default::default(),
                hash: None,
                operations: vec![],
                deps,
            });
        }

        self.transaction.as_mut().unwrap()
    }

    pub fn commit(&mut self, message: Option<String>, time: Option<i64>) -> Vec<ChangeHash> {
        let tx = self.tx();

        if message.is_some() {
            tx.message = message;
        }

        if let Some(t) = time {
            tx.time = t;
        }

        tx.operations.len();

        self.ensure_transaction_closed();

        self.get_heads()
    }

    pub fn ensure_transaction_closed(&mut self) {
        if let Some(tx) = self.transaction.take() {
            self.update_history(export_change(&tx, &self.ops.m.actors, &self.ops.m.props));
        }
    }

    pub fn rollback(&mut self) -> usize {
        if let Some(tx) = self.transaction.take() {
            let num = tx.operations.len();
            // remove in reverse order so sets are removed before makes etc...
            for op in tx.operations.iter().rev() {
                for pred_id in &op.pred {
                    // FIXME - use query to make this fast
                    if let Some(p) = self.ops.iter().position(|o| o.id == *pred_id) {
                        self.ops.replace(op.obj, p, |o| o.remove_succ(op));
                    }
                }
                if let Some(pos) = self.ops.iter().position(|o| o.id == op.id) {
                    self.ops.remove(op.obj, pos);
                }
            }
            num
        } else {
            0
        }
    }

    fn next_id(&mut self) -> OpId {
        let tx = self.tx();
        OpId(tx.start_op + tx.operations.len() as u64, tx.actor)
    }

    fn insert_local_op(&mut self, op: Op, pos: usize, succ_pos: &[usize]) {
        for succ in succ_pos {
            self.ops.replace(op.obj, *succ, |old_op| {
                old_op.add_succ(&op);
            });
        }

        if !op.is_del() {
            self.ops.insert(pos, op.clone());
        }

        self.tx().operations.push(op);
    }

    fn insert_op(&mut self, op: Op) -> Op {
        let q = self.ops.search(op.obj, query::SeekOp::new(&op));

        for i in q.succ {
            self.ops.replace(op.obj, i, |old_op| old_op.add_succ(&op));
        }

        if !op.is_del() {
            self.ops.insert(q.pos, op.clone());
        }
        op
    }

    // KeysAt::()
    // LenAt::()
    // PropAt::()
    // NthAt::()

    pub fn keys(&self, obj: &ExId) -> Vec<String> {
        if let Ok(obj) = self.exid_to_obj(obj) {
            let q = self.ops.search(obj, query::Keys::new());
            q.keys.iter().map(|k| self.to_string(*k)).collect()
        } else {
            vec![]
        }
    }

    pub fn keys_at(&self, obj: &ExId, heads: &[ChangeHash]) -> Vec<String> {
        if let Ok(obj) = self.exid_to_obj(obj) {
            let clock = self.clock_at(heads);
            let q = self.ops.search(obj, query::KeysAt::new(clock));
            q.keys.iter().map(|k| self.to_string(*k)).collect()
        } else {
            vec![]
        }
    }

    pub fn length(&self, obj: &ExId) -> usize {
        if let Ok(inner_obj) = self.exid_to_obj(obj) {
            match self.ops.object_type(&inner_obj) {
                Some(ObjType::Map) | Some(ObjType::Table) => self.keys(obj).len(),
                Some(ObjType::List) | Some(ObjType::Text) => {
                    self.ops.search(inner_obj, query::Len::new()).len
                }
                None => 0,
            }
        } else {
            0
        }
    }

    pub fn length_at(&self, obj: &ExId, heads: &[ChangeHash]) -> usize {
        if let Ok(inner_obj) = self.exid_to_obj(obj) {
            let clock = self.clock_at(heads);
            match self.ops.object_type(&inner_obj) {
                Some(ObjType::Map) | Some(ObjType::Table) => self.keys_at(obj, heads).len(),
                Some(ObjType::List) | Some(ObjType::Text) => {
                    self.ops.search(inner_obj, query::LenAt::new(clock)).len
                }
                None => 0,
            }
        } else {
            0
        }
    }

    // set(obj, prop, value) - value can be scalar or objtype
    // del(obj, prop)
    // inc(obj, prop, value)
    // insert(obj, index, value)

    /// Set the value of property `P` to value `V` in object `obj`.
    ///
    /// # Returns
    ///
    /// The opid of the operation which was created, or None if this operation doesn't change the
    /// document or create a new object.
    ///
    /// # Errors
    ///
    /// This will return an error if
    /// - The object does not exist
    /// - The key is the wrong type for the object
    /// - The key does not exist in the object
    pub fn set<P: Into<Prop>, V: Into<Value>>(
        &mut self,
        obj: &ExId,
        prop: P,
        value: V,
    ) -> Result<Option<ExId>, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        let value = value.into();
        if let Some(id) = self.local_op(obj, prop.into(), value.into())? {
            Ok(Some(self.id_to_exid(id)))
        } else {
            Ok(None)
        }
    }

    fn exid_to_obj(&self, id: &ExId) -> Result<ObjId, AutomergeError> {
        match id {
            ExId::Root => Ok(ObjId::root()),
            ExId::Id(ctr, actor, idx) => {
                // do a direct get here b/c this could be foriegn and not be within the array
                // bounds
                if self.ops.m.actors.cache.get(*idx) == Some(actor) {
                    Ok(ObjId(OpId(*ctr, *idx)))
                } else {
                    // FIXME - make a real error
                    let idx = self
                        .ops
                        .m
                        .actors
                        .lookup(actor)
                        .ok_or(AutomergeError::Fail)?;
                    Ok(ObjId(OpId(*ctr, idx)))
                }
            }
        }
    }

    fn id_to_exid(&self, id: OpId) -> ExId {
        ExId::Id(id.0, self.ops.m.actors.cache[id.1].clone(), id.1)
    }

    pub fn insert<V: Into<Value>>(
        &mut self,
        obj: &ExId,
        index: usize,
        value: V,
    ) -> Result<Option<ExId>, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        let value = value.into();
        if let Some(id) = self.do_insert(obj, index, value.into())? {
            Ok(Some(self.id_to_exid(id)))
        } else {
            Ok(None)
        }
    }

    fn do_insert(
        &mut self,
        obj: ObjId,
        index: usize,
        action: OpType,
    ) -> Result<Option<OpId>, AutomergeError> {
        let id = self.next_id();

        let query = self.ops.search(obj, query::InsertNth::new(index));

        let key = query.key()?;
        let is_make = matches!(&action, OpType::Make(_));

        let op = Op {
            change: self.history.len(),
            id,
            action,
            obj,
            key,
            succ: Default::default(),
            pred: Default::default(),
            insert: true,
        };

        self.ops.insert(query.pos(), op.clone());
        self.tx().operations.push(op);

        if is_make {
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }

    pub fn inc<P: Into<Prop>>(
        &mut self,
        obj: &ExId,
        prop: P,
        value: i64,
    ) -> Result<(), AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        self.local_op(obj, prop.into(), OpType::Inc(value))?;
        Ok(())
    }

    pub fn del<P: Into<Prop>>(&mut self, obj: &ExId, prop: P) -> Result<(), AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        self.local_op(obj, prop.into(), OpType::Del)?;
        Ok(())
    }

    /// Splice new elements into the given sequence. Returns a vector of the OpIds used to insert
    /// the new elements
    pub fn splice(
        &mut self,
        obj: &ExId,
        mut pos: usize,
        del: usize,
        vals: Vec<Value>,
    ) -> Result<Vec<ExId>, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        for _ in 0..del {
            // del()
            self.local_op(obj, pos.into(), OpType::Del)?;
        }
        let mut results = Vec::new();
        for v in vals {
            // insert()
            let id = self.do_insert(obj, pos, v.into())?;
            if let Some(id) = id {
                results.push(self.id_to_exid(id));
            }
            pos += 1;
        }
        Ok(results)
    }

    pub fn splice_text(
        &mut self,
        obj: &ExId,
        pos: usize,
        del: usize,
        text: &str,
    ) -> Result<Vec<ExId>, AutomergeError> {
        let mut vals = vec![];
        for c in text.to_owned().graphemes(true) {
            vals.push(c.into());
        }
        self.splice(obj, pos, del, vals)
    }

    pub fn text(&self, obj: &ExId) -> Result<String, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        let query = self.ops.search(obj, query::ListVals::new());
        let mut buffer = String::new();
        for q in &query.ops {
            if let OpType::Set(ScalarValue::Str(s)) = &q.action {
                buffer.push_str(s);
            }
        }
        Ok(buffer)
    }

    pub fn text_at(&self, obj: &ExId, heads: &[ChangeHash]) -> Result<String, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        let clock = self.clock_at(heads);
        let query = self.ops.search(obj, query::ListValsAt::new(clock));
        let mut buffer = String::new();
        for q in &query.ops {
            if let OpType::Set(ScalarValue::Str(s)) = &q.action {
                buffer.push_str(s);
            }
        }
        Ok(buffer)
    }

    pub fn spans(&self, obj: &ExId) -> Result<Vec<query::Span>, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        let mut query = self.ops.search(obj, query::Spans::new());
        query.check_marks();
        Ok(query.spans)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn mark(
        &mut self,
        obj: &ExId,
        start: usize,
        expand_start: bool,
        end: usize,
        expand_end: bool,
        mark: &str,
        value: ScalarValue,
    ) -> Result<(), AutomergeError> {
        let obj = self.exid_to_obj(obj)?;

        self.do_insert(obj, start, OpType::mark(mark.into(), expand_start, value))?;
        self.do_insert(obj, end, OpType::MarkEnd(expand_end))?;

        /*
                let (a, b) = query.ops()?;
                let (pos, key) = a;
                let id = self.next_id();
                let op = Op {
                    change: self.history.len(),
                    id,
                    action: OpType::Mark(MarkData { name: mark.into(), expand: expand_start, value}),
                    obj,
                    key,
                    succ: Default::default(),
                    pred: Default::default(),
                    insert: true,
                };
                self.ops.insert(pos, op.clone());
                self.tx().operations.push(op);

                let (pos, key) = b;
                let id = self.next_id();
                let op = Op {
                    change: self.history.len(),
                    id,
                    action: OpType::Unmark(expand_end),
                    obj,
                    key,
                    succ: Default::default(),
                    pred: Default::default(),
                    insert: true,
                };
                self.ops.insert(pos, op.clone());
                self.tx().operations.push(op);
        */

        Ok(())
    }

    pub fn unmark(
        &self,
        _obj: &ExId,
        _start: usize,
        _end: usize,
        _inclusive: bool,
        _mark: &str,
    ) -> Result<String, AutomergeError> {
        unimplemented!()
    }

    // TODO - I need to return these OpId's here **only** to get
    // the legacy conflicts format of { [opid]: value }
    // Something better?
    pub fn value<P: Into<Prop>>(
        &self,
        obj: &ExId,
        prop: P,
    ) -> Result<Option<(Value, ExId)>, AutomergeError> {
        Ok(self.values(obj, prop.into())?.last().cloned())
    }

    pub fn value_at<P: Into<Prop>>(
        &self,
        obj: &ExId,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value, ExId)>, AutomergeError> {
        Ok(self.values_at(obj, prop, heads)?.last().cloned())
    }

    pub fn values<P: Into<Prop>>(
        &self,
        obj: &ExId,
        prop: P,
    ) -> Result<Vec<(Value, ExId)>, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        let result = match prop.into() {
            Prop::Map(p) => {
                let prop = self.ops.m.props.lookup(&p);
                if let Some(p) = prop {
                    self.ops
                        .search(obj, query::Prop::new(p))
                        .ops
                        .into_iter()
                        .map(|o| (o.value(), self.id_to_exid(o.id)))
                        .collect()
                } else {
                    vec![]
                }
            }
            Prop::Seq(n) => self
                .ops
                .search(obj, query::Nth::new(n))
                .ops
                .into_iter()
                .map(|o| (o.value(), self.id_to_exid(o.id)))
                .collect(),
        };
        Ok(result)
    }

    pub fn values_at<P: Into<Prop>>(
        &self,
        obj: &ExId,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value, ExId)>, AutomergeError> {
        let prop = prop.into();
        let obj = self.exid_to_obj(obj)?;
        let clock = self.clock_at(heads);
        let result = match prop {
            Prop::Map(p) => {
                let prop = self.ops.m.props.lookup(&p);
                if let Some(p) = prop {
                    self.ops
                        .search(obj, query::PropAt::new(p, clock))
                        .ops
                        .into_iter()
                        .map(|o| (o.value(), self.id_to_exid(o.id)))
                        .collect()
                } else {
                    vec![]
                }
            }
            Prop::Seq(n) => self
                .ops
                .search(obj, query::NthAt::new(n, clock))
                .ops
                .into_iter()
                .map(|o| (o.value(), self.id_to_exid(o.id)))
                .collect(),
        };
        Ok(result)
    }

    pub fn load(data: &[u8]) -> Result<Self, AutomergeError> {
        let changes = Change::load_document(data)?;
        let mut doc = Self::new();
        doc.apply_changes(&changes)?;
        Ok(doc)
    }

    pub fn load_incremental(&mut self, data: &[u8]) -> Result<usize, AutomergeError> {
        let changes = Change::load_document(data)?;
        let start = self.ops.len();
        self.apply_changes(&changes)?;
        let delta = self.ops.len() - start;
        Ok(delta)
    }

    pub fn apply_changes(&mut self, changes: &[Change]) -> Result<Patch, AutomergeError> {
        self.ensure_transaction_closed();
        for c in changes {
            if !self.history_index.contains_key(&c.hash) {
                if self.is_causally_ready(c) {
                    self.apply_change(c.clone());
                } else {
                    self.queue.push(c.clone());
                }
            }
        }
        while let Some(c) = self.pop_next_causally_ready_change() {
            self.apply_change(c);
        }
        Ok(Patch {})
    }

    pub fn apply_change(&mut self, change: Change) {
        self.ensure_transaction_closed();
        let ops = self.import_ops(&change, self.history.len());
        self.update_history(change);
        for op in ops {
            self.insert_op(op);
        }
    }

    fn local_op(
        &mut self,
        obj: ObjId,
        prop: Prop,
        action: OpType,
    ) -> Result<Option<OpId>, AutomergeError> {
        match prop {
            Prop::Map(s) => self.local_map_op(obj, s, action),
            Prop::Seq(n) => self.local_list_op(obj, n, action),
        }
    }

    fn local_map_op(
        &mut self,
        obj: ObjId,
        prop: String,
        action: OpType,
    ) -> Result<Option<OpId>, AutomergeError> {
        if prop.is_empty() {
            return Err(AutomergeError::EmptyStringKey);
        }

        let id = self.next_id();
        let prop = self.ops.m.props.cache(prop);
        let query = self.ops.search(obj, query::Prop::new(prop));

        if query.ops.len() == 1 && query.ops[0].is_noop(&action) {
            return Ok(None);
        }

        let is_make = matches!(&action, OpType::Make(_));

        let pred = query.ops.iter().map(|op| op.id).collect();

        let op = Op {
            change: self.history.len(),
            id,
            action,
            obj,
            key: Key::Map(prop),
            succ: Default::default(),
            pred,
            insert: false,
        };

        self.insert_local_op(op, query.pos, &query.ops_pos);

        if is_make {
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }

    fn local_list_op(
        &mut self,
        obj: ObjId,
        index: usize,
        action: OpType,
    ) -> Result<Option<OpId>, AutomergeError> {
        let query = self.ops.search(obj, query::Nth::new(index));

        let id = self.next_id();
        let pred = query.ops.iter().map(|op| op.id).collect();
        let key = query.key()?;

        if query.ops.len() == 1 && query.ops[0].is_noop(&action) {
            return Ok(None);
        }

        let is_make = matches!(&action, OpType::Make(_));

        let op = Op {
            change: self.history.len(),
            id,
            action,
            obj,
            key,
            succ: Default::default(),
            pred,
            insert: false,
        };

        self.insert_local_op(op, query.pos, &query.ops_pos);

        if is_make {
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }

    fn is_causally_ready(&self, change: &Change) -> bool {
        change
            .deps
            .iter()
            .all(|d| self.history_index.contains_key(d))
    }

    fn pop_next_causally_ready_change(&mut self) -> Option<Change> {
        let mut index = 0;
        while index < self.queue.len() {
            if self.is_causally_ready(&self.queue[index]) {
                return Some(self.queue.swap_remove(index));
            }
            index += 1;
        }
        None
    }

    fn import_ops(&mut self, change: &Change, change_id: usize) -> Vec<Op> {
        change
            .iter_ops()
            .enumerate()
            .map(|(i, c)| {
                let actor = self.ops.m.actors.cache(change.actor_id().clone());
                let id = OpId(change.start_op + i as u64, actor);
                let obj = match c.obj {
                    legacy::ObjectId::Root => ObjId::root(),
                    legacy::ObjectId::Id(id) => ObjId(OpId(id.0, self.ops.m.actors.cache(id.1))),
                };
                let pred = c
                    .pred
                    .iter()
                    .map(|i| OpId(i.0, self.ops.m.actors.cache(i.1.clone())))
                    .collect();
                let key = match &c.key {
                    legacy::Key::Map(n) => Key::Map(self.ops.m.props.cache(n.to_string())),
                    legacy::Key::Seq(legacy::ElementId::Head) => Key::Seq(types::HEAD),
                    legacy::Key::Seq(legacy::ElementId::Id(i)) => {
                        Key::Seq(ElemId(OpId(i.0, self.ops.m.actors.cache(i.1.clone()))))
                    }
                };
                Op {
                    change: change_id,
                    id,
                    action: c.action,
                    obj,
                    key,
                    succ: Default::default(),
                    pred,
                    insert: c.insert,
                }
            })
            .collect()
    }

    /// Takes all the changes in `other` which are not in `self` and applies them
    pub fn merge(&mut self, other: &mut Self) {
        // TODO: Make this fallible and figure out how to do this transactionally
        other.ensure_transaction_closed();
        let changes = self
            .get_changes_added(other)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        self.apply_changes(&changes).unwrap();
    }

    pub fn save(&mut self) -> Result<Vec<u8>, AutomergeError> {
        self.ensure_transaction_closed();
        // TODO - would be nice if I could pass an iterator instead of a collection here
        let c: Vec<_> = self.history.iter().map(|c| c.decode()).collect();
        let ops: Vec<_> = self.ops.iter().cloned().collect();
        // TODO - can we make encode_document error free
        let bytes = encode_document(
            &c,
            ops.as_slice(),
            &self.ops.m.actors,
            &self.ops.m.props.cache,
        );
        if bytes.is_ok() {
            self.saved = self.get_heads().to_vec();
        }
        bytes
    }

    // should this return an empty vec instead of None?
    pub fn save_incremental(&mut self) -> Vec<u8> {
        self.ensure_transaction_closed();
        let changes = self._get_changes(self.saved.as_slice());
        let mut bytes = vec![];
        for c in changes {
            bytes.extend(c.raw_bytes());
        }
        if !bytes.is_empty() {
            self.saved = self._get_heads().to_vec()
        }
        bytes
    }

    /// Filter the changes down to those that are not transitive dependencies of the heads.
    ///
    /// Thus a graph with these heads has not seen the remaining changes.
    pub(crate) fn filter_changes(&self, heads: &[ChangeHash], changes: &mut HashSet<ChangeHash>) {
        // Reduce the working set to find to those which we may be able to find.
        // This filters out those hashes that are successors of or concurrent with all of the
        // heads.
        // This can help in avoiding traversing the entire graph back to the roots when we try to
        // search for a hash we can know won't be found there.
        let max_head_index = heads
            .iter()
            .map(|h| self.history_index.get(h).unwrap_or(&0))
            .max()
            .unwrap_or(&0);
        let mut may_find: HashSet<ChangeHash> = changes
            .iter()
            .filter(|hash| {
                let change_index = self.history_index.get(hash).unwrap_or(&0);
                change_index <= max_head_index
            })
            .copied()
            .collect();

        if may_find.is_empty() {
            return;
        }

        let mut queue: VecDeque<_> = heads.iter().collect();
        let mut seen = HashSet::new();
        while let Some(hash) = queue.pop_front() {
            if seen.contains(hash) {
                continue;
            }
            seen.insert(hash);

            let removed = may_find.remove(hash);
            changes.remove(hash);
            if may_find.is_empty() {
                break;
            }

            for dep in self
                .history_index
                .get(hash)
                .and_then(|i| self.history.get(*i))
                .map(|c| c.deps.as_slice())
                .unwrap_or_default()
            {
                // if we just removed something from our hashes then it is likely there is more
                // down here so do a quick inspection on the children.
                // When we don't remove anything it is less likely that there is something down
                // that chain so delay it.
                if removed {
                    queue.push_front(dep);
                } else {
                    queue.push_back(dep);
                }
            }
        }
    }

    pub fn get_missing_deps(&mut self, heads: &[ChangeHash]) -> Vec<ChangeHash> {
        self.ensure_transaction_closed();
        self._get_missing_deps(heads)
    }

    pub(crate) fn _get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<ChangeHash> {
        let in_queue: HashSet<_> = self.queue.iter().map(|change| change.hash).collect();
        let mut missing = HashSet::new();

        for head in self.queue.iter().flat_map(|change| &change.deps) {
            if !self.history_index.contains_key(head) {
                missing.insert(head);
            }
        }

        for head in heads {
            if !self.history_index.contains_key(head) {
                missing.insert(head);
            }
        }

        let mut missing = missing
            .into_iter()
            .filter(|hash| !in_queue.contains(hash))
            .copied()
            .collect::<Vec<_>>();
        missing.sort();
        missing
    }

    fn get_changes_fast(&self, have_deps: &[ChangeHash]) -> Option<Vec<&Change>> {
        if have_deps.is_empty() {
            return Some(self.history.iter().collect());
        }

        let lowest_idx = have_deps
            .iter()
            .filter_map(|h| self.history_index.get(h))
            .min()?
            + 1;

        let mut missing_changes = vec![];
        let mut has_seen: HashSet<_> = have_deps.iter().collect();
        for change in &self.history[lowest_idx..] {
            let deps_seen = change.deps.iter().filter(|h| has_seen.contains(h)).count();
            if deps_seen > 0 {
                if deps_seen != change.deps.len() {
                    // future change depends on something we haven't seen - fast path cant work
                    return None;
                }
                missing_changes.push(change);
                has_seen.insert(&change.hash);
            }
        }

        // if we get to the end and there is a head we haven't seen then fast path cant work
        if self._get_heads().iter().all(|h| has_seen.contains(h)) {
            Some(missing_changes)
        } else {
            None
        }
    }

    fn get_changes_slow(&self, have_deps: &[ChangeHash]) -> Vec<&Change> {
        let mut stack: Vec<_> = have_deps.iter().collect();
        let mut has_seen = HashSet::new();
        while let Some(hash) = stack.pop() {
            if has_seen.contains(&hash) {
                continue;
            }
            if let Some(change) = self
                .history_index
                .get(hash)
                .and_then(|i| self.history.get(*i))
            {
                stack.extend(change.deps.iter());
            }
            has_seen.insert(hash);
        }
        self.history
            .iter()
            .filter(|change| !has_seen.contains(&change.hash))
            .collect()
    }

    pub fn get_last_local_change(&mut self) -> Option<&Change> {
        self.ensure_transaction_closed();
        if let Some(actor) = &self.actor {
            let actor = &self.ops.m.actors[*actor];
            return self.history.iter().rev().find(|c| c.actor_id() == actor);
        }
        None
    }

    pub fn get_changes(&mut self, have_deps: &[ChangeHash]) -> Vec<&Change> {
        self.ensure_transaction_closed();
        self._get_changes(have_deps)
    }

    pub(crate) fn _get_changes(&self, have_deps: &[ChangeHash]) -> Vec<&Change> {
        if let Some(changes) = self.get_changes_fast(have_deps) {
            changes
        } else {
            self.get_changes_slow(have_deps)
        }
    }

    fn clock_at(&self, heads: &[ChangeHash]) -> Clock {
        let mut clock = Clock::new();
        let mut seen = HashSet::new();
        let mut to_see = heads.to_vec();
        // FIXME - faster
        while let Some(hash) = to_see.pop() {
            if let Some(c) = self._get_change_by_hash(&hash) {
                for h in &c.deps {
                    if !seen.contains(h) {
                        to_see.push(*h);
                    }
                }
                let actor = self.ops.m.actors.lookup(c.actor_id()).unwrap();
                clock.include(actor, c.max_op());
                seen.insert(hash);
            }
        }
        clock
    }

    pub fn get_change_by_hash(&mut self, hash: &ChangeHash) -> Option<&Change> {
        self.ensure_transaction_closed();
        self._get_change_by_hash(hash)
    }

    pub(crate) fn _get_change_by_hash(&self, hash: &ChangeHash) -> Option<&Change> {
        self.history_index
            .get(hash)
            .and_then(|index| self.history.get(*index))
    }

    pub fn get_changes_added<'a>(&mut self, other: &'a Self) -> Vec<&'a Change> {
        self.ensure_transaction_closed();
        self._get_changes_added(other)
    }

    pub(crate) fn _get_changes_added<'a>(&self, other: &'a Self) -> Vec<&'a Change> {
        // Depth-first traversal from the heads through the dependency graph,
        // until we reach a change that is already present in other
        let mut stack: Vec<_> = other._get_heads();
        let mut seen_hashes = HashSet::new();
        let mut added_change_hashes = Vec::new();
        while let Some(hash) = stack.pop() {
            if !seen_hashes.contains(&hash) && self._get_change_by_hash(&hash).is_none() {
                seen_hashes.insert(hash);
                added_change_hashes.push(hash);
                if let Some(change) = other._get_change_by_hash(&hash) {
                    stack.extend(&change.deps);
                }
            }
        }
        // Return those changes in the reverse of the order in which the depth-first search
        // found them. This is not necessarily a topological sort, but should usually be close.
        added_change_hashes.reverse();
        added_change_hashes
            .into_iter()
            .filter_map(|h| other._get_change_by_hash(&h))
            .collect()
    }

    pub fn get_heads(&mut self) -> Vec<ChangeHash> {
        self.ensure_transaction_closed();
        self._get_heads()
    }

    pub(crate) fn _get_heads(&self) -> Vec<ChangeHash> {
        let mut deps: Vec<_> = self.deps.iter().copied().collect();
        deps.sort_unstable();
        deps
    }

    fn get_hash(&mut self, actor: usize, seq: u64) -> Result<ChangeHash, AutomergeError> {
        self.states
            .get(&actor)
            .and_then(|v| v.get(seq as usize - 1))
            .and_then(|&i| self.history.get(i))
            .map(|c| c.hash)
            .ok_or(AutomergeError::InvalidSeq(seq))
    }

    fn update_history(&mut self, change: Change) -> usize {
        self.max_op = std::cmp::max(self.max_op, change.start_op + change.len() as u64 - 1);

        self.update_deps(&change);

        let history_index = self.history.len();

        self.states
            .entry(self.ops.m.actors.cache(change.actor_id().clone()))
            .or_default()
            .push(history_index);

        self.history_index.insert(change.hash, history_index);
        self.history.push(change);

        history_index
    }

    fn update_deps(&mut self, change: &Change) {
        for d in &change.deps {
            self.deps.remove(d);
        }
        self.deps.insert(change.hash);
    }

    pub fn import(&self, s: &str) -> Result<ExId, AutomergeError> {
        if s == "_root" {
            Ok(ExId::Root)
        } else {
            let n = s
                .find('@')
                .ok_or_else(|| AutomergeError::InvalidOpId(s.to_owned()))?;
            let counter = s[0..n]
                .parse()
                .map_err(|_| AutomergeError::InvalidOpId(s.to_owned()))?;
            let actor = ActorId::from(hex::decode(&s[(n + 1)..]).unwrap());
            let actor = self
                .ops
                .m
                .actors
                .lookup(&actor)
                .ok_or_else(|| AutomergeError::InvalidOpId(s.to_owned()))?;
            Ok(ExId::Id(
                counter,
                self.ops.m.actors.cache[actor].clone(),
                actor,
            ))
        }
    }

    fn to_string<E: Exportable>(&self, id: E) -> String {
        match id.export() {
            Export::Id(id) => format!("{}@{}", id.counter(), self.ops.m.actors[id.actor()]),
            Export::Prop(index) => self.ops.m.props[index].clone(),
            Export::Special(s) => s,
        }
    }

    pub fn dump(&self) {
        log!(
            "  {:12} {:12} {:12} {} {} {}",
            "id",
            "obj",
            "key",
            "value",
            "pred",
            "succ"
        );
        for i in self.ops.iter() {
            let id = self.to_string(i.id);
            let obj = self.to_string(i.obj);
            let key = match i.key {
                Key::Map(n) => self.ops.m.props[n].clone(),
                Key::Seq(n) => self.to_string(n),
            };
            let value: String = match &i.action {
                OpType::Set(value) => format!("{}", value),
                OpType::Make(obj) => format!("make({})", obj),
                OpType::Inc(obj) => format!("inc({})", obj),
                OpType::MarkBegin(m) => format!("mark({}={})", m.name, m.value),
                OpType::MarkEnd(_) => "/mark".into(),
                OpType::Del => format!("del{}", 0),
            };
            let pred: Vec<_> = i.pred.iter().map(|id| self.to_string(*id)).collect();
            let succ: Vec<_> = i.succ.iter().map(|id| self.to_string(*id)).collect();
            log!(
                "  {:12} {:12} {:12} {} {:?} {:?}",
                id,
                obj,
                key,
                value,
                pred,
                succ
            );
        }
    }

    #[cfg(feature = "optree-visualisation")]
    pub fn visualise_optree(&self) -> String {
        self.ops.visualise()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Transaction {
    pub actor: usize,
    pub seq: u64,
    pub start_op: u64,
    pub time: i64,
    pub message: Option<String>,
    pub extra_bytes: Vec<u8>,
    pub hash: Option<ChangeHash>,
    pub deps: Vec<ChangeHash>,
    pub operations: Vec<Op>,
}

impl Default for Automerge {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;
    use std::convert::TryInto;

    #[test]
    fn insert_op() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.set_actor(ActorId::random());
        doc.set(&ROOT, "hello", "world")?;
        assert!(doc.pending_ops() == 1);
        doc.value(&ROOT, "hello")?;
        Ok(())
    }

    #[test]
    fn test_set() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        // setting a scalar value shouldn't return an opid as no object was created.
        assert!(doc.set(&ROOT, "a", 1)?.is_none());
        // setting the same value shouldn't return an opid as there is no change.
        assert!(doc.set(&ROOT, "a", 1)?.is_none());

        assert!(doc.set(&ROOT, "b", Value::map())?.is_some());
        // object already exists at b but setting a map again overwrites it so we get an opid.
        assert!(doc.set(&ROOT, "b", Value::map())?.is_some());
        Ok(())
    }

    #[test]
    fn test_list() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.set_actor(ActorId::random());
        let list_id = doc.set(&ROOT, "items", Value::list())?.unwrap();
        doc.set(&ROOT, "zzz", "zzzval")?;
        assert!(doc.value(&ROOT, "items")?.unwrap().1 == list_id);
        doc.insert(&list_id, 0, "a")?;
        doc.insert(&list_id, 0, "b")?;
        doc.insert(&list_id, 2, "c")?;
        doc.insert(&list_id, 1, "d")?;
        assert!(doc.value(&list_id, 0)?.unwrap().0 == "b".into());
        assert!(doc.value(&list_id, 1)?.unwrap().0 == "d".into());
        assert!(doc.value(&list_id, 2)?.unwrap().0 == "a".into());
        assert!(doc.value(&list_id, 3)?.unwrap().0 == "c".into());
        assert!(doc.length(&list_id) == 4);
        doc.save()?;
        Ok(())
    }

    #[test]
    fn test_del() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.set_actor(ActorId::random());
        doc.set(&ROOT, "xxx", "xxx")?;
        assert!(!doc.values(&ROOT, "xxx")?.is_empty());
        doc.del(&ROOT, "xxx")?;
        assert!(doc.values(&ROOT, "xxx")?.is_empty());
        Ok(())
    }

    #[test]
    fn test_inc() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.set(&ROOT, "counter", Value::counter(10))?;
        assert!(doc.value(&ROOT, "counter")?.unwrap().0 == Value::counter(10));
        doc.inc(&ROOT, "counter", 10)?;
        assert!(doc.value(&ROOT, "counter")?.unwrap().0 == Value::counter(20));
        doc.inc(&ROOT, "counter", -5)?;
        assert!(doc.value(&ROOT, "counter")?.unwrap().0 == Value::counter(15));
        Ok(())
    }

    #[test]
    fn test_save_incremental() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();

        doc.set(&ROOT, "foo", 1)?;

        let save1 = doc.save().unwrap();

        doc.set(&ROOT, "bar", 2)?;

        let save2 = doc.save_incremental();

        doc.set(&ROOT, "baz", 3)?;

        let save3 = doc.save_incremental();

        let mut save_a: Vec<u8> = vec![];
        save_a.extend(&save1);
        save_a.extend(&save2);
        save_a.extend(&save3);

        assert!(doc.save_incremental().is_empty());

        let save_b = doc.save().unwrap();

        assert!(save_b.len() < save_a.len());

        let mut doc_a = Automerge::load(&save_a)?;
        let mut doc_b = Automerge::load(&save_b)?;

        assert!(doc_a.values(&ROOT, "baz")? == doc_b.values(&ROOT, "baz")?);

        assert!(doc_a.save().unwrap() == doc_b.save().unwrap());

        Ok(())
    }

    #[test]
    fn test_save_text() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        let text = doc.set(&ROOT, "text", Value::text())?.unwrap();
        let heads1 = doc.commit(None, None);
        doc.splice_text(&text, 0, 0, "hello world")?;
        let heads2 = doc.commit(None, None);
        doc.splice_text(&text, 6, 0, "big bad ")?;
        let heads3 = doc.commit(None, None);

        assert!(&doc.text(&text)? == "hello big bad world");
        assert!(&doc.text_at(&text, &heads1)?.is_empty());
        assert!(&doc.text_at(&text, &heads2)? == "hello world");
        assert!(&doc.text_at(&text, &heads3)? == "hello big bad world");

        Ok(())
    }

    #[test]
    fn test_props_vals_at() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.set_actor("aaaa".try_into().unwrap());
        doc.set(&ROOT, "prop1", "val1")?;
        doc.commit(None, None);
        let heads1 = doc.get_heads();
        doc.set(&ROOT, "prop1", "val2")?;
        doc.commit(None, None);
        let heads2 = doc.get_heads();
        doc.set(&ROOT, "prop2", "val3")?;
        doc.commit(None, None);
        let heads3 = doc.get_heads();
        doc.del(&ROOT, "prop1")?;
        doc.commit(None, None);
        let heads4 = doc.get_heads();
        doc.set(&ROOT, "prop3", "val4")?;
        doc.commit(None, None);
        let heads5 = doc.get_heads();
        assert!(doc.keys_at(&ROOT, &heads1) == vec!["prop1".to_owned()]);
        assert_eq!(doc.length_at(&ROOT, &heads1), 1);
        assert!(doc.value_at(&ROOT, "prop1", &heads1)?.unwrap().0 == Value::str("val1"));
        assert!(doc.value_at(&ROOT, "prop2", &heads1)? == None);
        assert!(doc.value_at(&ROOT, "prop3", &heads1)? == None);

        assert!(doc.keys_at(&ROOT, &heads2) == vec!["prop1".to_owned()]);
        assert_eq!(doc.length_at(&ROOT, &heads2), 1);
        assert!(doc.value_at(&ROOT, "prop1", &heads2)?.unwrap().0 == Value::str("val2"));
        assert!(doc.value_at(&ROOT, "prop2", &heads2)? == None);
        assert!(doc.value_at(&ROOT, "prop3", &heads2)? == None);

        assert!(doc.keys_at(&ROOT, &heads3) == vec!["prop1".to_owned(), "prop2".to_owned()]);
        assert_eq!(doc.length_at(&ROOT, &heads3), 2);
        assert!(doc.value_at(&ROOT, "prop1", &heads3)?.unwrap().0 == Value::str("val2"));
        assert!(doc.value_at(&ROOT, "prop2", &heads3)?.unwrap().0 == Value::str("val3"));
        assert!(doc.value_at(&ROOT, "prop3", &heads3)? == None);

        assert!(doc.keys_at(&ROOT, &heads4) == vec!["prop2".to_owned()]);
        assert_eq!(doc.length_at(&ROOT, &heads4), 1);
        assert!(doc.value_at(&ROOT, "prop1", &heads4)? == None);
        assert!(doc.value_at(&ROOT, "prop2", &heads4)?.unwrap().0 == Value::str("val3"));
        assert!(doc.value_at(&ROOT, "prop3", &heads4)? == None);

        assert!(doc.keys_at(&ROOT, &heads5) == vec!["prop2".to_owned(), "prop3".to_owned()]);
        assert_eq!(doc.length_at(&ROOT, &heads5), 2);
        assert_eq!(doc.length(&ROOT), 2);
        assert!(doc.value_at(&ROOT, "prop1", &heads5)? == None);
        assert!(doc.value_at(&ROOT, "prop2", &heads5)?.unwrap().0 == Value::str("val3"));
        assert!(doc.value_at(&ROOT, "prop3", &heads5)?.unwrap().0 == Value::str("val4"));

        assert!(doc.keys_at(&ROOT, &[]).is_empty());
        assert_eq!(doc.length_at(&ROOT, &[]), 0);
        assert!(doc.value_at(&ROOT, "prop1", &[])? == None);
        assert!(doc.value_at(&ROOT, "prop2", &[])? == None);
        assert!(doc.value_at(&ROOT, "prop3", &[])? == None);
        Ok(())
    }

    #[test]
    fn test_len_at() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.set_actor("aaaa".try_into().unwrap());

        let list = doc.set(&ROOT, "list", Value::list())?.unwrap();
        let heads1 = doc.commit(None, None);

        doc.insert(&list, 0, Value::int(10))?;
        let heads2 = doc.commit(None, None);

        doc.set(&list, 0, Value::int(20))?;
        doc.insert(&list, 0, Value::int(30))?;
        let heads3 = doc.commit(None, None);

        doc.set(&list, 1, Value::int(40))?;
        doc.insert(&list, 1, Value::int(50))?;
        let heads4 = doc.commit(None, None);

        doc.del(&list, 2)?;
        let heads5 = doc.commit(None, None);

        doc.del(&list, 0)?;
        let heads6 = doc.commit(None, None);

        assert!(doc.length_at(&list, &heads1) == 0);
        assert!(doc.value_at(&list, 0, &heads1)?.is_none());

        assert!(doc.length_at(&list, &heads2) == 1);
        assert!(doc.value_at(&list, 0, &heads2)?.unwrap().0 == Value::int(10));

        assert!(doc.length_at(&list, &heads3) == 2);
        assert!(doc.value_at(&list, 0, &heads3)?.unwrap().0 == Value::int(30));
        assert!(doc.value_at(&list, 1, &heads3)?.unwrap().0 == Value::int(20));

        assert!(doc.length_at(&list, &heads4) == 3);
        assert!(doc.value_at(&list, 0, &heads4)?.unwrap().0 == Value::int(30));
        assert!(doc.value_at(&list, 1, &heads4)?.unwrap().0 == Value::int(50));
        assert!(doc.value_at(&list, 2, &heads4)?.unwrap().0 == Value::int(40));

        assert!(doc.length_at(&list, &heads5) == 2);
        assert!(doc.value_at(&list, 0, &heads5)?.unwrap().0 == Value::int(30));
        assert!(doc.value_at(&list, 1, &heads5)?.unwrap().0 == Value::int(50));

        assert!(doc.length_at(&list, &heads6) == 1);
        assert!(doc.length(&list) == 1);
        assert!(doc.value_at(&list, 0, &heads6)?.unwrap().0 == Value::int(50));

        Ok(())
    }
}
