//! Many fragments flowing into one manifold.
//!
//! Each fragment's rows are already in final document order restricted to
//! that fragment, so the [`Intake`] merges them one register at a time.
//! Objects come in id order and map keys in key order. Sequence elements
//! follow the insert tree: the next element is the highest-id waiting
//! child of the deepest element on the path from the head to the last
//! element taken.
//!
//! A register's rows from every fragment are gathered before any is
//! released, which is where fragments are reconciled: a row two fragments
//! both carry is released once, and a row that names another fragment's
//! row as its pred becomes a successor of that row instead. A register
//! that spans fragments leaves a fix for each row its fragment indexed
//! differently, which the merge writes in place of the fragment's own.

use super::index::ObjIndex;
use super::manifold::{Inlet, SrcPos};
use super::op_iter::WidthIter;
use super::OpSet;
use crate::op_set2::types::Action;
use crate::storage::bundle::{BundleKey, CleanRun, ManifoldOp, ManifoldOps, TailRun};
use crate::types::{ObjId, ObjType, OpId};
use crate::MalformedBundle;

use rustc_hash::FxHashMap;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};

/// A row the merged document indexes differently from its fragment.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FixRow {
    pub(crate) src: u32,
    pub(crate) row: usize,
    /// the row's first succ entry, and how many it has, in its fragment
    pub(crate) sub: usize,
    pub(crate) sub_len: usize,
    /// whether its successors are replaced too
    pub(crate) succ: bool,
}

/// The fixed rows' values, appended as registers close: one entry per
/// [`FixRow`] for the index columns, and per row with `succ` for the
/// count and successor columns.
#[derive(Default)]
struct FixEncoders {
    visible: hexane::ColumnEncoder<'static, bool>,
    top: Prefixed<bool>,
    text: Prefixed<Option<u32>>,
    succ_count: Prefixed<u32>,
    succ_actor: hexane::ColumnEncoder<'static, crate::op_set2::types::ActorIdx>,
    succ_ctr: hexane::DeltaColumnEncoder<u32>,
    inc: hexane::ColumnEncoder<'static, Option<i64>>,
}

type Prefixed<T> =
    hexane::ColumnEncoder<'static, T, hexane::Leb128, hexane::prefix::PrefixWeightFn<T>>;

/// What [`Intake::finish`] leaves the merge to write.
pub(crate) struct Fixes {
    /// in release order, so ascending within each source
    pub(crate) rows: Vec<FixRow>,
    pub(crate) visible: hexane::Column<bool>,
    pub(crate) top: hexane::PrefixColumn<bool>,
    pub(crate) text: hexane::PrefixColumn<Option<u32>>,
    pub(crate) succ_count: hexane::PrefixColumn<u32>,
    /// logical actor indexes
    pub(crate) succ_actor: hexane::Column<crate::op_set2::types::ActorIdx>,
    pub(crate) succ_ctr: hexane::DeltaColumn<u32>,
    pub(crate) inc: hexane::Column<Option<i64>>,
}

struct Source<'a> {
    reader: ManifoldOps<'a>,
    head: Option<Row<'a>>,
    frag: &'a OpSet,
    width: WidthIter<'a>,
}

struct Row<'a> {
    op: ManifoldOp<'a>,
    /// member successors
    succ: Vec<OpId>,
}

impl<'a> Source<'a> {
    fn advance(&mut self) {
        self.head = (self.reader.pos < self.reader.len).then(|| {
            let mut succ = Vec::new();
            let op = self.reader.next_op_with_succ(&mut succ);
            Row { op, succ }
        });
    }
}

struct Stream {
    src: u32,
    /// rows of `src` still to stream
    left: usize,
}

const HINT_OVERSHOT: &str = "op position hint overshot its target row";

/// A gathered row and what the other fragments add to it.
struct GroupRow<'a> {
    src: u32,
    op: ManifoldOp<'a>,
    succ: Vec<OpId>,
    /// successors other fragments give it: `(id, inc)`
    gained: Vec<(OpId, Option<i64>)>,
    /// another fragment's copy of the same row is released instead
    dropped: bool,
    /// alive as its own fragment indexed it
    alive_in_frag: bool,
}

/// Feeds the manifold from several fragments at once. See the module docs.
pub(crate) struct Intake<'a> {
    sources: Vec<Source<'a>>,
    out: VecDeque<ManifoldOp<'a>>,
    /// the document's objects and every fragment's
    objs: &'a ObjIndex,
    /// a source releasing the rest of an object no other source has rows
    /// in, straight from its reader
    stream: Option<Stream>,

    /// sources whose head is in a later object, by that object
    later: BinaryHeap<Reverse<(ObjId, u32)>>,
    obj: Option<ObjId>,
    seq: bool,
    /// sources whose head is in `obj`
    in_obj: usize,
    /// map objects: sources by their head's key
    keys: BinaryHeap<Reverse<(&'a str, u32)>>,
    /// sequence objects: the path from the head to the last element, with
    /// the rank of each element's insert row in the object
    path: Vec<(OpId, usize)>,
    /// rows of `obj` released so far: the rank of the next one
    obj_rows: usize,
    /// insert heads by anchor: `(id, source)`
    waiting: FxHashMap<OpId, Vec<(OpId, u32)>>,
    /// update heads by element
    updates: FxHashMap<OpId, Vec<u32>>,
    /// the source of the last released row, preferred among duplicates
    /// so runs stay long
    last_src: u32,
    fix_rows: Vec<FixRow>,
    fix_vals: FixEncoders,
    error: Option<MalformedBundle>,
}

impl<'a> Intake<'a> {
    /// `sources` pairs each fragment's reader with its indexed op set;
    /// `objs` knows the objects of the document being applied to and
    /// every object the fragments create.
    pub(crate) fn new(sources: Vec<(ManifoldOps<'a>, &'a OpSet)>, objs: &'a ObjIndex) -> Self {
        let mut intake = Intake {
            sources: sources
                .into_iter()
                .enumerate()
                .map(|(i, (reader, frag))| Source {
                    reader: reader.with_src(i as u32),
                    head: None,
                    frag,
                    width: frag.width_iter(),
                })
                .collect(),
            fix_rows: Vec::new(),
            fix_vals: FixEncoders::default(),
            out: VecDeque::new(),
            objs,
            stream: None,
            later: BinaryHeap::new(),
            obj: None,
            seq: false,
            in_obj: 0,
            keys: BinaryHeap::new(),
            path: Vec::new(),
            obj_rows: 0,
            waiting: FxHashMap::default(),
            updates: FxHashMap::default(),
            last_src: 0,
            error: None,
        };
        for i in 0..intake.sources.len() {
            intake.sources[i].advance();
            intake.register(i as u32);
        }
        intake.fill();
        intake
    }

    /// The rows whose index or successors the merge must write itself, or
    /// why the fragments do not fit together.
    pub(crate) fn finish(self) -> Result<Fixes, MalformedBundle> {
        let v = self.fix_vals;
        Ok(Fixes {
            rows: self.fix_rows,
            visible: v.visible.into_column(),
            top: hexane::PrefixColumn::from_column(v.top.into_column()),
            text: hexane::PrefixColumn::from_column(v.text.into_column()),
            succ_count: hexane::PrefixColumn::from_column(v.succ_count.into_column()),
            succ_actor: v.succ_actor.into_column(),
            succ_ctr: v.succ_ctr.into_column(),
            inc: v.inc.into_column(),
        })
    }

    fn fail(&mut self, why: &'static str) {
        self.error.get_or_insert(MalformedBundle::Invalid(why));
        self.out.clear();
    }

    fn object_type(&self, obj: &ObjId) -> Option<ObjType> {
        self.objs.object_type(obj)
    }

    fn register(&mut self, src: u32) {
        let Some(row) = &self.sources[src as usize].head else {
            return;
        };
        let op = &row.op;
        if Some(op.obj) != self.obj {
            self.later.push(Reverse((op.obj, src)));
            return;
        }
        self.in_obj += 1;
        match &op.key {
            BundleKey::Map(k) => self.keys.push(Reverse((k, src))),
            BundleKey::Seq(e) if op.insert => {
                self.waiting.entry(e.0).or_default().push((op.id, src))
            }
            BundleKey::Seq(e) => self.updates.entry(e.0).or_default().push(src),
        }
    }

    /// Take `src`'s rows while they belong to the register `key`. In a
    /// sequence, `ranks` are those of the insert's anchor and of the
    /// element: floors for the rows' hints.
    fn take_rows(
        &mut self,
        src: u32,
        rows: &mut Vec<GroupRow<'a>>,
        key: &BundleKey<'_>,
        ranks: Option<(usize, usize)>,
    ) {
        let obj = self.obj;
        let s = &mut self.sources[src as usize];
        let mut first = true;
        let mut overshot = false;
        while let Some(head) = &s.head {
            let op = &head.op;
            let same = Some(op.obj) == obj
                && match (key, &op.key) {
                    (BundleKey::Map(k), BundleKey::Map(h)) => k == h,
                    // the element's own insert, then its updates
                    (BundleKey::Seq(e), BundleKey::Seq(h)) => {
                        (first && op.insert && op.id == e.0) || (!op.insert && h == e)
                    }
                    _ => false,
                };
            if !same {
                break;
            }
            if let (Some((anchor, elem)), Some(hint)) = (ranks, op.hint) {
                let rank = if op.insert { anchor } else { elem };
                overshot |= hint as usize > rank;
            }
            first = false;
            let row = s.head.take().unwrap();
            let alive_in_frag = row.op.alive;
            rows.push(GroupRow {
                src,
                op: row.op,
                succ: row.succ,
                gained: Vec::new(),
                dropped: false,
                alive_in_frag,
            });
            s.advance();
        }
        self.in_obj -= 1;
        self.register(src);
        if overshot {
            self.fail(HINT_OVERSHOT);
        }
    }

    fn fill(&mut self) {
        while self.out.is_empty() && self.stream.is_none() && self.error.is_none() {
            if self.in_obj == 0 && !self.next_object() {
                return;
            }
            if self.in_obj == 1 {
                self.stream_rest_of_object();
                continue;
            }
            let mut rows = Vec::new();
            if self.seq {
                let Some((elem, srcs, anchor_rank)) = self.next_element() else {
                    return self.fail("sequence rows out of document order");
                };
                let key = BundleKey::Seq(crate::types::ElemId(elem));
                let ranks = Some((anchor_rank, self.obj_rows));
                let updaters = self.updates.remove(&elem).unwrap_or_default();
                let alone = (srcs.len() == 1 && updaters.is_empty()).then_some(srcs[0]);
                for src in srcs {
                    self.take_rows(src, &mut rows, &key, ranks);
                }
                for src in updaters {
                    self.take_rows(src, &mut rows, &key, ranks);
                }
                self.path.push((elem, self.obj_rows));
                self.release(rows);
                if let Some(src) = alone {
                    self.stream_while_unrivalled(src);
                }
                continue;
            } else {
                let Reverse((key, src)) = self.keys.pop().unwrap();
                let mut srcs = vec![src];
                while let Some(Reverse((k, s))) = self.keys.peek() {
                    if *k != key {
                        break;
                    }
                    srcs.push(*s);
                    self.keys.pop();
                }
                let key = BundleKey::Map(key);
                for src in srcs {
                    self.take_rows(src, &mut rows, &key, None);
                }
            }
            self.release(rows);
        }
    }

    /// The one source with rows left in this object releases them all:
    /// its head through `out`, the rest straight from its reader.
    fn stream_rest_of_object(&mut self) {
        let src = match (
            self.keys.pop(),
            self.waiting.drain().next(),
            self.updates.drain().next(),
        ) {
            (Some(Reverse((_, src))), _, _) => src,
            (_, Some((_, mut w)), _) => w.pop().unwrap().1,
            (_, _, Some((_, mut u))) => u.pop().unwrap(),
            _ => unreachable!("one source is in the object"),
        };
        self.in_obj = 0;
        self.last_src = src;
        let s = &mut self.sources[src as usize];
        let head = s.head.take().unwrap();
        let left = s.reader.same_obj_run(s.reader.remaining());
        if head.op.action != Action::Delete || !head.op.preds.is_empty() {
            self.out.push_back(head.op);
        }
        if left > 0 {
            self.stream = Some(Stream { src, left });
        } else {
            self.end_stream(src);
        }
    }

    /// After an element only `src` had rows for, `src` keeps releasing
    /// straight from its reader for as long as no other source could come
    /// next: up to the first row another source's head waits on, or that
    /// competes with one.
    fn stream_while_unrivalled(&mut self, src: u32) {
        let Some(head) = &self.sources[src as usize].head else {
            return;
        };
        if Some(head.op.obj) != self.obj || !head.op.insert {
            return;
        }
        let (id, anchor) = (head.op.id, head.op.key.elemid().expect("a sequence key").0);
        let hint = head.op.hint;
        // the head is waiting on `anchor`; with no rival it is next
        let rivals = self.waiting.get(&anchor).is_some_and(|w| w.len() > 1);
        let Some(depth) = self.clear_path_to(anchor) else {
            return;
        };
        if rivals || self.waiting.contains_key(&id) || self.updates.contains_key(&id) {
            return;
        }
        if hint.is_some_and(|h| h as usize > self.path[depth].1) {
            return self.fail(HINT_OVERSHOT);
        }

        let w = self.waiting.get_mut(&anchor).expect("head is registered");
        w.retain(|(_, s)| *s != src);
        if w.is_empty() {
            self.waiting.remove(&anchor);
        }
        self.in_obj -= 1;
        self.last_src = src;
        self.path.truncate(depth + 1);
        self.path.push((id, self.obj_rows));
        self.obj_rows += 1;

        let reader = &self.sources[src as usize].reader;
        let max = reader.same_obj_run(reader.remaining());
        let mut cursor = reader.elem_cursor();
        let mut left = 0;
        let mut overshot = false;
        while left < max {
            let mut next = cursor.clone();
            let row = next.next_row();
            if row.insert {
                if self.waiting.contains_key(&row.key)
                    || self.waiting.contains_key(&row.id)
                    || self.updates.contains_key(&row.id)
                {
                    break;
                }
                let Some(depth) = self.clear_path_to(row.key) else {
                    break;
                };
                overshot |= row.hint.is_some_and(|h| h as usize > self.path[depth].1);
                self.path.truncate(depth + 1);
                self.path.push((row.id, self.obj_rows));
            } else {
                // an update belongs to the element just streamed, or waits
                let (top, rank) = *self.path.last().expect("the head is on the path");
                if row.key != top || self.updates.contains_key(&row.key) {
                    break;
                }
                overshot |= row.hint.is_some_and(|h| h as usize > rank);
            }
            self.obj_rows += 1;
            cursor = next;
            left += 1;
        }
        if overshot {
            return self.fail(HINT_OVERSHOT);
        }
        let s = &mut self.sources[src as usize];
        let head = s.head.take().unwrap();
        self.out.push_back(head.op);
        if left > 0 {
            self.stream = Some(Stream { src, left });
        } else {
            self.end_stream(src);
        }
    }

    /// The depth of `elem` on the path, if every element above it has no
    /// head waiting on it, so popping back to `elem` skips nothing.
    fn clear_path_to(&self, elem: OpId) -> Option<usize> {
        for depth in (0..self.path.len()).rev() {
            let e = self.path[depth].0;
            if e == elem {
                return Some(depth);
            }
            if self.waiting.contains_key(&e) {
                return None;
            }
        }
        None
    }

    fn end_stream(&mut self, src: u32) {
        self.stream = None;
        self.sources[src as usize].advance();
        self.register(src);
    }

    /// The streaming source's reader, when nothing is queued ahead of it.
    fn streaming(&self) -> Option<&ManifoldOps<'a>> {
        match &self.stream {
            Some(st) if self.out.is_empty() => Some(&self.sources[st.src as usize].reader),
            _ => None,
        }
    }

    fn streamed(&mut self, n: usize) {
        let st = self.stream.as_mut().expect("streaming");
        st.left -= n;
        if st.left == 0 {
            let src = st.src;
            self.end_stream(src);
            self.fill();
        }
    }

    fn next_object(&mut self) -> bool {
        if !self.waiting.is_empty() || !self.updates.is_empty() {
            self.fail("sequence rows name elements no fragment has");
            return false;
        }
        let Some(Reverse((obj, _))) = self.later.peek().copied() else {
            return false;
        };
        let Some(obj_type) = self.object_type(&obj) else {
            self.fail("rows in an object no fragment creates");
            return false;
        };
        self.obj = Some(obj);
        self.seq = obj_type.is_sequence();
        self.path.clear();
        self.path.push((OpId::new(0, 0), 0));
        self.obj_rows = 0;
        while let Some(Reverse((o, src))) = self.later.peek().copied() {
            if o != obj {
                break;
            }
            self.later.pop();
            self.register(src);
        }
        true
    }

    /// Also the sources whose head inserts the element, and the rank of
    /// its anchor.
    fn next_element(&mut self) -> Option<(OpId, Vec<u32>, usize)> {
        loop {
            let (top, rank) = *self.path.last()?;
            if let Some(children) = self.waiting.get_mut(&top) {
                let elem = children.iter().map(|(id, _)| *id).max().unwrap();
                let mut srcs = Vec::new();
                children.retain(|(id, src)| {
                    if *id == elem {
                        srcs.push(*src);
                        false
                    } else {
                        true
                    }
                });
                if children.is_empty() {
                    self.waiting.remove(&top);
                }
                return Some((elem, srcs, rank));
            }
            self.path.pop();
        }
    }

    fn release(&mut self, mut rows: Vec<GroupRow<'a>>) {
        if rows.is_empty() {
            return;
        }
        rows.sort_by_key(|r| r.op.id);
        let spans = rows.iter().any(|r| r.src != rows[0].src);

        if spans {
            self.drop_duplicates(&mut rows);
            self.link_preds(&mut rows);
        }

        let patched = rows.iter().any(|r| !r.gained.is_empty());
        if spans || patched {
            self.fix_register(&rows);
        }

        for r in rows {
            if r.dropped {
                continue;
            }
            if r.op.action == Action::Delete && r.op.preds.is_empty() {
                continue;
            }
            self.last_src = r.src;
            if r.op.action != Action::Delete {
                self.obj_rows += 1;
            }
            self.out.push_back(r.op);
        }
    }

    /// Keep one copy of each row, carrying every copy's successors.
    fn drop_duplicates(&self, rows: &mut [GroupRow<'a>]) {
        let mut i = 0;
        while i < rows.len() {
            let mut j = i + 1;
            while j < rows.len() && rows[j].op.id == rows[i].op.id {
                j += 1;
            }
            if j - i > 1 {
                let keep = (i..j).find(|k| rows[*k].src == self.last_src).unwrap_or(i);
                let mut extra: Vec<OpId> = Vec::new();
                for (k, row) in rows.iter_mut().enumerate().take(j).skip(i) {
                    if k != keep {
                        row.dropped = true;
                        extra.extend(row.succ.iter().copied());
                    }
                }
                for id in extra {
                    // an increment successor is a row of this register;
                    // anything else that succeeds a row is a delete
                    let inc = find_row(rows, id).and_then(|r| rows[r].op.inc);
                    rows[keep].gained.push((id, inc));
                }
            }
            i = j;
        }
    }

    /// Turn each pred naming another fragment's row into a successor of
    /// that row; preds left over name document rows.
    fn link_preds(&self, rows: &mut [GroupRow<'a>]) {
        for i in 0..rows.len() {
            if rows[i].dropped || rows[i].op.preds.is_empty() {
                continue;
            }
            let id = rows[i].op.id;
            let inc = rows[i].op.inc;
            let mut preds = std::mem::take(&mut rows[i].op.preds);
            preds.retain(|pred| match find_row(rows, *pred) {
                Some(t) => {
                    rows[t].gained.push((id, inc));
                    false
                }
                None => true,
            });
            rows[i].op.preds = preds;
        }
        for r in rows.iter_mut() {
            if r.gained.is_empty() {
                continue;
            }
            r.succ.sort_unstable();
            r.gained.sort_unstable_by_key(|(id, _)| *id);
            r.gained.dedup_by_key(|(id, _)| *id);
            let succ = &r.succ;
            r.gained.retain(|(id, _)| succ.binary_search(id).is_err());
            if r.gained
                .iter()
                .any(|(_, inc)| inc.is_none() || !r.op.counter)
            {
                r.op.alive = false;
            }
        }
    }

    /// Index the register's rows as the merged document has them: `top`
    /// on its last visible row, whichever fragment that came from, and
    /// each row's successors from every fragment. Rows already indexed so
    /// by their own fragment, which elected over just its own rows, are
    /// left alone.
    fn fix_register(&mut self, rows: &[GroupRow<'a>]) {
        let shows = |r: &GroupRow<'_>| !matches!(r.op.action, Action::Increment | Action::Delete);
        let landing = |r: &GroupRow<'_>| !r.dropped && r.op.action != Action::Delete;
        let visible = |r: &GroupRow<'_>| landing(r) && r.op.alive && shows(r);
        let last_vis = rows.iter().rposition(visible);
        // each source's own election: its last row it saw as visible
        let mut frag_top: FxHashMap<u32, usize> = FxHashMap::default();
        for (i, r) in rows.iter().enumerate() {
            if r.alive_in_frag && shows(r) {
                frag_top.insert(r.src, i);
            }
        }
        for (i, r) in rows.iter().enumerate() {
            if !landing(r) {
                continue;
            }
            let was_visible = r.alive_in_frag && shows(r);
            let was_top = frag_top.get(&r.src) == Some(&i);
            let top = Some(i) == last_vis;
            if visible(r) == was_visible && top == was_top && r.gained.is_empty() {
                continue;
            }
            let s = &mut self.sources[r.src as usize];
            let v = &mut self.fix_vals;
            v.visible.append(visible(r));
            v.top.append(top);
            v.text.append(top.then(|| s.width.seek_to(r.op.at.row)));
            let succ = !r.gained.is_empty();
            if succ {
                // both lists ascend: merge them straight into the encoders
                let mut incs = s
                    .frag
                    .inc_index()
                    .iter_range(r.op.at.sub..r.op.at.sub + r.op.sub_len);
                let mut own = r.succ.iter().peekable();
                let mut gained = r.gained.iter().peekable();
                let mut n = 0;
                loop {
                    let next = match (own.peek(), gained.peek()) {
                        (Some(o), Some((g, _))) if *o < g => None,
                        (Some(_), None) => None,
                        (_, Some(_)) => gained
                            .next()
                            .map(|(id, inc)| (*id, inc.filter(|_| r.op.counter))),
                        (None, None) => break,
                    };
                    let (id, inc) = match next {
                        Some(g) => g,
                        None => (*own.next().unwrap(), incs.next().flatten()),
                    };
                    v.succ_actor.append(id.actoridx());
                    v.succ_ctr.append(id.counter() as u32);
                    v.inc.append(inc);
                    n += 1;
                }
                v.succ_count.append(n);
            }
            self.fix_rows.push(FixRow {
                src: r.src,
                row: r.op.at.row,
                sub: r.op.at.sub,
                sub_len: r.op.sub_len,
                succ,
            });
        }
    }
}

impl<'a> Inlet<'a> for Intake<'a> {
    fn is_empty(&self) -> bool {
        self.out.is_empty() && self.stream.is_none()
    }

    fn remaining(&self) -> usize {
        match &self.stream {
            Some(st) if self.out.is_empty() => st.left,
            _ => self.out.len(),
        }
    }

    fn position(&self) -> SrcPos {
        match (self.out.front(), &self.stream) {
            (Some(op), _) => op.at,
            (None, Some(st)) => self.sources[st.src as usize].reader.position(),
            (None, None) => SrcPos::default(),
        }
    }

    fn next_op(&mut self) -> ManifoldOp<'a> {
        if let Some(op) = self.out.pop_front() {
            if self.out.is_empty() && self.stream.is_none() {
                self.fill();
            }
            return op;
        }
        let src = self
            .stream
            .as_ref()
            .expect("next_op on an empty intake")
            .src;
        let op = self.sources[src as usize].reader.next_op();
        self.streamed(1);
        op
    }

    fn same_obj_run(&self, max: usize) -> usize {
        self.streaming()
            .map_or(0, |r| r.same_obj_run(max.min(self.remaining())))
    }

    fn pred_free_run(&self) -> usize {
        self.streaming().map_or(0, |r| r.pred_free_run())
    }

    fn clean_insert_run(&self) -> usize {
        self.streaming()
            .map_or(0, |r| r.clean_insert_run().min(self.remaining()))
    }

    fn make_free_run(&self, max: usize) -> usize {
        self.streaming()
            .map_or(0, |r| r.make_free_run(max.min(self.remaining())))
    }

    fn skip_clean(&mut self, n: usize) -> CleanRun {
        let src = self.stream.as_ref().expect("streaming").src;
        let run = self.sources[src as usize].reader.skip_clean(n);
        self.streamed(n);
        run
    }

    fn skip_tail(&mut self, n: usize) -> TailRun {
        let src = self.stream.as_ref().expect("streaming").src;
        let run = self.sources[src as usize].reader.skip_tail(n);
        self.streamed(n);
        run
    }

    fn consume_rest(&mut self) -> (usize, usize) {
        let st = self.stream.as_ref().expect("streaming");
        let (src, n) = (st.src, st.left);
        let reader = &mut self.sources[src as usize].reader;
        let start = reader.position();
        let end = if n == reader.remaining() {
            reader.consume_rest()
        } else {
            let run = reader.skip_tail(n);
            (start.sub + run.sub, start.val + run.val)
        };
        self.streamed(n);
        end
    }
}

/// The released copy of row `id` among `rows`, which are sorted by id.
fn find_row(rows: &[GroupRow<'_>], id: OpId) -> Option<usize> {
    let first = rows.partition_point(|r| r.op.id < id);
    (first..rows.len())
        .take_while(|k| rows[*k].op.id == id)
        .find(|k| !rows[*k].dropped)
}
