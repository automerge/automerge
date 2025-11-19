use crate::op_set2::columns::Columns;
use crate::op_set2::op_set::{MarkIndexBuilder, MarkIndexColumn};
use crate::op_set2::{ChangeOp, Op, OpBuilder};
use crate::types::{ObjId, ObjType, OpId, SequenceType, TextEncoding};
use hexane::{BooleanCursor, ColumnData, IntCursor, UIntCursor};
use std::collections::HashMap;

// TODO : this could be faster and use less memory if
// hexane::Encoder was used here instead of Vec<>

pub(crate) struct IndexBuilder {
    counters: HashMap<OpId, Vec<(usize, usize)>>,
    succ: Vec<u32>,
    top: Vec<bool>,
    widths: Vec<u64>,
    incs: Vec<Option<i64>>,
    marks: Vec<Option<MarkIndexBuilder>>,
    obj_info: ObjIndex,
    last_flush: usize,
    text_encoding: TextEncoding,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct ObjIndex(pub(crate) HashMap<OpId, ObjInfo>);

impl ObjIndex {
    pub(crate) fn object_type(&self, obj: &ObjId) -> Option<ObjType> {
        if obj.is_root() {
            Some(ObjType::Map)
        } else {
            self.0.get(&obj.0).map(|p| p.obj_type)
        }
    }

    pub(crate) fn object_parent(&self, obj: &ObjId) -> Option<ObjId> {
        if obj.is_root() {
            None
        } else {
            self.0.get(&obj.0).map(|p| p.parent)
        }
    }

    pub(crate) fn insert(&mut self, id: OpId, obj_info: ObjInfo) {
        self.0.insert(id, obj_info);
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct ObjInfo {
    pub(crate) parent: ObjId,
    pub(crate) obj_type: ObjType,
}

impl ObjInfo {
    pub(crate) fn with_new_actor(self, idx: usize) -> Self {
        Self {
            parent: self.parent.with_new_actor(idx),
            obj_type: self.obj_type,
        }
    }

    pub(crate) fn without_actor(self, idx: usize) -> Option<Self> {
        Some(Self {
            parent: self.parent.without_actor(idx)?,
            obj_type: self.obj_type,
        })
    }
}

impl Op<'_> {
    pub(crate) fn obj_info(&self) -> Option<ObjInfo> {
        let obj_type = ObjType::try_from(self.action).ok()?;
        let parent = self.obj;
        Some(ObjInfo { parent, obj_type })
    }
}

impl ChangeOp {
    pub(crate) fn obj_info(&self) -> Option<ObjInfo> {
        self.bld.obj_info()
    }
}

impl OpBuilder<'_> {
    pub(crate) fn obj_info(&self) -> Option<ObjInfo> {
        let obj_type = ObjType::try_from(self.action).ok()?;
        let parent = self.obj;
        Some(ObjInfo { parent, obj_type })
    }
}

pub(crate) struct Indexes {
    pub(crate) text: ColumnData<UIntCursor>,
    pub(crate) top: ColumnData<BooleanCursor>,
    pub(crate) visible: ColumnData<BooleanCursor>,
    pub(crate) inc: ColumnData<IntCursor>,
    pub(crate) mark: MarkIndexColumn,
    pub(crate) obj_info: ObjIndex,
}

impl IndexBuilder {
    pub(crate) fn new(cols: &Columns, encoding: TextEncoding) -> Self {
        Self {
            counters: HashMap::new(),
            succ: Vec::with_capacity(cols.len()),
            top: Vec::with_capacity(cols.len()),
            widths: Vec::with_capacity(cols.len()),
            incs: Vec::with_capacity(cols.sub_len()),
            marks: Vec::with_capacity(cols.len()),
            obj_info: ObjIndex::default(),
            last_flush: 0,
            text_encoding: encoding,
        }
    }

    pub(crate) fn flush(&mut self) {
        let len = self.succ.len();
        for (delta, succ) in self.succ[self.last_flush..].iter().rev().enumerate() {
            if *succ == 0 {
                self.top[len - delta - 1] = true;
                break;
            }
        }
        self.last_flush = len;
    }
    pub(crate) fn process_op(&mut self, op: &Op<'_>) {
        self.marks.push(op.mark_index());

        self.succ.push(vis_num(op));
        self.top.push(false);

        self.widths
            .push(op.width(SequenceType::Text, self.text_encoding) as u64);

        let count = self.counters.remove(&op.id);

        if let Some(i) = op.get_increment_value() {
            for (succ_idx, op_idx) in count.into_iter().flatten() {
                self.incs[succ_idx] = Some(i);
                self.succ[op_idx] -= 1;
            }
        }

        if let Some(obj_info) = op.obj_info() {
            self.obj_info.insert(op.id, obj_info);
        }
    }

    pub(crate) fn process_succ(&mut self, op_is_counter: bool, id: OpId) {
        if op_is_counter {
            self.counters
                .entry(id)
                .or_default()
                .push((self.incs.len(), self.succ.len() - 1));
        }
        self.incs.push(None); // will update later
    }

    pub(crate) fn finish(mut self) -> Indexes {
        self.flush();

        let text = self
            .widths
            .iter()
            .zip(self.succ.iter())
            .map(|(w, t)| if *t == 0 { Some(*w) } else { None })
            .collect();

        let visible = self.succ.iter().map(|&n| n == 0).collect();

        let top = self.top.iter().collect();

        let mut inc = ColumnData::new();
        inc.splice(0, 0, self.incs);

        let mut mark = MarkIndexColumn::new();
        mark.splice(0, 0, self.marks);

        let obj_info = self.obj_info;

        Indexes {
            text,
            top,
            visible,
            inc,
            mark,
            obj_info,
        }
    }
}

fn vis_num(op: &Op<'_>) -> u32 {
    if op.is_inc() {
        u32::MAX
    } else {
        op.succ().len() as u32
    }
}
