use crate::op_set2::op_set::{MarkIndexColumn, MarkIndexValue};
use crate::op_set2::{Op, OpSet};
use crate::types::{ListEncoding, OpId};
use packer::{ColumnData, IntCursor, UIntCursor};
use std::collections::HashMap;

// TODO : this could be faster and use less memory if
// packer::Encoder was used here instead of Vec<>

pub(crate) struct IndexBuilder {
    counters: HashMap<OpId, Vec<usize>>,
    widths: Vec<u64>,
    incs: Vec<Option<i64>>,
    marks: Vec<Option<MarkIndexValue>>,
}

pub(crate) struct Indexes {
    pub(crate) text: ColumnData<UIntCursor>,
    pub(crate) inc: ColumnData<IntCursor>,
    pub(crate) mark: MarkIndexColumn,
}

impl IndexBuilder {
    pub(crate) fn new(op_set: &OpSet) -> Self {
        Self {
            counters: HashMap::new(),
            widths: Vec::with_capacity(op_set.len()),
            incs: Vec::with_capacity(op_set.sub_len()),
            marks: Vec::with_capacity(op_set.len()),
        }
    }

    #[inline(never)]
    pub(crate) fn process_op(&mut self, op: &Op<'_>) {
        self.marks.push(op.mark_index());

        if op.succ().len() == 0 {
            self.widths.push(op.width(ListEncoding::Text) as u64);
        } else {
            self.widths.push(0);
        }

        let count = self.counters.remove(&op.id);

        if let Some(i) = op.get_increment_value() {
            for idx in count.iter().flatten() {
                self.incs[*idx] = Some(i);
            }
        }
    }

    #[inline(never)]
    pub(crate) fn process_succ(&mut self, op_is_counter: bool, id: OpId) {
        if op_is_counter {
            self.counters.entry(id).or_default().push(self.incs.len());
        }
        self.incs.push(None); // will update later
    }

    #[inline(never)]
    pub(crate) fn finish(self) -> Indexes {
        let mut text = ColumnData::new();
        text.splice(0, 0, self.widths);

        let mut inc = ColumnData::new();
        inc.splice(0, 0, self.incs);

        let mut mark = MarkIndexColumn::new();
        mark.splice(0, 0, self.marks);

        Indexes { text, inc, mark }
    }
}
