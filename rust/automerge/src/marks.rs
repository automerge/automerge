use smol_str::SmolStr;
use std::fmt;
use std::fmt::Display;

use crate::types::ListEncoding;
use crate::types::{ObjId, OpId};
use crate::value::ScalarValue;
use crate::Automerge;

#[derive(Debug, Clone, PartialEq)]
pub struct Mark {
    pub start: usize,
    pub end: usize,
    pub expand_left: bool,
    pub expand_right: bool,
    pub name: smol_str::SmolStr,
    pub value: ScalarValue,
}

impl Default for Mark {
    fn default() -> Self {
        Mark {
            name: "".into(),
            value: ScalarValue::Null,
            start: 0,
            end: 0,
            expand_left: false,
            expand_right: false,
        }
    }
}

impl Mark {
    pub fn new<V: Into<ScalarValue>>(
        name: String,
        value: V,
        start: usize,
        end: usize,
        expand_left: bool,
        expand_right: bool,
    ) -> Self {
        Mark {
            name: name.into(),
            value: value.into(),
            start,
            end,
            expand_left,
            expand_right,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Span {
    pub(crate) start: OpId,
    pub(crate) end: OpId,
    pub(crate) data: MarkData,
}

impl Span {
    fn new(start: OpId, end: OpId, data: &MarkData) -> Self {
        Self {
            start,
            end,
            data: data.clone(),
        }
    }

    pub(crate) fn into_mark(self, obj: &ObjId, doc: &Automerge, encoding: ListEncoding) -> Mark {
        let start_index = doc
            .ops()
            .search(
                obj,
                crate::query::ElemIdPos::new(self.start.into(), encoding),
            )
            .index()
            .unwrap();
        let end_index = doc
            .ops()
            .search(obj, crate::query::ElemIdPos::new(self.end.into(), encoding))
            .index()
            .unwrap();
        Mark::new(
            self.data.name.into(),
            self.data.value,
            start_index,
            end_index,
            false,
            false,
        )
    }
}

pub(crate) struct MarkStateMachine {
    state: Vec<(OpId, Span)>,
    pub(crate) spans: Vec<Span>,
}

impl MarkStateMachine {
    pub(crate) fn new() -> Self {
        MarkStateMachine {
            state: Default::default(),
            spans: Default::default(),
        }
    }

    pub(crate) fn mark_begin(&mut self, id: OpId, data: &MarkData, doc: &Automerge) {
        let mut start = id;
        let end = id;

        let index = self.find(id, doc).err();
        if index.is_none() {
            return;
        }
        let index = index.unwrap();

        if let Some(above) = Self::mark_above(&self.state, index, data) {
            if above.data.value == data.value {
                start = above.start;
            }
        } else if let Some(below) = Self::mark_below(&mut self.state, index, data) {
            if below.data.value == data.value {
                start = below.start;
            } else {
                self.spans.push(Span::new(below.start, id, &below.data));
            }
        }

        let entry = (id, Span::new(start, end, data));
        self.state.insert(index, entry);
    }

    pub(crate) fn mark_end(&mut self, id: OpId, doc: &Automerge) {
        let index = self.find(id.prev(), doc).ok();
        if index.is_none() {
            return;
        }
        let index = index.unwrap();

        let mut span = self.state.remove(index).1;
        span.end = id;

        if Self::mark_above(&self.state, index, &span.data).is_none() {
            match Self::mark_below(&mut self.state, index, &span.data) {
                Some(below) if below.data.value == span.data.value => {}
                Some(below) => {
                    below.start = id;
                    self.spans.push(span);
                }
                None => {
                    self.spans.push(span);
                }
            }
        }
    }

    fn find(&self, target: OpId, doc: &Automerge) -> Result<usize, usize> {
        let metadata = &doc.ops().m;
        self.state
            .binary_search_by(|probe| metadata.lamport_cmp(probe.0, target))
    }

    fn mark_above<'a>(
        state: &'a [(OpId, Span)],
        index: usize,
        data: &MarkData,
    ) -> Option<&'a Span> {
        Some(
            &state[index..]
                .iter()
                .find(|(_, span)| span.data.name == data.name)?
                .1,
        )
    }

    fn mark_below<'a>(
        state: &'a mut [(OpId, Span)],
        index: usize,
        data: &MarkData,
    ) -> Option<&'a mut Span> {
        Some(
            &mut state[0..index]
                .iter_mut()
                .filter(|(_, span)| span.data.name == data.name)
                .last()?
                .1,
        )
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct MarkData {
    pub name: SmolStr,
    pub value: ScalarValue,
    pub expand: bool,
}

impl Display for MarkData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "value={} expand={}", self.value, self.value)
    }
}
