use smol_str::SmolStr;
use std::fmt;
use std::fmt::Display;

use crate::types::OpId;
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

    pub(crate) fn from_data(start: usize, end: usize, data: &MarkData) -> Self {
        Mark {
            name: data.name.clone(),
            value: data.value.clone(),
            start,
            end,
            expand_left: false,
            expand_right: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct MarkStateMachine {
    state: Vec<(OpId, Mark)>,
}

impl MarkStateMachine {
    pub(crate) fn mark_begin(
        &mut self,
        id: OpId,
        pos: usize,
        data: &MarkData,
        doc: &Automerge,
    ) -> Option<Mark> {
        let mut result = None;
        let index = self.find(id, doc).err()?;

        let mut mark = Mark::from_data(pos, pos, data);

        if let Some(above) = Self::mark_above(&self.state, index, &mark) {
            if above.value == mark.value {
                mark.start = above.start;
            }
        } else if let Some(below) = Self::mark_below(&mut self.state, index, &mark) {
            if below.value == mark.value {
                mark.start = below.start;
            } else {
                let mut m = below.clone();
                m.end = pos;
                result = Some(m);
            }
        }

        self.state.insert(index, (id, mark));

        result
    }

    pub(crate) fn mark_end(&mut self, id: OpId, pos: usize, doc: &Automerge) -> Option<Mark> {
        let mut result = None;
        let index = self.find(id.prev(), doc).ok()?;

        let mut mark = self.state.remove(index).1;
        mark.end = pos;

        if Self::mark_above(&self.state, index, &mark).is_none() {
            match Self::mark_below(&mut self.state, index, &mark) {
                Some(below) if below.value == mark.value => {}
                Some(below) => {
                    below.start = pos;
                    result = Some(mark.clone());
                }
                None => {
                    result = Some(mark.clone());
                }
            }
        }

        result
    }

    fn find(&self, target: OpId, doc: &Automerge) -> Result<usize, usize> {
        let metadata = &doc.ops().m;
        self.state
            .binary_search_by(|probe| metadata.lamport_cmp(probe.0, target))
    }

    fn mark_above<'a>(state: &'a [(OpId, Mark)], index: usize, mark: &Mark) -> Option<&'a Mark> {
        Some(&state[index..].iter().find(|(_, m)| m.name == mark.name)?.1)
    }

    fn mark_below<'a>(
        state: &'a mut [(OpId, Mark)],
        index: usize,
        mark: &Mark,
    ) -> Option<&'a mut Mark> {
        Some(
            &mut state[0..index]
                .iter_mut()
                .filter(|(_, m)| m.name == mark.name)
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
