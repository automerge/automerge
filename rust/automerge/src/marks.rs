use smol_str::SmolStr;
use std::fmt;
use std::fmt::Display;

use crate::types::OpId;
use crate::value::ScalarValue;
use crate::Automerge;
use std::borrow::Cow;

#[derive(Debug, Clone, PartialEq)]
pub struct Mark<'a> {
    pub start: usize,
    pub end: usize,
    pub(crate) data: Cow<'a, MarkData>,
}

impl<'a> Mark<'a> {
    pub fn new<V: Into<ScalarValue>>(
        key: String,
        value: V,
        start: usize,
        end: usize,
    ) -> Mark<'static> {
        Mark {
            data: Cow::Owned(MarkData {
                key: key.into(),
                value: value.into(),
            }),
            start,
            end,
        }
    }

    pub(crate) fn from_data(start: usize, end: usize, data: &MarkData) -> Mark<'_> {
        Mark {
            data: Cow::Borrowed(data),
            start,
            end,
        }
    }

    pub fn into_owned(self) -> Mark<'static> {
        Mark {
            data: Cow::Owned(self.data.into_owned()),
            start: self.start,
            end: self.end,
        }
    }

    pub fn key(&self) -> &str {
        self.data.key.as_str()
    }

    pub fn value(&self) -> &ScalarValue {
        &self.data.value
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct MarkStateMachine<'a> {
    state: Vec<(OpId, Mark<'a>)>,
}

impl<'a> MarkStateMachine<'a> {
    pub(crate) fn mark_begin(
        &mut self,
        id: OpId,
        pos: usize,
        data: &'a MarkData,
        doc: &Automerge,
    ) -> Option<Mark<'a>> {
        let mut result = None;
        let index = self.find(id, doc).err()?;

        let mut mark = Mark::from_data(pos, pos, data);

        if let Some(above) = Self::mark_above(&self.state, index, &mark) {
            if above.value() == mark.value() {
                mark.start = above.start;
            }
        } else if let Some(below) = Self::mark_below(&mut self.state, index, &mark) {
            if below.value() == mark.value() {
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

    pub(crate) fn mark_end(&mut self, id: OpId, pos: usize, doc: &Automerge) -> Option<Mark<'a>> {
        let mut result = None;
        let index = self.find(id.prev(), doc).ok()?;

        let mut mark = self.state.remove(index).1;
        mark.end = pos;

        if Self::mark_above(&self.state, index, &mark).is_none() {
            match Self::mark_below(&mut self.state, index, &mark) {
                Some(below) if below.value() == mark.value() => {}
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

    fn mark_above<'b>(
        state: &'b [(OpId, Mark<'a>)],
        index: usize,
        mark: &Mark<'a>,
    ) -> Option<&'b Mark<'a>> {
        Some(
            &state[index..]
                .iter()
                .find(|(_, m)| m.key() == mark.key())?
                .1,
        )
    }

    fn mark_below<'b>(
        state: &'b mut [(OpId, Mark<'a>)],
        index: usize,
        mark: &Mark<'a>,
    ) -> Option<&'b mut Mark<'a>> {
        Some(
            &mut state[0..index]
                .iter_mut()
                .filter(|(_, m)| m.data.key == mark.data.key)
                .last()?
                .1,
        )
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct MarkData {
    pub key: SmolStr,
    pub value: ScalarValue,
}

impl Display for MarkData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "key={} value={}", self.key, self.value)
    }
}
