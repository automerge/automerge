use smol_str::SmolStr;
use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

use crate::op_tree::OpSetData;
use crate::query::RichTextQueryState;
use crate::types::{OpId, OpType};
use crate::value::ScalarValue;
use std::borrow::Cow;
use std::collections::BTreeMap;

/// Marks let you store out-of-bound information about sequences.
///
/// The motivating use-case is rich text editing, see <https://www.inkandswitch.com/peritext/>.
/// Each position in the sequence can be affected by only one Mark of the same "name".
/// If multiple collaborators have set marks with the same name but different values
/// in overlapping ranges, automerge will chose a consistent (but arbitrary) value
/// when reading marks from the doc.
#[derive(Debug, Clone, PartialEq)]
pub struct Mark<'a> {
    pub start: usize,
    pub end: usize,
    pub(crate) data: Cow<'a, MarkData>,
}

impl<'a> Mark<'a> {
    pub(crate) fn len(&self) -> usize {
        self.end - self.start
    }
    pub(crate) fn into_mark_set(self) -> Arc<MarkSet> {
        let mut m = MarkSet::default();
        let data = self.data.into_owned();
        m.insert(data.name, data.value);
        Arc::new(m)
    }
}

#[derive(Debug, PartialEq, Clone)]
struct MarkAccItem {
    index: usize,
    len: usize,
    value: ScalarValue,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct MarkAccumulator {
    marks: BTreeMap<SmolStr, Vec<MarkAccItem>>,
}

impl MarkAccumulator {
    pub(crate) fn into_iter(self) -> impl Iterator<Item = Mark<'static>> {
        self.marks.into_iter().flat_map(|(name, items)| {
            items.into_iter().map(move |i| {
                Mark::new(name.to_string(), i.value.clone(), i.index, i.index + i.len)
            })
        })
    }

    pub(crate) fn into_iter_no_unmark(self) -> impl Iterator<Item = Mark<'static>> {
        self.marks.into_iter().flat_map(|(name, items)| {
            items
                .into_iter()
                .filter(|i| !i.value.is_null())
                .map(move |i| {
                    Mark::new(name.to_string(), i.value.clone(), i.index, i.index + i.len)
                })
        })
    }

    pub(crate) fn add(&mut self, index: usize, len: usize, other: &MarkSet) {
        for (name, value) in other.marks.iter() {
            let entry = self.marks.entry(name.clone()).or_default();
            if let Some(last) = entry.last_mut() {
                if &last.value == value && last.index + last.len == index {
                    last.len += len;
                    continue;
                }
            }
            entry.push(MarkAccItem {
                index,
                len,
                value: value.clone(),
            })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct MarkSet {
    marks: BTreeMap<SmolStr, ScalarValue>,
}

impl MarkSet {
    pub fn iter(&self) -> impl Iterator<Item = (&str, &ScalarValue)> {
        self.marks
            .iter()
            .map(|(name, value)| (name.as_str(), value))
    }

    pub fn num_marks(&self) -> usize {
        self.marks.len()
    }

    fn inner(&self) -> &BTreeMap<SmolStr, ScalarValue> {
        &self.marks
    }

    pub fn len(&self) -> usize {
        self.marks.len()
    }

    fn insert(&mut self, name: SmolStr, value: ScalarValue) {
        self.marks.insert(name, value);
    }

    fn remove(&mut self, name: &SmolStr) {
        self.marks.remove(name);
    }

    pub fn is_empty(&self) -> bool {
        self.inner().is_empty()
    }

    pub(crate) fn diff(&self, other: &Self) -> Self {
        let mut diff = BTreeMap::default();
        for (name, value) in self.marks.iter() {
            match other.marks.get(name) {
                Some(v) if v != value => {
                    diff.insert(name.clone(), v.clone());
                }
                None => {
                    diff.insert(name.clone(), ScalarValue::Null);
                }
                _ => {}
            }
        }
        for (name, value) in other.marks.iter() {
            if !self.marks.contains_key(name) {
                diff.insert(name.clone(), value.clone());
            }
        }
        MarkSet { marks: diff }
    }

    pub(crate) fn from_query_state(
        q: &RichTextQueryState<'_>,
        osd: &OpSetData,
    ) -> Option<Arc<Self>> {
        let mut marks = MarkStateMachine::default();
        for (id, mark_data) in q.iter() {
            marks.mark_begin(*id, mark_data, osd);
        }
        marks.current().cloned()
    }
}

// FromIterator implementation for an iterator of (String, ScalarValue) tuples
impl std::iter::FromIterator<(String, ScalarValue)> for MarkSet {
    fn from_iter<I: IntoIterator<Item = (String, ScalarValue)>>(iter: I) -> Self {
        let mut marks = BTreeMap::new();
        for (name, value) in iter {
            marks.insert(name.into(), value);
        }
        MarkSet { marks }
    }
}

impl<'a> Mark<'a> {
    pub fn new<V: Into<ScalarValue>>(
        name: String,
        value: V,
        start: usize,
        end: usize,
    ) -> Mark<'static> {
        Mark {
            data: Cow::Owned(MarkData {
                name: name.into(),
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

    pub fn name(&self) -> &str {
        self.data.name.as_str()
    }

    pub fn value(&self) -> &ScalarValue {
        &self.data.value
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct MarkStateMachine<'a> {
    state: Vec<(OpId, &'a MarkData)>,
    current: Arc<MarkSet>,
}

impl<'a> MarkStateMachine<'a> {
    pub(crate) fn current(&self) -> Option<&Arc<MarkSet>> {
        if self.current.is_empty() {
            None
        } else {
            Some(&self.current)
        }
    }

    pub(crate) fn process(&mut self, opid: OpId, action: &'a OpType, osd: &OpSetData) -> bool {
        match action {
            OpType::MarkBegin(_, data) => self.mark_begin(opid, data, osd),
            OpType::MarkEnd(_) => self.mark_end(opid, osd),
            _ => false,
        }
    }

    pub(crate) fn mark_begin(&mut self, id: OpId, mark: &'a MarkData, osd: &OpSetData) -> bool {
        let mut result = false;

        let index = match self.find(id, osd).err() {
            Some(index) => index,
            None => return false,
        };

        if Self::mark_above(&self.state, index, mark).is_none() {
            if let Some(below) = Self::mark_below(&mut self.state, index, mark) {
                if below.value != mark.value {
                    Arc::make_mut(&mut self.current).insert(mark.name.clone(), mark.value.clone());
                    result = true
                }
            } else {
                // nothing above or below
                Arc::make_mut(&mut self.current).insert(mark.name.clone(), mark.value.clone());
                result = true
            }
        }

        self.state.insert(index, (id, mark));

        result
    }

    pub(crate) fn mark_end(&mut self, id: OpId, osd: &OpSetData) -> bool {
        let mut result = false;
        let index = match self.find(id.prev(), osd).ok() {
            Some(index) => index,
            None => return false,
        };

        let mark = self.state.remove(index).1;

        if Self::mark_above(&self.state, index, mark).is_none() {
            match Self::mark_below(&mut self.state, index, mark) {
                Some(below) if below.value == mark.value => {}
                Some(below) => {
                    Arc::make_mut(&mut self.current)
                        .insert(below.name.clone(), below.value.clone());
                    result = true;
                }
                None => {
                    Arc::make_mut(&mut self.current).remove(&mark.name);
                    result = true;
                }
            }
        }

        result
    }

    fn find(&self, target: OpId, osd: &OpSetData) -> Result<usize, usize> {
        self.state
            .binary_search_by(|probe| osd.lamport_cmp(probe.0, target))
    }

    fn mark_above<'b>(
        state: &'b [(OpId, &'a MarkData)],
        index: usize,
        mark: &MarkData,
    ) -> Option<&'b MarkData> {
        Some(state[index..].iter().find(|(_, m)| m.name == mark.name)?.1)
    }

    fn mark_below<'b>(
        state: &'b mut [(OpId, &'a MarkData)],
        index: usize,
        mark: &MarkData,
    ) -> Option<&'b MarkData> {
        Some(
            state[0..index]
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
}

impl Display for MarkData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "name={} value={}", self.name, self.value)
    }
}

/// ExpandMark allows you to decide whether new text inserted at the start/end of your
/// mark should also inherit the mark.
/// See <https://www.inkandswitch.com/peritext/> for details and
/// suggestions of which value to use for which operations when building a rich text editor.
#[derive(PartialEq, Debug, Clone, Copy)]
pub enum ExpandMark {
    Before,
    After,
    Both,
    None,
}

impl Default for ExpandMark {
    fn default() -> Self {
        Self::After
    }
}

impl ExpandMark {
    pub fn from(before: bool, after: bool) -> Self {
        match (before, after) {
            (true, true) => Self::Both,
            (false, true) => Self::After,
            (true, false) => Self::Before,
            (false, false) => Self::None,
        }
    }
    pub fn before(&self) -> bool {
        matches!(self, Self::Before | Self::Both)
    }
    pub fn after(&self) -> bool {
        matches!(self, Self::After | Self::Both)
    }
}
