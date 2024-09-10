use fxhash::FxBuildHasher;
use smol_str::SmolStr;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

use crate::op_set2;
use crate::op_set2::{MarkData, Op, OpType};
use crate::types::Clock;
use crate::types::{ObjType, OpId};
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
pub struct Mark {
    pub start: usize,
    pub end: usize,
    pub name: SmolStr,
    pub value: ScalarValue,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OldMark<'a> {
    pub start: usize,
    pub end: usize,
    pub(crate) data: Cow<'a, OldMarkData>,
}

impl<'a> OldMark<'a> {
    pub(crate) fn len(&self) -> usize {
        self.end - self.start
    }

    pub(crate) fn into_mark_set(self) -> Arc<MarkSet> {
        let mut m = MarkSet::default();
        let data = self.data.into_owned();
        //let name = SmolStr::from(self.name);
        //m.insert(data.name, data.value);
        m.insert(data.name, data.value);
        Arc::new(m)
    }

    pub(crate) fn from_data(start: usize, end: usize, data: &'a OldMarkData) -> OldMark<'a> {
        OldMark {
            data: Cow::Borrowed(data),
            start,
            end,
        }
    }
}

impl Mark {
    pub(crate) fn data(&self) -> MarkData<'_> {
        MarkData {
            name: self.name.as_str(),
            value: (&self.value).into(),
        }
    }

    pub(crate) fn old_data(&self) -> OldMarkData {
        OldMarkData {
            name: SmolStr::from(self.name.as_str()),
            value: self.value.clone(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.end - self.start
    }
    pub(crate) fn into_mark_set(self) -> Arc<MarkSet> {
        let mut m = MarkSet::default();
        //let data = self.data.into_owned();
        let name = SmolStr::from(self.name);
        //m.insert(data.name, data.value);
        m.insert(name, self.value);
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
    pub(crate) fn into_iter(self) -> impl Iterator<Item = Mark> {
        self.marks.into_iter().flat_map(|(name, items)| {
            items.into_iter().map(move |i| {
                Mark::new(name.to_string(), i.value.clone(), i.index, i.index + i.len)
            })
        })
    }

    pub(crate) fn into_iter_no_unmark(self) -> impl Iterator<Item = Mark> {
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
    pub(crate) fn new(name: &str, value: op_set2::ScalarValue<'_>) -> Arc<MarkSet> {
        let mut m = MarkSet::default();
        m.insert(SmolStr::from(name), value.into_owned());
        Arc::new(m)
    }

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

    pub(crate) fn from_query_state(q: &RichTextQueryState<'_>) -> Option<Arc<Self>> {
        let mut marks = MarkStateMachine::default();
        for (id, mark_data) in q.iter() {
            marks.mark_begin(*id, *mark_data);
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

impl Mark {
    pub fn new<V: Into<ScalarValue>>(name: String, value: V, start: usize, end: usize) -> Mark {
        Mark {
            name: SmolStr::from(&name),
            value: value.into(),
            start,
            end,
        }
    }

    pub fn name(&self) -> &str {
        &self.name //.as_str()
    }

    pub fn value(&self) -> &ScalarValue {
        &self.value
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct MarkStateMachine<'a> {
    // would this make more sense as a BTree<OpId, MarkData<'a>>?
    state: Vec<(OpId, MarkData<'a>)>,
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

    pub(crate) fn process(&mut self, opid: OpId, action: OpType<'a>) -> bool {
        match action {
            OpType::MarkBegin(_, data) => self.mark_begin(opid, data),
            OpType::MarkEnd(_) => self.mark_end(opid),
            _ => false,
        }
    }

    pub(crate) fn mark_begin(&mut self, id: OpId, mark: MarkData<'a>) -> bool {
        let mut result = false;

        let index = match self.find(id).err() {
            Some(index) => index,
            None => return false,
        };

        if Self::mark_above(&self.state, index, mark).is_none() {
            if let Some(below) = Self::mark_below(&mut self.state, index, mark) {
                if below.value != mark.value {
                    Arc::make_mut(&mut self.current)
                        .insert(SmolStr::from(mark.name), mark.value.into());
                    result = true
                }
            } else {
                // nothing above or below
                Arc::make_mut(&mut self.current)
                    .insert(SmolStr::from(mark.name), mark.value.into());
                result = true
            }
        }

        self.state.insert(index, (id, mark));

        result
    }

    pub(crate) fn mark_end(&mut self, id: OpId) -> bool {
        let mut result = false;
        let index = match self.find(id.prev()).ok() {
            Some(index) => index,
            None => return false,
        };

        let mark = self.state.remove(index).1;

        if Self::mark_above(&self.state, index, mark).is_none() {
            match Self::mark_below(&mut self.state, index, mark) {
                Some(below) if below.value == mark.value => {}
                Some(below) => {
                    Arc::make_mut(&mut self.current)
                        .insert(SmolStr::from(below.name), below.value.into());
                    result = true;
                }
                None => {
                    Arc::make_mut(&mut self.current).remove(&SmolStr::from(mark.name));
                    result = true;
                }
            }
        }

        result
    }

    fn find(&self, target: OpId) -> Result<usize, usize> {
        self.state.binary_search_by(|probe| probe.0.cmp(&target))
    }

    pub(crate) fn covered(&self, id: OpId, name: &str) -> bool {
        let index = self
            .state
            .binary_search_by(|probe| probe.0.cmp(&id))
            .ok()
            .unwrap_or(0);
        self.state[index..]
            .iter()
            .find(|(i, m)| *i > id && m.name == name)
            .is_some()
    }

    fn mark_above(
        state: &[(OpId, MarkData<'a>)],
        index: usize,
        mark: MarkData<'a>,
    ) -> Option<MarkData<'a>> {
        Some(state[index..].iter().find(|(_, m)| m.name == mark.name)?.1)
    }

    fn mark_below<'b>(
        state: &mut [(OpId, MarkData<'a>)],
        index: usize,
        mark: MarkData<'a>,
    ) -> Option<MarkData<'a>> {
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
pub struct OldMarkData {
    pub name: SmolStr,
    pub value: ScalarValue,
}

impl<'a> Display for MarkData<'a> {
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

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct RichTextQueryState<'a> {
    map: HashMap<OpId, MarkData<'a>, FxBuildHasher>,
    block: Option<OpId>,
}

impl<'a> RichTextQueryState<'a> {
    pub(crate) fn process(&mut self, op: Op<'a>, clock: Option<&Clock>) {
        if !(clock.map(|c| c.covers(&op.id)).unwrap_or(true)) {
            // if the op is not visible in the current clock
            // we can ignore it
            return;
        }
        match op.action() {
            OpType::MarkBegin(_, data) => {
                self.map.insert(op.id, data);
            }
            OpType::MarkEnd(_) => {
                self.map.remove(&op.id.prev());
            }
            OpType::Make(ObjType::Map) => {
                self.block = Some(op.id);
            }
            _ => {}
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&OpId, &MarkData<'a>)> {
        self.map.iter()
    }

    pub(crate) fn insert(&mut self, op: OpId, data: MarkData<'a>) {
        self.map.insert(op, data);
    }

    pub(crate) fn remove(&mut self, op: &OpId) {
        self.map.remove(op);
    }
}
