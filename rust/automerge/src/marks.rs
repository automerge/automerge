use smol_str::SmolStr;

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

use crate::op_set2::{MarkData, Op, OpType};
use crate::types::{Clock, ObjType, OpId, SmallHashMap};
use crate::value::ScalarValue;

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

impl Mark {
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
        m.insert(self.name, self.value);
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

use std::collections::btree_map;

#[derive(Debug, Clone, Default)]
pub struct MarkSetIter<'a> {
    set: Option<btree_map::Iter<'a, SmolStr, ScalarValue>>,
}

impl<'a> MarkSetIter<'a> {
    fn new(set: &'a MarkSet) -> Self {
        Self {
            set: Some(set.marks.iter()),
        }
    }
}

impl<'a> Iterator for MarkSetIter<'a> {
    type Item = (&'a str, &'a ScalarValue);

    fn next(&mut self) -> Option<Self::Item> {
        self.set
            .as_mut()?
            .next()
            .map(|(name, value)| (name.as_str(), value))
    }
}

impl MarkSet {
    pub fn iter(&self) -> MarkSetIter<'_> {
        MarkSetIter::new(self)
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

    pub(crate) fn insert(&mut self, name: SmolStr, value: ScalarValue) {
        self.marks.insert(name, value);
    }

    fn remove(&mut self, name: &SmolStr) {
        self.marks.remove(name);
    }

    pub fn is_empty(&self) -> bool {
        self.inner().is_empty()
    }

    pub(crate) fn from_query_state(q: &RichTextQueryState<'_>) -> Option<Arc<Self>> {
        let mut marks = MarkStateMachine::default();
        for (id, mark_data) in q.iter() {
            marks.mark_begin(*id, mark_data.clone());
        }
        marks.current().cloned()
    }

    /// Return this MarkSet without any marks which have a value of Null, i.e.
    /// marks which have been removed.
    pub(crate) fn without_unmarks(self) -> Self {
        // FIXME - do I need this clone?
        let mut marks = self.marks.clone();
        marks.retain(|_, value| !matches!(value, ScalarValue::Null));
        MarkSet { marks }
    }

    // Returns a wrapper for comparing two MarkSets while ignoring marks that have been deleted.
    //
    // Marksets track which marks have been deleted by storing them with a value of `ScalarValue::Null`.
    // When we want to compare the marks in two MarkSets, we often want to ignore these deleted marks so
    // that we can focus on whether the user visible state has changed.
    //
    // ## Example
    //
    // ```rust
    // let markset1 = MarkSet::from_iter(vec![
    //     ("bold".to_string(), ScalarValue::String("true".to_string())),
    //     ("italic".to_string(), ScalarValue::Null),
    // ]);
    // let markset2 = MarkSet::from_iter(vec![
    //     ("bold".to_string(), ScalarValue::String("true".to_string())),
    //     ("underlined".to_string(), ScalarValue::Null),
    // ]);
    // assert_eq!(markset1.non_deleted_marks(), markset2.non_deleted_marks());
    // ```
    pub fn non_deleted_marks(&self) -> NonDeletedMarks<'_> {
        NonDeletedMarks(self)
    }
}

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

        if Self::mark_above(&self.state, index, mark.clone()).is_none() {
            if let Some(below) = Self::mark_below(&mut self.state, index, mark.clone()) {
                if below.value != mark.value {
                    Arc::make_mut(&mut self.current)
                        .insert(SmolStr::from(mark.name.as_ref()), mark.value.to_owned());
                    result = true
                }
            } else {
                // nothing above or below
                Arc::make_mut(&mut self.current)
                    .insert(SmolStr::from(mark.name.as_ref()), mark.value.to_owned());
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

        if Self::mark_above(&self.state, index, mark.clone()).is_none() {
            match Self::mark_below(&mut self.state, index, mark.clone()) {
                Some(below) if below.value == mark.value => {}
                Some(below) => {
                    Arc::make_mut(&mut self.current)
                        .insert(SmolStr::from(below.name), below.value.into());
                    result = true;
                }
                None => {
                    Arc::make_mut(&mut self.current).remove(&SmolStr::from(mark.name.as_ref()));
                    result = true;
                }
            }
        }

        result
    }

    fn find(&self, target: OpId) -> Result<usize, usize> {
        self.state.binary_search_by(|probe| probe.0.cmp(&target))
    }

    fn mark_above(
        state: &[(OpId, MarkData<'a>)],
        index: usize,
        mark: MarkData<'a>,
    ) -> Option<MarkData<'a>> {
        Some(
            state[index..]
                .iter()
                .find(|(_, m)| m.name == mark.name)?
                .1
                .clone(),
        )
    }

    fn mark_below(
        state: &mut [(OpId, MarkData<'a>)],
        index: usize,
        mark: MarkData<'a>,
    ) -> Option<MarkData<'a>> {
        Some(
            state[0..index]
                .iter_mut()
                .filter(|(_, m)| m.name == mark.name)
                .next_back()?
                .1
                .clone(),
        )
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct OldMarkData {
    pub name: SmolStr,
    pub value: ScalarValue,
}

impl Display for MarkData<'_> {
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
    pub(crate) map: SmallHashMap<OpId, MarkData<'a>>,
    pub(crate) block: Option<OpId>,
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
}

// Useful for comparing MarkSets while ignoring marks that have been deleted (i.e. have a value of Null).
//
// Returned by [`MarkSet::non_deleted_marks()`]
#[derive(Debug)]
pub struct NonDeletedMarks<'a>(&'a MarkSet);

impl PartialEq for NonDeletedMarks<'_> {
    fn eq(&self, other: &Self) -> bool {
        let us_in_them = self.0.marks.iter().all(|(name, value)| {
            matches!(value, ScalarValue::Null) || other.0.marks.get(name) == Some(value)
        });
        let them_in_us = other.0.marks.iter().all(|(name, value)| {
            matches!(value, ScalarValue::Null) || self.0.marks.get(name) == Some(value)
        });
        us_in_them && them_in_us
    }
}

impl NonDeletedMarks<'_> {
    pub fn len(&self) -> usize {
        self.0
            .marks
            .iter()
            .filter(|(_, v)| !matches!(v, ScalarValue::Null))
            .count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Configure the expand flag used when creating marks in [`update_spans`](crate::transaction::Transactable::update_spans)
#[derive(Default, Debug, Clone)]
pub struct UpdateSpansConfig {
    /// The expand flag to use when the mark does not have a flag set in Self::per_mark_expands.
    pub default_expand: ExpandMark,
    /// A map of mark names to the expand flag to use for that mark
    pub per_mark_expands: HashMap<String, ExpandMark>,
}

impl UpdateSpansConfig {
    pub fn with_default_expand(mut self, expand: ExpandMark) -> Self {
        self.default_expand = expand;
        self
    }

    pub fn with_mark_expand<S: AsRef<str>>(mut self, mark_name: S, expand: ExpandMark) -> Self {
        self.per_mark_expands
            .insert(mark_name.as_ref().to_string(), expand);
        self
    }
}
