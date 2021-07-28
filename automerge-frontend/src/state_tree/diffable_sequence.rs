use std::collections::HashMap;

use amp::{ActorId, OpId};
use automerge_protocol as amp;
use smol_str::SmolStr;

use super::{MultiGrapheme, MultiValue, StateTreeValue};
use crate::error::InvalidPatch;

pub(crate) trait DiffableValue: Sized {
    fn take(&mut self) -> Self;

    fn check_construct(
        opid: &amp::OpId,
        diff: &amp::Diff,
        parent_object_id: &amp::ObjectId,
    ) -> Result<(), InvalidPatch>;

    fn construct(opid: amp::OpId, diff: amp::Diff) -> Self;

    fn check_diff(
        &self,
        opid: &amp::OpId,
        diff: &amp::Diff,
        parent_object_id: &amp::ObjectId,
    ) -> Result<(), InvalidPatch>;

    fn apply_diff(&mut self, opid: amp::OpId, diff: amp::Diff);

    fn apply_diff_iter<I>(&mut self, diff: &mut I)
    where
        I: Iterator<Item = (amp::OpId, amp::Diff)>;

    fn default_opid(&self) -> amp::OpId;

    fn only_for_opid(&self, opid: amp::OpId) -> Option<Self>;

    fn add_values_from(&mut self, other: Self);
}

impl DiffableValue for MultiGrapheme {
    fn take(&mut self) -> Self {
        Self {
            winning_value: (amp::OpId(0, ActorId::from(&[][..])), SmolStr::default()),
            conflicts: HashMap::default(),
        }
    }

    fn check_construct(
        opid: &amp::OpId,
        diff: &amp::Diff,
        parent_object_id: &amp::ObjectId,
    ) -> Result<(), InvalidPatch> {
        MultiGrapheme::check_new_from_diff(opid, diff, parent_object_id)
    }

    fn construct(opid: amp::OpId, diff: amp::Diff) -> Self {
        MultiGrapheme::new_from_diff(opid, diff)
    }

    fn check_diff(
        &self,
        opid: &amp::OpId,
        diff: &amp::Diff,
        parent_object_id: &amp::ObjectId,
    ) -> Result<(), InvalidPatch> {
        MultiGrapheme::check_diff(self, opid, diff, parent_object_id)
    }

    fn apply_diff(&mut self, opid: amp::OpId, diff: amp::Diff) {
        MultiGrapheme::apply_diff(self, opid, diff)
    }

    fn apply_diff_iter<I>(&mut self, diff: &mut I)
    where
        I: Iterator<Item = (amp::OpId, amp::Diff)>,
    {
        self.apply_diff_iter(diff)
        //MultiGrapheme::apply_diff_iter(self, diff)
    }

    fn default_opid(&self) -> amp::OpId {
        self.default_opid().clone()
    }

    fn only_for_opid(&self, opid: amp::OpId) -> Option<MultiGrapheme> {
        self.only_for_opid(opid)
    }

    fn add_values_from(&mut self, other: MultiGrapheme) {
        self.add_values_from(other)
    }
}

impl DiffableValue for MultiValue {
    fn take(&mut self) -> Self {
        Self {
            winning_value: (
                amp::OpId(0, ActorId::from(&[][..])),
                StateTreeValue::default(),
            ),
            conflicts: HashMap::default(),
        }
    }

    fn check_construct(
        opid: &amp::OpId,
        diff: &amp::Diff,
        _parent_object_id: &amp::ObjectId,
    ) -> Result<(), InvalidPatch> {
        MultiValue::check_new_from_diff(opid, diff)
    }

    fn construct(opid: amp::OpId, diff: amp::Diff) -> Self {
        MultiValue::new_from_diff(opid, diff)
    }

    fn check_diff(
        &self,
        opid: &amp::OpId,
        diff: &amp::Diff,
        _parent_object_id: &amp::ObjectId,
    ) -> Result<(), InvalidPatch> {
        self.check_diff(opid, diff)
    }

    fn apply_diff(&mut self, opid: amp::OpId, diff: amp::Diff) {
        self.apply_diff(opid, diff)
    }

    fn apply_diff_iter<I>(&mut self, diff: &mut I)
    where
        I: Iterator<Item = (amp::OpId, amp::Diff)>,
    {
        self.apply_diff_iter(diff)
    }

    fn default_opid(&self) -> amp::OpId {
        self.default_opid()
    }

    fn only_for_opid(&self, opid: amp::OpId) -> Option<MultiValue> {
        self.only_for_opid(opid)
    }

    fn add_values_from(&mut self, other: MultiValue) {
        self.add_values_from(other)
    }
}

#[derive(Clone, Debug, PartialEq)]
struct SequenceElement<T>
where
    T: DiffableValue,
    T: Clone,
    T: PartialEq,
{
    opid: OpId,
    value: SequenceValue<T>,
}

impl<T> SequenceElement<T>
where
    T: Clone,
    T: DiffableValue,
    T: PartialEq,
{
    fn original(value: T) -> Self {
        Self {
            opid: value.default_opid(),
            value: SequenceValue::Original(value),
        }
    }

    fn new(value: T) -> Self {
        Self {
            opid: value.default_opid(),
            value: SequenceValue::New(value),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DiffableSequence<T>
where
    T: DiffableValue,
    T: Clone,
    T: PartialEq,
{
    // stores the opid that created the element and the diffable value
    underlying: Box<im_rc::Vector<Box<SequenceElement<T>>>>,
}

impl<T> DiffableSequence<T>
where
    T: Clone,
    T: DiffableValue,
    T: PartialEq,
{
    pub fn new() -> DiffableSequence<T> {
        DiffableSequence {
            underlying: Box::new(im_rc::Vector::new()),
        }
    }

    pub(super) fn new_from<I>(i: I) -> DiffableSequence<T>
    where
        I: IntoIterator<Item = T>,
    {
        DiffableSequence {
            underlying: Box::new(
                i.into_iter()
                    .map(SequenceElement::original)
                    .map(Box::new)
                    .collect(),
            ),
        }
    }

    pub fn check_diff(
        &self,
        object_id: &amp::ObjectId,
        edits: &[amp::DiffEdit],
    ) -> Result<(), InvalidPatch> {
        let mut size = self.underlying.len();
        for edit in edits {
            match edit {
                amp::DiffEdit::Remove { index, count } => {
                    let index = *index as usize;
                    let count = *count as usize;
                    if index >= size {
                        return Err(InvalidPatch::InvalidIndex {
                            object_id: object_id.clone(),
                            index,
                        });
                    }
                    if index + count > size {
                        return Err(InvalidPatch::InvalidIndex {
                            object_id: object_id.clone(),
                            index: size,
                        });
                    }
                    size -= count;
                }
                amp::DiffEdit::SingleElementInsert {
                    index,
                    elem_id: _,
                    op_id,
                    value,
                } => {
                    T::check_construct(op_id, value, object_id)?;
                    if *index as usize > size {
                        return Err(InvalidPatch::InvalidIndex {
                            object_id: object_id.clone(),
                            index: *index as usize,
                        });
                    }
                    size += 1;
                }
                amp::DiffEdit::MultiElementInsert(amp::MultiElementInsert {
                    elem_id,
                    values,
                    index,
                }) => {
                    let index = *index as usize;
                    if index > size {
                        return Err(InvalidPatch::InvalidIndex {
                            index,
                            object_id: object_id.clone(),
                        });
                    }
                    for (i, value) in values.iter().enumerate() {
                        let opid = elem_id.as_opid().unwrap().increment_by(i as u64);
                        T::check_construct(&opid, &amp::Diff::Value(value.clone()), object_id)?;
                    }
                    size += values.len();
                }
                amp::DiffEdit::Update {
                    index,
                    value: _,
                    op_id: _,
                } => {
                    // TODO: handle updates after things like inserts shifting them
                    if *index as usize >= size {
                        return Err(InvalidPatch::InvalidIndex {
                            index: *index as usize,
                            object_id: object_id.clone(),
                        });
                    }

                    // if let Some((_id, elem)) = self.underlying.get(*index as usize) {
                    //     elem.check_diff(op_id, value)?;
                    // } else {
                    // }
                }
            };
        }

        Ok(())
    }

    pub fn apply_diff(&mut self, _object_id: &amp::ObjectId, edits: Vec<amp::DiffEdit>) {
        let mut changed_indices = Vec::new();
        for edit in edits {
            match edit {
                amp::DiffEdit::Remove { index, count } => {
                    let index = index as usize;
                    let count = count as usize;
                    self.underlying.slice(index..(index + count));

                    let mut i = 0;
                    while i < changed_indices.len() {
                        let changed_index = changed_indices.get_mut(i).unwrap();
                        if *changed_index >= index as u64 {
                            if *changed_index >= (index + count) as u64 {
                                *changed_index -= count as u64;
                            } else {
                                changed_indices.swap_remove(i);
                                continue;
                            }
                        }
                        i += 1;
                    }
                }
                amp::DiffEdit::SingleElementInsert {
                    index,
                    elem_id: _,
                    op_id,
                    value,
                } => {
                    let node = T::construct(op_id, value);
                    if (index as usize) == self.underlying.len() {
                        self.underlying
                            .push_back(Box::new(SequenceElement::new(node)));
                    } else {
                        self.underlying
                            .insert(index as usize, Box::new(SequenceElement::new(node)));

                        for changed_index in changed_indices.iter_mut() {
                            if *changed_index >= index as u64 {
                                *changed_index += 1;
                            }
                        }
                    };
                    changed_indices.push(index);
                }
                amp::DiffEdit::MultiElementInsert(amp::MultiElementInsert {
                    elem_id,
                    values,
                    index,
                }) => {
                    let index = index as usize;
                    // building an intermediate vector can be better than just inserting
                    // TODO: only do this if there are a certain (to be worked out) number of
                    // values
                    // TODO: if all inserts are at the end then use push_back
                    let mut intermediate = im_rc::Vector::new();
                    for (i, value) in values.iter().enumerate() {
                        let opid = elem_id.as_opid().unwrap().increment_by(i as u64);
                        let mv = T::construct(opid, amp::Diff::Value(value.clone()));
                        intermediate.push_back(Box::new(SequenceElement::new(mv)));
                    }
                    let right = self.underlying.split_off(index);
                    self.underlying.append(intermediate);
                    self.underlying.append(right);

                    for changed_index in changed_indices.iter_mut() {
                        if *changed_index >= index as u64 {
                            *changed_index += values.len() as u64;
                        }
                    }

                    for i in index..(index + values.len()) {
                        changed_indices.push(i as u64);
                    }
                }
                amp::DiffEdit::Update {
                    index,
                    value,
                    op_id,
                } => {
                    if let Some(v) = self.underlying.get_mut(index as usize) {
                        v.value.apply_diff(op_id, value);
                    }
                    changed_indices.push(index);
                }
            };
        }

        for i in changed_indices {
            if let Some(u) = self.underlying.get_mut(i as usize) {
                u.value.finish()
            }
        }

        debug_assert!(
            self.underlying
                .iter()
                .all(|u| matches!(u.value, SequenceValue::Original(_))),
            "diffable sequence apply_diff_iter didn't call finish on all values"
        );
    }

    pub(super) fn remove(&mut self, index: usize) -> T {
        match self.underlying.remove(index).value {
            SequenceValue::Original(t) => t,
            _ => unreachable!(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.underlying.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.underlying.is_empty()
    }

    pub(super) fn set(&mut self, index: usize, value: T) -> T {
        let elem_id = self
            .underlying
            .get(index)
            .map(|existing| existing.opid.clone())
            .expect("Failed to get existing index in set");
        self.underlying
            .set(
                index,
                Box::new(SequenceElement {
                    opid: elem_id,
                    value: SequenceValue::Original(value),
                }),
            )
            .value
            .get()
            .clone()
    }

    pub(crate) fn get(&self, index: usize) -> Option<(&OpId, &T)> {
        self.underlying.get(index).map(|e| (&e.opid, e.value.get()))
    }

    pub(super) fn get_mut(&mut self, index: usize) -> Option<(&mut OpId, &mut T)> {
        self.underlying
            .get_mut(index)
            .map(|e| (&mut e.opid, e.value.get_mut()))
    }

    pub(super) fn insert(&mut self, index: usize, value: T) {
        self.underlying
            .insert(index, Box::new(SequenceElement::original(value)))
    }

    pub(crate) fn iter(&self) -> impl std::iter::Iterator<Item = &T> {
        // Making this unwrap safe is the entire point of this data structure
        self.underlying.iter().map(|i| i.value.get())
    }
}

#[derive(Clone, Debug, PartialEq)]
enum SequenceValue<T>
where
    T: DiffableValue,
{
    Original(T),
    New(T),
    Updated { original: T, updates: Vec<T> },
}

impl<T> SequenceValue<T>
where
    T: DiffableValue,
    T: Clone,
{
    fn finish(&mut self) {
        match self {
            SequenceValue::Original(_) => { // do nothing, this is the finished state
            }
            SequenceValue::New(v) => *self = SequenceValue::Original(v.take()),
            SequenceValue::Updated { updates, .. } => {
                let initial_update = updates.remove(0);
                let t =
                    std::mem::take(updates)
                        .into_iter()
                        .fold(initial_update, |mut acc, elem| {
                            acc.add_values_from(elem);
                            acc
                        });
                *self = SequenceValue::Original(t)
            }
        }
    }

    fn get(&self) -> &T {
        match self {
            SequenceValue::Original(v) => v,
            _ => unreachable!(),
        }
    }

    fn get_mut(&mut self) -> &mut T {
        match self {
            SequenceValue::Original(v) => v,
            _ => unreachable!(),
        }
    }

    fn apply_diff(&mut self, opid: amp::OpId, diff: amp::Diff) {
        match self {
            SequenceValue::Original(v) => {
                let updated = if let Some(mut existing) = v.only_for_opid(opid.clone()) {
                    existing.apply_diff(opid, diff);
                    existing
                } else {
                    T::construct(opid, diff)
                };
                *self = SequenceValue::Updated {
                    original: v.take(),
                    updates: vec![updated],
                };
            }
            SequenceValue::New(v) => {
                let updated = if let Some(mut existing) = v.only_for_opid(opid.clone()) {
                    existing.apply_diff(opid, diff);
                    existing
                } else {
                    T::construct(opid, diff)
                };
                *self = SequenceValue::Updated {
                    original: v.clone(),
                    updates: vec![v.take(), updated],
                };
            }
            SequenceValue::Updated { original, updates } => {
                let updated = if let Some(mut update) = updates
                    .get(1..)
                    .and_then(|i| i.iter().find_map(|v| v.only_for_opid(opid.clone())))
                {
                    update.apply_diff(opid, diff);
                    update
                } else if let Some(mut initial) =
                    updates.get(0).and_then(|u| u.only_for_opid(opid.clone()))
                {
                    initial.apply_diff(opid, diff);
                    initial
                } else if let Some(mut original) = original.only_for_opid(opid.clone()) {
                    original.apply_diff(opid, diff);
                    original
                } else {
                    T::construct(opid, diff)
                };
                updates.push(updated);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use amp::{ActorId, Diff, DiffEdit, MultiElementInsert, ObjectId, ScalarValue, ScalarValues};

    use super::*;

    #[test]
    fn insert_single() {
        let mut ds = DiffableSequence::<MultiValue>::new();

        let oid = ObjectId::Root;
        ds.apply_diff(
            &oid,
            vec![
                DiffEdit::SingleElementInsert {
                    index: 0,
                    elem_id: amp::ElementId::Head,
                    op_id: OpId(0, ActorId::random()),
                    value: Diff::Value(ScalarValue::Null),
                },
                DiffEdit::SingleElementInsert {
                    index: 0,
                    elem_id: amp::ElementId::Head,
                    op_id: OpId(1, ActorId::random()),
                    value: Diff::Value(ScalarValue::Null),
                },
            ],
        )
    }

    #[test]
    fn insert_many() {
        let mut ds = DiffableSequence::<MultiValue>::new();

        let oid = ObjectId::Root;
        let mut values = ScalarValues::new(amp::ScalarValueKind::Null);
        values.append(ScalarValue::Null);
        values.append(ScalarValue::Null);

        ds.apply_diff(
            &oid,
            vec![
                DiffEdit::MultiElementInsert(MultiElementInsert {
                    index: 0,
                    elem_id: amp::ElementId::Id(OpId(0, ActorId::random())),
                    values: values.clone(),
                }),
                DiffEdit::MultiElementInsert(MultiElementInsert {
                    index: 0,
                    elem_id: amp::ElementId::Id(OpId(1, ActorId::random())),
                    values,
                }),
            ],
        )
    }

    #[test]
    fn remove() {
        let mut ds = DiffableSequence::<MultiValue>::new();

        let oid = ObjectId::Root;
        ds.apply_diff(
            &oid,
            vec![
                DiffEdit::SingleElementInsert {
                    index: 0,
                    elem_id: amp::ElementId::Head,
                    op_id: OpId(0, ActorId::random()),
                    value: Diff::Value(ScalarValue::Null),
                },
                DiffEdit::SingleElementInsert {
                    index: 1,
                    elem_id: amp::ElementId::Head,
                    op_id: OpId(0, ActorId::random()),
                    value: Diff::Value(ScalarValue::Null),
                },
                DiffEdit::Remove { index: 0, count: 1 },
            ],
        )
    }
}
