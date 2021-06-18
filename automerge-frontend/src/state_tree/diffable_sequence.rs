use std::collections::HashSet;

use amp::OpId;
use automerge_protocol as amp;

use super::{MultiGrapheme, MultiValue};
use crate::error::InvalidPatch;

pub(super) trait DiffableValue: Sized + Default {
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
pub(super) struct DiffableSequence<T>
where
    T: DiffableValue,
    T: Clone,
    T: PartialEq,
{
    // stores the opid that created the element and the diffable value
    underlying: Box<im_rc::Vector<SequenceElement<T>>>,
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
            underlying: Box::new(i.into_iter().map(SequenceElement::original).collect()),
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
        let mut changed_indices = HashSet::new();
        for edit in edits {
            match edit {
                amp::DiffEdit::Remove { index, count } => {
                    let index = index as usize;
                    let count = count as usize;
                    self.underlying.slice(index..(index + count));

                    for i in changed_indices.clone().iter() {
                        // if the index is to the right of that being removed we need to shift it
                        if *i >= index as u64 {
                            // we don't need to keep the old value
                            changed_indices.remove(i);
                            // but if the value is not in the removed range then we need to add the
                            // updated value in again
                            if *i >= (index + count) as u64 {
                                changed_indices.insert(*i - count as u64);
                            }
                        }
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
                        self.underlying.push_back(SequenceElement::new(node));
                    } else {
                        self.underlying
                            .insert(index as usize, SequenceElement::new(node));
                    };
                    changed_indices.insert(index);
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
                        intermediate.push_back(SequenceElement::new(mv));
                    }
                    let right = self.underlying.split_off(index);
                    self.underlying.append(intermediate);
                    self.underlying.append(right);
                    for i in index..(index + values.len()) {
                        changed_indices.insert(i as u64);
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
                    changed_indices.insert(index);
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

    pub(super) fn len(&self) -> usize {
        self.underlying.len()
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
                SequenceElement {
                    opid: elem_id,
                    value: SequenceValue::Original(value),
                },
            )
            .value
            .get()
            .clone()
    }

    pub(super) fn get(&self, index: usize) -> Option<(&OpId, &T)> {
        self.underlying.get(index).map(|e| (&e.opid, e.value.get()))
    }

    pub(super) fn get_mut(&mut self, index: usize) -> Option<(&mut OpId, &mut T)> {
        self.underlying
            .get_mut(index)
            .map(|e| (&mut e.opid, e.value.get_mut()))
    }

    pub(super) fn insert(&mut self, index: usize, value: T) {
        self.underlying
            .insert(index, SequenceElement::original(value))
    }

    pub(super) fn mutate<F>(&mut self, index: usize, f: F)
    where
        F: FnOnce(&T) -> T,
    {
        if let Some(entry) = self.underlying.get_mut(index) {
            *entry = SequenceElement {
                opid: entry.opid.clone(),
                value: SequenceValue::Original(f(entry.value.get())),
            };
        }
    }

    pub(super) fn iter(&self) -> impl std::iter::Iterator<Item = &T> {
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
            SequenceValue::New(v) => *self = SequenceValue::Original(std::mem::take(v)),
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

    fn check_diff(
        &self,
        opid: &amp::OpId,
        diff: &amp::Diff,
        parent_object_id: &amp::ObjectId,
    ) -> Result<(), InvalidPatch> {
        match self {
            SequenceValue::Original(v) | SequenceValue::New(v) => {
                if let Some(existing) = v.only_for_opid(opid.clone()) {
                    existing.check_diff(opid, diff, parent_object_id)?;
                } else {
                    T::check_construct(opid, diff, parent_object_id)?
                };
                Ok(())
            }
            SequenceValue::Updated { original, updates } => {
                if let Some(update) = updates
                    .get(1..)
                    .and_then(|i| i.iter().find_map(|v| v.only_for_opid(opid.clone())))
                {
                    update.check_diff(opid, diff, parent_object_id)?;
                } else if let Some(initial) =
                    updates.get(0).and_then(|u| u.only_for_opid(opid.clone()))
                {
                    initial.check_diff(opid, diff, parent_object_id)?;
                } else if let Some(original) = original.only_for_opid(opid.clone()) {
                    original.check_diff(opid, diff, parent_object_id)?;
                } else {
                    T::check_construct(opid, diff, parent_object_id)?
                };
                Ok(())
            }
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
                    original: std::mem::take(v),
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
                    updates: vec![std::mem::take(v), updated],
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
