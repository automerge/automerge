use std::borrow::Cow;

use amp::OpId;
use automerge_protocol as amp;

use super::{DiffToApply, MultiGrapheme, MultiValue, StateTreeComposite};
use crate::error::InvalidPatch;

pub(super) trait DiffableValue: Sized {
    fn construct<K>(
        opid: &amp::OpId,
        diff: DiffToApply<K, amp::Diff>,
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<Self, InvalidPatch>
    where
        K: Into<amp::Key>;
    fn apply_diff<K>(
        &mut self,
        opid: &amp::OpId,
        diff: DiffToApply<K, amp::Diff>,
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<Self, InvalidPatch>
    where
        K: Into<amp::Key>;
    fn apply_diff_iter<'a, 'b, 'c, I, K: 'c>(
        &'a mut self,
        diff: &mut I,
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<Self, InvalidPatch>
    where
        K: Into<amp::Key>,
        I: Iterator<Item = (&'b amp::OpId, DiffToApply<'c, K, amp::Diff>)>;
    fn default_opid(&self) -> amp::OpId;

    fn only_for_opid(&self, opid: &amp::OpId) -> Option<Self>;

    fn add_values_from(&mut self, other: Self);
}

impl DiffableValue for MultiGrapheme {
    fn construct<K>(
        opid: &amp::OpId,
        diff: DiffToApply<K, amp::Diff>,
        _current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<Self, InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        let c = MultiGrapheme::new_from_diff(opid, diff)?;
        Ok(c)
    }

    fn apply_diff<K>(
        &mut self,
        opid: &amp::OpId,
        diff: DiffToApply<K, amp::Diff>,
        _current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<Self, InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        MultiGrapheme::apply_diff(self, opid, diff)
    }

    fn apply_diff_iter<'a, 'b, 'c, 'd, I, K: 'c>(
        &'a mut self,
        diff: &mut I,
        _current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<Self, InvalidPatch>
    where
        K: Into<amp::Key>,
        I: Iterator<Item = (&'b amp::OpId, DiffToApply<'c, K, amp::Diff>)>,
    {
        self.apply_diff_iter(diff)
        //MultiGrapheme::apply_diff_iter(self, diff)
    }

    fn default_opid(&self) -> amp::OpId {
        self.default_opid().clone()
    }

    fn only_for_opid(&self, opid: &amp::OpId) -> Option<MultiGrapheme> {
        self.only_for_opid(opid)
    }

    fn add_values_from(&mut self, other: MultiGrapheme) {
        self.add_values_from(other)
    }
}

impl DiffableValue for MultiValue {
    fn construct<K>(
        opid: &amp::OpId,
        diff: DiffToApply<K, amp::Diff>,
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<Self, InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        MultiValue::new_from_diff(opid.clone(), diff, current_objects)
    }

    fn apply_diff<K>(
        &mut self,
        opid: &amp::OpId,
        diff: DiffToApply<K, amp::Diff>,
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<Self, InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        self.apply_diff(opid, diff, current_objects)
    }

    fn apply_diff_iter<'a, 'b, 'c, I, K: 'c>(
        &'a mut self,
        diff: &mut I,
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<Self, InvalidPatch>
    where
        K: Into<amp::Key>,
        I: Iterator<Item = (&'b amp::OpId, DiffToApply<'c, K, amp::Diff>)>,
    {
        Self::apply_diff_iter(
            self,
            &mut diff.map(|(o, d)| (Cow::Borrowed(o), d)),
            current_objects,
        )
    }

    fn default_opid(&self) -> amp::OpId {
        self.default_opid()
    }

    fn only_for_opid(&self, opid: &amp::OpId) -> Option<MultiValue> {
        self.only_for_opid(opid)
    }

    fn add_values_from(&mut self, other: MultiValue) {
        self.add_values_from(other)
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
    underlying: Box<im_rc::Vector<(OpId, UpdatingSequenceElement<T>)>>,
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
                    .map(|i| (i.default_opid(), UpdatingSequenceElement::Original(i)))
                    .collect(),
            ),
        }
    }

    pub fn apply_diff(
        &mut self,
        object_id: &amp::ObjectId,
        edits: Vec<amp::DiffEdit>,
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<DiffableSequence<T>, InvalidPatch> {
        let mut opids_in_this_diff: std::collections::HashSet<amp::OpId> =
            std::collections::HashSet::new();

        for edit in edits {
            match edit {
                amp::DiffEdit::Remove { index, count } => {
                    let index = index as usize;
                    let count = count as usize;
                    if index >= self.underlying.len() {
                        return Err(InvalidPatch::InvalidIndex {
                            object_id: object_id.clone(),
                            index,
                        });
                    }
                    if index + count > self.underlying.len() {
                        return Err(InvalidPatch::InvalidIndex {
                            object_id: object_id.clone(),
                            index: self.underlying.len(),
                        });
                    }
                    for i in index..(index + count) {
                        self.underlying.remove(i);
                    }
                }
                amp::DiffEdit::SingleElementInsert {
                    index,
                    elem_id: _,
                    op_id,
                    value,
                } => {
                    opids_in_this_diff.insert(op_id.clone());
                    let value = T::construct(
                        &op_id,
                        DiffToApply {
                            parent_object_id: object_id,
                            parent_key: &op_id,
                            diff: value,
                        },
                        current_objects,
                    )?;
                    if (index as usize) == self.underlying.len() {
                        self.underlying
                            .push_back((value.default_opid(), UpdatingSequenceElement::New(value)));
                    } else {
                        self.underlying.insert(
                            index as usize,
                            (value.default_opid(), UpdatingSequenceElement::New(value)),
                        );
                    };
                }
                amp::DiffEdit::MultiElementInsert {
                    elem_id,
                    values,
                    index,
                } => {
                    let index = index as usize;
                    if index > self.underlying.len() {
                        return Err(InvalidPatch::InvalidIndex {
                            index,
                            object_id: object_id.clone(),
                        });
                    }
                    for (i, value) in values.iter().enumerate() {
                        let opid = elem_id.as_opid().unwrap().increment_by(i as u64);
                        let value = T::construct(
                            &opid,
                            DiffToApply {
                                parent_object_id: object_id,
                                parent_key: &opid,
                                diff: amp::Diff::Value(value.clone()),
                            },
                            current_objects,
                        )?;
                        self.underlying.insert(
                            index + i,
                            (value.default_opid(), UpdatingSequenceElement::New(value)),
                        );
                    }
                }
                amp::DiffEdit::Update {
                    index,
                    value,
                    op_id,
                } => {
                    if let Some((_id, elem)) = self.underlying.get_mut(index as usize) {
                        elem.apply_diff(
                            &op_id,
                            DiffToApply {
                                parent_object_id: object_id,
                                parent_key: &op_id,
                                diff: value,
                            },
                            current_objects,
                        )?;
                    } else {
                        return Err(InvalidPatch::InvalidIndex {
                            index: index as usize,
                            object_id: object_id.clone(),
                        });
                    }
                }
            };
        }

        Ok(self.clone())
    }

    pub(super) fn remove(&mut self, index: usize) -> T {
        self.underlying.remove(index).1.finish()
    }

    pub(super) fn len(&self) -> usize {
        self.underlying.len()
    }

    pub(super) fn update(&mut self, index: usize, value: T) {
        let elem_id = if let Some(existing) = self.underlying.get(index) {
            existing.0.clone()
        } else {
            value.default_opid()
        };
        self.underlying
            .set(index, (elem_id, UpdatingSequenceElement::Original(value)));
    }

    pub(super) fn get(&self, index: usize) -> Option<(&OpId, T)> {
        self.underlying
            .get(index)
            .map(|(i, u)| (i, u.clone().finish()))
    }

    pub(super) fn insert(&mut self, index: usize, value: T) {
        self.underlying.insert(
            index,
            (
                value.default_opid(),
                UpdatingSequenceElement::Original(value),
            ),
        )
    }

    pub(super) fn mutate<F>(&mut self, index: usize, f: F)
    where
        F: FnOnce(&T) -> T,
    {
        if let Some(entry) = self.underlying.get_mut(index) {
            *entry = (
                entry.0.clone(),
                UpdatingSequenceElement::Original(f(&entry.1.clone().finish())),
            );
        }
    }

    pub(super) fn iter(&self) -> impl std::iter::Iterator<Item = T> + '_ {
        // Making this unwrap safe is the entire point of this data structure
        self.underlying.iter().map(|i| i.1.clone().finish())
    }
}

#[derive(Clone, Debug, PartialEq)]
enum UpdatingSequenceElement<T>
where
    T: DiffableValue,
{
    Original(T),
    New(T),
    Updated {
        original: T,
        initial_update: T,
        remaining_updates: Vec<T>,
    },
}

impl<T> UpdatingSequenceElement<T>
where
    T: DiffableValue,
    T: Clone,
{
    fn finish(self) -> T {
        match self {
            UpdatingSequenceElement::Original(v) => v,
            UpdatingSequenceElement::New(v) => v,
            UpdatingSequenceElement::Updated {
                initial_update,
                remaining_updates,
                ..
            } => remaining_updates
                .into_iter()
                .fold(initial_update, |mut acc, elem| {
                    acc.add_values_from(elem);
                    acc
                }),
        }
    }

    fn apply_diff<K>(
        &mut self,
        opid: &amp::OpId,
        diff: DiffToApply<K, amp::Diff>,
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<(), InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        match self {
            UpdatingSequenceElement::Original(v) => {
                let value = if let Some(mut existing) = v.only_for_opid(opid) {
                    existing.apply_diff(opid, diff, current_objects)?
                } else {
                    T::construct(opid, diff, current_objects)?
                };
                *self = UpdatingSequenceElement::Updated {
                    original: v.clone(),
                    initial_update: value,
                    remaining_updates: Vec::new(),
                };
            }
            UpdatingSequenceElement::New(v) => {
                let value = if let Some(mut existing) = v.only_for_opid(opid) {
                    existing.apply_diff(opid, diff, current_objects)?
                } else {
                    T::construct(opid, diff, current_objects)?
                };
                *self = UpdatingSequenceElement::Updated {
                    original: v.clone(),
                    initial_update: v.clone(),
                    remaining_updates: vec![value],
                };
            }
            UpdatingSequenceElement::Updated {
                original,
                initial_update,
                remaining_updates,
            } => {
                println!("UPdating already updated value");
                let value = if let Some(mut update) =
                    remaining_updates.iter().find_map(|v| v.only_for_opid(opid))
                {
                    update.apply_diff(opid, diff, current_objects)?
                } else if let Some(mut initial) = initial_update.only_for_opid(opid) {
                    initial.apply_diff(opid, diff, current_objects)?
                } else if let Some(mut original) = original.only_for_opid(opid) {
                    original.apply_diff(opid, diff, current_objects)?
                } else {
                    T::construct(opid, diff, current_objects)?
                };
                remaining_updates.push(value);
            }
        }
        Ok(())
    }
}
