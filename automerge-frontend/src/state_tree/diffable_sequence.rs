use amp::OpId;
use automerge_protocol as amp;

use super::{
    DiffApplicationResult, DiffToApply, MultiGrapheme, MultiValue, StateTreeChange,
    StateTreeComposite,
};
use crate::error::InvalidPatch;

pub(super) trait DiffableValue: Sized {
    fn construct<K>(
        opid: &amp::OpId,
        diff: DiffToApply<K, &amp::Diff>,
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<DiffApplicationResult<Self>, InvalidPatch>
    where
        K: Into<amp::Key>;
    fn apply_diff<K>(
        &self,
        opid: &amp::OpId,
        diff: DiffToApply<K, &amp::Diff>,
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<DiffApplicationResult<Self>, InvalidPatch>
    where
        K: Into<amp::Key>;
    fn apply_diff_iter<'a, 'b, 'c, 'd, I, K: 'c>(
        &'a self,
        diff: &mut I,
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<DiffApplicationResult<Self>, InvalidPatch>
    where
        K: Into<amp::Key>,
        I: Iterator<Item = (&'b amp::OpId, DiffToApply<'c, K, &'d amp::Diff>)>;
    fn default_opid(&self) -> amp::OpId;

    fn only_for_opid(&self, opid: &amp::OpId) -> Option<Self>;

    fn add_values_from(&mut self, other: Self);
}

impl DiffableValue for MultiGrapheme {
    fn construct<K>(
        opid: &amp::OpId,
        diff: DiffToApply<K, &amp::Diff>,
        _current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<DiffApplicationResult<Self>, InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        let c = MultiGrapheme::new_from_diff(opid, diff)?;
        Ok(DiffApplicationResult::pure(c))
    }

    fn apply_diff<K>(
        &self,
        opid: &amp::OpId,
        diff: DiffToApply<K, &amp::Diff>,
        _current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<DiffApplicationResult<Self>, InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        MultiGrapheme::apply_diff(self, opid, diff).map(DiffApplicationResult::pure)
    }

    fn apply_diff_iter<'a, 'b, 'c, 'd, I, K: 'c>(
        &'a self,
        diff: &mut I,
        _current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<DiffApplicationResult<Self>, InvalidPatch>
    where
        K: Into<amp::Key>,
        I: Iterator<Item = (&'b amp::OpId, DiffToApply<'c, K, &'d amp::Diff>)>,
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
        diff: DiffToApply<K, &amp::Diff>,
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<DiffApplicationResult<Self>, InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        MultiValue::new_from_diff(opid.clone(), diff, current_objects)
    }

    fn apply_diff<K>(
        &self,
        opid: &amp::OpId,
        diff: DiffToApply<K, &amp::Diff>,
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<DiffApplicationResult<Self>, InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        self.apply_diff(opid, diff, current_objects)
    }

    fn apply_diff_iter<'a, 'b, 'c, 'd, I, K: 'c>(
        &'a self,
        diff: &mut I,
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<DiffApplicationResult<Self>, InvalidPatch>
    where
        K: Into<amp::Key>,
        I: Iterator<Item = (&'b amp::OpId, DiffToApply<'c, K, &'d amp::Diff>)>,
    {
        self.apply_diff_iter(diff, current_objects)
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
    underlying: Box<im_rc::Vector<(OpId, T)>>,
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
            underlying: Box::new(i.into_iter().map(|i| (i.default_opid(), i)).collect()),
        }
    }

    pub fn apply_diff(
        &self,
        object_id: &amp::ObjectId,
        edits: &[amp::DiffEdit],
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<DiffApplicationResult<DiffableSequence<T>>, InvalidPatch> {
        let mut opids_in_this_diff: std::collections::HashSet<amp::OpId> =
            std::collections::HashSet::new();
        let mut old_conflicts: Vec<Option<T>> = vec![None; self.underlying.len()];
        let mut updating: Vec<_> = self
            .underlying
            .clone()
            .into_iter()
            .map(|i| (i.0, UpdatingSequenceElement::from_original(i.1)))
            .collect();
        let mut changes = StateTreeChange::empty();

        for edit in edits.iter() {
            match edit {
                amp::DiffEdit::Remove { index, count } => {
                    let index = *index as usize;
                    let count = *count as usize;
                    if index >= updating.len() {
                        return Err(InvalidPatch::InvalidIndex {
                            object_id: object_id.clone(),
                            index,
                        });
                    }
                    if index + count > updating.len() {
                        return Err(InvalidPatch::InvalidIndex {
                            object_id: object_id.clone(),
                            index: updating.len(),
                        });
                    }
                    updating.splice(index..(index + count), None);
                }
                amp::DiffEdit::SingleElementInsert {
                    index,
                    elem_id: _,
                    op_id,
                    value,
                } => {
                    opids_in_this_diff.insert(op_id.clone());
                    let node = T::construct(
                        &op_id,
                        DiffToApply {
                            parent_object_id: object_id,
                            parent_key: &op_id,
                            diff: value,
                        },
                        current_objects,
                    )?;
                    if (*index as usize) == updating.len() {
                        old_conflicts.push(None);
                        updating.push((
                            node.value.default_opid(),
                            UpdatingSequenceElement::new(node.value),
                        ));
                    } else {
                        old_conflicts.insert(*index as usize, None);
                        updating.insert(
                            *index as usize,
                            (
                                node.value.default_opid(),
                                UpdatingSequenceElement::new(node.value),
                            ),
                        );
                    };
                    changes.update_with(node.change);
                }
                amp::DiffEdit::MultiElementInsert {
                    elem_id,
                    values,
                    index,
                } => {
                    let index = *index as usize;
                    if index > updating.len() {
                        return Err(InvalidPatch::InvalidIndex {
                            index,
                            object_id: object_id.clone(),
                        });
                    }
                    for (i, value) in values.iter().enumerate() {
                        let opid = elem_id.as_opid().unwrap().increment_by(i as u64);
                        let mv = T::construct(
                            &opid,
                            DiffToApply {
                                parent_object_id: object_id,
                                parent_key: &opid,
                                diff: &amp::Diff::Value(value.clone()),
                            },
                            current_objects,
                        )?;
                        changes.update_with(mv.change);
                        updating.insert(
                            index + i,
                            (
                                mv.value.default_opid(),
                                UpdatingSequenceElement::New(mv.value),
                            ),
                        );
                    }
                }
                amp::DiffEdit::Update {
                    index,
                    value,
                    op_id,
                } => {
                    if let Some((_id, elem)) = updating.get_mut(*index as usize) {
                        let change = elem.apply_diff(
                            op_id,
                            DiffToApply {
                                parent_object_id: object_id,
                                parent_key: &op_id,
                                diff: value,
                            },
                            current_objects,
                        )?;
                        changes.update_with(change);
                    } else {
                        return Err(InvalidPatch::InvalidIndex {
                            index: *index as usize,
                            object_id: object_id.clone(),
                        });
                    }
                }
            };
        }

        let new_sequence = DiffableSequence {
            underlying: Box::new(updating.into_iter().map(|e| (e.0, e.1.finish())).collect()),
        };
        Ok(DiffApplicationResult::pure(new_sequence).with_changes(changes))
    }

    pub(super) fn remove(&mut self, index: usize) -> T {
        self.underlying.remove(index).1
    }

    pub(super) fn len(&self) -> usize {
        self.underlying.len()
    }

    pub(super) fn update(&self, index: usize, value: T) -> Self {
        let elem_id = if let Some(existing) = self.underlying.get(index) {
            existing.0.clone()
        } else {
            value.default_opid()
        };
        DiffableSequence {
            underlying: Box::new(self.underlying.update(index, (elem_id, value))),
        }
    }

    pub(super) fn get(&self, index: usize) -> Option<&(OpId, T)> {
        self.underlying.get(index)
    }

    pub(super) fn insert(&mut self, index: usize, value: T) {
        self.underlying.insert(index, (value.default_opid(), value))
    }

    pub(super) fn mutate<F>(&mut self, index: usize, f: F)
    where
        F: FnOnce(&T) -> T,
    {
        if let Some(entry) = self.underlying.get_mut(index) {
            *entry = (entry.0.clone(), f(&entry.1));
        }
    }

    pub(super) fn iter(&self) -> impl std::iter::Iterator<Item = &T> {
        // Making this unwrap safe is the entire point of this data structure
        self.underlying.iter().map(|i| &i.1)
    }
}

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
    fn from_original(value: T) -> UpdatingSequenceElement<T> {
        UpdatingSequenceElement::Original(value)
    }

    fn new(value: T) -> UpdatingSequenceElement<T> {
        UpdatingSequenceElement::New(value)
    }

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
        diff: DiffToApply<K, &amp::Diff>,
        current_objects: &mut im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
    ) -> Result<StateTreeChange, InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        match self {
            UpdatingSequenceElement::Original(v) => {
                let updated = if let Some(existing) = v.only_for_opid(opid) {
                    existing.apply_diff(opid, diff, current_objects)?
                } else {
                    T::construct(opid, diff, current_objects)?
                };
                *self = UpdatingSequenceElement::Updated {
                    original: v.clone(),
                    initial_update: updated.value,
                    remaining_updates: Vec::new(),
                };
                Ok(updated.change)
            }
            UpdatingSequenceElement::New(v) => {
                let updated = if let Some(existing) = v.only_for_opid(opid) {
                    existing.apply_diff(opid, diff, current_objects)?
                } else {
                    T::construct(opid, diff, current_objects)?
                };
                *self = UpdatingSequenceElement::Updated {
                    original: v.clone(),
                    initial_update: v.clone(),
                    remaining_updates: vec![updated.value],
                };
                Ok(updated.change)
            }
            UpdatingSequenceElement::Updated {
                original,
                initial_update,
                remaining_updates,
            } => {
                println!("UPdating already updated value");
                let updated = if let Some(update) =
                    remaining_updates.iter().find_map(|v| v.only_for_opid(opid))
                {
                    update.apply_diff(opid, diff, current_objects)?
                } else if let Some(initial) = initial_update.only_for_opid(opid) {
                    initial.apply_diff(opid, diff, current_objects)?
                } else if let Some(original) = original.only_for_opid(opid) {
                    original.apply_diff(opid, diff, current_objects)?
                } else {
                    T::construct(opid, diff, current_objects)?
                };
                remaining_updates.push(updated.value);
                Ok(updated.change)
            }
        }
    }
}
