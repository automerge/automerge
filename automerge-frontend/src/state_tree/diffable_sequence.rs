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
    ) -> Result<DiffApplicationResult<Self>, InvalidPatch>
    where
        K: Into<amp::Key>;
    fn apply_diff<K>(
        &self,
        opid: &amp::OpId,
        diff: DiffToApply<K, &amp::Diff>,
    ) -> Result<DiffApplicationResult<Self>, InvalidPatch>
    where
        K: Into<amp::Key>;
    fn apply_diff_iter<'a, 'b, 'c, 'd, I, K: 'c>(
        &'a self,
        diff: &mut I,
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
    ) -> Result<DiffApplicationResult<Self>, InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        MultiGrapheme::apply_diff(self, opid, diff).map(DiffApplicationResult::pure)
    }

    fn apply_diff_iter<'a, 'b, 'c, 'd, I, K: 'c>(
        &'a self,
        diff: &mut I,
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
    ) -> Result<DiffApplicationResult<Self>, InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        MultiValue::new_from_diff(opid.clone(), diff)
    }

    fn apply_diff<K>(
        &self,
        opid: &amp::OpId,
        diff: DiffToApply<K, &amp::Diff>,
    ) -> Result<DiffApplicationResult<Self>, InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        self.apply_diff(opid, diff)
    }

    fn apply_diff_iter<'a, 'b, 'c, 'd, I, K: 'c>(
        &'a self,
        diff: &mut I,
    ) -> Result<DiffApplicationResult<Self>, InvalidPatch>
    where
        K: Into<amp::Key>,
        I: Iterator<Item = (&'b amp::OpId, DiffToApply<'c, K, &'d amp::Diff>)>,
    {
        self.apply_diff_iter(diff)
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

#[derive(Clone, Debug)]
pub(super) struct DiffableSequence<T>
where
    T: DiffableValue,
    T: Clone,
{
    underlying: Box<im_rc::Vector<T>>,
}

impl<T> DiffableSequence<T>
where
    T: Clone,
    T: DiffableValue,
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
            underlying: Box::new(i.into_iter().collect()),
        }
    }

    pub fn apply_diff(
        &self,
        current_objects: im_rc::HashMap<amp::ObjectId, StateTreeComposite>,
        object_id: &amp::ObjectId,
        edits: &[amp::DiffEdit],
    ) -> Result<DiffApplicationResult<DiffableSequence<T>>, InvalidPatch> {
        let mut opids_in_this_diff: std::collections::HashSet<amp::OpId> =
            std::collections::HashSet::new();
        let mut old_conflicts: Vec<Option<T>> = vec![None; self.underlying.len()];
        let mut updating: Vec<UpdatingSequenceElement<T>> = self
            .underlying
            .clone()
            .into_iter()
            .map(|e| UpdatingSequenceElement::from_original(e))
            .collect();
        let mut changes = StateTreeChange::empty();
        for edit in edits.iter() {
            let current_objects = changes.objects().union(current_objects.clone());
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
                    elem_id,
                    value,
                } => {
                    let op_id = match elem_id {
                        amp::ElementId::Head => return Err(InvalidPatch::DiffEditWithHeadElemId),
                        amp::ElementId::Id(oid) => oid.clone(),
                    };
                    opids_in_this_diff.insert(op_id.clone());
                    let node = T::construct(
                        &op_id,
                        DiffToApply {
                            current_objects,
                            parent_object_id: object_id,
                            parent_key: &op_id,
                            diff: value,
                        },
                    )?;
                    if (*index) == updating.len() {
                        old_conflicts.push(None);
                        updating.push(UpdatingSequenceElement::new(node.value));
                    } else {
                        old_conflicts.insert(*index, None);
                        updating.insert(*index, UpdatingSequenceElement::new(node.value));
                    };
                    changes.update_with(node.change);
                }
                amp::DiffEdit::MultiElementInsert {
                    first_opid,
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
                        let opid = first_opid.increment_by(i as u64);
                        let current_objects = changes.objects().union(current_objects.clone());
                        let mv = T::construct(
                            &opid,
                            DiffToApply {
                                current_objects,
                                parent_object_id: object_id,
                                parent_key: &opid,
                                diff: &amp::Diff::Value(value.clone()),
                            },
                        )?;
                        changes.update_with(mv.change);
                        updating.insert(i, UpdatingSequenceElement::New(mv.value));
                    }
                }
                amp::DiffEdit::Update { index, value, opid } => {
                    if let Some(elem) = updating.get_mut(*index as usize) {
                        let change = elem.apply_diff(
                            opid,
                            DiffToApply {
                                current_objects,
                                parent_object_id: object_id,
                                parent_key: &opid,
                                diff: value,
                            },
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
            underlying: Box::new(updating.into_iter().map(|e| e.finish()).collect()),
        };
        Ok(DiffApplicationResult::pure(new_sequence).with_changes(changes))
    }

    pub(super) fn remove(&mut self, index: usize) -> T {
        let a = self.underlying.remove(index);
        a
    }

    pub(super) fn len(&self) -> usize {
        self.underlying.len()
    }

    pub(super) fn update(&self, index: usize, value: T) -> Self {
        DiffableSequence {
            underlying: Box::new(self.underlying.update(index, value)),
        }
    }

    pub(super) fn get(&self, index: usize) -> Option<&T> {
        self.underlying.get(index)
    }

    pub(super) fn insert(&mut self, index: usize, value: T) {
        self.underlying.insert(index, value)
    }

    pub(super) fn mutate<F>(&mut self, index: usize, f: F)
    where
        F: FnOnce(&T) -> T,
    {
        if let Some(entry) = self.underlying.get_mut(index) {
            *entry = f(&entry);
        }
    }

    pub(super) fn iter(&self) -> impl std::iter::Iterator<Item = &T> {
        // Making this unwrap safe is the entire point of this data structure
        self.underlying.iter()
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
    ) -> Result<StateTreeChange, InvalidPatch>
    where
        K: Into<amp::Key>,
    {
        match self {
            UpdatingSequenceElement::Original(v) => {
                let updated = if let Some(existing) = v.only_for_opid(opid) {
                    existing.apply_diff(opid, diff)?
                } else {
                    T::construct(opid, diff)?
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
                    existing.apply_diff(opid, diff)?
                } else {
                    T::construct(opid, diff)?
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
                    update.apply_diff(opid, diff)?
                } else if let Some(initial) = initial_update.only_for_opid(opid) {
                    initial.apply_diff(opid, diff)?
                } else if let Some(original) = original.only_for_opid(opid) {
                    original.apply_diff(opid, diff)?
                } else {
                    T::construct(opid, diff)?
                };
                remaining_updates.push(updated.value);
                Ok(updated.change)
            }
        }
    }
}
