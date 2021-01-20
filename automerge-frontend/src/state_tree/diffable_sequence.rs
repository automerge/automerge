use super::{MultiChar, MultiValue, StateTreeChange};
use crate::error::InvalidPatch;
use automerge_protocol as amp;
use std::collections::HashMap;

pub(super) trait DiffableValue: Sized {
    fn construct(
        parent_object_id: &amp::ObjectID,
        opid: &amp::OpID,
        diff: &amp::Diff,
    ) -> Result<StateTreeChange<Self>, InvalidPatch>;
    fn apply_diff(
        &self,
        parent_object_id: &amp::ObjectID,
        opid: &amp::OpID,
        diff: &amp::Diff,
    ) -> Result<StateTreeChange<Self>, InvalidPatch>;
    fn apply_diff_iter<'a, 'b, I>(
        &'a self,
        parent_object_id: &amp::ObjectID,
        diff: &mut I,
    ) -> Result<StateTreeChange<Self>, InvalidPatch>
    where
        I: Iterator<Item = (&'b amp::OpID, &'b amp::Diff)>;
    fn default_opid(&self) -> amp::OpID;
}

impl DiffableValue for MultiChar {
    fn construct(
        parent_object_id: &amp::ObjectID,
        opid: &amp::OpID,
        diff: &amp::Diff,
    ) -> Result<StateTreeChange<Self>, InvalidPatch> {
        let c = MultiChar::new_from_diff(parent_object_id, opid, diff)?;
        Ok(StateTreeChange::pure(c))
    }

    fn apply_diff(
        &self,
        parent_object_id: &amp::ObjectID,
        opid: &amp::OpID,
        diff: &amp::Diff,
    ) -> Result<StateTreeChange<Self>, InvalidPatch> {
        MultiChar::apply_diff(self, parent_object_id, opid, diff).map(StateTreeChange::pure)
    }

    fn apply_diff_iter<'a, 'b, I>(
        &'a self,
        parent_object_id: &amp::ObjectID,
        diff: &mut I,
    ) -> Result<StateTreeChange<Self>, InvalidPatch>
    where
        I: Iterator<Item = (&'b amp::OpID, &'b amp::Diff)>,
    {
        MultiChar::apply_diff_iter(self, parent_object_id, diff)
    }

    fn default_opid(&self) -> amp::OpID {
        self.default_opid().clone()
    }
}

impl DiffableValue for MultiValue {
    fn construct(
        _parent_object_id: &amp::ObjectID,
        opid: &amp::OpID,
        diff: &amp::Diff,
    ) -> Result<StateTreeChange<Self>, InvalidPatch> {
        MultiValue::new_from_diff(opid.clone(), diff)
    }

    fn apply_diff(
        &self,
        _parent_object_id: &amp::ObjectID,
        opid: &amp::OpID,
        diff: &amp::Diff,
    ) -> Result<StateTreeChange<Self>, InvalidPatch> {
        self.apply_diff(opid, diff)
    }

    fn apply_diff_iter<'a, 'b, I>(
        &'a self,
        _parent_object_id: &amp::ObjectID,
        diff: &mut I,
    ) -> Result<StateTreeChange<Self>, InvalidPatch>
    where
        I: Iterator<Item = (&'b amp::OpID, &'b amp::Diff)>,
    {
        self.apply_diff_iter(diff)
    }

    fn default_opid(&self) -> amp::OpID {
        self.default_opid()
    }
}

/// This represents a sequence which can be updated with a diff. The reason we need it is that
/// whilst diffing a sequence we need to be able to insert placeholder values when processing the
/// `edits` key of the diff. We don't want to have to unwrap options the whole time though so we
/// guarantee the invariant that every value contains a `Some(T)` after each diff application.
#[derive(Clone, Debug)]
pub(super) struct DiffableSequence<T>
where
    T: DiffableValue,
    T: Clone,
{
    underlying: im::Vector<(amp::OpID, Option<T>)>,
}

impl<T> DiffableSequence<T>
where
    T: Clone,
    T: DiffableValue,
{
    pub fn new() -> DiffableSequence<T> {
        DiffableSequence {
            underlying: im::Vector::new(),
        }
    }

    pub fn apply_diff(
        &self,
        object_id: &amp::ObjectID,
        edits: &[amp::DiffEdit],
        new_props: &HashMap<usize, HashMap<amp::OpID, amp::Diff>>,
    ) -> Result<StateTreeChange<DiffableSequence<T>>, InvalidPatch> {
        let mut new_underlying = self.underlying.clone();
        for edit in edits.iter() {
            match edit {
                amp::DiffEdit::Remove { index } => {
                    if *index >= new_underlying.len() {
                        return Err(InvalidPatch::InvalidIndex {
                            object_id: object_id.clone(),
                            index: *index,
                        });
                    }
                    new_underlying.remove(*index);
                }
                amp::DiffEdit::Insert { index, elem_id } => {
                    let op_id = match elem_id {
                        amp::ElementID::Head => return Err(InvalidPatch::DiffEditWithHeadElemID),
                        amp::ElementID::ID(oid) => oid.clone(),
                    };
                    if (*index) == new_underlying.len() {
                        new_underlying.push_back((op_id, None));
                    } else {
                        new_underlying.insert(*index, (op_id, None));
                    }
                }
            };
        }
        let init_changed_props = Ok(StateTreeChange::pure(new_underlying));
        let updated =
            new_props
                .iter()
                .fold(init_changed_props, |changes_so_far, (index, prop_diff)| {
                    let mut diff_iter = prop_diff.iter();
                    match diff_iter.next() {
                        None => changes_so_far.map(|cr| {
                            cr.map(|c| {
                                let mut result = c;
                                result.remove(*index);
                                result
                            })
                        }),
                        Some((opid, diff)) => {
                            changes_so_far?.fallible_and_then(move |changes_so_far| {
                                let mut node = match changes_so_far.get(*index) {
                                    Some((_, Some(n))) => n.apply_diff(object_id, opid, diff)?,
                                    Some((_, None)) => T::construct(object_id, opid, diff)?,
                                    None => {
                                        return Err(InvalidPatch::InvalidIndex {
                                            object_id: object_id.clone(),
                                            index: *index,
                                        })
                                    }
                                };
                                node = node.fallible_and_then(move |n| {
                                    n.apply_diff_iter(object_id, &mut diff_iter)
                                })?;
                                Ok(node.map(|n| {
                                    changes_so_far.update(*index, (n.default_opid(), Some(n)))
                                }))
                            })
                        }
                    }
                })?;
        //This is where we maintain the invariant that allows us to provide an iterator over T
        //rather than Option<T>
        updated.fallible_and_then(|new_elements_and_opids| {
            for (index, (_, maybe_elem)) in new_elements_and_opids.iter().enumerate() {
                if maybe_elem.is_none() {
                    return Err(InvalidPatch::InvalidIndex {
                        object_id: object_id.clone(),
                        index,
                    });
                }
            }
            let new_sequence = DiffableSequence {
                underlying: new_elements_and_opids,
            };
            Ok(StateTreeChange::pure(new_sequence))
        })
    }

    pub(super) fn remove(&mut self, index: usize) -> T {
        let a = self.underlying.remove(index);
        a.1.unwrap()
    }

    pub(super) fn len(&self) -> usize {
        self.underlying.len()
    }

    pub(super) fn update(&self, index: usize, value: T) -> Self {
        DiffableSequence {
            underlying: self
                .underlying
                .update(index, (value.default_opid(), Some(value))),
        }
    }

    pub(super) fn get(&self, index: usize) -> Option<&T> {
        self.underlying.get(index).and_then(|(_, v)| v.as_ref())
    }

    pub(super) fn insert(&mut self, index: usize, value: T) {
        self.underlying
            .insert(index, (value.default_opid(), Some(value)))
    }

    pub(super) fn iter(&self) -> impl std::iter::Iterator<Item = &T> {
        // Making this unwrap safe is the entire point of this data structure
        self.underlying.iter().map(|(_, v)| v.as_ref().unwrap())
    }

    pub(super) fn push_back(&mut self, value: T) {
        self.underlying
            .push_back((value.default_opid(), Some(value)))
    }
}
