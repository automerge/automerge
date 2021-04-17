use std::collections::HashMap;

use automerge_protocol as amp;

use super::{DiffApplicationResult, DiffToApply, MultiGrapheme, MultiValue, StateTreeChange};
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
    underlying: im_rc::Vector<Box<(amp::OpId, Option<T>)>>,
}

impl<T> DiffableSequence<T>
where
    T: Clone,
    T: DiffableValue,
{
    pub fn new() -> DiffableSequence<T> {
        DiffableSequence {
            underlying: im_rc::Vector::new(),
        }
    }

    pub(super) fn new_from<I>(i: I) -> DiffableSequence<T>
    where
        I: IntoIterator<Item = (amp::OpId, T)>,
    {
        DiffableSequence {
            underlying: i
                .into_iter()
                .map(|(oid, v)| Box::new((oid, Some(v))))
                .collect(),
        }
    }

    pub fn apply_diff<K>(
        &self,
        object_id: &amp::ObjectId,
        edits: &[amp::DiffEdit],
        new_props: DiffToApply<K, &HashMap<usize, HashMap<amp::OpId, amp::Diff>>>,
    ) -> Result<DiffApplicationResult<DiffableSequence<T>>, InvalidPatch>
    where
        K: Into<amp::Key>,
    {
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
                        amp::ElementId::Head => return Err(InvalidPatch::DiffEditWithHeadElemId),
                        amp::ElementId::Id(oid) => oid.clone(),
                    };
                    if (*index) == new_underlying.len() {
                        new_underlying.push_back(Box::new((op_id, None)));
                    } else {
                        new_underlying.insert(*index, Box::new((op_id, None)));
                    }
                }
            };
        }
        let mut changes = StateTreeChange::empty();
        for (index, prop_diff) in new_props.diff.iter() {
            let mut diff_iter = prop_diff.iter();
            match diff_iter.next() {
                None => {
                    new_underlying.remove(*index);
                }
                Some((opid, diff)) => {
                    let current_objects =
                        changes.objects().union(new_props.current_objects.clone());
                    let entry = new_underlying.get_mut(*index);
                    match entry {
                        Some(e) => {
                            let mut updated_node = match &e.1 {
                                Some(n) => n.apply_diff(
                                    opid,
                                    DiffToApply {
                                        current_objects: current_objects.clone(),
                                        parent_object_id: object_id,
                                        parent_key: opid,
                                        diff,
                                    },
                                )?,
                                None => T::construct(
                                    opid,
                                    DiffToApply {
                                        current_objects: current_objects.clone(),
                                        parent_object_id: object_id,
                                        parent_key: opid,
                                        diff,
                                    },
                                )?,
                            };
                            let mut diffiter2 = diff_iter.map(|(oid, diff)| {
                                (
                                    oid,
                                    DiffToApply {
                                        current_objects: current_objects.clone(),
                                        parent_object_id: object_id,
                                        parent_key: oid,
                                        diff,
                                    },
                                )
                            });
                            updated_node = updated_node
                                .try_and_then(move |n| n.apply_diff_iter(&mut diffiter2))?;
                            changes += updated_node.change;
                            e.1 = Some(updated_node.value);
                        }
                        None => {
                            return Err(InvalidPatch::InvalidIndex {
                                object_id: object_id.clone(),
                                index: *index,
                            })
                        }
                    };
                }
            };
        }
        //This is where we maintain the invariant that allows us to provide an iterator over T
        //rather than Option<T>
        for (index, b) in new_underlying.iter().enumerate() {
            if b.1.is_none() {
                return Err(InvalidPatch::InvalidIndex {
                    object_id: object_id.clone(),
                    index,
                });
            }
        }
        let new_sequence = DiffableSequence {
            underlying: new_underlying,
        };
        Ok(DiffApplicationResult::pure(new_sequence).with_changes(changes))
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
                .update(index, Box::new((value.default_opid(), Some(value)))),
        }
    }

    pub(super) fn get(&self, index: usize) -> Option<&T> {
        self.underlying.get(index).and_then(|b| b.1.as_ref())
    }

    pub(super) fn insert(&mut self, index: usize, value: T) {
        self.underlying
            .insert(index, Box::new((value.default_opid(), Some(value))))
    }

    pub(super) fn mutate<F>(&mut self, index: usize, f: F)
    where
        F: FnOnce(T) -> T,
    {
        if let Some(entry) = self.underlying.get_mut(index) {
            if let Some(v) = entry.1.take() {
                entry.1 = Some(f(v));
            }
        }
    }

    pub(super) fn iter(&self) -> impl std::iter::Iterator<Item = &T> {
        // Making this unwrap safe is the entire point of this data structure
        self.underlying.iter().map(|b| b.1.as_ref().unwrap())
    }
}
