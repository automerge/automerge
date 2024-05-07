use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
};

use crate::convert;

use super::AsChangeOp;

/// This struct represents the ordering of actor indices in a change chunk. Operations in a change
/// chunk are encoded with the actor ID represented as an offset into an array of actors which are
/// encoded at the start of the chunk. This array is in a specific order: the author of the change
/// is always the first actor, then all other actors referenced in a change are encoded in
/// lexicographic order.
///
/// The intended usage is to construct a `ChangeActors` from an iterator over `AsChangeOp` where
/// the `ActorId` of the `AsChangeOp` implementation is the original actor ID. The resulting
/// `ChangeActors` implements `Iterator` where the `item` implements
/// `AsChangeOp<OpId=convert::OpId<usize>>`, which can be passed to `ChangeOpColumns::encode`.
///
/// Once encoding is complete you can use `ChangeActors::done` to retrieve the original actor and the
/// other actors in the change.
///
/// # Note on type parameters
///
/// The type paramters are annoying, they basically exist because we can't have generic associated
/// types, so we have to feed the concrete types of the associated types of the `AsChangeOp`
/// implementation through here. Here's what they all refer to:
///
/// * A - The type of the actor ID used in the operation IDs of the incoming changes
/// * I - The type of the iterator over the `AsChangeOp` implementation of the incoming changes
/// * O - The concrete type of the operation ID which implementas `convert::OpId`
/// * C - The concrete type (which implements `AsChangeOp`) of the incoming changes
/// * 'a - The lifetime bound for the AsChangeOp trait and it's associated types
///
/// Maybe when GATs land we can make this simpler.
pub(crate) struct ChangeActors<'a, ActorId, I, O, C> {
    actor: ActorId,
    other_actors: Vec<ActorId>,
    index: BTreeMap<ActorId, usize>,
    wrapped: I,
    num_ops: usize,
    _phantom: std::marker::PhantomData<(&'a O, C)>,
}

#[derive(Debug, thiserror::Error)]
#[error("pred OpIds out of order")]
pub(crate) struct PredOutOfOrder;

impl<'a, A, I, O, C> ChangeActors<'a, A, I, O, C>
where
    A: PartialEq + Ord + Clone + std::hash::Hash + 'static,
    O: convert::OpId<&'a A> + 'a,
    C: AsChangeOp<'a, OpId = O> + 'a,
    I: Iterator<Item = C> + Clone + 'a,
{
    /// Create a new change actor mapping
    ///
    /// # Arguments
    /// * actor - the actor ID of the actor who authored this change
    /// * ops - an iterator containing the operations which will be encoded into the change
    ///
    /// # Errors
    /// * If one of the ops herein contains a `pred` with ops which are not in lamport timestamp
    ///   order
    pub(crate) fn new(actor: A, ops: I) -> Result<ChangeActors<'a, A, I, O, C>, PredOutOfOrder> {
        // Change actors indices are encoded with the 0th element being the actor who authored the
        // change and all other actors referenced in the chain following the author in
        // lexicographic order. Here we collect all the actors referenced by operations in `ops`
        let (num_ops, mut other_actors) =
            ops.clone()
                .try_fold((0, BTreeSet::new()), |(count, mut acc), op| {
                    if let convert::Key::Elem(convert::ElemId::Op(o)) = op.key() {
                        if o.actor() != &actor {
                            acc.insert(o.actor());
                        }
                    }

                    if !are_sorted(op.pred()) {
                        return Err(PredOutOfOrder);
                    }
                    for pred in op.pred() {
                        if pred.actor() != &actor {
                            acc.insert(pred.actor());
                        }
                    }
                    if let convert::ObjId::Op(o) = op.obj() {
                        if o.actor() != &actor {
                            acc.insert(o.actor());
                        }
                    }
                    Ok((count + 1, acc))
                })?;
        // This shouldn't be necessary but just in case
        other_actors.remove(&actor);
        let mut other_actors = other_actors.into_iter().cloned().collect::<Vec<_>>();
        other_actors.sort();
        let index = std::iter::once(actor.clone())
            .chain(other_actors.clone())
            .enumerate()
            .map(|(idx, actor)| (actor, idx))
            .collect();
        Ok(ChangeActors {
            actor,
            other_actors,
            index,
            wrapped: ops,
            num_ops,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Translate an OpID from the OpSet index to the change index
    fn translate_opid(&self, opid: &O) -> ChangeOpId {
        ChangeOpId {
            actor: *self.index.get(opid.actor()).unwrap(),
            counter: opid.counter(),
        }
    }

    /// Returns a clonable iterator over the converted operations. The item of the iterator is an
    /// implementation of `AsChangeOp` which uses the index of the actor of each operation into the
    /// actors as encoded in a change. This is suitable for passing to `ChangeOpColumns::encode`
    pub(crate) fn iter<'b>(&'b self) -> WithChangeActorsOpIter<'b, 'a, A, I, O, C> {
        WithChangeActorsOpIter {
            change_actors: self,
            inner: self.wrapped.clone(),
        }
    }

    pub(crate) fn done(self) -> (A, Vec<A>) {
        (self.actor, self.other_actors)
    }
}

/// The actual implementation of the converted iterator
pub(crate) struct WithChangeActorsOpIter<'actors, 'aschangeop, A, I, O, C> {
    change_actors: &'actors ChangeActors<'aschangeop, A, I, O, C>,
    inner: I,
}

impl<'actors, 'aschangeop, A: 'aschangeop, I, O, C> Clone
    for WithChangeActorsOpIter<'actors, 'aschangeop, A, I, O, C>
where
    I: Clone,
{
    fn clone(&self) -> Self {
        Self {
            change_actors: self.change_actors,
            inner: self.inner.clone(),
        }
    }
}

impl<'actors, 'aschangeop, A: 'aschangeop, I, O, C> Iterator
    for WithChangeActorsOpIter<'actors, 'aschangeop, A, I, O, C>
where
    C: AsChangeOp<'aschangeop, OpId = O>,
    O: convert::OpId<&'aschangeop A>,
    I: Iterator<Item = C> + Clone,
{
    type Item = WithChangeActors<'actors, 'aschangeop, A, I, O, C>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|o| WithChangeActors {
            op: o,
            actors: self.change_actors,
        })
    }
}

impl<'actors, 'aschangeop, A: 'aschangeop, I, O, C> ExactSizeIterator
    for WithChangeActorsOpIter<'actors, 'aschangeop, A, I, O, C>
where
    C: AsChangeOp<'aschangeop, OpId = O>,
    O: convert::OpId<&'aschangeop A>,
    I: Iterator<Item = C> + Clone,
{
    fn len(&self) -> usize {
        self.change_actors.num_ops
    }
}

pub(crate) struct ChangeOpId {
    actor: usize,
    counter: u64,
}

impl convert::OpId<usize> for ChangeOpId {
    fn actor(&self) -> usize {
        self.actor
    }

    fn counter(&self) -> u64 {
        self.counter
    }
}

/// A struct which implements `AsChangeOp` by translating the actor IDs in the incoming operations
/// into the index into the actors in the `ChangeActors`.
pub(crate) struct WithChangeActors<'actors, 'aschangeop, A, I, O, C> {
    op: C,
    actors: &'actors ChangeActors<'aschangeop, A, I, O, C>,
}

impl<'actors, 'aschangeop, A, I, O, P, C> AsChangeOp<'aschangeop>
    for WithChangeActors<'actors, 'aschangeop, A, I, O, C>
where
    A: PartialEq + Ord + Clone + std::hash::Hash + 'static,
    O: convert::OpId<&'aschangeop A>,
    P: Iterator<Item = O> + ExactSizeIterator + 'aschangeop,
    C: AsChangeOp<'aschangeop, PredIter = P, OpId = O> + 'aschangeop,
    I: Iterator<Item = C> + Clone + 'aschangeop,
{
    type ActorId = usize;
    type OpId = ChangeOpId;
    type PredIter = WithChangeActorsPredIter<'actors, 'aschangeop, A, I, O, C, P>;

    fn action(&self) -> u64 {
        self.op.action()
    }

    fn insert(&self) -> bool {
        self.op.insert()
    }

    fn pred(&self) -> Self::PredIter {
        WithChangeActorsPredIter {
            wrapped: self.op.pred(),
            actors: self.actors,
            _phantom: std::marker::PhantomData,
        }
    }

    fn key(&self) -> convert::Key<'aschangeop, Self::OpId> {
        self.op.key().map(|o| self.actors.translate_opid(&o))
    }

    fn obj(&self) -> convert::ObjId<Self::OpId> {
        self.op.obj().map(|o| self.actors.translate_opid(&o))
    }

    fn val(&self) -> std::borrow::Cow<'aschangeop, crate::ScalarValue> {
        self.op.val()
    }

    fn expand(&self) -> bool {
        self.op.expand()
    }

    fn mark_name(&self) -> Option<Cow<'aschangeop, smol_str::SmolStr>> {
        self.op.mark_name()
    }
}

pub(crate) struct WithChangeActorsPredIter<'actors, 'aschangeop, A, I, O, C, P> {
    wrapped: P,
    actors: &'actors ChangeActors<'aschangeop, A, I, O, C>,
    _phantom: std::marker::PhantomData<O>,
}

impl<'actors, 'aschangeop, A, I, O, C, P> ExactSizeIterator
    for WithChangeActorsPredIter<'actors, 'aschangeop, A, I, O, C, P>
where
    A: PartialEq + Ord + Clone + std::hash::Hash + 'static,
    O: convert::OpId<&'aschangeop A>,
    P: Iterator<Item = O> + ExactSizeIterator + 'aschangeop,
    C: AsChangeOp<'aschangeop, OpId = O> + 'aschangeop,
    I: Iterator<Item = C> + Clone + 'aschangeop,
{
    fn len(&self) -> usize {
        self.wrapped.len()
    }
}

impl<'actors, 'aschangeop, A, I, O, C, P> Iterator
    for WithChangeActorsPredIter<'actors, 'aschangeop, A, I, O, C, P>
where
    A: PartialEq + Ord + Clone + std::hash::Hash + 'static,
    O: convert::OpId<&'aschangeop A>,
    P: Iterator<Item = O> + 'aschangeop,
    C: AsChangeOp<'aschangeop, OpId = O> + 'aschangeop,
    I: Iterator<Item = C> + Clone + 'aschangeop,
{
    type Item = ChangeOpId;

    fn next(&mut self) -> Option<Self::Item> {
        self.wrapped.next().map(|o| self.actors.translate_opid(&o))
    }
}

fn are_sorted<A, O, I>(mut opids: I) -> bool
where
    A: PartialEq + Ord + Clone,
    O: convert::OpId<A>,
    I: Iterator<Item = O>,
{
    if let Some(first) = opids.next() {
        let mut prev = first;
        for opid in opids {
            if opid.counter() < prev.counter() {
                return false;
            }
            if opid.counter() == prev.counter() && opid.actor() < prev.actor() {
                return false;
            }
            prev = opid;
        }
    }
    true
}
