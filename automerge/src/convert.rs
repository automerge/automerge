//! Types for converting between different OpId representations
//!
//! In various places throughout the codebase we refer to operation IDs. The canonical type for
//! representing an operation ID is [`crate::types::OpId`]. This type holds the counter of the operation
//! ID but it does not store the actor ID, instead storing an index into an array of actor IDs
//! stored elsewhere. This makes using OpIds very memory efficient. We also store operation IDs on
//! disc. Here again we use a representation where the actor ID is stored as an offset into an
//! array which is held elsewhere. We occasionally do need to refer to an operation ID which
//! contains the full actor ID - typically when exporting to other processes or to the user.
//!
//! This is problematic when we want to write code which is generic over all these representations,
//! or which needs to convert between them. This module hopes to solve that problem. The basic
//! approach is to define the trait `OpId`, which is generic over the type of its `actor`. Using a
//! trait means that there is no need to allocate intermediate collections of operation IDs when
//! converting (for example when encoding a bunch of OpSet operation IDs into a change, where we
//! have to translate the indices).
//!
//! Having defined the `OpId` trait we then define a bunch of enums representing each of the
//! entities in the automerge data model which contain an `OpId`, namely `ObjId`, `Key`, and
//! `ElemId`. Each of these enums implements a `map` method, which allows you to convert the actor
//! ID of any contained operation using a mappping function.

use std::borrow::Cow;

pub(crate) trait OpId<ActorId> {
    fn actor(&self) -> ActorId;
    fn counter(&self) -> u64;
}

#[derive(Clone, Debug)]
pub(crate) enum ObjId<O> {
    Root,
    Op(O),
}

impl<O> ObjId<O> {
    pub(crate) fn map<F, P>(self, f: F) -> ObjId<P>
    where
        F: Fn(O) -> P,
    {
        match self {
            ObjId::Root => ObjId::Root,
            ObjId::Op(o) => ObjId::Op(f(o)),
        }
    }
}

#[derive(Clone)]
pub(crate) enum ElemId<O> {
    Head,
    Op(O),
}

impl<O> ElemId<O> {
    pub(crate) fn map<F, P>(self, f: F) -> ElemId<P>
    where
        F: Fn(O) -> P,
    {
        match self {
            ElemId::Head => ElemId::Head,
            ElemId::Op(o) => ElemId::Op(f(o)),
        }
    }
}

#[derive(Clone)]
pub(crate) enum Key<'a, O> {
    Prop(Cow<'a, smol_str::SmolStr>),
    Elem(ElemId<O>),
}

impl<'a, O> Key<'a, O> {
    pub(crate) fn map<F, P>(self, f: F) -> Key<'a, P>
    where
        F: Fn(O) -> P,
    {
        match self {
            Key::Prop(p) => Key::Prop(p),
            Key::Elem(e) => Key::Elem(e.map(f)),
        }
    }
}

impl OpId<usize> for crate::types::OpId {
    fn counter(&self) -> u64 {
        self.counter()
    }

    fn actor(&self) -> usize {
        self.actor()
    }
}

impl<'a> OpId<usize> for &'a crate::types::OpId {
    fn counter(&self) -> u64 {
        crate::types::OpId::counter(self)
    }

    fn actor(&self) -> usize {
        crate::types::OpId::actor(self)
    }
}
