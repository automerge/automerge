// This module contains types which are intended to be exposed to users of the automerge library
// rather than being used internally. Internally we use a variety of types (`Key`, `ElemId`, `ObjId`)
// which are based on the internal `OpId`. `OpId` is designed to be extremely lightweight, it is
// just a (u64, usize). This means it can be cheaply copied and many of them can fit in a cache
// line but to achieve this the "actor ID" component of an `OpId` is actually just an index into an
// internal array of actor IDs. This means that we prefer not to expose `OpId` to API users -
// `OpId`s would only be usable with the document they were generated for and we would need to
// either build a bunch of tricky type system magic to ensure that users do not mix up their
// documents and ops or just document everywhere that `OpId`s are giant footguns and please be
// careful.
//
// What we want to achieve for external OpIds (external meaning "exposed to the user") is the following:
// - OpIds are just values. You can clone them, store them, use them from one document to another
//   without thinking about it.
// - Using OpIds with the mutation APIs of automerge documents is natural and performant
//
// The first requirement gives us a straightforward specification for an external `OpId`. It must
// contain both the counter and the actual bytes of the actor ID - otherwise we would need to
// include a reference to a document and some kind of lifetime, serialization becomes tricky, etc.
// etc.
//
// The second requirement is more interesting. Frequently within an automerge document you would
// like to do things like this:
//
//     let list_id = doc.set(ObjId::Root, "list", automerge::Value::list()).unwrap().unwrap();
//     doc.set(&list_id, 0, "first").unwrap()
//
// We would like the first argument of `doc.set` to be an `ObjectId` enum, this is so that we
// can distinguish between setting on the `Root` object ID and setting on some contained object; on
// the other hand `doc.set` should return an external `OpId` - it may after all not be creating an
// object. How do we line this types up? The basic principle is that we represent all external
// types which may contain an external OpId as wrappers which contain a reference to an external
// OpId, we use `Into<ObjId>` (or `Into<Key>` or `Into<ElemId>` etc.) as the arguments to `doc.set`
// or similar functions, finally we implement `Into<ObjId> for OpId` by just wrapping the
// referenced opid.
//
// For example, Object ID looks like this:
//
//     enum ObjId<'a> {
//          Root,
//          Op(Cow<'a, OpId>)
//     }
//
//  Note that we use a `Cow` to contain the OpId, this allows us to create `ObjId<'static>` when
//  deserializing and it allows us to present an `ObjId::into_owned() -> ObjId<'static>` for
//  situations where you need to take ownership of the underlying OpId`.
//
//  Then we implement `Into<ObjId> for &OpId` like so:
//
//      impl<'a> Into<ObjId> for &'a OpId {
//          fn from(op: &'a OpId) -> ObjId<'a> {
//              Self::Op(Cow::Borrowed(op))
//          }
//      }
//
//  Finally the signature of `Automerge::set` looks like this:
//
//      fn set<O: Into<ObjId<'_>>(&mut self, objid: O, ...)
//
//  Hopefully Rust's niche optimisation means that this has zero overhead compared to just the op
//  reference directly. But even if not it seems to have negligible impacts on performance.
use std::{
    borrow::Cow,
    fmt::{self, Display},
    str::FromStr,
};

use crate::{legacy, op_tree::OpSetMetadata, types::OpId, ActorId};

const ROOT_STR: &str = "_root";
const HEAD_STR: &str = "_head";

#[derive(Debug, PartialEq, Clone, Hash, Eq)]
pub struct ExternalOpId {
    counter: u64,
    actor: ActorId,
}

impl ExternalOpId {
    pub(crate) fn from_internal(opid: &OpId, metadata: &OpSetMetadata) -> Option<ExternalOpId> {
        metadata
            .actors
            .get_safe(opid.actor())
            .map(|actor| ExternalOpId {
                counter: opid.counter(),
                actor: actor.clone(),
            })
    }

    pub(crate) fn counter(&self) -> u64 {
        self.counter
    }

    pub(crate) fn actor(&self) -> &ActorId {
        &self.actor
    }
}

impl From<&legacy::OpId> for ExternalOpId {
    fn from(l: &legacy::OpId) -> Self {
        ExternalOpId {
            counter: l.counter(),
            actor: l.actor().clone(),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Hash, Eq)]
pub enum ExternalObjId<'a> {
    Root,
    Op(Cow<'a, ExternalOpId>),
}

impl<'a> ExternalObjId<'a> {
    pub fn into_owned(self) -> ExternalObjId<'static> {
        match self {
            Self::Root => ExternalObjId::Root,
            Self::Op(cow) => ExternalObjId::Op(Cow::Owned(cow.into_owned())),
        }
    }
}

impl From<&legacy::ObjectId> for ExternalObjId<'static> {
    fn from(l: &legacy::ObjectId) -> Self {
        match l {
            legacy::ObjectId::Root => ExternalObjId::Root,
            legacy::ObjectId::Id(opid) => ExternalObjId::Op(Cow::Owned(ExternalOpId::from(opid))),
        }
    }
}

impl From<&legacy::OpId> for ExternalObjId<'static> {
    fn from(l: &legacy::OpId) -> Self {
        ExternalObjId::Op(Cow::Owned(ExternalOpId {
            counter: l.counter(),
            actor: l.actor().clone(),
        }))
    }
}

impl<'a> From<&'a ExternalOpId> for ExternalObjId<'a> {
    fn from(op: &'a ExternalOpId) -> Self {
        ExternalObjId::Op(Cow::Borrowed(op))
    }
}

impl From<ExternalOpId> for ExternalObjId<'static> {
    fn from(op: ExternalOpId) -> Self {
        ExternalObjId::Op(Cow::Owned(op))
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    #[error("op IDs did not have format <counter: usize>@<hex encoded actor>")]
    BadFormat,
    #[error("the counter was not a positive integer")]
    InvalidCounter,
    #[error("the actor was not valid hex encoded bytes")]
    InvalidActor,
}

impl FromStr for ExternalOpId {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split('@');
        let first_part = parts.next().ok_or(ParseError::BadFormat)?;
        let second_part = parts.next().ok_or(ParseError::BadFormat)?;
        let counter: u64 = first_part.parse().map_err(|_| ParseError::InvalidCounter)?;
        let actor: ActorId = second_part.parse().map_err(|_| ParseError::InvalidActor)?;
        Ok(ExternalOpId { counter, actor })
    }
}

impl FromStr for ExternalObjId<'static> {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == ROOT_STR {
            Ok(ExternalObjId::Root)
        } else {
            let op = s.parse::<ExternalOpId>()?;
            Ok(ExternalObjId::Op(Cow::Owned(op)))
        }
    }
}

impl Display for ExternalOpId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.counter, self.actor)
    }
}

impl<'a> Display for ExternalObjId<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Root => write!(f, "{}", ROOT_STR),
            Self::Op(op) => write!(f, "{}", op),
        }
    }
}

pub enum ExternalElemId<'a> {
    Head,
    Op(Cow<'a, ExternalOpId>),
}

impl<'a> ExternalElemId<'a> {
    pub fn into_owned(&'a self) -> ExternalElemId<'static> {
        match self {
            ExternalElemId::Head => ExternalElemId::Head,
            ExternalElemId::Op(op) => ExternalElemId::Op(Cow::Owned(op.clone().into_owned())),
        }
    }
}

impl<'a> From<ExternalOpId> for ExternalElemId<'static> {
    fn from(op: ExternalOpId) -> Self {
        ExternalElemId::Op(Cow::Owned(op))
    }
}

impl<'a> From<&'a ExternalOpId> for ExternalElemId<'a> {
    fn from(opid: &'a ExternalOpId) -> Self {
        ExternalElemId::Op(Cow::Borrowed(opid))
    }
}

impl<'a> Display for ExternalElemId<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Head => write!(f, "{}", HEAD_STR),
            Self::Op(op) => write!(f, "{}", op),
        }
    }
}

pub enum ExternalKey<'a> {
    Map(Cow<'a, str>),
    Seq(ExternalElemId<'a>),
}

impl<'a> ExternalKey<'a> {
    pub fn into_owned(self) -> ExternalKey<'static> {
        match self {
            Self::Map(key) => ExternalKey::Map(key.into_owned().into()),
            Self::Seq(elemid) => ExternalKey::Seq(elemid.into_owned()),
        }
    }
}

impl<'a> From<&'a ExternalOpId> for ExternalKey<'a> {
    fn from(op: &'a ExternalOpId) -> Self {
        ExternalKey::Seq(op.into())
    }
}

impl From<ExternalOpId> for ExternalKey<'static> {
    fn from(op: ExternalOpId) -> Self {
        ExternalKey::Seq(op.into())
    }
}

impl<'a> From<&'a str> for ExternalKey<'a> {
    fn from(s: &'a str) -> Self {
        ExternalKey::Map(Cow::Borrowed(s))
    }
}

impl From<String> for ExternalKey<'static> {
    fn from(key: String) -> Self {
        ExternalKey::Map(Cow::Owned(key))
    }
}

impl<'a> From<ExternalElemId<'a>> for ExternalKey<'a> {
    fn from(elemid: ExternalElemId<'a>) -> Self {
        ExternalKey::Seq(elemid)
    }
}

impl<'a> Display for ExternalKey<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Map(k) => write!(f, "{}", k),
            Self::Seq(op) => write!(f, "{}", op),
        }
    }
}
