use std::{borrow::Cow, fmt::Display, str::FromStr};

use crate::{op_tree::OpSetMetadata, types::OpId, ActorId};

const ROOT_STR: &str = "_root";

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

#[derive(Debug, PartialEq, Clone, Hash, Eq)]
pub enum ExternalObjId<'a> {
    Root,
    Op(Cow<'a, ExternalOpId>),
}

impl<'a> ExternalObjId<'a> {
    pub fn into_owned(self) -> ExternalObjId<'static> {
        match self {
            Self::Root => ExternalObjId::Root,
            Self::Op(cow) => ExternalObjId::Op(Cow::<'static, _>::Owned(cow.into_owned().into())),
        }
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
    #[error("op IDs should have the format <counter>@<hex encoded actor>")]
    BadFormat,
    #[error("the counter of an opid should be a positive integer")]
    InvalidCounter,
    #[error("the actor of an opid should be valid hex encoded bytes")]
    InvalidActor,
}

impl FromStr for ExternalOpId {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split("@");
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
            let op = s.parse::<ExternalOpId>()?.into();
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
