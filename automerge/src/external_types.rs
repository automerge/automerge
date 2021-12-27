use std::{str::FromStr, borrow::Cow, fmt::Display};

use crate::{ActorId, types::OpId, op_tree::OpSetMetadata};

const ROOT_STR: &str = "_root";

#[derive(Copy, Debug, PartialEq, Clone, Hash, Eq)]
pub struct ExternalOpId<'a> {
    counter: u64,
    actor: Cow<'a, ActorId>,
}

impl<'a> ExternalOpId<'a> {
    pub(crate) fn from_internal(opid: OpId, metadata: &OpSetMetadata) -> Option<ExternalOpId> {
        metadata.actors.get_safe(opid.actor()).map(|actor| {
            ExternalOpId{
                counter: opid.counter(),
                actor: actor.into(),
            }
        })
    }

    pub(crate) fn into_opid(self, metadata: &mut OpSetMetadata) -> OpId {
        let actor = metadata.actors.cache(self.actor);
        OpId::new(self.counter, actor)
    }
}

#[derive(Debug, PartialEq, Clone, Hash, Eq)]
pub enum ExternalObjId<'a> {
    Root,
    Op(ExternalOpId<'a>),
}

impl<'a> From<ExternalOpId<'a>> for ExternalObjId<'a> {
    fn from(op: ExternalOpId) -> Self {
        ExternalObjId::Op(op)
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

impl FromStr for ExternalOpId<'static> {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split("@");
        let first_part = parts.next().ok_or(ParseError::BadFormat)?;
        let second_part = parts.next().ok_or(ParseError::BadFormat)?;
        let counter: u64 = first_part.parse().map_err(|_| ParseError::InvalidCounter)?;
        let actor: ActorId = second_part.parse().map_err(|_| ParseError::InvalidActor)?; 
        Ok(ExternalOpId{counter, actor})
    }
}

impl<'a> FromStr for ExternalObjId<'a> {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == ROOT_STR {
            Ok(ExternalObjId::Root)
        } else {
            Ok(s.parse::<ExternalOpId>()?.into())
        }
    }
}

impl Display for ExternalOpId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.counter, self.actor)
    }
}

impl Display for ExternalObjId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Root => write!(f, "{}", ROOT_STR),
            Self::Op(op) => write!(f, "{}", op),
        }
    }
}
