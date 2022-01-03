use crate::ActorId;
use std::fmt;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone)]
pub enum ExId {
    Root,
    Id(u64, ActorId, usize),
}

impl PartialEq for ExId {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ExId::Root, ExId::Root) => true,
            (ExId::Id(ctr1, actor1, _), ExId::Id(ctr2, actor2, _))
                if ctr1 == ctr2 && actor1 == actor2 =>
            {
                true
            }
            _ => false,
        }
    }
}

impl Eq for ExId {}

impl fmt::Display for ExId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExId::Root => write!(f, "_root"),
            ExId::Id(ctr, actor, _) => write!(f, "{}@{}", ctr, actor),
        }
    }
}

impl Hash for ExId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            ExId::Root => 0.hash(state),
            ExId::Id(ctr, actor, _) => {
                ctr.hash(state);
                actor.hash(state);
            }
        }
    }
}
