use crate::ActorId;
use std::cmp::{Ord, Ordering};
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

impl Ord for ExId {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (ExId::Root, ExId::Root) => Ordering::Equal,
            (ExId::Root, _) => Ordering::Less,
            (_, ExId::Root) => Ordering::Greater,
            (ExId::Id(c1, a1, _), ExId::Id(c2, a2, _)) if c1 == c2 => a2.cmp(a1),
            (ExId::Id(c1, _, _), ExId::Id(c2, _, _)) => c1.cmp(c2),
        }
    }
}

impl PartialOrd for ExId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
