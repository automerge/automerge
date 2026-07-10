use crate::exid::ExId;
use crate::ActorId;
use serde::{Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

/// An identifier for a change in a document
///
/// A change is identified by the pair of the [`ActorId`] which made it and
/// the sequence number of the change in that actor's history (1-based). This
/// is a stable identity: it is the same in every document containing the
/// change, and unlike a [`crate::ChangeHash`] it is meaningful to humans.
///
/// `ChangeId`s are obtained from methods such as
/// [`crate::Automerge::get_heads`] or [`crate::Change::id`]. If you are
/// holding [`crate::ChangeHash`]es (e.g. from [`crate::Change::deps`] or the
/// sync protocol) convert them with
/// [`crate::Automerge::hashes_to_change_ids`].
///
/// # Example
///
/// ```
/// use automerge::{transaction::Transactable, AutoCommit, ChangeId, ReadDoc, ROOT};
///
/// let mut doc = AutoCommit::new();
/// doc.put(ROOT, "key", "first").unwrap();
/// let id = doc.commit().unwrap();
/// assert_eq!(id.seq(), 1);
/// assert_eq!(id.actor(), doc.get_actor());
///
/// // read the document as of that change
/// doc.put(ROOT, "key", "second").unwrap();
/// doc.commit();
/// let (old, _) = doc.get_at(ROOT, "key", &[id.clone()]).unwrap().unwrap();
/// assert_eq!(old.to_str(), Some("first"));
///
/// // ids display as "seq@actor" and can be parsed back
/// let parsed: ChangeId = id.to_string().parse().unwrap();
/// assert_eq!(parsed, id);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChangeId(pub(crate) ExId);

impl ChangeId {
    pub(crate) fn new(seq: u64, actor: ActorId, actor_idx_hint: usize) -> Self {
        ChangeId(ExId::Id(seq, actor, actor_idx_hint))
    }

    /// The actor which made the change
    pub fn actor(&self) -> &ActorId {
        match &self.0 {
            ExId::Id(_, actor, _) => actor,
            // A ChangeId can only be constructed from ExId::Id
            ExId::Root => unreachable!("ChangeId cannot be Root"),
        }
    }

    /// The 1-based sequence number of the change in the actor's history
    pub fn seq(&self) -> u64 {
        match &self.0 {
            ExId::Id(seq, ..) => *seq,
            ExId::Root => unreachable!("ChangeId cannot be Root"),
        }
    }
}

impl fmt::Display for ChangeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // "seq@actorhex", matching ExId's display format
        write!(f, "{}", self.0)
    }
}

/// Error returned when parsing a string as a [`ChangeId`]
#[derive(Debug, thiserror::Error)]
pub enum ParseChangeIdError {
    #[error("change id must be of the form <seq>@<actor>")]
    Format,
    #[error("invalid sequence number: {0}")]
    Seq(String),
    #[error("sequence number must be greater than zero")]
    ZeroSeq,
    #[error("invalid actor id: {0}")]
    Actor(String),
}

impl FromStr for ChangeId {
    type Err = ParseChangeIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (seq, actor) = s.split_once('@').ok_or(ParseChangeIdError::Format)?;
        let seq: u64 = seq
            .parse()
            .map_err(|e: std::num::ParseIntError| ParseChangeIdError::Seq(e.to_string()))?;
        if seq == 0 {
            return Err(ParseChangeIdError::ZeroSeq);
        }
        let actor =
            ActorId::from_str(actor).map_err(|e| ParseChangeIdError::Actor(e.to_string()))?;
        Ok(ChangeId::new(seq, actor, 0))
    }
}

impl Serialize for ChangeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_and_parse_roundtrip() {
        let actor = ActorId::from(&[0xaa, 0xbb, 0xcc][..]);
        let id = ChangeId::new(7, actor.clone(), 3);
        assert_eq!(id.to_string(), "7@aabbcc");
        let parsed: ChangeId = "7@aabbcc".parse().unwrap();
        // hint differs (0 vs 3) but is non-semantic
        assert_eq!(parsed, id);
        assert_eq!(parsed.actor(), &actor);
        assert_eq!(parsed.seq(), 7);
    }

    #[test]
    fn parse_rejects_bad_input() {
        assert!("no-at-sign".parse::<ChangeId>().is_err());
        assert!("x@aabb".parse::<ChangeId>().is_err());
        assert!("0@aabb".parse::<ChangeId>().is_err());
        assert!("1@not-hex".parse::<ChangeId>().is_err());
    }
}
