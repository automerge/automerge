//! Persistent visibility filters.
//!
//! A [`Filter`] tells the document which changes should be rendered. Changes
//! that are rejected by the active filter are still ingested and synced to
//! peers — only their effect on the rendered state is suppressed. Changing
//! the filter rebuilds the op-set index in one O(n) pass; afterwards reads
//! stay at full speed because the index itself is the filtered view.
//!
//! Each rule applies to a *scope* (the document default, an [`Author`], or
//! an [`ActorId`]) and selects one of three behaviours:
//!
//! * [`Rule::Allow`] — accept every matching change.
//! * [`Rule::AllowUpTo`] — accept matching changes that are ancestors of the
//!   given heads. Heads do not have to be known to the document yet; a rule
//!   referencing an unseen head takes effect once the change arrives.
//! * [`Rule::Deny`] — reject every matching change.
//!
//! Resolution for a given actor is *most-specific-wins*: an [`ActorId`] rule
//! overrides an [`Author`] rule, which overrides the document default.
//!
//! # Examples
//!
//! Reject every change made by an author after a known set of heads:
//!
//! ```ignore
//! let filter = Filter::default()
//!     .with_author(alice.clone(), Rule::AllowUpTo { heads: known_good });
//! doc.set_filter(filter);
//! ```
//!
//! Render only a validated prefix, but keep accepting changes from the
//! local actor and from one trusted peer:
//!
//! ```ignore
//! let filter = Filter {
//!     default: Rule::AllowUpTo { heads: validated_heads },
//!     ..Filter::default()
//! }
//! .with_actor(local_actor.clone(), Rule::Allow)
//! .with_actor(peer_actor.clone(), Rule::Allow);
//! doc.set_filter(filter);
//! ```

use std::collections::BTreeMap;

use crate::types::{ActorId, Author};
use crate::ChangeHash;

/// What the filter does with changes that fall in a rule's scope.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum Rule {
    /// Accept every matching change.
    #[default]
    Allow,
    /// Accept matching changes only if they are ancestors of `heads`.
    ///
    /// Heads do not need to be present in the document at the time the rule
    /// is set: a head referenced here that arrives later is honoured as
    /// soon as it lands.
    AllowUpTo { heads: Vec<ChangeHash> },
    /// Reject every matching change.
    Deny,
}

/// A persistent visibility filter. See the [module docs][self] for an
/// overview.
///
/// `Filter` is a pure rule set — the document holds onto it across saves
/// and loads, and resolves it into a per-actor mask whenever it changes.
/// Construct one with [`Filter::default`] and the `with_*` builders, or by
/// populating the public fields directly.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Filter {
    /// Applied to changes that don't match a more specific rule.
    pub default: Rule,
    /// Per-author overrides. Override the [`Filter::default`] rule for any
    /// actor whose change carries this author.
    pub authors: BTreeMap<Author, Rule>,
    /// Per-actor overrides. Override both [`Filter::default`] and any
    /// matching [`Filter::authors`] rule.
    pub actors: BTreeMap<ActorId, Rule>,
}

impl Filter {
    /// An empty filter — every change is accepted.
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the default rule.
    pub fn with_default(mut self, rule: Rule) -> Self {
        self.default = rule;
        self
    }

    /// Set or replace the rule for `author`.
    pub fn with_author(mut self, author: Author, rule: Rule) -> Self {
        self.authors.insert(author, rule);
        self
    }

    /// Set or replace the rule for `actor`.
    pub fn with_actor(mut self, actor: ActorId, rule: Rule) -> Self {
        self.actors.insert(actor, rule);
        self
    }

    /// Whether the filter is identical to [`Filter::default`].
    ///
    /// When this is true the filter accepts every change, and read paths
    /// can skip filter-related work entirely.
    pub(crate) fn is_noop(&self) -> bool {
        matches!(self.default, Rule::Allow) && self.authors.is_empty() && self.actors.is_empty()
    }
}

/// The rule that applies to a particular actor, after most-specific-wins
/// resolution. Borrows the rule out of the filter to avoid cloning the
/// (potentially long) head list inside `AllowUpTo`.
pub(crate) enum ResolvedRule<'a> {
    Allow,
    AllowUpTo(&'a [ChangeHash]),
    Deny,
}

impl<'a> From<&'a Rule> for ResolvedRule<'a> {
    fn from(r: &'a Rule) -> Self {
        match r {
            Rule::Allow => ResolvedRule::Allow,
            Rule::AllowUpTo { heads } => ResolvedRule::AllowUpTo(heads.as_slice()),
            Rule::Deny => ResolvedRule::Deny,
        }
    }
}

impl Filter {
    /// Every head referenced by any rule. Used by the change graph to
    /// track which rules are waiting on changes that haven't arrived yet.
    pub(crate) fn referenced_heads(&self) -> impl Iterator<Item = &ChangeHash> {
        fn heads_of(rule: &Rule) -> &[ChangeHash] {
            match rule {
                Rule::AllowUpTo { heads } => heads.as_slice(),
                _ => &[],
            }
        }
        heads_of(&self.default)
            .iter()
            .chain(self.authors.values().flat_map(|r| heads_of(r).iter()))
            .chain(self.actors.values().flat_map(|r| heads_of(r).iter()))
    }
}
