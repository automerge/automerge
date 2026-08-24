use core::fmt;
use std::str::FromStr;

use crate::error;

/// [`Authors`] records change authorship in an Automerge document.
///
/// # Authors and Actors
///
/// An [`Author`] consists of free-form bytes, to identify a single author.
/// For each author, there will be one or more actors associated with the
/// author, giving a 1-to-N relationship. [`Authors`] maintains an index of
/// these relationships.
///
/// # Current Author
///
/// [`Authors`] will also keep track of the current [`Author`] that is acting on
/// the document, if set.
#[derive(Clone, Debug, Default)]
pub(crate) struct Authors {
    /// Previously recorded [`Author`]s.
    ///
    /// The `Vec` must contain unique [`Authors`], and should be indexed by the
    /// existing actor index of an Automerge document.
    ///
    /// Each [`Author`] will have one or more [`ActorId`]s related to it, since
    /// a new [`ActorId`] is produced when an [`Author`] changes on a document.
    authors: Vec<Author>,
    // TODO(finto): This is never actually used. It's set to `None` and
    // re-assigned in `put_author`.
    /// The current [`Author`] acting on the document.
    ///
    /// Using the [`AuthorIdx`] to index into [`Authors::authors`].
    current: Option<AuthorIdx>,
    /// A mapping from an actor to an author.
    ///
    /// Each index of the `Vec` is the same as the index into an actors set, and
    /// the entry at the index corresponds to an entry in [`Authors::authors`].
    actor_to_author: Vec<Option<AuthorIdx>>,
}

impl Authors {
    /// Initialize the [`Authors`] set with a known size of actors, setting all
    /// mapping entries to `None`.
    pub(crate) fn with_actors(actors: usize) -> Self {
        Self {
            authors: Vec::default(),
            current: None,
            actor_to_author: vec![None; actors],
        }
    }

    /// Return the set of previously recorded [`Author`]s.
    pub(crate) fn get_authors(&self) -> &[Author] {
        &self.authors
    }

    /// Return the [`Author`] that is associated with the given `actor`.
    pub(crate) fn get_author_for_actor(&self, actor: usize) -> Option<&Author> {
        let author = self.actor_to_author.get(actor)?.as_ref()?.as_usize();
        self.authors.get(author)
    }

    /// Return the actor indices for the given `author`.
    ///
    /// If the `author` exists, this will return at least one element in the
    /// iterator. Otherwise, the iterator will be empty.
    pub(crate) fn get_actors_for_author(
        &self,
        author: &Author,
    ) -> impl Iterator<Item = usize> + '_ {
        self.authors
            .binary_search(author)
            .ok()
            .map(|idx| {
                let idx = AuthorIdx::from(idx);
                self.actor_to_author
                    .iter()
                    .enumerate()
                    .filter_map(move |(i, a)| (a.as_ref()? == &idx).then_some(i))
            })
            .into_iter()
            .flatten()
    }

    /// Assign the given `actor` to the given `author`.
    pub(crate) fn assign_author(&mut self, author: Author, actor: usize) {
        let author_id = self.put_author(author);
        self.actor_to_author[actor] = Some(author_id);
    }

    /// Insert the `actor` into the actor-to-author mapping, setting the author
    /// as `None`.
    pub(crate) fn insert_actor(&mut self, actor: usize) {
        self.actor_to_author.insert(actor, None);
    }

    /// Remove the `actor` from the actor to author mapping.
    ///
    /// Note that this may leave a dangling [`Author`] in the set of authors,
    /// once the final actor has been removed. In reality, actors are only
    /// removed as part of rollback semantics, so that should never happen.
    pub(crate) fn remove_actor(&mut self, actor: usize) {
        self.actor_to_author.remove(actor);
    }

    /// [`Author`] is inserted into the set of authors, and the [`Author`] is
    /// ensured to be unique and in-order.
    ///
    /// When a new [`Author`] is being inserted, this function re-indexes.
    #[must_use]
    fn put_author(&mut self, author: Author) -> AuthorIdx {
        match self.authors.binary_search(&author) {
            Err(index) => {
                self.authors.insert(index, author);
                for a in self.actor_to_author.iter_mut().flatten() {
                    a.with_new_author(index)
                }
                if let Some(a) = self.current.as_mut() {
                    a.with_new_author(index);
                }
                index.into()
            }
            Ok(index) => index.into(),
        }
    }
}

/// An [`Author`] is identified by a free form set of bytes.
///
/// # Construction
///
/// An [`Author`] can be constructed from a raw set of bytes, either `Vec<u8>`
/// or `&[u8]`, or else by parsing string values (see [`hex::decode`]).
///
/// # Formatting
///
/// The author is formatted as a hexadecimal string (see [`hex::encode`]).
#[derive(PartialEq, Hash, Clone, Ord, Eq, PartialOrd)]
pub struct Author(Vec<u8>);

impl Author {
    /// Return the raw bytes of the [`Author`].
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Encode the [`Author`] to a hexadecimal string (see [`hex::encode`]).
    pub fn to_hex_string(&self) -> String {
        hex::encode(&self.0)
    }
}

impl fmt::Display for Author {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex_string())
    }
}

impl From<Vec<u8>> for Author {
    fn from(v: Vec<u8>) -> Self {
        Author(v)
    }
}

impl<'a> From<&'a [u8]> for Author {
    fn from(s: &'a [u8]) -> Self {
        Author(s.to_vec())
    }
}

impl fmt::Debug for Author {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Author")
            .field(&hex::encode(&self.0))
            .finish()
    }
}

impl TryFrom<&str> for Author {
    type Error = error::InvalidAuthor;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        hex::decode(s)
            .map(Author::from)
            .map_err(|_| error::InvalidAuthor(s.into()))
    }
}

impl TryFrom<String> for Author {
    type Error = error::InvalidAuthor;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        hex::decode(&s)
            .map(Author::from)
            .map_err(|_| error::InvalidAuthor(s))
    }
}

impl FromStr for Author {
    type Err = error::InvalidAuthor;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Author::try_from(s)
    }
}

/// An index into the [`Authors::authors`] field.
#[derive(PartialEq, Debug, Clone, Copy)]
struct AuthorIdx(usize);

impl AuthorIdx {
    fn as_usize(&self) -> usize {
        self.0
    }

    fn with_new_author(&mut self, idx: usize) {
        if self.0 >= idx {
            self.0 += 1;
        }
    }
}

impl From<usize> for AuthorIdx {
    fn from(n: usize) -> Self {
        AuthorIdx(n)
    }
}

#[cfg(test)]
mod tests {
    use super::{Author, Authors};
    use proptest::prelude::*;

    // === Generators ===

    fn gen_bytes() -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(any::<u8>(), 0..64)
    }

    fn gen_author() -> impl Strategy<Value = Author> {
        gen_bytes().prop_map(Author::from)
    }

    /// Authors drawn from a deliberately tiny space, so that operation
    /// sequences frequently re-use the same author and frequently insert
    /// authors that sort before existing ones. This exercises both branches
    /// of `Authors::put_author` and the `AuthorIdx` re-indexing.
    fn gen_small_author() -> impl Strategy<Value = Author> {
        proptest::collection::vec(0u8..4, 0..3).prop_map(Author::from)
    }

    /// A valid, possibly mixed-case hex string together with the bytes it
    /// encodes.
    fn gen_hex_string() -> impl Strategy<Value = (String, Vec<u8>)> {
        proptest::collection::vec((any::<u8>(), any::<bool>()), 0..64).prop_map(|pairs| {
            let mut s = String::new();
            let mut bytes = Vec::with_capacity(pairs.len());
            for (byte, upper) in pairs {
                let h = hex::encode([byte]);
                if upper {
                    s.push_str(&h.to_uppercase());
                } else {
                    s.push_str(&h);
                }
                bytes.push(byte);
            }
            (s, bytes)
        })
    }

    proptest! {
        /// For all authors `a`: `try_from(to_hex_string(a)) == Ok(a)`.
        #[test]
        fn hex_roundtrip(author in gen_author()) {
            prop_assert_eq!(Author::try_from(author.to_hex_string()).unwrap(), author.clone());
            prop_assert_eq!(Author::try_from(author.to_hex_string().as_str()).unwrap(), author);
        }

        /// For all authors `a`: `Display` output equals `to_hex_string(a)`,
        /// and parsing it back yields `a`.
        #[test]
        fn display_is_hex_and_fromstr_roundtrips(author in gen_author()) {
            let displayed = format!("{}", author);
            prop_assert_eq!(&displayed, &author.to_hex_string());
            prop_assert_eq!(displayed.parse::<Author>().unwrap(), author);
        }

        /// Every even-length hex string parses; the bytes are its decoding
        /// and re-encoding yields its lowercase form.
        #[test]
        fn valid_hex_parses_and_normalizes((s, bytes) in gen_hex_string()) {
            let author = Author::try_from(s.as_str()).unwrap();
            prop_assert_eq!(author.as_bytes(), &bytes[..]);
            prop_assert_eq!(author.to_hex_string(), s.to_lowercase());
        }

        /// Every odd-length hex string fails to parse.
        #[test]
        fn odd_length_hex_is_rejected((s, _) in gen_hex_string(), extra in 0u8..16) {
            let mut s = s;
            s.push(char::from_digit(u32::from(extra), 16).unwrap());
            prop_assert!(Author::try_from(s.as_str()).is_err());
        }

        /// Every string containing at least one non-hex character fails to
        /// parse.
        #[test]
        fn non_hex_char_is_rejected(
            (s, _) in gen_hex_string(),
            c in any::<char>().prop_filter("non-hex", |c| !c.is_ascii_hexdigit()),
            at in any::<proptest::sample::Index>(),
        ) {
            let mut s = s;
            // `s` is pure ASCII, so any byte index is a char boundary.
            let at = at.index(s.len() + 1);
            s.insert(at, c);
            prop_assert!(Author::try_from(s.as_str()).is_err());
        }

        /// For all byte vectors `b`: `Author::from(b).as_bytes() == b`.
        ///
        /// Construction from bytes is lossless: no normalization is applied.
        #[test]
        fn bytes_preserved(bytes in gen_bytes()) {
            let from_vec = Author::from(bytes.clone());
            let from_slice = Author::from(&bytes[..]);
            prop_assert_eq!(from_vec.as_bytes(), &bytes[..]);
            prop_assert_eq!(from_slice.as_bytes(), &bytes[..]);
        }

        /// For all authors `a`: `a` serializes to exactly the JSON string
        /// `to_hex_string(a)`, and deserializing that yields `a` back.
        #[test]
        fn serde_roundtrip_via_hex_string(author in gen_author()) {
            let value = serde_json::to_value(&author).unwrap();
            prop_assert_eq!(&value, &serde_json::Value::String(author.to_hex_string()));
            prop_assert_eq!(serde_json::from_value::<Author>(value).unwrap(), author);
        }

        /// For all byte vectors `x` and `y`: `Author(x) <= Author(y)` if
        /// and only if `x <= y`.
        ///
        /// `Ord` on `Author` must coincide with `Ord` on the underlying
        /// bytes, because `Authors::put_author` relies on it for
        /// `binary_search`.
        #[test]
        fn ord_agrees_with_bytes(a in gen_bytes(), b in gen_bytes()) {
            prop_assert_eq!(Author::from(a.clone()).cmp(&Author::from(b.clone())), a.cmp(&b));
        }
    }


    /// One step of an op sequence, applied by `apply` to the `Authors`
    /// under test and the model in lock-step.
    ///
    /// Indices are unconstrained at generation time; `apply` reduces them
    /// to valid slots.
    #[derive(Debug, Clone)]
    enum Op {
        Assign { author: Author, actor: usize },
        InsertActor { at: usize },
        RemoveActor { at: usize },
    }

    fn gen_ops() -> impl Strategy<Value = Vec<Op>> {
        proptest::collection::vec(
            prop_oneof![
                4 => (gen_small_author(), any::<usize>())
                    .prop_map(|(author, actor)| Op::Assign { author, actor }),
                1 => any::<usize>().prop_map(|at| Op::InsertActor { at }),
                1 => any::<usize>().prop_map(|at| Op::RemoveActor { at }),
            ],
            0..40,
        )
    }

    /// Apply `ops` to a fresh `Authors::with_actors(init_actors)` and to the
    /// model in lock-step. Raw indices are reduced modulo the current number
    /// of actor slots so the documented preconditions hold.
    ///
    /// The model for `Authors` is a plain `Vec<Option<Author>>` mapping each
    /// actor slot to its author. The real implementation maintains a sorted,
    /// deduplicated author list with re-indexed pointers into it; the model
    /// does none of that, so any re-indexing bug shows up as a divergence.
    fn apply(init_actors: usize, ops: &[Op]) -> (Authors, Vec<Option<Author>>) {
        let mut authors = Authors::with_actors(init_actors);
        let mut model: Vec<Option<Author>> = vec![None; init_actors];
        for op in ops {
            match op {
                Op::Assign { author, actor } => {
                    if model.is_empty() {
                        continue;
                    }
                    let actor = actor % model.len();
                    authors.assign_author(author.clone(), actor);
                    model[actor] = Some(author.clone());
                }
                Op::InsertActor { at } => {
                    let at = at % (model.len() + 1);
                    authors.insert_actor(at);
                    model.insert(at, None);
                }
                Op::RemoveActor { at } => {
                    if model.is_empty() {
                        continue;
                    }
                    let at = at % model.len();
                    authors.remove_actor(at);
                    model.remove(at);
                }
            }
        }
        (authors, model)
    }

    /// The observable state of an `Authors`, given `slots` actor slots.
    fn observe(authors: &Authors, slots: usize) -> (Vec<Author>, Vec<Option<Author>>) {
        (
            authors.get_authors().to_vec(),
            (0..slots)
                .map(|i| authors.get_author_for_actor(i).cloned())
                .collect(),
        )
    }

    proptest! {
        /// After any op sequence, `get_author_for_actor` agrees with the
        /// model at every actor slot, and returns `None` (rather than
        /// panicking) out of range.
        #[test]
        fn lookup_agrees_with_model(init in 0usize..8, ops in gen_ops()) {
            let (authors, model) = apply(init, &ops);
            for (actor, expected) in model.iter().enumerate() {
                prop_assert_eq!(authors.get_author_for_actor(actor), expected.as_ref());
            }
            // Out-of-range lookups are `None`, not a panic.
            prop_assert_eq!(authors.get_author_for_actor(model.len()), None);
        }

        /// After any op sequence, `get_actors_for_author` returns exactly
        /// the model's slots for that author, in ascending order; an author
        /// absent from `get_authors` yields no actors.
        #[test]
        fn reverse_lookup_agrees_with_model(
            init in 0usize..8,
            ops in gen_ops(),
            probe in gen_author(),
        ) {
            let (authors, model) = apply(init, &ops);
            for author in authors.get_authors() {
                let actual: Vec<usize> = authors.get_actors_for_author(author).collect();
                let expected: Vec<usize> = model
                    .iter()
                    .enumerate()
                    .filter_map(|(i, a)| (a.as_ref() == Some(author)).then_some(i))
                    .collect();
                prop_assert_eq!(actual, expected);
            }
            if !authors.get_authors().contains(&probe) {
                prop_assert_eq!(authors.get_actors_for_author(&probe).count(), 0);
            }
        }

        /// Forward and reverse lookups are mutually consistent, without
        /// reference to the model: if an actor maps to an author, that
        /// author's actors include it; and every actor listed for an author
        /// maps back to that author.
        #[test]
        fn forward_and_reverse_lookups_are_consistent(init in 0usize..8, ops in gen_ops()) {
            let (authors, model) = apply(init, &ops);
            for actor in 0..model.len() {
                if let Some(author) = authors.get_author_for_actor(actor) {
                    prop_assert!(authors.get_actors_for_author(author).any(|i| i == actor));
                }
            }
            for author in authors.get_authors() {
                for actor in authors.get_actors_for_author(author).collect::<Vec<_>>() {
                    prop_assert_eq!(authors.get_author_for_actor(actor), Some(author));
                }
            }
        }

        /// After any op sequence, `get_authors` is strictly sorted: sorted,
        /// with no duplicates.
        ///
        /// This is the invariant `Authors::put_author` relies on for
        /// `binary_search`.
        #[test]
        fn authors_are_sorted_and_unique(init in 0usize..8, ops in gen_ops()) {
            let (authors, _) = apply(init, &ops);
            prop_assert!(authors.get_authors().is_sorted_by(|a, b| a < b));
        }

        /// `assign_author(a, i)` makes actor `i` map to `a`, and assigning
        /// the same pair again leaves all observable state unchanged.
        #[test]
        fn assign_is_effective_and_idempotent(
            init in 1usize..8,
            ops in gen_ops(),
            author in gen_small_author(),
            actor in any::<usize>(),
        ) {
            let (mut authors, mut model) = apply(init, &ops);
            if model.is_empty() {
                // The ops may have removed every actor slot; restore one so
                // `assign_author`'s precondition holds.
                authors.insert_actor(0);
                model.push(None);
            }
            let actor = actor % model.len();

            authors.assign_author(author.clone(), actor);
            prop_assert_eq!(authors.get_author_for_actor(actor), Some(&author));
            let first = observe(&authors, model.len());

            authors.assign_author(author.clone(), actor);
            prop_assert_eq!(observe(&authors, model.len()), first);
        }

        /// `with_actors(n)` starts with no authors, every actor slot
        /// unmapped, and no actors for any author.
        #[test]
        fn with_actors_is_empty(n in 0usize..16, probe in gen_author()) {
            let authors = Authors::with_actors(n);
            prop_assert!(authors.get_authors().is_empty());
            // Go out of range, to ensure `None` is still returned
            for i in 0..n + 3 {
                prop_assert_eq!(authors.get_author_for_actor(i), None);
            }
            prop_assert_eq!(authors.get_actors_for_author(&probe).count(), 0);
        }

        /// Every author the model maps some actor to appears in
        /// `get_authors`.
        ///
        /// Subset, not equality: re-assignment and `remove_actor` may leave
        /// dangling authors, as documented on `Authors::remove_actor`.
        #[test]
        fn get_authors_covers_all_live_authors(init in 0usize..8, ops in gen_ops()) {
            let (authors, model) = apply(init, &ops);
            let author_set = authors.get_authors();
            for author in model.iter().flatten() {
                prop_assert!(author_set.contains(author));
            }
        }
    }
}
