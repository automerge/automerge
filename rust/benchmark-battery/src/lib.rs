use automerge::transaction::Transaction;
use automerge::{Author, ChangeHash, ObjId};
use rand::distr::Alphanumeric;
use rand::{rng, Rng, RngExt};
use std::collections::HashMap;
use std::fmt::{Display, Error, Formatter};
use std::ops::Range;

pub use automerge;

pub use automerge::{transaction::Transactable, Automerge, ObjType, ScalarValue, ROOT};

pub const N_AUTHORS: usize = 4;
pub const N_ROUNDS: usize = 2;
const N_BLOCKS: usize = N_AUTHORS * N_ROUNDS;

pub struct TestItem<T> {
    pub label: String,
    pub item: T,
}

impl<T> TestItem<T> {
    pub fn new<S: Into<String>>(label: S, item: T) -> TestItem<T> {
        Self {
            label: label.into(),
            item,
        }
    }

    pub fn map<B, F: Fn(&T) -> B>(&self, f: F) -> TestItem<B> {
        TestItem {
            label: self.label.clone(),
            item: f(&self.item),
        }
    }
}

impl<T> Display for TestItem<T> {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> Result<(), Error> {
        self.label.fmt(fmt)
    }
}

/// The author for author number `i`.
///
/// Shared by the `N_AUTHORS` generator authors and by the `load_actors`
/// benchmarks, which go up to a thousand, so the id is the full `usize`.
pub fn author(i: usize) -> Author<'static> {
    Author::from(i.to_be_bytes().to_vec())
}

/// All `N_AUTHORS` authors, in block order.
pub fn authors() -> Vec<Author<'static>> {
    (0..N_AUTHORS).map(author).collect()
}

/// The local user of a generated document: `author(0)`.
///
/// The local user creates the `"content"` container, is the current author of
/// every generator's result so that benchmark writes are theirs, and is never
/// named by a policy — so their writes stay visible under any frontier. A
/// fresh actor of a masked author is masked too, so this has to be an author
/// the policies exclude, not merely a new actor.
pub fn local_user() -> Author<'static> {
    author(0)
}

/// The authors a policy may mask: every author except the [`local_user`].
fn maskable_authors() -> impl Iterator<Item = Author<'static>> {
    authors().into_iter().skip(1)
}

/// Produce a write-frontier which contains every author except the
/// [`local_user`], and the document's current heads is the frontier.
///
/// Nothing is hidden unless the document's heads advance, and writes by the
/// [`local_user`] after the frontier is set remain visible.
pub fn policy_masked_at_heads(doc: &Automerge) -> HashMap<Author<'static>, Vec<ChangeHash>> {
    let current = doc.get_heads();
    maskable_authors()
        .map(|author| (author, current.clone()))
        .collect()
}

/// Produce a write-frontier for the second half of the [`authors`], i.e.
/// `N_AUTHORS / 2 .. authors().len()`.
///
/// The frontier for each author is `[]`, meaning that every op written by that
/// author is masked.
///
/// Note that the second half is chosen because the [`local_user`] is in the
/// first half.
pub fn policy_hiding_half() -> HashMap<Author<'static>, Vec<ChangeHash>> {
    authors()
        .into_iter()
        .skip(N_AUTHORS / 2)
        .map(|author| (author, vec![]))
        .collect()
}

/// Produce a write-frontier for the second half of the [`authors`], i.e.
/// `N_AUTHORS / 2 .. authors().len()`.
///
/// The frontier for each author is a single random hash, meaning that the there
/// will be a pending change (unless the produced hash manages to be in the
/// graph, which is unlikely.)
///
/// Note that the second half is chosen because the [`local_user`] is in the
/// first half.
pub fn policy_pending_half() -> HashMap<Author<'static>, Vec<ChangeHash>> {
    authors()
        .into_iter()
        .skip(N_AUTHORS / 2)
        .map(|author| {
            let mut bytes = [0; 32];
            rng().fill(&mut bytes);
            (author, vec![ChangeHash(bytes)])
        })
        .collect()
}

fn random_string(n: u64) -> String {
    rng()
        .sample_iter(&Alphanumeric)
        .take(n as usize)
        .map(char::from)
        .collect()
}

pub fn rand() -> usize {
    let mut buf = [0u8; size_of::<usize>()];
    getrandom::fill(&mut buf).unwrap();
    usize::from_ne_bytes(buf)
}

/// Split `ops` into `N_BLOCKS` equal, contiguous blocks and run `block` on
/// each, switching author at every boundary. Block `b` is written by
/// `author(b % N_AUTHORS)`, so every author owns `N_ROUNDS` blocks and, because
/// each switch mints a new actor, `N_ROUNDS` actors.
///
/// `block` receives the op indices for its block and the document, and is
/// responsible for its own transactions: most generators open one per block,
/// `deep_history_doc` opens one per op.
///
/// Afterwards the document's author is the [`local_user`], so that anything a
/// benchmark writes is theirs. Setting the author binds nothing until the
/// first write, so this adds no actor to documents that are only read.
fn in_author_blocks(
    doc: &mut Automerge,
    ops: usize,
    mut block: impl FnMut(&mut Automerge, Range<usize>),
) {
    let block_len = ops / N_BLOCKS;
    for b in 0..N_BLOCKS {
        doc.set_author(Some(author(b % N_AUTHORS)));
        block(doc, b * block_len..(b + 1) * block_len);
    }
    doc.set_author(Some(local_user()));
}

/// Like [`in_author_blocks`], with one transaction per block.
fn in_author_transactions(
    doc: &mut Automerge,
    ops: usize,
    mut block: impl FnMut(&mut Transaction<'_>, Range<usize>),
) {
    in_author_blocks(doc, ops, |doc, range| {
        let mut tx = doc.transaction();
        block(&mut tx, range);
        tx.commit();
    });
}

/// Create the `"content"` container in its own change by the [`local_user`],
/// who is never masked. Block 0 is also `author(0)`, so this adds a change but
/// no extra actor.
fn content_container(doc: &mut Automerge, ty: ObjType) -> ObjId {
    doc.set_author(Some(local_user()));
    let mut tx = doc.transaction();
    let obj = tx.put_object(ROOT, "content", ty).unwrap();
    tx.commit();
    obj
}

#[inline(never)]
pub fn maps_in_maps_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    let mut map = ROOT;
    in_author_transactions(&mut doc, n as usize, |tx, range| {
        for i in range {
            map = tx.put_object(&map, i.to_string(), ObjType::Map).unwrap();
        }
    });
    doc
}

#[inline(never)]
pub fn big_paste_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    let text = content_container(&mut doc, ObjType::Text);
    in_author_transactions(&mut doc, n as usize, |tx, range| {
        tx.splice_text(&text, range.start, 0, &random_string(range.len() as u64))
            .unwrap();
    });
    doc
}

#[inline(never)]
pub fn big_random_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    let text = content_container(&mut doc, ObjType::Text);
    in_author_transactions(&mut doc, n as usize, |tx, range| {
        for len in range {
            let pos = rng().next_u32() as usize % (len + 1);
            tx.splice_text(&text, pos, 0, &random_string(1)).unwrap();
        }
    });
    doc
}

#[inline(never)]
pub fn text_splice_100(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    let text = content_container(&mut doc, ObjType::Text);
    in_author_transactions(&mut doc, (n / 100) as usize, |tx, range| {
        for len in range {
            let pos = rng().next_u32() as usize % (len + 1);
            tx.splice_text(&text, pos, 0, &random_string(100)).unwrap();
        }
    });
    doc
}

#[inline(never)]
pub fn list_splice_100(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    let list = content_container(&mut doc, ObjType::List);
    in_author_transactions(&mut doc, (n / 100) as usize, |tx, range| {
        for len in range {
            let pos = rng().next_u32() as usize % (len + 1);
            let string = random_string(100);
            let letters = string.chars().map(ScalarValue::from);
            tx.splice(&list, pos, 0, letters).unwrap();
        }
    });
    doc
}

#[inline(never)]
pub fn poorly_simulated_typing_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    let text = content_container(&mut doc, ObjType::Text);
    in_author_blocks(&mut doc, n as usize, |doc, range| {
        for pos in range {
            let mut tx = doc.transaction();
            tx.splice_text(&text, pos, 0, &random_string(1)).unwrap();
            tx.commit();
        }
    });
    doc
}

#[inline(never)]
pub fn deep_history_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    in_author_blocks(&mut doc, n as usize, |doc, range| {
        for i in range {
            let mut tx = doc.transaction();
            tx.put(ROOT, "x", i.to_string()).unwrap();
            tx.put(ROOT, "y", i.to_string()).unwrap();
            tx.commit();
        }
    });
    doc
}

#[cfg(test)]
mod tests {
    //! Invariants the frontier benchmarks depend on. If the author layout is
    //! wrong the benchmarks still run and report a number, so these are the
    //! only place the layout is checked.

    use super::{
        author, authors, big_paste_doc, big_random_doc, deep_history_doc, list_splice_100,
        local_user, maps_in_maps_doc, policy_hiding_half, policy_masked_at_heads,
        policy_pending_half, poorly_simulated_typing_doc, text_splice_100, N_AUTHORS, N_ROUNDS,
    };
    use automerge::transaction::Transactable;
    use automerge::{Author, Automerge, ChangeHash, ReadDoc, ROOT};
    use std::collections::HashMap;

    /// Divisible by `N_AUTHORS * N_ROUNDS` blocks and by the 100-wide splices.
    const N: u64 = 800;

    /// Every shared generator, so a new one cannot be added without being
    /// covered here.
    fn generators() -> Vec<(&'static str, Automerge)> {
        vec![
            ("maps_in_maps_doc", maps_in_maps_doc(N)),
            ("big_paste_doc", big_paste_doc(N)),
            ("big_random_doc", big_random_doc(N)),
            ("text_splice_100", text_splice_100(N)),
            ("list_splice_100", list_splice_100(N)),
            (
                "poorly_simulated_typing_doc",
                poorly_simulated_typing_doc(N),
            ),
            ("deep_history_doc", deep_history_doc(N)),
        ]
    }

    fn content_len(doc: &Automerge) -> usize {
        let (_, content) = doc.get(ROOT, "content").unwrap().unwrap();
        doc.length(&content)
    }

    #[test]
    fn author_ids_are_deterministic_and_distinct() {
        assert_eq!(authors().len(), N_AUTHORS);
        for i in 0..N_AUTHORS {
            assert_eq!(author(i), Author::from(i.to_be_bytes().to_vec()));
            assert_eq!(authors()[i], author(i));
        }
        let mut sorted = authors();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), N_AUTHORS, "authors must be distinct");
    }

    #[test]
    fn every_generator_records_all_authors() {
        for (name, doc) in generators() {
            let mut recorded = doc.get_authors().to_vec();
            recorded.sort();
            let mut expected = authors();
            expected.sort();
            assert_eq!(recorded, expected, "{name}: authors recorded on the doc");
        }
    }

    #[test]
    fn every_author_has_one_actor_per_round() {
        for (name, doc) in generators() {
            for a in authors() {
                assert_eq!(
                    doc.get_actors_for_author(&a).len(),
                    N_ROUNDS,
                    "{name}: actors for author {a:?}"
                );
            }
        }
    }

    #[test]
    fn generators_preserve_op_content() {
        assert_eq!(content_len(&big_paste_doc(N)), N as usize);
        assert_eq!(content_len(&big_random_doc(N)), N as usize);
        assert_eq!(content_len(&text_splice_100(N)), N as usize);
        assert_eq!(content_len(&list_splice_100(N)), N as usize);
        assert_eq!(content_len(&poorly_simulated_typing_doc(N)), N as usize);

        let deep = deep_history_doc(N);
        assert_eq!(deep.length(ROOT), 2, "deep_history keeps x and y");
        assert_eq!(
            deep.get_changes(&[]).len(),
            N as usize,
            "one change per step"
        );

        let nested = maps_in_maps_doc(N);
        let mut depth = 0;
        let mut map = ROOT;
        while let Some((_, next)) = nested.get(&map, depth.to_string()).unwrap() {
            map = next;
            depth += 1;
        }
        assert_eq!(depth, N as usize);
    }

    #[test]
    fn every_generator_ends_as_the_local_user() {
        for (name, doc) in generators() {
            assert_eq!(doc.get_author(), Some(&local_user()), "{name}");
        }
    }

    #[test]
    fn masked_at_heads_masks_every_author_but_the_local_user_and_hides_nothing() {
        let mut doc = text_splice_100(N);
        let heads = doc.get_heads();
        let policy = policy_masked_at_heads(&doc);

        assert_eq!(policy.len(), N_AUTHORS - 1);
        assert!(!policy.contains_key(&local_user()));
        for a in authors().into_iter().skip(1) {
            assert_eq!(policy[&a], heads, "author {a:?} bounded at current heads");
        }

        doc.set_write_frontier(policy);
        assert!(!doc.is_author_masked(&local_user()));
        for a in authors().into_iter().skip(1) {
            assert!(doc.is_author_masked(&a));
        }
        assert_eq!(content_len(&doc), N as usize, "mask at heads hides nothing");
    }

    /// The invariant that makes an always-on frontier sound for benchmarks that
    /// write: the generator leaves the local user as the author, no policy
    /// names the local user, so a write after the frontier is set is visible.
    #[test]
    fn local_user_writes_are_visible_under_every_policy() {
        type Policy = fn(&Automerge) -> HashMap<Author<'static>, Vec<ChangeHash>>;
        let policies: [(&str, Policy); 3] = [
            ("masked", |doc| policy_masked_at_heads(doc)),
            ("hidden", |_| policy_hiding_half()),
            ("pending", |_| policy_pending_half()),
        ];
        for (name, policy) in policies {
            let mut doc = text_splice_100(N);
            doc.set_write_frontier(policy(&doc));
            let before = content_len(&doc);
            let (_, text) = doc.get(ROOT, "content").unwrap().unwrap();

            let mut tx = doc.transaction();
            tx.splice_text(&text, 0, 0, "local").unwrap();
            tx.commit();

            assert_eq!(content_len(&doc), before + 5, "{name}: local write visible");
        }
    }

    #[test]
    fn hiding_half_hides_exactly_the_last_half_of_authors() {
        let mut doc = text_splice_100(N);
        let policy = policy_hiding_half();

        assert_eq!(policy.len(), (N_AUTHORS / 2));
        for (a, heads) in &policy {
            assert!(heads.is_empty(), "author {a:?} bounded at []");
        }

        doc.set_write_frontier(policy);
        for (i, a) in authors().iter().enumerate() {
            assert_eq!(doc.is_author_masked(a), i >= N_AUTHORS / 2, "author {a:?}");
        }
        // Blocks are contiguous and round-robin, so the last half of the
        // authors wrote exactly half of the blocks.
        assert_eq!(content_len(&doc), (N / 2) as usize);
    }

    #[test]
    fn pending_half_uses_heads_absent_from_the_graph() {
        let doc = text_splice_100(N);
        let policy = policy_pending_half();

        assert_eq!(policy.len(), N_AUTHORS / 2);
        let mut hashes = Vec::new();
        for (i, a) in authors().iter().enumerate().skip(N_AUTHORS / 2) {
            let heads = &policy[a];
            assert_eq!(heads.len(), 1, "author {i} has a single pending head");
            assert!(
                doc.get_change_by_hash(&heads[0]).is_none(),
                "pending head must not be in the graph"
            );
            hashes.push(heads[0]);
        }
        hashes.sort();
        hashes.dedup();
        assert_eq!(hashes.len(), (N_AUTHORS / 2), "pending heads are distinct");
    }

    #[test]
    fn pending_half_masks_the_same_ops_as_hiding_half() {
        let mut hidden = text_splice_100(N);
        hidden.set_write_frontier(policy_hiding_half());

        let mut pending = hidden.fork();
        pending.set_write_frontier(policy_pending_half());

        for a in authors() {
            assert_eq!(hidden.is_author_masked(&a), pending.is_author_masked(&a));
        }
        assert_eq!(content_len(&pending), content_len(&hidden));
        let (_, h_text) = hidden.get(ROOT, "content").unwrap().unwrap();
        let (_, p_text) = pending.get(ROOT, "content").unwrap().unwrap();
        assert_eq!(
            pending.text(&p_text).unwrap(),
            hidden.text(&h_text).unwrap()
        );
    }
}
