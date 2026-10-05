use automerge::transaction::Transaction;
use automerge::{Author, ChangeHash, ObjId, ReadDoc};
use rand::distr::Alphanumeric;
use rand::{rng, Rng, RngExt};
use std::collections::HashMap;
use std::fmt::{Display, Error, Formatter};
use std::ops::Range;

pub use automerge;

pub use automerge::{
    transaction::Transactable, AutoCommit, Automerge, LoadOptions, ObjType, ScalarValue, ROOT,
};

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
    policy_masked_for(&doc.get_heads())
}

/// Produce a write-frontier which contains every author except the
/// [`local_user`], and the given `heads` is the frontier.
///
/// Nothing is hidden unless the document's heads advance, and writes by the
/// [`local_user`] after the frontier is set remain visible.
pub fn policy_masked_for(heads: &[ChangeHash]) -> HashMap<Author<'static>, Vec<ChangeHash>> {
    maskable_authors()
        .map(|author| (author, heads.to_vec()))
        .collect()
}

/// Sets the write-frontier of `doc` using [`policy_masked_at_heads`].
pub fn masked(doc: Automerge) -> Automerge {
    let write_frontier = policy_masked_at_heads(&doc);
    doc.with_write_frontier(write_frontier)
}

/// [`masked`] for an [`AutoCommit`]: every author except the [`local_user`]
/// is masked at the document's current heads.
///
/// [`AutoCommit`] has no `set_write_frontier`, so the policy is applied one
/// author at a time.
pub fn masked_autocommit(mut doc: AutoCommit) -> AutoCommit {
    let heads = doc.get_heads();
    for author in maskable_authors() {
        doc.mask_author(author, &heads);
    }
    doc
}

/// The [`policy_masked_for`] policy for the heads that `bytes` will produce.
///
/// The heads are not known until the bytes are parsed, so this costs one
/// unmasked load. Resolve it once in setup; a measured load is then
/// `Automerge::load_with_options(bytes, LoadOptions::new().write_frontier(policy.clone()))`
/// ([`LoadOptions`] is not `Clone`, the policy is).
pub fn policy_masked_for_bytes(bytes: &[u8]) -> HashMap<Author<'static>, Vec<ChangeHash>> {
    policy_masked_for(&Automerge::load(bytes).unwrap().get_heads())
}

/// Load `bytes` under [`policy_masked_for_bytes`], so that the loaded document
/// is in the same state as [`masked`] would leave it. For setup, not for a
/// measured load.
pub fn load_masked(bytes: &[u8]) -> Automerge {
    let policy = policy_masked_for_bytes(bytes);
    Automerge::load_with_options(bytes, LoadOptions::new().write_frontier(policy)).unwrap()
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
pub fn in_author_blocks(
    doc: &mut Automerge,
    ops: usize,
    mut block: impl FnMut(&mut Automerge, Range<usize>),
) {
    let block_len = ops / N_BLOCKS;
    for b in 0..N_BLOCKS {
        doc.set_author(Some(author(b % N_AUTHORS)));
        let start = b * block_len;
        let end = if b + 1 == N_BLOCKS {
            ops
        } else {
            start + block_len
        };
        block(doc, start..end);
    }
    doc.set_author(Some(local_user()));
}

/// Like [`in_author_blocks`], with one transaction per block.
pub fn in_author_transactions(
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

/// A map with `n` distinct keys on `ROOT`, `"key0"` through `"key{n-1}"`.
#[inline(never)]
pub fn wide_map_doc(n: u64) -> Automerge {
    let mut doc = Automerge::new();
    in_author_transactions(&mut doc, n as usize, |tx, range| {
        for i in range {
            tx.put(ROOT, format!("key{i}"), format!("value{i}"))
                .unwrap();
        }
    });
    doc
}

/// A text document of `n + chunk` characters built by `n / chunk` random
/// `chunk`-wide splices from a seeded `rng`, so that the typing and apply
/// series start from the same document on every run.
///
/// The splices are the single transaction of an [`AutoCommit`] per author
/// block; the first `chunk` characters are the [`local_user`]'s together with
/// the container.
#[inline(never)]
pub fn seeded_text_doc(n: u64, chunk: u64, rng: &mut impl Rng) -> AutoCommit {
    let mut doc = AutoCommit::new().with_author(Some(local_user()));
    let text = doc.put_object(ROOT, "content", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, &seeded_string(chunk, rng))
        .unwrap();
    doc.commit();

    let splices = (n / chunk) as usize;
    let block_len = splices / N_BLOCKS;
    let mut len = chunk;
    for b in 0..N_BLOCKS {
        doc.set_author(Some(author(b % N_AUTHORS)));
        let end = if b + 1 == N_BLOCKS {
            splices
        } else {
            (b + 1) * block_len
        };
        for _ in b * block_len..end {
            let pos = (rng.next_u32() as u64 % len) as usize;
            doc.splice_text(&text, pos, 0, &seeded_string(chunk, rng))
                .unwrap();
            len += chunk;
        }
        doc.commit();
    }
    doc.set_author(Some(local_user()));
    assert_eq!(doc.stats().num_ops, n + 1 + chunk);
    doc
}

fn seeded_string(n: u64, rng: &mut impl Rng) -> String {
    rng.sample_iter(&Alphanumeric)
        .take(n as usize)
        .map(char::from)
        .collect()
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
        author, authors, big_paste_doc, big_random_doc, deep_history_doc, in_author_transactions,
        list_splice_100, load_masked, local_user, maps_in_maps_doc, masked, masked_autocommit,
        policy_hiding_half, policy_masked_at_heads, policy_masked_for, policy_masked_for_bytes,
        policy_pending_half, poorly_simulated_typing_doc, seeded_text_doc, text_splice_100,
        wide_map_doc, N_AUTHORS, N_ROUNDS,
    };
    use automerge::transaction::Transactable;
    use automerge::{Author, Automerge, ChangeHash, LoadOptions, ObjType, ReadDoc, ROOT};
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
            ("wide_map_doc", wide_map_doc(N)),
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

        assert_eq!(wide_map_doc(N).length(ROOT), N as usize);

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

    /// `map/` sizes (100, 1000, 10000) are not multiples of `N_BLOCKS`; the
    /// remainder must not be dropped.
    #[test]
    fn author_blocks_cover_every_op_when_not_a_multiple_of_the_block_count() {
        for ops in [1usize, 7, 100, 1000] {
            let mut doc = Automerge::new();
            let list = {
                let mut tx = doc.transaction();
                let list = tx.put_object(ROOT, "content", ObjType::List).unwrap();
                tx.commit();
                list
            };
            let mut seen = Vec::new();
            in_author_transactions(&mut doc, ops, |tx, range| {
                for i in range.clone() {
                    tx.insert(&list, i, i as i64).unwrap();
                }
                seen.push(range);
            });

            assert_eq!(doc.length(&list), ops, "{ops} ops: all inserted");
            let covered: Vec<usize> = seen.iter().flat_map(|r| r.clone()).collect();
            assert_eq!(
                covered,
                (0..ops).collect::<Vec<_>>(),
                "{ops} ops: contiguous"
            );
            assert_eq!(
                doc.get_author(),
                Some(&local_user()),
                "{ops} ops: ends as local user"
            );
        }
    }

    #[test]
    fn masked_puts_every_generator_under_the_masked_at_heads_policy() {
        for (name, doc) in generators() {
            let expected = policy_masked_at_heads(&doc);
            let doc = masked(doc);

            assert_eq!(doc.get_write_frontier(), expected, "{name}: policy");
            assert!(!doc.is_author_masked(&local_user()), "{name}: local user");
            for a in authors().into_iter().skip(1) {
                assert!(doc.is_author_masked(&a), "{name}: author {a:?}");
            }
            assert_eq!(
                doc.get_author(),
                Some(&local_user()),
                "{name}: still the local user"
            );
        }
    }

    #[test]
    fn masked_hides_nothing() {
        let plain = text_splice_100(N);
        let (_, text) = plain.get(ROOT, "content").unwrap().unwrap();
        let expected = plain.text(&text).unwrap();

        let doc = masked(plain);
        let (_, text) = doc.get(ROOT, "content").unwrap().unwrap();
        assert_eq!(doc.text(&text).unwrap(), expected);
    }

    /// A receiving document (`apply_changes`, `sync`, `load`) has no heads of
    /// its own yet, so its policy is pinned to the *source* document's heads.
    #[test]
    fn masked_for_matches_masked_at_heads_of_the_source() {
        let source = text_splice_100(N);
        let heads = source.get_heads();
        assert_eq!(policy_masked_for(&heads), policy_masked_at_heads(&source));
    }

    #[test]
    fn masked_for_lets_a_receiver_apply_the_source_without_hiding_it() {
        let source = text_splice_100(N);
        let (_, text) = source.get(ROOT, "content").unwrap().unwrap();
        let expected = source.text(&text).unwrap();

        let mut receiver =
            Automerge::new().with_write_frontier(policy_masked_for(&source.get_heads()));
        receiver.apply_changes(source.get_changes(&[])).unwrap();

        let (_, text) = receiver.get(ROOT, "content").unwrap().unwrap();
        assert_eq!(
            receiver.text(&text).unwrap(),
            expected,
            "everything up to heads is visible"
        );
        for a in authors().into_iter().skip(1) {
            assert!(receiver.is_author_masked(&a));
        }
    }

    #[test]
    fn seeded_text_doc_has_the_author_layout_and_is_reproducible() {
        use rand::rngs::StdRng;
        use rand::SeedableRng;

        let mut a = seeded_text_doc(N, 1, &mut StdRng::seed_from_u64(7));
        let b = seeded_text_doc(N, 1, &mut StdRng::seed_from_u64(7));

        let (_, ta) = a.get(ROOT, "content").unwrap().unwrap();
        let (_, tb) = b.get(ROOT, "content").unwrap().unwrap();
        assert_eq!(
            a.text(&ta).unwrap(),
            b.text(&tb).unwrap(),
            "same seed, same text"
        );

        let doc = a.document();
        let mut recorded = doc.get_authors().to_vec();
        recorded.sort();
        let mut expected = authors();
        expected.sort();
        assert_eq!(recorded, expected);
        assert_eq!(doc.get_author(), Some(&local_user()));
        for author in authors() {
            assert!(doc.get_actors_for_author(&author).len() >= N_ROUNDS);
        }
    }

    #[test]
    fn masked_autocommit_matches_masked() {
        use rand::rngs::StdRng;
        use rand::SeedableRng;

        let mut doc = seeded_text_doc(N, 1, &mut StdRng::seed_from_u64(8));
        let expected = policy_masked_at_heads(doc.document());

        let mut doc = masked_autocommit(doc);
        assert_eq!(doc.document().get_write_frontier(), expected);
        assert!(!doc.is_author_masked(&local_user()));

        let (_, text) = doc.get(ROOT, "content").unwrap().unwrap();
        let before = doc.length(&text);
        doc.splice_text(&text, 0, 0, "local").unwrap();
        doc.commit();
        assert_eq!(doc.length(&text), before + 5, "local write visible");
    }

    #[test]
    fn load_masked_matches_masked_on_the_same_bytes() {
        let source = text_splice_100(N);
        let bytes = source.save();

        let expected = masked(source);
        let loaded = load_masked(&bytes);

        assert_eq!(loaded.get_write_frontier(), expected.get_write_frontier());
        assert_eq!(
            policy_masked_for_bytes(&bytes),
            expected.get_write_frontier()
        );
        let (_, e) = expected.get(ROOT, "content").unwrap().unwrap();
        let (_, l) = loaded.get(ROOT, "content").unwrap().unwrap();
        assert_eq!(loaded.text(&l).unwrap(), expected.text(&e).unwrap());
    }

    #[test]
    fn masked_for_loads_the_source_bytes_without_hiding_them() {
        let source = text_splice_100(N);
        let (_, text) = source.get(ROOT, "content").unwrap().unwrap();
        let expected = source.text(&text).unwrap();
        let bytes = source.save();

        let loaded = Automerge::load_with_options(
            &bytes,
            LoadOptions::new().write_frontier(policy_masked_for(&source.get_heads())),
        )
        .unwrap();

        let (_, text) = loaded.get(ROOT, "content").unwrap().unwrap();
        assert_eq!(loaded.text(&text).unwrap(), expected);
        assert_eq!(loaded.get_write_frontier(), policy_masked_at_heads(&source));
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
