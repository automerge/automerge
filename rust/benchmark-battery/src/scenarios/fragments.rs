//! Matched unsigned vs. required-signing benchmarks over identical fragment histories.
//!
//! Both modes use the exact same deterministic `ActorId`s, `Author` public keys,
//! operations, and commit timestamps so every `ChangeHash` and canonical
//! `Fragment` boundary is identical between the unsigned and signed runs.
//! External Ed25519 key-signing is precomputed during setup so timed loops
//! measure Automerge's own readiness, verification, admission, and GC work.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::hint::black_box;

use benchmark_battery::automerge::signature::{ed25519, signing_message};
use benchmark_battery::automerge::transaction::{CommitOptions, Transactable};
use benchmark_battery::automerge::{
    ActorId, Author, Automerge, ChangeHash, LoadOptions, ObjType, SignatureBatchOutcome, ROOT,
};
use ed25519_dalek::{Signer, SigningKey};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

#[cfg(any(feature = "memory", test))]
use super::MemoryMeasurement;
use super::{Benchmark, MemoryBenchmark, SampledBenchmark, SeriesBenchmark};

const BUILD_COMMITS: u64 = 2_000;
const DEEP_HISTORY_COMMITS: u64 = 5_000;
const MULTI_ACTOR_COMMITS: usize = 2_000;
const ACTOR_SWEEP: [usize; 3] = [10, 100, 1_000];

const INC_BASE_CHARS: usize = 5_000;
const INC_STEPS: usize = 100;
const INC_EDITS_PER_STEP: usize = 50;

pub fn benchmarks() -> Vec<Benchmark> {
    let mut out: Vec<Benchmark> = Vec::new();

    // 1. Local commit & signature-insertion overhead.
    out.push(
        SampledBenchmark::no_setup(
            "fragments",
            "fragments/build/deep_history_2000/unsigned",
            || {
                let (_, author) = actor_credentials(0);
                let actor = actor_id(0);
                Box::new(move || {
                    black_box(build_deep_history(
                        Automerge::new_with_author(Some(author.clone())),
                        &actor,
                        &author,
                        BUILD_COMMITS,
                    ));
                })
            },
        )
        .into(),
    );
    out.push(
        SampledBenchmark::no_setup(
            "fragments",
            "fragments/build/deep_history_2000/signing_unaccepted",
            || {
                let (_, author) = actor_credentials(0);
                let actor = actor_id(0);
                Box::new(move || {
                    black_box(build_deep_history(
                        signing_doc(&author),
                        &actor,
                        &author,
                        BUILD_COMMITS,
                    ));
                })
            },
        )
        .into(),
    );
    out.push(
        SampledBenchmark::no_setup(
            "fragments",
            "fragments/build/deep_history_2000/signing_verified",
            || {
                let (key, author) = actor_credentials(0);
                let actor = actor_id(0);
                let reference = build_deep_history(
                    Automerge::new_with_author(Some(author.clone())),
                    &actor,
                    &author,
                    BUILD_COMMITS,
                );
                let frontier_sigs: Vec<(ChangeHash, ed25519::Signature)> =
                    required_frontiers(&reference)
                        .into_iter()
                        .map(|h| (h, sign_hash(&key, &h)))
                        .collect();
                Box::new(move || {
                    let mut doc =
                        build_deep_history(signing_doc(&author), &actor, &author, BUILD_COMMITS);
                    insert_all(&mut doc, frontier_sigs.iter().copied());
                    black_box(doc);
                })
            },
        )
        .into(),
    );

    // 2. Fragment discovery + bundle export (1 actor deep history + actor sweep).
    out.push(
        SampledBenchmark::no_setup(
            "fragments",
            "fragments/export/deep_history_5000/unsigned",
            || {
                let (unsigned, _) = prepared_deep_history_pair(DEEP_HISTORY_COMMITS);
                Box::new(move || {
                    black_box(unsigned.bundle_fragments(unsigned.fragments(..)));
                })
            },
        )
        .into(),
    );
    out.push(
        SampledBenchmark::no_setup(
            "fragments",
            "fragments/export/deep_history_5000/signed",
            || {
                let (_, signed) = prepared_deep_history_pair(DEEP_HISTORY_COMMITS);
                Box::new(move || {
                    black_box(signed.bundle_fragments(signed.fragments(..)));
                })
            },
        )
        .into(),
    );

    for actors in ACTOR_SWEEP {
        out.push(
            SampledBenchmark::no_setup(
                "fragments",
                leaked(format!("fragments/export/actors_{actors}/unsigned")),
                move || {
                    let (unsigned, _) = prepared_multi_actor_pair(actors, MULTI_ACTOR_COMMITS);
                    Box::new(move || {
                        black_box(unsigned.bundle_fragments(unsigned.fragments(..)));
                    })
                },
            )
            .into(),
        );
        out.push(
            SampledBenchmark::no_setup(
                "fragments",
                leaked(format!("fragments/export/actors_{actors}/signed")),
                move || {
                    let (_, signed) = prepared_multi_actor_pair(actors, MULTI_ACTOR_COMMITS);
                    Box::new(move || {
                        black_box(signed.bundle_fragments(signed.fragments(..)));
                    })
                },
            )
            .into(),
        );
    }

    // 3. Full stream load: snapshot vs unsigned fragments vs signed in-order vs signed reversed.
    out.push(
        SampledBenchmark::no_setup(
            "fragments",
            "fragments/load/deep_history_5000/snapshot",
            || {
                let (unsigned, _) = prepared_deep_history_pair(DEEP_HISTORY_COMMITS);
                let snapshot = unsigned.save();
                Box::new(move || {
                    black_box(Automerge::load(&snapshot).unwrap());
                })
            },
        )
        .into(),
    );
    out.push(
        SampledBenchmark::no_setup(
            "fragments",
            "fragments/load/deep_history_5000/unsigned",
            || {
                let (unsigned, _) = prepared_deep_history_pair(DEEP_HISTORY_COMMITS);
                let stream = unsigned.bundle_fragments(unsigned.fragments(..)).concat();
                Box::new(move || {
                    black_box(Automerge::load(&stream).unwrap());
                })
            },
        )
        .into(),
    );
    out.push(
        SampledBenchmark::no_setup(
            "fragments",
            "fragments/load/deep_history_5000/signed_in_order",
            || {
                let (_, signed) = prepared_deep_history_pair(DEEP_HISTORY_COMMITS);
                let stream = signed.bundle_fragments(signed.fragments(..)).concat();
                Box::new(move || {
                    black_box(Automerge::load_with_options(&stream, signing_load()).unwrap());
                })
            },
        )
        .into(),
    );
    out.push(
        SampledBenchmark::no_setup(
            "fragments",
            "fragments/load/deep_history_5000/signed_reversed",
            || {
                let (_, signed) = prepared_deep_history_pair(DEEP_HISTORY_COMMITS);
                let mut groups = signed.bundle_fragments(signed.fragments(..));
                groups.reverse();
                let stream = groups.concat();
                Box::new(move || {
                    black_box(Automerge::load_with_options(&stream, signing_load()).unwrap());
                })
            },
        )
        .into(),
    );

    for actors in ACTOR_SWEEP {
        out.push(
            SampledBenchmark::no_setup(
                "fragments",
                leaked(format!("fragments/load/actors_{actors}/unsigned")),
                move || {
                    let (unsigned, _) = prepared_multi_actor_pair(actors, MULTI_ACTOR_COMMITS);
                    let stream = unsigned.bundle_fragments(unsigned.fragments(..)).concat();
                    Box::new(move || {
                        black_box(Automerge::load(&stream).unwrap());
                    })
                },
            )
            .into(),
        );
        out.push(
            SampledBenchmark::no_setup(
                "fragments",
                leaked(format!("fragments/load/actors_{actors}/signed_in_order")),
                move || {
                    let (_, signed) = prepared_multi_actor_pair(actors, MULTI_ACTOR_COMMITS);
                    let stream = signed.bundle_fragments(signed.fragments(..)).concat();
                    Box::new(move || {
                        black_box(Automerge::load_with_options(&stream, signing_load()).unwrap());
                    })
                },
            )
            .into(),
        );
        out.push(
            SampledBenchmark::no_setup(
                "fragments",
                leaked(format!("fragments/load/actors_{actors}/signed_reversed")),
                move || {
                    let (_, signed) = prepared_multi_actor_pair(actors, MULTI_ACTOR_COMMITS);
                    let mut groups = signed.bundle_fragments(signed.fragments(..));
                    groups.reverse();
                    let stream = groups.concat();
                    Box::new(move || {
                        black_box(Automerge::load_with_options(&stream, signing_load()).unwrap());
                    })
                },
            )
            .into(),
        );
    }

    // 4. Incremental fragment replication series (checks step-over-step scaling).
    out.push(
        SeriesBenchmark {
            group: "fragments",
            name: "fragments/incremental_load_50_edits/unsigned",
            steps: INC_STEPS,
            setup: incremental_unsigned,
        }
        .into(),
    );
    out.push(
        SeriesBenchmark {
            group: "fragments",
            name: "fragments/incremental_load_50_edits/signed",
            steps: INC_STEPS,
            setup: incremental_signed,
        }
        .into(),
    );

    // 5. Peak and steady-state heap memory comparisons.
    out.extend(memory_benchmarks().into_iter().map(Into::into));

    out
}

fn memory_benchmarks() -> Vec<MemoryBenchmark> {
    let mut mem = Vec::new();
    for (label, actors) in [
        ("deep_history_5000", None),
        ("actors_100", Some(100usize)),
        ("actors_1000", Some(1_000usize)),
    ] {
        for signed in [false, true] {
            let mode = if signed { "signed" } else { "unsigned" };
            let name = leaked(format!("memory/fragments_load/{label}/{mode}"));
            #[cfg(any(feature = "memory", test))]
            {
                mem.push(MemoryBenchmark::new("memory", name, move || {
                    let (unsigned_doc, signed_doc) = match actors {
                        None => prepared_deep_history_pair(DEEP_HISTORY_COMMITS),
                        Some(count) => prepared_multi_actor_pair(count, MULTI_ACTOR_COMMITS),
                    };
                    if signed {
                        let stream = signed_doc
                            .bundle_fragments(signed_doc.fragments(..))
                            .concat();
                        Box::new(move || {
                            measure_alloc(|| {
                                Automerge::load_with_options(&stream, signing_load()).unwrap()
                            })
                        })
                    } else {
                        let stream = unsigned_doc
                            .bundle_fragments(unsigned_doc.fragments(..))
                            .concat();
                        Box::new(move || measure_alloc(|| Automerge::load(&stream).unwrap()))
                    }
                }));
            }
            #[cfg(not(any(feature = "memory", test)))]
            {
                let _ = actors;
                mem.push(MemoryBenchmark::unavailable("memory", name));
            }
        }
    }
    mem
}

#[cfg(feature = "memory")]
fn measure_alloc<F, Output>(op: F) -> MemoryMeasurement
where
    F: FnOnce() -> Output,
{
    crate::memory::measure(op)
}

#[cfg(all(test, not(feature = "memory")))]
fn measure_alloc<F, Output>(op: F) -> MemoryMeasurement
where
    F: FnOnce() -> Output,
{
    let _ = black_box(op());
    MemoryMeasurement {
        peak_bytes: 0,
        steady_bytes: 0,
    }
}

fn leaked(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

fn actor_id(index: usize) -> ActorId {
    ActorId::from(&(index as u64 + 1).to_be_bytes()[..])
}

/// Author for receiving replicas that never sign.
fn receiver_author() -> Author<'static> {
    actor_credentials(usize::MAX / 2).1
}

fn signing_doc(author: &Author<'static>) -> Automerge {
    Automerge::signing(author.clone()).expect("benchmark author is a valid Ed25519 key")
}

fn signing_load() -> LoadOptions<'static> {
    LoadOptions::new().signing().author(receiver_author())
}

fn actor_credentials(index: usize) -> (SigningKey, Author<'static>) {
    let mut seed = [0u8; 32];
    seed[..8].copy_from_slice(&(index as u64 + 1).to_le_bytes());
    seed[8..16].copy_from_slice(&0x9e37_79b9_7f4a_7c15u64.to_le_bytes());
    let key = SigningKey::from_bytes(&seed);
    let author = Author::from(key.verifying_key().to_bytes().to_vec());
    (key, author)
}

fn sign_hash(key: &SigningKey, hash: &ChangeHash) -> ed25519::Signature {
    ed25519::Signature::from_bytes(key.sign(&signing_message(hash)).to_bytes())
}

/// Insert pre-computed signatures through the public batch API, asserting
/// every entry was accepted.
fn insert_all(
    doc: &mut Automerge,
    inputs: impl IntoIterator<Item = (ChangeHash, ed25519::Signature)>,
) {
    match doc.insert_signatures(inputs) {
        SignatureBatchOutcome::Completed { insertions } => {
            for insertion in insertions {
                insertion.result.expect("benchmark signatures verify");
            }
        }
        other => panic!("benchmark signature batch did not complete: {other:?}"),
    }
}

/// `doc` must already carry `author` as its fixed author.
fn build_deep_history(
    mut doc: Automerge,
    actor: &ActorId,
    author: &Author<'static>,
    commits: u64,
) -> Automerge {
    assert_eq!(doc.get_author(), Some(author));
    doc.set_actor(actor.clone());
    for step in 0..commits {
        let mut tx = doc.transaction();
        tx.put(ROOT, "x", step as i64).unwrap();
        tx.put(ROOT, "y", (step * 3) as i64).unwrap();
        tx.commit_with(CommitOptions::default().with_time(step as i64));
    }
    doc
}

/// Compute the exact frontier hashes required to export every fragment in `doc`'s cover.
fn required_frontiers(doc: &Automerge) -> Vec<ChangeHash> {
    let by_hash: HashMap<ChangeHash, (ActorId, u64)> = doc
        .get_changes(&[])
        .into_iter()
        .map(|c| (c.hash(), (c.actor_id().clone(), c.seq())))
        .collect();
    let mut frontiers = BTreeMap::<ChangeHash, ()>::new();
    for fragment in doc.fragments(..) {
        let mut actor_max: BTreeMap<&ActorId, (u64, ChangeHash)> = BTreeMap::new();
        for &member in &fragment.members {
            let (ref actor, seq) = by_hash[&member];
            actor_max
                .entry(actor)
                .and_modify(|curr| {
                    if seq > curr.0 {
                        *curr = (seq, member);
                    }
                })
                .or_insert((seq, member));
        }
        for (_, hash) in actor_max.into_values() {
            frontiers.insert(hash, ());
        }
    }
    frontiers.into_keys().collect()
}

fn prepared_deep_history_pair(commits: u64) -> (Automerge, Automerge) {
    let (key, author) = actor_credentials(0);
    let actor = actor_id(0);
    let unsigned = build_deep_history(
        Automerge::new_with_author(Some(author.clone())),
        &actor,
        &author,
        commits,
    );
    let mut signed = build_deep_history(signing_doc(&author), &actor, &author, commits);
    insert_all(
        &mut signed,
        required_frontiers(&unsigned)
            .into_iter()
            .map(|hash| (hash, sign_hash(&key, &hash))),
    );
    assert_eq!(unsigned.get_heads(), signed.get_heads());
    assert_same_fragment_cover(&unsigned, &signed);
    (unsigned, signed)
}

fn prepared_multi_actor_pair(actor_count: usize, total_commits: usize) -> (Automerge, Automerge) {
    // One fixed author writing through several actors.
    let (key, author) = actor_credentials(0);
    let actors: Vec<ActorId> = (0..actor_count).map(actor_id).collect();

    let build = |mut doc: Automerge| -> Automerge {
        for step in 0..total_commits {
            doc.set_actor(actors[step % actor_count].clone());
            let mut tx = doc.transaction();
            tx.put(ROOT, format!("k{}", step % 32), step as i64)
                .unwrap();
            tx.commit_with(CommitOptions::default().with_time(step as i64));
        }
        doc
    };

    let unsigned = build(Automerge::new_with_author(Some(author.clone())));
    let mut signed = build(signing_doc(&author));
    insert_all(
        &mut signed,
        required_frontiers(&unsigned)
            .into_iter()
            .map(|hash| (hash, sign_hash(&key, &hash))),
    );
    assert_eq!(unsigned.get_heads(), signed.get_heads());
    assert_same_fragment_cover(&unsigned, &signed);
    (unsigned, signed)
}

fn assert_same_fragment_cover(unsigned: &Automerge, signed: &Automerge) {
    let u = unsigned.fragments(..);
    let s = signed.fragments(..);
    assert_eq!(u.len(), s.len());
    let u_heads: HashSet<_> = u.iter().map(|f| f.head).collect();
    let s_heads: HashSet<_> = s.iter().map(|f| f.head).collect();
    assert_eq!(u_heads, s_heads);
}

fn incremental_unsigned() -> Box<dyn FnMut(usize)> {
    incremental_series(false)
}

fn incremental_signed() -> Box<dyn FnMut(usize)> {
    incremental_series(true)
}

fn incremental_series(signed: bool) -> Box<dyn FnMut(usize)> {
    let (key, author) = actor_credentials(0);
    let actor = actor_id(0);
    let mut rng = StdRng::seed_from_u64(2026);

    let make_base = |mut doc: Automerge,
                     rng: &mut StdRng|
     -> (Automerge, benchmark_battery::automerge::ObjId) {
        assert_eq!(doc.get_author(), Some(&author));
        doc.set_actor(actor.clone());
        let text = {
            let mut tx = doc.transaction();
            let t = tx.put_object(ROOT, "content", ObjType::Text).unwrap();
            tx.commit_with(CommitOptions::default().with_time(0));
            t
        };
        for i in 0..INC_BASE_CHARS {
            let pos = if i == 0 { 0 } else { rng.random_range(0..=i) };
            let mut tx = doc.transaction();
            tx.splice_text(&text, pos, 0, "a").unwrap();
            tx.commit_with(CommitOptions::default().with_time(i as i64 + 1));
        }
        (doc, text)
    };

    let mut sim_rng = StdRng::seed_from_u64(2026);
    let (mut sim_doc, sim_text) = make_base(Automerge::new(), &mut sim_rng);
    let base_frontiers = required_frontiers(&sim_doc);

    // Precompute deterministic edit positions and Ed25519 signatures for all future steps.
    let mut current_len = INC_BASE_CHARS;
    let mut step_plans: Vec<Vec<(usize, i64)>> = Vec::with_capacity(INC_STEPS);
    let mut step_signatures: Vec<Vec<(ChangeHash, ed25519::Signature)>> =
        Vec::with_capacity(INC_STEPS);

    for step in 0..INC_STEPS {
        let mut edits = Vec::with_capacity(INC_EDITS_PER_STEP);
        let mut step_hashes = Vec::with_capacity(INC_EDITS_PER_STEP);
        for edit in 0..INC_EDITS_PER_STEP {
            let pos = sim_rng.random_range(0..=current_len);
            let time = (INC_BASE_CHARS + 1 + step * INC_EDITS_PER_STEP + edit) as i64;
            let mut tx = sim_doc.transaction();
            tx.splice_text(&sim_text, pos, 0, ".").unwrap();
            let (hash, _) = tx.commit_with(CommitOptions::default().with_time(time));
            current_len += 1;
            edits.push((pos, time));
            step_hashes.push(hash.unwrap());
        }
        // Sign any hard head produced in this step plus any trailing level-0 tail
        // after the last hard head so the step's outgoing cover is complete.
        let last_hard = step_hashes.iter().rposition(|h| get_level(h) > 0);
        let mut sigs = Vec::new();
        for (idx, &hash) in step_hashes.iter().enumerate() {
            if get_level(&hash) > 0 || last_hard.is_none_or(|last| idx > last) {
                sigs.push((hash, sign_hash(&key, &hash)));
            }
        }
        step_plans.push(edits);
        step_signatures.push(sigs);
    }

    let (mut sender, text) = make_base(
        if signed {
            signing_doc(&author)
        } else {
            Automerge::new_with_author(Some(author.clone()))
        },
        &mut rng,
    );
    if signed {
        insert_all(
            &mut sender,
            base_frontiers
                .iter()
                .map(|hash| (*hash, sign_hash(&key, hash))),
        );
    }

    let base_fragments = sender.fragments(..);
    let mut delivered_members: HashSet<ChangeHash> = base_fragments
        .iter()
        .flat_map(|f| f.members.iter().copied())
        .collect();
    let base_stream = sender.bundle_fragments(base_fragments).concat();

    let mut receiver = if signed {
        Automerge::load_with_options(&base_stream, signing_load()).unwrap()
    } else {
        Automerge::load(&base_stream).unwrap()
    };
    assert_eq!(sender.get_heads(), receiver.get_heads());

    Box::new(move |step| {
        for &(pos, time) in &step_plans[step] {
            let mut tx = sender.transaction();
            tx.splice_text(&text, pos, 0, ".").unwrap();
            tx.commit_with(CommitOptions::default().with_time(time));
        }
        if signed {
            insert_all(&mut sender, step_signatures[step].iter().copied());
        }
        let delta_fragments: Vec<_> = sender
            .fragments(..)
            .into_iter()
            .filter(|f| f.members.iter().any(|m| !delivered_members.contains(m)))
            .collect();
        for frag in &delta_fragments {
            delivered_members.extend(frag.members.iter().copied());
        }
        let payload = sender.bundle_fragments(delta_fragments).concat();
        receiver.load_incremental(&payload).unwrap();
        assert_eq!(sender.get_heads(), receiver.get_heads());
    })
}

fn get_level(hash: &ChangeHash) -> usize {
    hash.0.iter().take_while(|&&b| b == 0).count()
}

#[cfg(test)]
mod tests {
    use super::benchmarks;

    #[test]
    fn registers_matched_unsigned_and_signed_fragment_benchmarks() {
        let names: Vec<&'static str> = benchmarks().iter().map(|b| b.name()).collect();
        for expected in [
            "fragments/build/deep_history_2000/unsigned",
            "fragments/build/deep_history_2000/signing_unaccepted",
            "fragments/build/deep_history_2000/signing_verified",
            "fragments/export/deep_history_5000/unsigned",
            "fragments/export/deep_history_5000/signed",
            "fragments/load/deep_history_5000/unsigned",
            "fragments/load/deep_history_5000/signed_in_order",
            "fragments/load/deep_history_5000/signed_reversed",
            "fragments/incremental_load_50_edits/unsigned",
            "fragments/incremental_load_50_edits/signed",
            "memory/fragments_load/deep_history_5000/unsigned",
            "memory/fragments_load/deep_history_5000/signed",
        ] {
            assert!(names.contains(&expected), "missing {expected}");
        }
    }
}
