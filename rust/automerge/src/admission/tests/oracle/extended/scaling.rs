use std::hint::black_box;
use std::time::{Duration, Instant};

use crate::admission::{select, AcceptedHistory, CandidateHistory};
use crate::{Change, Fragment};

use super::super::assert_matches;
use super::{at_level, complete_fragment, evidence, valid_history};

fn chain(length: usize, fragment_heads: &[usize]) -> Vec<Change> {
    let mut changes: Vec<Change> = Vec::new();
    for index in 0..length {
        let deps: Vec<_> = changes.last().map(|c| c.hash()).into_iter().collect();
        changes.push(at_level(
            0,
            index as u64 + 1,
            &deps,
            usize::from(fragment_heads.contains(&index)),
        ));
    }
    valid_history(&changes);
    changes
}

#[test]
fn larger_histories_with_two_candidates_match_subset_oracle() {
    for length in [8, 16, 32, 64] {
        let split = length / 2;
        let changes = chain(length, &[split - 1, length - 1]);
        let first = complete_fragment(&changes[..split]);
        // The preceding equal-level head is the boundary. Only the second
        // half belongs to this fragment, not its boundary or ancestors.
        let second = Fragment {
            head: changes[length - 1].hash(),
            level: 1,
            boundary: vec![first.head],
            checkpoints: Vec::new(),
            members: changes[split..].iter().map(Change::hash).collect(),
        };
        let candidates = [
            CandidateHistory::CanonicalFragment {
                fragment: &first,
                members: &changes[..split],
            },
            CandidateHistory::CanonicalFragment {
                fragment: &second,
                members: &changes[split..],
            },
        ];
        let (store, signed) = evidence(&changes, &[length - 1]);
        assert_matches(
            &[],
            &candidates,
            &store,
            &signed,
            &format!("two fragments, {length} changes"),
        );
        let accepted = AcceptedHistory::default();
        let result = select(&accepted, &candidates, &store);
        assert_eq!(result.selected().len(), 2);
        assert!(result.pending().is_empty());
        for candidate in &candidates {
            let only = std::slice::from_ref(candidate);
            assert_matches(&[], only, &store, &signed, "one half missing");
            assert!(select(&accepted, only, &store).selected().is_empty());
        }
    }
}

#[test]
fn larger_loose_cascades_match_known_signed_prefixes() {
    // Too many candidates for subset enumeration. The independent answer for
    // a loose linear chain is exactly its longest directly-signed prefix.
    for length in [8, 32, 64] {
        let changes = chain(length, &[]);
        for gap in [0, length / 2, length - 1, length] {
            let signed_indices: Vec<_> = (0..length).filter(|i| *i != gap).collect();
            let (store, _) = evidence(&changes, &signed_indices);
            let accepted = AcceptedHistory::default();
            for reverse in [false, true] {
                let mut ordered: Vec<_> = changes.iter().map(CandidateHistory::Loose).collect();
                if reverse {
                    ordered.reverse();
                }
                let result = select(&accepted, &ordered, &store);
                assert_eq!(result.selected().len(), gap);
                assert_eq!(result.pending().len(), length - gap);
                for (index, candidate) in ordered.iter().enumerate() {
                    let original = if reverse { length - index - 1 } else { index };
                    let count = result
                        .selected()
                        .iter()
                        .filter(|entry| std::ptr::eq(**entry, candidate))
                        .count();
                    assert_eq!(count, usize::from(original < gap));
                    let count = result
                        .pending()
                        .iter()
                        .filter(|entry| std::ptr::eq(**entry, candidate))
                        .count();
                    assert_eq!(count, usize::from(original >= gap));
                }
            }
        }
    }
}

// Measure only select, excluding setup, signing, validity checks, and assertions.
fn median_select_time(mut run: impl FnMut()) -> Duration {
    run(); // Warm up separately from the samples.
    let mut samples = Vec::new();
    for _ in 0..5 {
        let start = Instant::now();
        run();
        samples.push(start.elapsed());
    }
    samples.sort();
    samples[samples.len() / 2]
}

/// Exploratory timing, not a CI performance assertion. Run in release mode:
/// cargo test -p automerge --release --lib selector_scaling -- --ignored --nocapture
#[test]
#[ignore = "opt-in release-mode selector timing; no performance threshold"]
fn selector_scaling() {
    println!("scenario,changes,candidates,median_us (5 samples, fixture/signing excluded)");
    for length in [64, 128, 256] {
        let changes = chain(length, &[length - 1]);
        let fragment = complete_fragment(&changes);
        let candidates = [CandidateHistory::CanonicalFragment {
            fragment: &fragment,
            members: &changes,
        }];
        let accepted = AcceptedHistory::default();
        for dense in [false, true] {
            let indices: Vec<_> = if dense {
                (0..length).collect()
            } else {
                vec![length - 1]
            };
            let (store, _) = evidence(&changes, &indices);
            assert_eq!(select(&accepted, &candidates, &store).selected().len(), 1);
            let time = median_select_time(|| {
                let _ = black_box(select(
                    black_box(&accepted),
                    black_box(&candidates),
                    black_box(&store),
                ));
            });
            println!(
                "{}, {length}, 1, {}",
                if dense {
                    "fragment_all_signed"
                } else {
                    "fragment_tip_signed"
                },
                time.as_micros()
            );
        }

        let mut extended = changes.clone();
        extended.push(at_level(0, length as u64 + 1, &[fragment.head], 0));
        let candidates = [CandidateHistory::Loose(&extended[length])];
        let accepted = AcceptedHistory::from_admitted(&extended[..length]);
        let (store, _) = evidence(&extended, &[length - 1, length]);
        assert_eq!(select(&accepted, &candidates, &store).selected().len(), 1);
        let time = median_select_time(|| {
            let _ = black_box(select(
                black_box(&accepted),
                black_box(&candidates),
                black_box(&store),
            ));
        });
        println!(
            "accepted_prefix_new_loose, {}, 1, {}",
            length + 1,
            time.as_micros()
        );

        let loose = chain(length, &[]);
        let candidates: Vec<_> = loose.iter().map(CandidateHistory::Loose).collect();
        let accepted = AcceptedHistory::default();
        let indices: Vec<_> = (1..length).collect();
        let (store, _) = evidence(&loose, &indices);
        let result = select(&accepted, &candidates, &store);
        assert!(result.selected().is_empty());
        assert_eq!(result.pending().len(), length);
        let time = median_select_time(|| {
            let _ = black_box(select(
                black_box(&accepted),
                black_box(&candidates),
                black_box(&store),
            ));
        });
        println!(
            "loose_root_blocked, {length}, {length}, {}",
            time.as_micros()
        );
    }
}
