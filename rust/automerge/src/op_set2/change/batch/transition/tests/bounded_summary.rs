use super::*;

/// All 78 insertion orders/origin assignments for nonempty subsets of three
/// distinct IDs: 3*2 + 3*2!*4 + 3!*8. Equal payloads must not collapse identities.
#[test]
fn bounded_distinct_candidate_addition_is_order_invariant() {
    use itertools::Itertools;

    let mut cases = 0;
    for count in 1..=3 {
        for ids in (1..=3).combinations(count) {
            let origins = (0..count)
                .map(|_| [CandidateOrigin::Existing, CandidateOrigin::Incoming])
                .multi_cartesian_product();
            for origins in origins {
                let candidates: Vec<_> = ids.iter().copied().zip(origins).collect();
                // Combinations retain ascending ID order; the last candidate wins.
                let (greatest, expected_origin) = *candidates.last().unwrap();
                for order in candidates.iter().permutations(count) {
                    let mut summary = CandidateSummary::default();
                    for &(candidate, origin) in order {
                        match origin {
                            CandidateOrigin::Existing => {
                                summary.add_existing(id(candidate), Value::scalar(false));
                            }
                            CandidateOrigin::Incoming => {
                                summary.add_incoming(id(candidate), Value::scalar(false));
                            }
                        }
                    }
                    let winner = present(&summary);
                    assert_eq!(winner.winner.id, id(greatest));
                    assert_eq!(winner.winner.value, Value::scalar(false));
                    assert_eq!(winner.other_candidates + 1, count);
                    assert_eq!(winner.conflicted(), count > 1);
                    assert_eq!(winner.winner.origin, expected_origin);
                    cases += 1;
                }
            }
        }
    }
    assert_eq!(cases, 78);
}

/// Presence is independent of counts, payloads, and the after-winner's origin.
/// 3 cardinalities * 4 payloads * (2 appeared origins + 1 disappeared) = 36.
#[test]
fn bounded_presence_classification_carries_the_nonempty_endpoint() {
    let mut cases = 0;
    for count in 1..=3 {
        for value in [Value::scalar(false), counter(-2), counter(0), counter(2)] {
            for origin in [CandidateOrigin::Existing, CandidateOrigin::Incoming] {
                let mut after = CandidateSummary::default();
                for n in 1..=count {
                    after.add(Candidate {
                        id: id(n),
                        value: value.clone(),
                        origin,
                    });
                }
                let ValueTransition(Transition::Appeared { after }) =
                    ValueTransition::new(CandidateSummary::default(), after)
                else {
                    panic!("presence must classify as Appeared");
                };
                assert_eq!(after.winner.id, id(count));
                assert_eq!(after.winner.value, value);
                assert_eq!(after.winner.origin, origin);
                assert_eq!(after.other_candidates, count as usize - 1);
                cases += 1;
            }
            let mut before = CandidateSummary::default();
            for n in 1..=count {
                before.add_existing(id(n), value.clone());
            }
            let ValueTransition(Transition::Disappeared { before }) =
                ValueTransition::new(before, CandidateSummary::default())
            else {
                panic!("absence must classify as Disappeared");
            };
            assert_eq!(before.winner.id, id(count));
            assert_eq!(before.winner.value, value);
            assert_eq!(before.other_candidates, count as usize - 1);
            cases += 1;
        }
    }
    assert_eq!(cases, 36);
}

/// Equal scalar payloads cannot turn a changed identity into Retained. Incoming
/// replacements may rank above or below the old winner. Existing survivors were
/// already in the before set and therefore rank below its deleted winner.
#[test]
fn bounded_replacement_classification_ignores_rank_and_equal_payloads() {
    let mut cases = 0;
    for before_count in 1..=3 {
        for after_count in 1..=3 {
            for (old, new, origin) in [
                (10, 20, CandidateOrigin::Incoming),
                (20, 10, CandidateOrigin::Incoming),
                (20, 10, CandidateOrigin::Existing),
            ] {
                if origin == CandidateOrigin::Existing && before_count == 1 {
                    continue; // an existing survivor must have been a candidate
                }
                let mut before = existing(&[(old, Value::scalar(false))]);
                for n in 1..before_count {
                    let candidate = if origin == CandidateOrigin::Existing && n == 1 {
                        new
                    } else {
                        n
                    };
                    before.add_existing(id(candidate), Value::scalar(false));
                }
                let mut after = CandidateSummary::default();
                after.add(Candidate {
                    id: id(new),
                    value: Value::scalar(false),
                    origin,
                });
                for n in 1..after_count {
                    // Disjoint incoming losers, never duplicate endpoint IDs.
                    after.add_incoming(id(n + 3), Value::scalar(false));
                }
                let ValueTransition(Transition::WinnerReplaced { before, after }) =
                    ValueTransition::new(before, after)
                else {
                    panic!("different identity must classify as Replaced");
                };
                assert_eq!(before.winner.id, id(old));
                assert_eq!(after.winner.id, id(new));
                assert_eq!(before.winner.origin, CandidateOrigin::Existing);
                assert_eq!(after.winner.origin, origin);
                assert_eq!(before.other_candidates, before_count as usize - 1);
                assert_eq!(after.other_candidates, after_count as usize - 1);
                cases += 1;
            }
        }
    }
    assert_eq!(cases, 24);
}
