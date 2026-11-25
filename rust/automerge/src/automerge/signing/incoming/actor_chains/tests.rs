use super::{propagate, AuthorSource, Candidate, Dependency, Resolution};

fn row(actor: u8, seq: u64, deps: Vec<Dependency<u8>>) -> Candidate<u8> {
    Candidate {
        actor,
        seq,
        actor_already_applied: false,
        deps,
    }
}

#[test]
fn links_through_candidates_to_the_exact_initial_or_applied_source() {
    use Dependency::{Applied, Candidate as Parent};
    use Resolution::{Broken, Linked};

    let rows = [
        row(3, 1, vec![]),
        row(4, 1, vec![]),
        row(3, 2, vec![Parent(1), Parent(0)]),
        row(3, 3, vec![Parent(0), Parent(2)]),
        row(
            5,
            9,
            vec![Applied {
                actor: 5,
                seq: 8,
                slot: 42,
            }],
        ),
        row(5, 10, vec![Parent(4)]),
        row(4, 3, vec![Parent(1), Parent(2)]),
    ];
    assert_eq!(
        propagate(&rows),
        [
            Linked(AuthorSource::Initial(0)),
            Linked(AuthorSource::Initial(1)),
            Linked(AuthorSource::Initial(0)),
            Linked(AuthorSource::Initial(0)),
            Linked(AuthorSource::Applied(42)),
            Linked(AuthorSource::Applied(42)),
            Broken(6),
        ],
    );
}

#[test]
fn a_broken_dependency_wins_even_after_a_matching_predecessor_was_seen() {
    use Dependency::Candidate as Parent;
    use Resolution::{Broken, Linked};

    let rows = [
        Candidate {
            actor_already_applied: true,
            ..row(8, 1, vec![])
        },
        row(9, 1, vec![]),
        row(9, 2, vec![Parent(1), Parent(0)]),
        row(10, 1, vec![Parent(2)]),
        row(11, 2, vec![]),
        row(12, 1, vec![Parent(4), Parent(2)]),
    ];
    assert_eq!(
        propagate(&rows),
        [
            Broken(0),
            Linked(AuthorSource::Initial(1)),
            Broken(0),
            Broken(0),
            Broken(4),
            Broken(4)
        ],
    );
}
