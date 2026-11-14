use super::*;

fn hash(value: u8) -> ChangeHash {
    ChangeHash([value; 32])
}

#[test]
fn requirements_are_fixed_actor_frontiers_not_all_members() {
    let requirements =
        FragmentRequirements::for_members([(0, 2, hash(2)), (1, 1, hash(3)), (0, 1, hash(1))]);
    assert_eq!(
        requirements.hashes().copied().collect::<Vec<_>>(),
        [hash(2), hash(3)]
    );
    assert_eq!(requirements.hashes().len(), 2);
}

#[test]
fn one_actor_singleton_has_the_same_obligation_at_any_fragment_level() {
    for h in [hash(1), ChangeHash([0; 32])] {
        let requirements = FragmentRequirements::for_members([(0, 5, h)]);
        assert_eq!(requirements.hashes().copied().collect::<Vec<_>>(), [h]);
    }
}
