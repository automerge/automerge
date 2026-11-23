//! Identity stability across anonymous bindings and actor-table maintenance.

use proptest::prelude::*;

use crate::actor::{ActorRemoval, ActorShift};
use crate::author::{Author, Authors};

#[derive(Clone, Debug)]
enum Binding {
    Unbound,
    Established(Option<Author<'static>>),
}

#[test]
fn rollback_discards_a_tentative_masked_author_binding() {
    use crate::transaction::Transactable;
    use crate::{ActorId, Automerge, Config, PatchLog, ROOT};

    let actor = ActorId::from([1]);
    let local = Author::from(vec![7]);
    let mut doc = Automerge::from_config(Config::default().author(Some(local.clone())))
        .with_actor(actor.clone())
        .unwrap();
    doc.mask_author(local.clone(), &[], &mut PatchLog::inactive())
        .unwrap();
    let mut tx = doc.transaction();
    tx.put(ROOT, "v", 1).unwrap();
    tx.rollback();
    assert_eq!(doc.get_author_for_actor(&actor), None);
    assert!(doc.ops.actors.lookup(&actor).is_none());
    let mut tx = doc.transaction();
    tx.put(ROOT, "v", 2).unwrap();
    tx.commit();
    assert_eq!(doc.get_author_for_actor(&actor), Some(local));
}

proptest! {
    #[test]
    fn binding_identity_survives_assignments_and_actor_shifts(
        ops in prop::collection::vec((0u8..4, any::<usize>(), prop::option::of(0u8..4)), 0..80)
    ) {
        let mut actual = Authors::with_actors(2);
        let mut model = vec![Binding::Unbound; 2];
        for (op, index, name) in ops {
            match op {
                0 | 1 if !model.is_empty() => {
                    let actor = index % model.len();
                    let author = name.map(|value| Author::from(vec![value]));
                    let expected = match &model[actor] {
                        Binding::Unbound => true,
                        Binding::Established(existing) => existing == &author,
                    };
                    let before = format!("{actual:?}");
                    prop_assert_eq!(actual.assign_identity(author.clone(), actor).is_ok(), expected);
                    if expected {
                        model[actor] = Binding::Established(author);
                    } else {
                        prop_assert_eq!(format!("{actual:?}"), before);
                    }
                }
                2 => {
                    let at = index % (model.len() + 1);
                    actual.insert_actor(&ActorShift::for_test(at));
                    model.insert(at, Binding::Unbound);
                }
                3 if !model.is_empty() => {
                    let at = index % model.len();
                    actual.remove_actor(&ActorRemoval::for_test(at));
                    model.remove(at);
                }
                _ => {}
            }
            for (actor, expected) in model.iter().enumerate() {
                match expected {
                    Binding::Unbound => {
                        prop_assert!(!actual.is_bound(actor));
                        prop_assert!(actual.get_author_for_actor(actor).is_none());
                    }
                    Binding::Established(author) => {
                        prop_assert!(actual.is_bound(actor));
                        prop_assert_eq!(actual.get_author_for_actor(actor).map(Author::into_owned), author.clone());
                    }
                }
            }
        }
    }
}
