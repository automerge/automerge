use super::*;

// Construct observations directly, without asking the encoder which patch to use.
fn map_observation(value: &Value, conflict: bool) -> Value {
    let mut map = crate::hydrate::Map::default();
    map.insert(
        "n".into(),
        crate::hydrate::MapValue {
            value: value.clone(),
            conflict,
        },
    );
    Value::Map(map)
}

fn list_observation(value: &Value, conflict: bool) -> Value {
    let mut list = crate::hydrate::List::default();
    list.push(value.clone(), ROOT, conflict);
    Value::List(list)
}

#[test]
fn bounded_retained_encoding_replays_final_observations() {
    let fx = fixture();
    let counters = [counter(-2), counter(0), counter(2)];
    let mut pairs: Vec<_> = counters
        .iter()
        .flat_map(|old| counters.iter().map(move |new| (old.clone(), new.clone())))
        .collect();
    for scalar in [
        Value::scalar(false),
        Value::scalar(7),
        Value::scalar("same"),
    ] {
        pairs.push((scalar.clone(), scalar));
    }
    let mut cases = 0;
    for before_count in 1..=3 {
        for after_count in 1..=3 {
            for (old, new) in &pairs {
                let (before, after) = retained_endpoints(&fx, old, new, before_count, after_count);
                for (site, actions, mut view, expected) in [
                    (
                        "map",
                        map_patches(&fx, before.clone(), after.clone()),
                        map_observation(old, before_count > 1),
                        map_observation(new, after_count > 1),
                    ),
                    (
                        "list",
                        list_patches(&fx, before, after),
                        list_observation(old, before_count > 1),
                        list_observation(new, after_count > 1),
                    ),
                ] {
                    for action in &actions {
                        view.apply(std::iter::empty(), fx.doc.text_encoding(), action.clone())
                            .unwrap();
                    }
                    assert_eq!(view, expected,
                        "{site}, counts={before_count}->{after_count}, {old:?}->{new:?}, patches={actions:?}");
                    cases += 1;
                }
            }
        }
    }
    // 9 cardinality pairs * (9 counter pairs + 3 stable scalars) * map/list.
    assert_eq!(cases, 216);
}
