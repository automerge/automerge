use automerge::{transaction::Transactable, ActorId, AutoCommit, AutomergeError, ObjType, ROOT};

#[test]
fn same_actor_fork_merges_if_only_the_fork_writes() {
    let actor = ActorId::from(b"worker" as &[u8]);
    let mut doc = AutoCommit::new().with_actor(actor.clone());
    doc.put(ROOT, "base", 1).unwrap();
    doc.commit();

    let mut fork = doc.fork();
    fork.set_actor(actor.clone());
    fork.put(ROOT, "fork", 1).unwrap();
    fork.commit();

    doc.merge(&mut fork).unwrap();
}

#[test]
fn same_actor_fork_reuses_seq_after_concurrent_writes() {
    let actor = ActorId::from(b"worker" as &[u8]);
    let mut doc = AutoCommit::new().with_actor(actor.clone());
    doc.put(ROOT, "base", 1).unwrap();
    doc.commit();

    let mut fork = doc.fork();
    fork.set_actor(actor.clone());

    doc.put(ROOT, "main", 1).unwrap();
    doc.commit();

    fork.put(ROOT, "fork", 1).unwrap();
    fork.commit();

    assert!(matches!(
        doc.merge(&mut fork),
        Err(AutomergeError::DuplicateSeqNumber(2, duplicate_actor)) if duplicate_actor == actor
    ));
}

#[test]
fn two_same_actor_async_forks_collide_on_second_merge() {
    let actor = ActorId::from(b"worker" as &[u8]);
    let mut doc = AutoCommit::new().with_actor(actor.clone());
    doc.put(ROOT, "base", 1).unwrap();
    doc.commit();

    let mut fork1 = doc.fork();
    fork1.set_actor(actor.clone());
    let mut fork2 = doc.fork();
    fork2.set_actor(actor.clone());

    fork1.put(ROOT, "fork1", 1).unwrap();
    fork1.commit();
    fork2.put(ROOT, "fork2", 1).unwrap();
    fork2.commit();

    doc.merge(&mut fork1).unwrap();
    assert!(matches!(
        doc.merge(&mut fork2),
        Err(AutomergeError::DuplicateSeqNumber(2, duplicate_actor)) if duplicate_actor == actor
    ));
}

#[test]
fn isolated_historical_autocommit_writes_use_concurrent_actor() {
    let actor = ActorId::from(b"worker" as &[u8]);
    let mut doc = AutoCommit::new().with_actor(actor.clone());
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "abc").unwrap();
    doc.commit();
    let historical_heads = doc.get_heads();

    doc.splice_text(&text, 3, 0, " current").unwrap();
    doc.commit();

    doc.isolate(&historical_heads);
    doc.splice_text(&text, 3, 0, " isolated").unwrap();
    doc.commit();
    doc.integrate();

    let actors = doc
        .get_changes_meta(&[])
        .into_iter()
        .map(|change| change.actor.into_owned())
        .collect::<std::collections::HashSet<_>>();

    assert!(actors.contains(&actor));
    assert!(actors.iter().any(|actor| {
        actor.to_bytes().starts_with(&[0x13, 0xb2, 0x23, 0x09])
            && actor.to_bytes().ends_with(b"worker")
    }));
}

#[test]
fn full_fork_plus_isolate_allows_same_logical_actor_async_merge() {
    let actor = ActorId::from(b"worker" as &[u8]);
    let mut doc = AutoCommit::new().with_actor(actor.clone());
    doc.put(ROOT, "base", 1).unwrap();
    doc.commit();
    let historical_heads = doc.get_heads();

    doc.put(ROOT, "main", 1).unwrap();
    doc.commit();

    let mut fork = doc.fork();
    fork.set_actor(actor.clone());
    fork.isolate(&historical_heads);
    fork.put(ROOT, "fork", 1).unwrap();
    fork.commit();

    doc.merge(&mut fork).unwrap();

    let actors = doc
        .get_changes_meta(&[])
        .into_iter()
        .map(|change| change.actor.into_owned())
        .collect::<std::collections::HashSet<_>>();

    assert!(actors.contains(&actor));
    assert!(actors.iter().any(|actor| {
        actor.to_bytes().starts_with(&[0x13, 0xb2, 0x23, 0x09])
            && actor.to_bytes().ends_with(b"worker")
    }));
}

#[test]
fn fork_at_then_same_actor_lacks_future_seq_context_and_collides() {
    let actor = ActorId::from(b"worker" as &[u8]);
    let mut doc = AutoCommit::new().with_actor(actor.clone());
    doc.put(ROOT, "base", 1).unwrap();
    doc.commit();
    let historical_heads = doc.get_heads();

    doc.put(ROOT, "main", 1).unwrap();
    doc.commit();

    let mut fork = doc.fork_at(&historical_heads).unwrap();
    fork.set_actor(actor.clone());
    fork.put(ROOT, "fork", 1).unwrap();
    fork.commit();

    assert!(matches!(
        doc.merge(&mut fork),
        Err(AutomergeError::DuplicateSeqNumber(2, duplicate_actor)) if duplicate_actor == actor
    ));
}
