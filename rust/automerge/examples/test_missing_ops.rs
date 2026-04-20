// Attempt to reproduce the MissingOps panic in automerge's change collector.
//
// The panic occurs at collector.rs:761 when from_build_meta_inner calls
// finish() on a VecEncoder that has gaps (None slots) in its op sequence.
//
// Key hypothesis: character-by-character splice_text from one actor,
// interleaved with fork+merge from another actor, with
// generate_sync_message calls in between, triggers the bug.

use automerge::{
    sync, sync::SyncDoc, ActorId, AutoCommit, ObjType, ReadDoc, ROOT,
    transaction::Transactable,
};

fn main() {
    println!("=== Test 1: Per-char splices + fork+merge + sync ===");
    test_char_splices_with_fork_merge_sync();

    println!("=== Test 2: Per-char from WASM peer + daemon fork+merge + 2-peer sync ===");
    test_wasm_typing_with_daemon_fork_merge();

    println!("=== Test 3: Interleaved per-char + fork_at(save_heads) + merge ===");
    test_interleaved_typing_fork_at();

    println!("=== Test 4: Many actors, per-char, partial sync ===");
    test_many_actors_partial_sync();

    println!("=== Test 5: Rapid fork+merge during active sync ===");
    test_fork_merge_during_sync();

    println!("=== Test 6: Per-char typing + execution outputs + sync ===");
    test_typing_outputs_sync();

    println!("=== Test 7: Character deletion interleaved with fork+merge ===");
    test_deletion_with_fork_merge();

    println!("=== Test 8: Stress: 1000 char splices + fork_at + merge + sync ===");
    test_stress_splices_fork_sync();

    println!("All tests passed without panic");
}

/// Simulate: frontend types char-by-char, daemon forks+merges for each
/// output write, generate_sync_message after each cycle.
fn test_char_splices_with_fork_merge_sync() {
    let mut doc = AutoCommit::new();
    doc.set_actor(ActorId::from(b"daemon" as &[u8]));

    let text = doc.put_object(ROOT, "source", ObjType::Text).unwrap();
    doc.commit();

    let mut peer_state = sync::State::new();

    // Simulate 100 cycles of: frontend types a char, daemon forks+merges
    for i in 0..100 {
        // Frontend char (simulated on same doc, different actor temporarily)
        let saved_actor = doc.get_actor().clone();
        doc.set_actor(ActorId::from(b"frontend" as &[u8]));
        doc.splice_text(&text, i, 0, &format!("{}", (b'a' + (i % 26) as u8) as char)).unwrap();
        doc.commit();
        doc.set_actor(saved_actor);

        // Daemon fork+merge (output write)
        let mut fork = doc.fork();
        fork.set_actor(ActorId::from(format!("daemon:fork-{}", i).as_bytes()));
        fork.put(ROOT, "output_hash", format!("hash-{}", i)).unwrap();
        fork.commit();
        doc.merge(&mut fork).unwrap();

        // Generate sync message
        let _msg = doc.sync().generate_sync_message(&mut peer_state);
    }

    println!("  PASS");
}

/// Two separate documents syncing: WASM peer types char-by-char,
/// daemon does fork+merge, sync after each round.
fn test_wasm_typing_with_daemon_fork_merge() {
    let mut daemon = AutoCommit::new();
    daemon.set_actor(ActorId::from(b"daemon" as &[u8]));

    let text = daemon.put_object(ROOT, "source", ObjType::Text).unwrap();
    daemon.commit();

    let mut wasm = AutoCommit::new();
    wasm.set_actor(ActorId::from(b"wasm-frontend" as &[u8]));

    let mut sw = sync::State::new();
    let mut sd = sync::State::new();

    // Initial sync
    sync_docs(&mut daemon, &mut sd, &mut wasm, &mut sw);

    // 200 rounds of typing + daemon fork+merge + sync
    for i in 0..200 {
        // WASM types one character
        wasm.splice_text(&text, i, 0, &format!("{}", (b'a' + (i % 26) as u8) as char)).unwrap();
        wasm.commit();

        // Sync wasm -> daemon (one round)
        sync_one(&mut wasm, &mut sw, &mut daemon, &mut sd);

        // Daemon fork+merge (simulating output write or format)
        let mut fork = daemon.fork();
        fork.set_actor(ActorId::from(format!("daemon:op-{}", i).as_bytes()));
        fork.put(ROOT, &format!("key_{}", i % 10), i as i64).unwrap();
        fork.commit();
        daemon.merge(&mut fork).unwrap();

        // Sync daemon -> wasm
        sync_one(&mut daemon, &mut sd, &mut wasm, &mut sw);
    }

    println!("  PASS");
}

/// Interleave per-char typing with fork_at(save_heads) + merge.
/// This is the exact pattern from our file watcher.
fn test_interleaved_typing_fork_at() {
    let mut daemon = AutoCommit::new();
    daemon.set_actor(ActorId::from(b"daemon" as &[u8]));

    let text = daemon.put_object(ROOT, "source", ObjType::Text).unwrap();
    daemon.splice_text(&text, 0, 0, "initial content").unwrap();
    daemon.commit();

    let save_heads = daemon.get_heads();

    let mut wasm = AutoCommit::new();
    wasm.set_actor(ActorId::from(b"wasm" as &[u8]));

    let mut sw = sync::State::new();
    let mut sd = sync::State::new();
    sync_docs(&mut daemon, &mut sd, &mut wasm, &mut sw);

    // WASM types 50 characters (each is a separate change)
    for i in 0..50 {
        let pos = 15 + i; // after "initial content"
        wasm.splice_text(&text, pos, 0, &format!("{}", i % 10)).unwrap();
        wasm.commit();

        // Partial sync every 5 chars
        if i % 5 == 0 {
            sync_one(&mut wasm, &mut sw, &mut daemon, &mut sd);
        }
    }

    // Full sync
    sync_docs(&mut wasm, &mut sw, &mut daemon, &mut sd);

    // Daemon does fork_at(save_heads) + merge — file watcher pattern
    match daemon.fork_at(&save_heads) {
        Ok(mut file_fork) => {
            file_fork.set_actor(ActorId::from(b"filesystem" as &[u8]));
            let len = file_fork.text(&text).unwrap().len() as isize;
            file_fork.splice_text(&text, 0, len, "disk content replaces everything").unwrap();
            file_fork.commit();
            daemon.merge(&mut file_fork).unwrap();
        }
        Err(e) => println!("  fork_at failed: {}", e),
    }

    // Now generate sync message — crash point in production
    let _msg = daemon.sync().generate_sync_message(&mut sd);

    println!("  PASS");
}

/// Many actors: daemon, 3 WASM peers, each typing chars, all syncing
/// through the daemon hub.
fn test_many_actors_partial_sync() {
    let mut daemon = AutoCommit::new();
    daemon.set_actor(ActorId::from(b"daemon" as &[u8]));

    let text = daemon.put_object(ROOT, "source", ObjType::Text).unwrap();
    daemon.commit();

    let mut peers: Vec<(AutoCommit, sync::State, sync::State)> = (0..3)
        .map(|i| {
            let mut p = AutoCommit::new();
            p.set_actor(ActorId::from(format!("peer-{}", i).as_bytes()));
            (p, sync::State::new(), sync::State::new())
        })
        .collect();

    // Initial sync all peers
    for (peer, sp, sd) in &mut peers {
        sync_docs(&mut daemon, sd, peer, sp);
    }

    // 100 rounds: each peer types a char, sync to daemon, daemon syncs to others
    for round in 0..100 {
        let peer_idx = round % 3;

        // This peer types
        let pos = daemon.text(&text).unwrap().len();
        let (peer, sp, sd) = &mut peers[peer_idx];
        let ch = format!("{}", (b'A' + (round % 26) as u8) as char);
        peer.splice_text(&text, pos, 0, &ch).unwrap();
        peer.commit();

        // Sync typing peer -> daemon
        sync_one(peer, sp, &mut daemon, sd);

        // Daemon writes kernel status
        if round % 3 == 0 {
            let mut fork = daemon.fork();
            fork.set_actor(ActorId::from(b"daemon:status" as &[u8]));
            fork.put(ROOT, "status", if round % 6 == 0 { "idle" } else { "busy" }).unwrap();
            fork.commit();
            daemon.merge(&mut fork).unwrap();
        }

        // Sync daemon -> other peers
        for (j, (peer, sp, sd)) in peers.iter_mut().enumerate() {
            if j != peer_idx {
                sync_one(&mut daemon, sd, peer, sp);
            }
        }
    }

    println!("  PASS");
}

/// Rapid fork+merge operations while sync is actively exchanging messages.
fn test_fork_merge_during_sync() {
    let mut daemon = AutoCommit::new();
    daemon.set_actor(ActorId::from(b"daemon" as &[u8]));

    let text = daemon.put_object(ROOT, "source", ObjType::Text).unwrap();
    daemon.splice_text(&text, 0, 0, "hello world").unwrap();
    daemon.commit();

    let mut peer = AutoCommit::new();
    peer.set_actor(ActorId::from(b"peer" as &[u8]));
    let mut sp = sync::State::new();
    let mut sd = sync::State::new();
    sync_docs(&mut daemon, &mut sd, &mut peer, &mut sp);

    // 50 rounds: peer types, partial sync, daemon fork+merge, partial sync
    for i in 0..50 {
        // Peer types
        let len = peer.text(&text).unwrap().len();
        peer.splice_text(&text, len, 0, &format!("{}", i % 10)).unwrap();
        peer.commit();

        // Partial sync (just one direction)
        if let Some(msg) = peer.sync().generate_sync_message(&mut sp) {
            daemon.sync().receive_sync_message(&mut sd, msg).unwrap();
        }

        // Daemon fork+merge (before responding with sync)
        let mut fork = daemon.fork();
        fork.set_actor(ActorId::from(format!("daemon:f{}", i).as_bytes()));
        fork.put(ROOT, "out", format!("{}", i)).unwrap();
        fork.commit();
        daemon.merge(&mut fork).unwrap();

        // Now daemon responds
        if let Some(msg) = daemon.sync().generate_sync_message(&mut sd) {
            peer.sync().receive_sync_message(&mut sp, msg).unwrap();
        }
    }

    println!("  PASS");
}

/// Simulate typing + execution outputs interleaved.
/// Each execution: daemon forks, writes output hash, merges back.
/// Meanwhile peer keeps typing.
fn test_typing_outputs_sync() {
    let mut daemon = AutoCommit::new();
    daemon.set_actor(ActorId::from(b"daemon" as &[u8]));

    let text = daemon.put_object(ROOT, "source", ObjType::Text).unwrap();
    let outputs = daemon.put_object(ROOT, "outputs", ObjType::Map).unwrap();
    daemon.commit();

    let mut peer = AutoCommit::new();
    peer.set_actor(ActorId::from(b"frontend" as &[u8]));
    let mut sp = sync::State::new();
    let mut sd = sync::State::new();
    sync_docs(&mut daemon, &mut sd, &mut peer, &mut sp);

    for i in 0..100 {
        // Peer types 3 chars rapidly
        for j in 0..3 {
            let pos = peer.text(&text).unwrap().len();
            peer.splice_text(&text, pos, 0, &format!("{}", (i * 3 + j) % 10)).unwrap();
        }
        peer.commit();

        // Sync peer -> daemon
        sync_one(&mut peer, &mut sp, &mut daemon, &mut sd);

        // Daemon writes output (fork+merge)
        let mut fork = daemon.fork();
        fork.set_actor(ActorId::from(b"daemon:output" as &[u8]));
        fork.put(&outputs, &format!("cell_{}", i % 5), format!("result_{}", i)).unwrap();
        fork.commit();
        daemon.merge(&mut fork).unwrap();

        // Sync daemon -> peer
        sync_one(&mut daemon, &mut sd, &mut peer, &mut sp);
    }

    // Final: get_changes from various points
    let heads = daemon.get_heads();
    let _changes = daemon.get_changes(&[]);
    let _changes = daemon.get_changes(&heads);

    println!("  PASS");
}

/// Character DELETION interleaved with fork+merge.
/// Deletions create tombstones — these might cause iter_ctr_range gaps.
fn test_deletion_with_fork_merge() {
    let mut daemon = AutoCommit::new();
    daemon.set_actor(ActorId::from(b"daemon" as &[u8]));

    let text = daemon.put_object(ROOT, "source", ObjType::Text).unwrap();

    // Type 200 characters
    for i in 0..200 {
        daemon.splice_text(&text, i, 0, &format!("{}", i % 10)).unwrap();
    }
    daemon.commit();

    let mid_heads = daemon.get_heads();

    let mut peer = AutoCommit::new();
    peer.set_actor(ActorId::from(b"peer" as &[u8]));
    let mut sp = sync::State::new();
    let mut sd = sync::State::new();
    sync_docs(&mut daemon, &mut sd, &mut peer, &mut sp);

    // Peer deletes characters one by one (creates tombstones)
    for i in (0..100).rev() {
        peer.splice_text(&text, i, 1, "").unwrap();
        peer.commit();

        // Sync every 10 deletions
        if i % 10 == 0 {
            sync_one(&mut peer, &mut sp, &mut daemon, &mut sd);

            // Daemon fork+merge during the deletion
            let mut fork = daemon.fork();
            fork.set_actor(ActorId::from(format!("daemon:del-{}", i).as_bytes()));
            fork.put(ROOT, "marker", format!("del-{}", i)).unwrap();
            fork.commit();
            daemon.merge(&mut fork).unwrap();
        }
    }

    // Full sync
    sync_docs(&mut peer, &mut sp, &mut daemon, &mut sd);

    // fork_at mid-point (before deletions)
    match daemon.fork_at(&mid_heads) {
        Ok(_) => {}
        Err(e) => println!("  fork_at failed (expected?): {}", e),
    }

    // generate_sync_message
    let _msg = daemon.sync().generate_sync_message(&mut sd);

    println!("  PASS");
}

/// Stress test: 1000 per-char splices from peer, interleaved with
/// fork_at(historical) + merge + sync on daemon.
fn test_stress_splices_fork_sync() {
    let mut daemon = AutoCommit::new();
    daemon.set_actor(ActorId::from(b"daemon" as &[u8]));

    let text = daemon.put_object(ROOT, "source", ObjType::Text).unwrap();
    daemon.splice_text(&text, 0, 0, "# notebook cell\n").unwrap();
    daemon.commit();

    let initial_heads = daemon.get_heads();

    let mut peer = AutoCommit::new();
    peer.set_actor(ActorId::from(b"wasm" as &[u8]));
    let mut sp = sync::State::new();
    let mut sd = sync::State::new();
    sync_docs(&mut daemon, &mut sd, &mut peer, &mut sp);

    let mut checkpoint_heads = vec![initial_heads.clone()];

    for i in 0..1000 {
        // Peer types one character
        let pos = peer.text(&text).unwrap().len();
        peer.splice_text(&text, pos, 0, &format!("{}", (b'a' + (i % 26) as u8) as char)).unwrap();
        peer.commit();

        // Sync every 10 chars
        if i % 10 == 0 {
            sync_one(&mut peer, &mut sp, &mut daemon, &mut sd);

            // Daemon fork+merge
            let mut fork = daemon.fork();
            fork.set_actor(ActorId::from(format!("d:f{}", i).as_bytes()));
            fork.put(ROOT, "exec_count", (i / 10) as i64).unwrap();
            fork.commit();
            daemon.merge(&mut fork).unwrap();

            sync_one(&mut daemon, &mut sd, &mut peer, &mut sp);
        }

        // Save checkpoint every 100 chars
        if i % 100 == 0 && i > 0 {
            let heads = daemon.get_heads();
            checkpoint_heads.push(heads);
        }

        // Every 200 chars, do fork_at from an older checkpoint
        if i % 200 == 0 && i > 0 {
            let old_heads = &checkpoint_heads[checkpoint_heads.len() / 2];
            match daemon.fork_at(old_heads) {
                Ok(mut fork) => {
                    fork.set_actor(ActorId::from(b"filesystem" as &[u8]));
                    fork.put(ROOT, "disk_marker", format!("save-{}", i)).unwrap();
                    fork.commit();
                    daemon.merge(&mut fork).unwrap();
                }
                Err(e) => println!("  fork_at failed at i={}: {}", i, e),
            }

            // Generate sync message after fork_at+merge
            let _msg = daemon.sync().generate_sync_message(&mut sd);
        }
    }

    // Final sync
    sync_docs(&mut peer, &mut sp, &mut daemon, &mut sd);

    // Try get_changes from each checkpoint
    for heads in &checkpoint_heads {
        let _changes = daemon.get_changes(heads);
    }

    println!("  PASS");
}

fn sync_docs(
    a: &mut AutoCommit, sa: &mut sync::State,
    b: &mut AutoCommit, sb: &mut sync::State,
) {
    for _ in 0..20 {
        let mut progressed = false;
        if let Some(msg) = a.sync().generate_sync_message(sa) {
            b.sync().receive_sync_message(sb, msg).unwrap();
            progressed = true;
        }
        if let Some(msg) = b.sync().generate_sync_message(sb) {
            a.sync().receive_sync_message(sa, msg).unwrap();
            progressed = true;
        }
        if !progressed { break; }
    }
}

fn sync_one(
    from: &mut AutoCommit, sf: &mut sync::State,
    to: &mut AutoCommit, st: &mut sync::State,
) {
    if let Some(msg) = from.sync().generate_sync_message(sf) {
        to.sync().receive_sync_message(st, msg).unwrap();
    }
    if let Some(msg) = to.sync().generate_sync_message(st) {
        from.sync().receive_sync_message(sf, msg).unwrap();
    }
}
