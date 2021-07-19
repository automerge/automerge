use std::{
    fs::File,
    io::Read,
    time::{Duration, Instant},
};

use automerge::{Backend, Frontend, InvalidChangeRequest, LocalChange, Path, Primitive};
use automerge_frontend::Value;
use maplit::hashmap;
use rand::{thread_rng, Rng};
use smol_str::SmolStr;

fn f() {
    let mut doc = Frontend::new();
    let mut backend = Backend::new();

    let start = Instant::now();

    let mut m = hashmap! {
        "arstarstoien".into() =>
        Value::Map(hashmap!{
            "aboairentssroien".into()=>
            Value::Map(
                hashmap! {
                    "arostnaritsnabc".into() => Value::Primitive(Primitive::Str("hello world".into()))
                },
            ),
            "arsotind".into() => Value::Primitive(Primitive::Uint(20)),
        },)
    };

    for _ in 0..10 {
        let random_key: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        let random_value: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(50)
            .map(char::from)
            .collect();
        m.insert(
            random_key.into(),
            Value::Primitive(Primitive::Str(random_value.into())),
        );
    }

    let mut changes = Vec::new();
    let mut apply_changes = Vec::new();
    let mut apply_patches = Vec::new();

    let iterations = 10_000;
    for _ in 0..iterations {
        let random_string: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        let a = Instant::now();
        let change = doc
            .change::<_, _, InvalidChangeRequest>(None, |d| {
                d.add_change(LocalChange::set(
                    Path::root().key(random_string),
                    Value::Map(m.clone()),
                ))
            })
            .unwrap()
            .1
            .unwrap();
        changes.push(a.elapsed());
        let a = Instant::now();
        let (patch, _) = backend.apply_local_change(change).unwrap();
        apply_changes.push(a.elapsed());
        let a = Instant::now();
        doc.apply_patch(patch).unwrap();
        apply_patches.push(a.elapsed());
    }

    let save = Instant::now();
    let bytes = backend.save().unwrap();
    println!("len {}", bytes.len());
    let save = save.elapsed();
    let load = Instant::now();
    Backend::load(bytes).unwrap();
    let load = load.elapsed();

    println!(
        "maps x{} total:{:?} change:{:?} apply_change:{:?} apply_patch:{:?} save:{:?} load:{:?}",
        iterations,
        start.elapsed(),
        changes.iter().sum::<Duration>(),
        apply_changes.iter().sum::<Duration>(),
        apply_patches.iter().sum::<Duration>(),
        save,
        load,
    );
}

fn f_sync() {
    let mut doc = Frontend::new();
    let mut doc2 = Frontend::new();
    let mut backend = Backend::new();
    let mut backend2 = Backend::new();

    let mut sync_state = automerge_backend::SyncState::default();
    let mut sync_state2 = automerge_backend::SyncState::default();

    let start = Instant::now();

    let mut m = hashmap! {
        "arstarstoien".into() =>
        Value::Map(hashmap!{
            "aboairentssroien".into()=>
            Value::Map(
                hashmap! {
                    "arostnaritsnabc".into() => Value::Primitive(Primitive::Str("hello world".into()))
                },
            ),
            "arsotind".into() => Value::Primitive(Primitive::Uint(20)),
        },)
    };

    for _ in 0..10 {
        let random_key: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        let random_value: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(50)
            .map(char::from)
            .collect();
        m.insert(
            random_key.into(),
            Value::Primitive(Primitive::Str(random_value.into())),
        );
    }

    let mut changes = Vec::new();
    let mut apply_changes = Vec::new();
    let mut apply_patches = Vec::new();

    let iterations = 10_000;
    for _ in 0..iterations {
        let random_string: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        let a = Instant::now();

        let (doc, b) = if thread_rng().gen() {
            (&mut doc, &mut backend)
        } else {
            (&mut doc2, &mut backend2)
        };

        let change = doc
            .change::<_, _, InvalidChangeRequest>(None, |d| {
                d.add_change(LocalChange::set(
                    Path::root().key(random_string),
                    Value::Map(m.clone()),
                ))
            })
            .unwrap()
            .1
            .unwrap();
        changes.push(a.elapsed());
        let a = Instant::now();

        let (patch, _) = b.apply_local_change(change).unwrap();
        apply_changes.push(a.elapsed());
        let a = Instant::now();
        doc.apply_patch(patch).unwrap();
        apply_patches.push(a.elapsed());

        if let Some(msg) = backend.generate_sync_message(&mut sync_state) {
            backend2
                .receive_sync_message(&mut sync_state2, msg)
                .unwrap();
        }

        if let Some(msg) = backend2.generate_sync_message(&mut sync_state2) {
            backend.receive_sync_message(&mut sync_state, msg).unwrap();
        }

        if let Some(msg) = backend.generate_sync_message(&mut sync_state) {
            backend2
                .receive_sync_message(&mut sync_state2, msg)
                .unwrap();
        }

        if let Some(msg) = backend2.generate_sync_message(&mut sync_state2) {
            backend.receive_sync_message(&mut sync_state, msg).unwrap();
        }
    }

    let save = Instant::now();
    let bytes = backend.save().unwrap();
    println!("len {}", bytes.len());
    let save = save.elapsed();
    let load = Instant::now();
    Backend::load(bytes).unwrap();
    let load = load.elapsed();

    println!(
        "maps x{} total:{:?} change:{:?} apply_change:{:?} apply_patch:{:?} save:{:?} load:{:?}",
        iterations,
        start.elapsed(),
        changes.iter().sum::<Duration>(),
        apply_changes.iter().sum::<Duration>(),
        apply_patches.iter().sum::<Duration>(),
        save,
        load,
    );
}

fn g() {
    let mut doc = Frontend::new();
    let mut backend = Backend::new();

    let start = Instant::now();

    let change = doc
        .change::<_, _, InvalidChangeRequest>(None, |d| {
            d.add_change(LocalChange::set(Path::root().key("a"), Value::Text(vec![])))
        })
        .unwrap()
        .1
        .unwrap();
    let (patch, _) = backend.apply_local_change(change).unwrap();
    doc.apply_patch(patch).unwrap();

    let iterations = 10_000;
    for i in 0..iterations {
        let random_string: SmolStr = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(1)
            .map(char::from)
            .collect();
        let change = doc
            .change::<_, _, InvalidChangeRequest>(None, |d| {
                d.add_change(LocalChange::insert(
                    Path::root().key("a").index(i),
                    Value::Primitive(Primitive::Str(random_string)),
                ))
            })
            .unwrap()
            .1
            .unwrap();
        let (patch, _) = backend.apply_local_change(change).unwrap();
        doc.apply_patch(patch).unwrap();
    }

    let patch = backend.get_patch().unwrap();

    let mut f = Frontend::new();

    f.apply_patch(patch).unwrap();

    let save = Instant::now();
    let bytes = backend.save().unwrap();
    println!("len {}", bytes.len());
    let save = save.elapsed();
    let load = Instant::now();
    Backend::load(bytes).unwrap();
    let load = load.elapsed();

    println!(
        "seqs x{} {:?} save:{:?} load:{:?}",
        iterations,
        start.elapsed(),
        save,
        load
    );
}

fn h() {
    let start = Instant::now();

    let mut doc1 = Frontend::new();
    let changedoc1 = doc1
        .change::<_, _, InvalidChangeRequest>(None, |d| {
            d.add_change(LocalChange::set(
                Path::root().key("text"),
                Value::Text(Vec::new()),
            ))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();
    let mut backend1 = Backend::new();
    let (patch1, _) = backend1.apply_local_change(changedoc1).unwrap();
    doc1.apply_patch(patch1).unwrap();

    let mut doc2 = Frontend::new();
    let changedoc2 = backend1.get_changes(&[]);
    let mut backend2 = Backend::new();
    let patch2 = backend2
        .apply_changes(changedoc2.into_iter().cloned().collect())
        .unwrap();
    doc2.apply_patch(patch2).unwrap();

    let mut changes = Vec::new();
    let mut applys = Vec::new();

    let mut len = 0;
    let iterations = 10_000;
    for _ in 0..iterations {
        let random_string: char = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .next()
            .map(char::from)
            .unwrap();

        let a = Instant::now();
        let doc1_insert_change = doc1
            .change::<_, _, InvalidChangeRequest>(None, |d| {
                let weight = if len > 100 {
                    if len > 1000 {
                        0.45
                    } else {
                        0.55
                    }
                } else {
                    1.
                };
                if rand::thread_rng().gen_bool(weight) {
                    d.add_change(LocalChange::insert(
                        Path::root()
                            .key("text")
                            .index(rand::thread_rng().gen_range(0..=len)),
                        random_string.into(),
                    ))?;
                    len += 1;
                } else {
                    d.add_change(LocalChange::delete(
                        Path::root()
                            .key("text")
                            .index(rand::thread_rng().gen_range(0..len)),
                    ))?;
                    len -= 1;
                }
                Ok(())
            })
            .unwrap()
            .1
            .unwrap();
        changes.push(a.elapsed());

        let (patch, change_to_send) = backend1.apply_local_change(doc1_insert_change).unwrap();

        let a = Instant::now();
        doc1.apply_patch(patch).unwrap();
        applys.push(a.elapsed());

        let patch2 = backend2
            .apply_changes(vec![(change_to_send).clone()])
            .unwrap();
        let a = Instant::now();
        doc2.apply_patch(patch2).unwrap();
        applys.push(a.elapsed());
    }

    let save = Instant::now();
    let bytes = backend1.save().unwrap();
    println!("len {}", bytes.len());
    let save = save.elapsed();
    let load = Instant::now();
    Backend::load(bytes).unwrap();
    let load = load.elapsed();

    println!(
        "rand x{} {:?} change:{:?} apply:{:?} save:{:?} load:{:?}",
        iterations,
        start.elapsed(),
        changes.iter().sum::<Duration>(),
        applys.iter().sum::<Duration>(),
        save,
        load,
    );
}

fn trace(edits: Vec<(u32, u32, Option<String>)>) {
    let mut doc = Frontend::new();
    let mut backend = Backend::new();

    let start = Instant::now();

    let change = doc
        .change::<_, _, InvalidChangeRequest>(None, |d| {
            d.add_change(LocalChange::set(
                Path::root().key("text"),
                Value::Text(vec![]),
            ))
        })
        .unwrap()
        .1
        .unwrap();
    let (patch, _) = backend.apply_local_change(change).unwrap();
    doc.apply_patch(patch).unwrap();

    let loop_start = Instant::now();
    let num_chunks = 10;
    for (i, edits) in edits.chunks(num_chunks).enumerate() {
        if (i * num_chunks) % 10000 == 0 {
            println!(
                "processed {} changes in {:?}",
                i * num_chunks,
                loop_start.elapsed()
            );
        }
        let change = doc
            .change::<_, _, InvalidChangeRequest>(None, |d| {
                for edit in edits {
                    // if (edits[i][1] > 0) doc.text.deleteAt(edits[i][0], edits[i][1])
                    if edit.1 > 0 {
                        for j in 0..edit.1 {
                            d.add_change(LocalChange::delete(
                                Path::root().key("text").index(edit.0 + (j as u32)),
                            ))?;
                        }
                    }

                    // if (edits[i].length > 2) doc.text.insertAt(edits[i][0], ...edits[i].slice(2))
                    if let Some(c) = edit.2.clone() {
                        d.add_change(LocalChange::insert(
                            Path::root().key("text").index(edit.0),
                            Value::Primitive(Primitive::Str(SmolStr::new(c))),
                        ))?;
                    }
                }

                Ok(())
            })
            .unwrap()
            .1
            .unwrap();

        let (patch, _) = backend.apply_local_change(change).unwrap();
        doc.apply_patch(patch).unwrap();
    }
    println!("processed all changes in {:?}", loop_start.elapsed());

    let save = Instant::now();
    let bytes = backend.save().unwrap();
    println!("len {}", bytes.len());
    let save = save.elapsed();
    let load = Instant::now();
    Backend::load(bytes).unwrap();
    let load = load.elapsed();

    println!(
        "trace {:?} save:{:?} load:{:?}",
        start.elapsed(),
        save,
        load
    );
}

fn main() {
    let mut edits_file = File::open("perf/edits.json").unwrap();
    let mut buf = String::new();
    edits_file.read_to_string(&mut buf).unwrap();
    let edits: Vec<serde_json::Value> = serde_json::from_str(&buf).unwrap();
    let edits = edits
        .into_iter()
        .map(|e| {
            (
                if let serde_json::Value::Number(n) = &e[0] {
                    n.as_u64().unwrap() as u32
                } else {
                    panic!("not a number")
                },
                if let serde_json::Value::Number(n) = &e[1] {
                    n.as_u64().unwrap() as u32
                } else {
                    panic!("not a number")
                },
                e.get(2).map(|v| {
                    if let serde_json::Value::String(s) = v {
                        s.clone()
                    } else {
                        panic!("not a string")
                    }
                }),
            )
        })
        .collect::<Vec<_>>();

    println!("starting");

    let repeats = 1;

    for _ in 0..repeats {
        f()
    }
    for _ in 0..repeats {
        f_sync()
    }
    for _ in 0..repeats {
        g()
    }
    for _ in 0..repeats {
        h()
    }
    trace(edits)
}
