use std::{
    fs::File,
    io::Read,
    time::{Duration, Instant},
};

use automerge::{Backend, Frontend, InvalidChangeRequest, LocalChange, Path, Primitive};
use automerge_frontend::Value;
use maplit::hashmap;
use rand::Rng;

fn f() {
    let mut doc = Frontend::new();
    let mut backend = Backend::new();

    let start = Instant::now();

    let m = hashmap! {
        "a".to_owned() =>
        Value::Map(hashmap!{
            "b".to_owned()=>
            Value::Map(
                hashmap! {
                    "abc".to_owned() => Value::Primitive(Primitive::Str("hello world".to_owned()))
                },
            ),
            "d".to_owned() => Value::Primitive(Primitive::Uint(20)),
        },)
    };

    let mut changes = Vec::new();
    let mut applys = Vec::new();

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
        let (patch, _) = backend.apply_local_change(change).unwrap();
        let a = Instant::now();
        doc.apply_patch(patch).unwrap();
        applys.push(a.elapsed());
    }

    let save = Instant::now();
    let bytes = backend.save().unwrap();
    let save = save.elapsed();
    let load = Instant::now();
    Backend::load(bytes).unwrap();
    let load = load.elapsed();

    println!(
        "maps x{} total:{:?} change:{:?} apply:{:?} save:{:?} load:{:?}",
        iterations,
        start.elapsed(),
        changes.iter().sum::<Duration>(),
        applys.iter().sum::<Duration>(),
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
        let random_string: String = rand::thread_rng()
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
                            Value::Primitive(Primitive::Str(c)),
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

    let save = Instant::now();
    let bytes = backend.save().unwrap();
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
        g()
    }
    for _ in 0..repeats {
        h()
    }
    trace(edits)
}
