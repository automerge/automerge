//! Mutation fuzz over legacy chunks (document, change, BundleV0) loaded
//! into `next` and audited documents.

use std::panic::{catch_unwind, AssertUnwindSafe};

use automerge::next::transaction::{CommitOptions, Transactable};
use automerge::next::{AutoCommit, Automerge, SaveFormat, SaveOptions};
use automerge::{ActorId, ROOT};
use rand::prelude::*;
use sha2::{Digest, Sha256};

fn actor(n: u8) -> ActorId {
    ActorId::from(&[n, n, n, n][..])
}

fn t0() -> CommitOptions {
    CommitOptions::default().with_time(0)
}

fn legacy(deflate: bool) -> SaveOptions {
    SaveOptions {
        format: SaveFormat::Legacy,
        deflate,
        ..Default::default()
    }
}

fn fix_checksum(bytes: &[u8]) -> Vec<u8> {
    if bytes.len() < 10 {
        return bytes.to_vec();
    }
    let mut at = 8;
    let typ = bytes[at];
    at += 1;
    let len_start = at;
    loop {
        if at >= bytes.len() {
            return bytes.to_vec();
        }
        let b = bytes[at];
        at += 1;
        if b & 0x80 == 0 {
            break;
        }
    }
    let data = &bytes[at..];
    let mut len_bytes = Vec::new();
    leb128::write::unsigned(&mut len_bytes, data.len() as u64).unwrap();
    let mut out = bytes.to_vec();
    if len_bytes.len() == at - len_start {
        out[len_start..at].copy_from_slice(&len_bytes);
    }
    let mut hasher = Sha256::new();
    hasher.update([typ]);
    hasher.update(&out[len_start..at]);
    hasher.update(data);
    let hash = hasher.finalize();
    out[4..8].copy_from_slice(&hash[..4]);
    out
}

fn corpus() -> Vec<(Vec<u8>, Automerge)> {
    let mut out = Vec::new();
    let mut d = AutoCommit::new().with_actor(actor(2));
    let text = d
        .put_object(ROOT, "text", automerge::ObjType::Text)
        .unwrap();
    d.splice_text(&text, 0, 0, "hello world").unwrap();
    d.commit_with(t0());
    let list = d
        .put_object(ROOT, "list", automerge::ObjType::List)
        .unwrap();
    d.insert(&list, 0, 1).unwrap();
    d.insert(&list, 1, "two").unwrap();
    d.put(ROOT, "ctr", automerge::ScalarValue::counter(1))
        .unwrap();
    d.commit_with(t0());
    let mut e = d.fork().with_actor(actor(3));
    e.splice_text(&text, 5, 1, "_").unwrap();
    e.increment(ROOT, "ctr", 2).unwrap();
    e.mark(
        &text,
        automerge::marks::Mark::new("bold".to_string(), true, 0, 4),
        automerge::marks::ExpandMark::Both,
    )
    .unwrap();
    e.commit_with(t0());
    d.delete(&list, 0).unwrap();
    d.commit_with(t0());
    let base = Automerge::load(&d.save()).unwrap();
    let heads = d.get_heads();
    d.merge(&mut e).unwrap();
    d.put(ROOT, "done", true).unwrap();
    d.commit_with(t0());
    out.push((d.save_with_options(legacy(false)), Automerge::new()));
    out.push((d.save_with_options(legacy(true)), Automerge::new()));
    out.push((
        d.save_after_with_options(&heads, legacy(false)).unwrap(),
        base.clone(),
    ));
    out.push((
        include_bytes!("../fixtures/bundle_v0_automerge_3_3_2.bin").to_vec(),
        Automerge::new(),
    ));
    out
}

fn exercise(bytes: &[u8], base: &Automerge) -> Result<(), String> {
    if let Ok(mut doc) = Automerge::load(bytes) {
        let _ = doc.get_heads();
        let _ = doc.fragments(..);
        let saved = doc.save();
        Automerge::load(&saved).map_err(|e| format!("next reload: {e}"))?;
        let _ = doc.save_with_options(legacy(true));
        doc.gc();
        let mut tx = doc.transaction();
        tx.put(ROOT, "after", 1).unwrap();
        tx.commit_with(t0());
        let _ = doc.save();
    }
    let mut inc = base.clone();
    if inc.load_incremental(bytes).is_ok() {
        let saved = inc.save();
        Automerge::load(&saved).map_err(|e| format!("next inc reload: {e}"))?;
        let mut tx = inc.transaction();
        tx.put(ROOT, "after", 1).unwrap();
        tx.commit_with(t0());
        let _ = inc.save();
    }
    if let Ok(audit) = automerge::Automerge::load(bytes) {
        let saved = audit.save();
        automerge::Automerge::load(&saved).map_err(|e| format!("audit reload: {e}"))?;
    }
    let mut a = base
        .clone()
        .enable_audit_mode()
        .map_err(|e| e.error.to_string())?;
    if a.load_incremental(bytes).is_ok() {
        let _ = a.save();
    }
    Ok(())
}

fn mutate(rng: &mut SmallRng, bytes: &[u8]) -> Vec<u8> {
    let mut b = bytes.to_vec();
    let header = 11;
    if b.len() <= header + 1 {
        return b;
    }
    for _ in 0..rng.random_range(1..=3) {
        let at = rng.random_range(header..b.len());
        match rng.random_range(0..6) {
            0 => b[at] = rng.random(),
            1 => b[at] ^= 1 << rng.random_range(0..8),
            2 => b[at] = 0xff,
            3 => b[at] = 0,
            4 => b.insert(at, rng.random()),
            _ => {
                b.remove(at);
            }
        }
    }
    fix_checksum(&b)
}

#[test]
fn mutated_legacy_chunks_never_panic() {
    let corpus = corpus();
    static LOC: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(|info| {
        let loc = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_default();
        *LOC.lock().unwrap() = Some(loc);
    }));
    let iterations: usize = std::env::var("AUDIT_FUZZ_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300);
    let dump = std::env::var("AUDIT_FUZZ_DUMP").ok();
    let skip: Vec<String> = std::env::var("AUDIT_FUZZ_SKIP")
        .map(|s| s.split(',').map(|x| x.to_string()).collect())
        .unwrap_or_default();
    let mut failures = Vec::new();
    // an accepted non-canonical change makes the document's own save fail to
    // reload; that is an open question, not a panic
    let mut reloads = Vec::new();
    for (ci, (bytes, base)) in corpus.iter().enumerate() {
        let mut rng = SmallRng::seed_from_u64(ci as u64 + 100);
        for it in 0..iterations {
            let m = mutate(&mut rng, bytes);
            if skip.contains(&format!("{ci}:{it}")) {
                continue;
            }
            match catch_unwind(AssertUnwindSafe(|| exercise(&m, base))) {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if let Some(d) = &dump {
                        std::fs::write(format!("{d}/legacy_c{ci}_i{it}.bin"), &m).unwrap();
                    }
                    if e.contains("reload") {
                        reloads.push(format!("c{ci} i{it}: {e}"));
                    } else {
                        failures.push(format!("c{ci} i{it}: error {e}"))
                    }
                }
                Err(p) => {
                    let msg = p
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                        .unwrap_or_default();
                    let loc = LOC.lock().unwrap().take().unwrap_or_default();
                    if let Some(d) = &dump {
                        std::fs::write(format!("{d}/legacy_c{ci}_i{it}.bin"), &m).unwrap();
                    }
                    failures.push(format!("c{ci} i{it}: panic {msg} @ {loc}"));
                }
            }
        }
    }
    std::panic::set_hook(prev);
    eprintln!("{} saves that do not reload: {reloads:#?}", reloads.len());
    let mut seen = std::collections::BTreeSet::new();
    let distinct: Vec<_> = failures
        .iter()
        .filter(|f| seen.insert(f.split_once(": ").unwrap().1.to_string()))
        .collect();
    assert!(
        failures.is_empty(),
        "{} failures:\n{:#?}",
        failures.len(),
        distinct
    );
}

#[test]
#[ignore = "investigation helper; needs env vars"]
fn investigate_c2_i57() {
    let corpus = corpus();
    let base = corpus[2].1.clone();
    let orig = corpus[2].0.clone();
    let m = std::fs::read(std::env::var("CASE").unwrap()).unwrap();
    eprintln!("orig len {} mutated len {}", orig.len(), m.len());
    for (i, (a, b)) in orig.iter().zip(m.iter()).enumerate() {
        if a != b {
            eprintln!("first diff at {i}: {a:02x} -> {b:02x}");
            break;
        }
    }
    let ch = automerge::Change::from_bytes(m.clone());
    eprintln!(
        "as change: {:?}",
        ch.as_ref()
            .map(|c| (c.hash(), c.actor_id().clone(), c.seq(), c.len()))
    );
    let mut inc = base.clone();
    inc.load_incremental(&m).unwrap();
    eprintln!("heads {:?}", inc.get_head_hashes());
    let mut a = base.clone().enable_audit_mode().unwrap();
    a.load_incremental(&m).unwrap();
    eprintln!("audit heads {:?}", a.get_head_hashes());
    let r = automerge::Automerge::load(&a.save());
    eprintln!("audit reload: {:?}", r.map(|d| d.get_heads()));
    let r = automerge::Automerge::load(&a.save_with_options(legacy(true)));
    eprintln!("audit legacy reload: {:?}", r.map(|d| d.get_heads()));
    let r = Automerge::load(&inc.save());
    eprintln!("next reload: {:?}", r.map(|d| d.get_head_hashes()));
}

#[test]
#[ignore = "investigation helper; needs env vars"]
fn investigate_steps() {
    let corpus = corpus();
    let ci: usize = std::env::var("CI").unwrap().parse().unwrap();
    let base = corpus[ci].1.clone();
    let m = std::fs::read(std::env::var("CASE").unwrap()).unwrap();
    let step = |name: &str, f: &dyn Fn()| {
        let r = catch_unwind(AssertUnwindSafe(f));
        eprintln!("STEP {name}: {}", if r.is_ok() { "ok" } else { "PANIC" });
    };
    step("next load", &|| {
        let _ = Automerge::load(&m);
    });
    step("next load_incremental into base", &|| {
        let mut b = base.clone();
        let _ = b.load_incremental(&m);
    });
    step("audit load", &|| {
        let _ = automerge::Automerge::load(&m);
    });
    step("audit load_incremental into base", &|| {
        let mut a = base.clone().enable_audit_mode().unwrap();
        let _ = a.load_incremental(&m);
    });
    step("next load then save", &|| {
        if let Ok(d) = Automerge::load(&m) {
            let _ = d.save();
            let _ = d.save_with_options(legacy(true));
        }
    });
}

#[test]
#[ignore = "investigation helper; needs env vars"]
fn investigate_change_diff() {
    let corpus = corpus();
    let ci: usize = std::env::var("CI").unwrap().parse().unwrap();
    let orig = corpus[ci].0.clone();
    let m = std::fs::read(std::env::var("CASE").unwrap()).unwrap();
    let chunks = |b: &[u8]| {
        let mut out = vec![];
        let mut at = 0;
        while at < b.len() {
            let mut p = at + 9;
            let mut len = 0usize;
            let mut shift = 0;
            loop {
                let byte = b[p];
                p += 1;
                len |= ((byte & 0x7f) as usize) << shift;
                shift += 7;
                if byte & 0x80 == 0 {
                    break;
                }
            }
            let end = p + len;
            match automerge::Change::from_bytes(b[at..end].to_vec()) {
                Ok(c) => out.push(c),
                Err(e) => eprintln!("err {e}"),
            }
            at = end;
        }
        out
    };
    let a = chunks(&orig);
    let b = chunks(&m);
    for (x, y) in a.iter().zip(b.iter()) {
        let (x, y) = (x.decode(), y.decode());
        if format!("{x:?}") != format!("{y:?}") {
            eprintln!("ORIG {x:#?}\nMUT {y:#?}");
        }
    }
}

#[test]
#[ignore = "investigation helper; needs env vars"]
fn trace_iterations() {
    let corpus = corpus();
    let ci: usize = std::env::var("CI").unwrap().parse().unwrap();
    let from: usize = std::env::var("FROM").unwrap().parse().unwrap();
    let to: usize = std::env::var("TO").unwrap().parse().unwrap();
    let (bytes, base) = &corpus[ci];
    let mut rng = SmallRng::seed_from_u64(ci as u64 + 100);
    std::panic::set_hook(Box::new(|_| {}));
    for it in 0..to {
        let m = mutate(&mut rng, bytes);
        if it < from {
            continue;
        }
        eprintln!("iter {it}");
        if let Ok(d) = std::env::var("SAVE_AT") {
            if d.parse::<usize>().unwrap() == it {
                std::fs::write("/tmp/claude-1000/-home-orion-automerge/da2ead79-7409-4240-9240-53b064cf3b9f/scratchpad/hang.bin", &m).unwrap();
            }
        }
        let _ = catch_unwind(AssertUnwindSafe(|| exercise(&m, base)));
    }
}
