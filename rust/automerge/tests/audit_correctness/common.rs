use std::panic::{catch_unwind, AssertUnwindSafe};

use automerge::next::transaction::CommitOptions;
use automerge::next::{Automerge, Fragment};
use automerge::{ActorId, ChangeId};
use rand::prelude::*;
use sha2::{Digest, Sha256};

pub(crate) fn actor(n: u8) -> ActorId {
    ActorId::from(&[n, n, n, n][..])
}

pub(crate) fn t0() -> CommitOptions {
    CommitOptions::default().with_time(0)
}

pub(crate) fn id(a: u8, seq: u64) -> ChangeId {
    ChangeId::from_parts(actor(a), std::num::NonZeroU64::new(seq).unwrap())
}

pub(crate) fn pair_mut<T>(items: &mut [T], i: usize, j: usize) -> (&mut T, &mut T) {
    assert_ne!(i, j);
    if i < j {
        let (l, r) = items.split_at_mut(j);
        (&mut l[i], &mut r[0])
    } else {
        let (l, r) = items.split_at_mut(i);
        (&mut r[0], &mut l[j])
    }
}

pub(crate) fn panic_msg(e: &Box<dyn std::any::Any + Send>) -> String {
    e.downcast_ref::<String>()
        .cloned()
        .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_default()
}

/// Arrival order may reorder a fragment's lists, never change them.
pub(crate) fn normalized(fragments: Vec<Fragment>) -> Vec<Fragment> {
    let mut fragments: Vec<_> = fragments
        .into_iter()
        .map(|mut f| {
            f.members.sort();
            f.checkpoints.sort();
            f.boundary.sort();
            f
        })
        .collect();
    fragments.sort_by_key(|f| f.head);
    fragments
}

pub(crate) fn read_uleb(b: &[u8], at: &mut usize) -> u64 {
    let mut out = 0u64;
    let mut shift = 0;
    loop {
        let byte = b[*at];
        *at += 1;
        out |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return out;
        }
        shift += 7;
    }
}

fn write_uleb(out: &mut Vec<u8>, v: u64) {
    leb128::write::unsigned(out, v).unwrap();
}

fn read_hash(b: &[u8], at: &mut usize) -> [u8; 32] {
    let h: [u8; 32] = b[*at..*at + 32].try_into().unwrap();
    *at += 32;
    h
}

/// Rewrites a chunk's length and checksum so a mutated body still parses.
pub(crate) fn fix_checksum(bytes: &[u8]) -> Vec<u8> {
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
    write_uleb(&mut len_bytes, data.len() as u64);
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

/// One to three byte edits of a chunk body, header left alone.
pub(crate) fn mutate(rng: &mut SmallRng, bytes: &[u8]) -> Vec<u8> {
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

pub(crate) struct FuzzOutcome {
    pub(crate) panics: Vec<String>,
    pub(crate) errors: Vec<String>,
}

/// Runs `exercise` over `AUDIT_FUZZ_ITERS` (default `iterations`) mutations
/// of each corpus entry. With `AUDIT_FUZZ_DUMP` set, every input that
/// panicked or errored is written to that directory.
pub(crate) fn mutation_fuzz(
    corpus: &[(Vec<u8>, Automerge)],
    seed: u64,
    iterations: usize,
    dump_tag: &str,
    exercise: fn(&[u8], &Automerge) -> Result<(), String>,
) -> FuzzOutcome {
    static LOC: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(|info| {
        let loc = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_default();
        *LOC.lock().unwrap() = Some(loc);
    }));
    let dump = std::env::var("AUDIT_FUZZ_DUMP").ok();
    let iterations = std::env::var("AUDIT_FUZZ_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(iterations);
    let mut outcome = FuzzOutcome {
        panics: Vec::new(),
        errors: Vec::new(),
    };
    for (ci, (bytes, base)) in corpus.iter().enumerate() {
        let mut rng = SmallRng::seed_from_u64(ci as u64 + seed);
        for it in 0..iterations {
            let m = mutate(&mut rng, bytes);
            match catch_unwind(AssertUnwindSafe(|| exercise(&m, base))) {
                Ok(Ok(())) => continue,
                Ok(Err(e)) => outcome.errors.push(format!("c{ci} i{it}: {e}")),
                Err(p) => {
                    let loc = LOC.lock().unwrap().take().unwrap_or_default();
                    outcome
                        .panics
                        .push(format!("c{ci} i{it}: {} @ {loc}", panic_msg(&p)));
                }
            }
            if let Some(d) = &dump {
                std::fs::write(format!("{d}/{dump_tag}c{ci}_i{it}.bin"), &m).unwrap();
            }
        }
    }
    std::panic::set_hook(prev);
    outcome
}

/// Fails listing one panic per distinct location.
#[track_caller]
pub(crate) fn assert_no_panics(panics: &[String]) {
    let mut seen = std::collections::BTreeSet::new();
    let distinct: Vec<&str> = panics
        .iter()
        .filter(|p| seen.insert(p.rsplit_once(" @ ").map_or(p.as_str(), |x| x.1)))
        .map(|p| p.as_str())
        .take(40)
        .collect();
    assert!(
        panics.is_empty(),
        "{} mutated inputs panicked; distinct locations:\n{}",
        panics.len(),
        distinct.join("\n")
    );
}

/// A bundle chunk's prefix, decoded so a test can forge it and still have
/// it parse.
#[derive(Debug, Clone)]
pub(crate) struct Prefix {
    pub(crate) actors: Vec<Vec<u8>>,
    pub(crate) heads: Vec<([u8; 32], u64)>,
    pub(crate) checkpoints: Vec<(u64, [u8; 32])>,
    pub(crate) retained: Vec<(u64, [u8; 32])>,
    pub(crate) boundary: Vec<([u8; 32], u64, u64)>,
    pub(crate) deps: Vec<(u64, u64)>,
    pub(crate) columns: Vec<u8>,
}

impl Prefix {
    pub(crate) fn parse(chunk: &[u8]) -> Prefix {
        let mut at = 8;
        assert_eq!(chunk[at], 4, "bundle chunk type");
        at += 1;
        let len = read_uleb(chunk, &mut at) as usize;
        let data = &chunk[at..at + len];
        let mut at = 0;
        let n = read_uleb(data, &mut at);
        let mut actors = Vec::new();
        for _ in 0..n {
            let l = read_uleb(data, &mut at) as usize;
            actors.push(data[at..at + l].to_vec());
            at += l;
        }
        let mut heads = Vec::new();
        for _ in 0..read_uleb(data, &mut at) {
            let h = read_hash(data, &mut at);
            heads.push((h, read_uleb(data, &mut at)));
        }
        let indexed = |at: &mut usize| {
            let mut v = Vec::new();
            for _ in 0..read_uleb(data, at) {
                let i = read_uleb(data, at);
                v.push((i, read_hash(data, at)));
            }
            v
        };
        let checkpoints = indexed(&mut at);
        let retained = indexed(&mut at);
        let mut boundary = Vec::new();
        for _ in 0..read_uleb(data, &mut at) {
            let h = read_hash(data, &mut at);
            let a = read_uleb(data, &mut at);
            boundary.push((h, a, read_uleb(data, &mut at)));
        }
        let mut deps = Vec::new();
        for _ in 0..read_uleb(data, &mut at) {
            let a = read_uleb(data, &mut at);
            deps.push((a, read_uleb(data, &mut at)));
        }
        Prefix {
            actors,
            heads,
            checkpoints,
            retained,
            boundary,
            deps,
            columns: data[at..].to_vec(),
        }
    }

    pub(crate) fn bytes(&self) -> Vec<u8> {
        let mut d = Vec::new();
        write_uleb(&mut d, self.actors.len() as u64);
        for a in &self.actors {
            write_uleb(&mut d, a.len() as u64);
            d.extend(a);
        }
        write_uleb(&mut d, self.heads.len() as u64);
        for (h, i) in &self.heads {
            d.extend(h);
            write_uleb(&mut d, *i);
        }
        for list in [&self.checkpoints, &self.retained] {
            write_uleb(&mut d, list.len() as u64);
            for (i, h) in list {
                write_uleb(&mut d, *i);
                d.extend(h);
            }
        }
        write_uleb(&mut d, self.boundary.len() as u64);
        for (h, a, s) in &self.boundary {
            d.extend(h);
            write_uleb(&mut d, *a);
            write_uleb(&mut d, *s);
        }
        write_uleb(&mut d, self.deps.len() as u64);
        for (a, s) in &self.deps {
            write_uleb(&mut d, *a);
            write_uleb(&mut d, *s);
        }
        d.extend(&self.columns);
        let mut len = Vec::new();
        write_uleb(&mut len, d.len() as u64);
        let mut hasher = Sha256::new();
        hasher.update([4u8]);
        hasher.update(&len);
        hasher.update(&d);
        let sum = hasher.finalize();
        let mut out = vec![0x85, 0x6f, 0x4a, 0x83];
        out.extend(&sum[..4]);
        out.push(4);
        out.extend(len);
        out.extend(d);
        out
    }
}
