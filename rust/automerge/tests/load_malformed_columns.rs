//! Loading a document or a bundle ran hexane's streaming decoders over
//! some of its columns without checking them first, and those decoders
//! panic (or loop forever) on malformed bytes. These tests replace one
//! column of a valid chunk with malformed bytes, fix up the checksum, and
//! expect an error.

use automerge::{transaction::Transactable, AutoCommit, Automerge, Bundle, ReadDoc, ROOT};
use sha2::{Digest, Sha256};

fn uleb(b: &[u8], pos: &mut usize) -> u64 {
    let (mut v, mut shift) = (0u64, 0);
    loop {
        let byte = b[*pos];
        *pos += 1;
        v |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return v;
        }
        shift += 7;
    }
}

fn write_uleb(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Skip a count-prefixed list of `item`s.
fn skip_list(b: &[u8], pos: &mut usize, item: impl Fn(&[u8], &mut usize)) {
    for _ in 0..uleb(b, pos) {
        item(b, pos);
    }
}

fn skip_hash(_: &[u8], pos: &mut usize) {
    *pos += 32;
}

fn skip_actor(b: &[u8], pos: &mut usize) {
    let len = uleb(b, pos) as usize;
    *pos += len;
}

/// Column metadata: (spec, data length) pairs.
fn columns(b: &[u8], pos: &mut usize) -> Vec<(u64, usize)> {
    let n = uleb(b, pos);
    (0..n)
        .map(|_| (uleb(b, pos), uleb(b, pos) as usize))
        .collect()
}

fn write_columns(out: &mut Vec<u8>, cols: &[(u64, usize)]) {
    write_uleb(out, cols.len() as u64);
    for &(spec, len) in cols {
        write_uleb(out, spec);
        write_uleb(out, len as u64);
    }
}

/// Column data laid out by `cols`, with the data of column `spec` (if
/// there) replaced by `new` of it.
fn split(
    data: &[u8],
    cols: &mut [(u64, usize)],
    spec: u64,
    new: &dyn Fn(&[u8]) -> Vec<u8>,
) -> (Vec<u8>, bool) {
    let (mut out, mut found, mut at) = (vec![], false, 0);
    for (s, len) in cols.iter_mut() {
        let old = &data[at..at + *len];
        at += *len;
        if *s == spec {
            let new = new(old);
            *len = new.len();
            out.extend(new);
            found = true;
        } else {
            out.extend_from_slice(old);
        }
    }
    assert_eq!(at, data.len());
    (out, found)
}

/// `chunk` (a document or a bundle, uncompressed) with the data of the
/// change column (`ops == false`) or op column (`ops == true`) `spec`
/// replaced by `new` of it.
fn replace_column(chunk: &[u8], ops: bool, spec: u64, new: &dyn Fn(&[u8]) -> Vec<u8>) -> Vec<u8> {
    let chunk_type = chunk[8];
    let mut pos = 9;
    let len = uleb(chunk, &mut pos) as usize;
    let body = &chunk[pos..pos + len];
    assert_eq!(pos + len, chunk.len(), "one chunk");
    let mut p = 0;
    let mut out = vec![];
    let found = match chunk_type {
        // document: actors, heads, change meta, op meta, change data, op data, rest
        0 => {
            skip_list(body, &mut p, skip_actor);
            skip_list(body, &mut p, skip_hash);
            out.extend_from_slice(&body[..p]);
            let mut ch = columns(body, &mut p);
            let mut op = columns(body, &mut p);
            let ch_len: usize = ch.iter().map(|c| c.1).sum();
            let op_len: usize = op.iter().map(|c| c.1).sum();
            let ch_data = &body[p..p + ch_len];
            let op_data = &body[p + ch_len..p + ch_len + op_len];
            let rest = &body[p + ch_len + op_len..];
            let (ch_data, f1) = split(ch_data, &mut ch, if ops { u64::MAX } else { spec }, new);
            let (op_data, f2) = split(op_data, &mut op, if ops { spec } else { u64::MAX }, new);
            write_columns(&mut out, &ch);
            write_columns(&mut out, &op);
            out.extend(ch_data);
            out.extend(op_data);
            out.extend_from_slice(rest);
            f1 || f2
        }
        // bundle: deps, actors, change meta, change data, op meta, op data, rest
        3 => {
            skip_list(body, &mut p, skip_hash);
            skip_list(body, &mut p, skip_actor);
            out.extend_from_slice(&body[..p]);
            let mut ch = columns(body, &mut p);
            let ch_len: usize = ch.iter().map(|c| c.1).sum();
            let ch_data = &body[p..p + ch_len];
            p += ch_len;
            let mut op = columns(body, &mut p);
            let op_len: usize = op.iter().map(|c| c.1).sum();
            let op_data = &body[p..p + op_len];
            let rest = &body[p + op_len..];
            let (ch_data, f1) = split(ch_data, &mut ch, if ops { u64::MAX } else { spec }, new);
            let (op_data, f2) = split(op_data, &mut op, if ops { spec } else { u64::MAX }, new);
            write_columns(&mut out, &ch);
            out.extend(ch_data);
            write_columns(&mut out, &op);
            out.extend(op_data);
            out.extend_from_slice(rest);
            f1 || f2
        }
        t => panic!("chunk type {t}"),
    };
    assert!(found, "no column {spec:#x}");
    let mut header = vec![chunk_type];
    write_uleb(&mut header, out.len() as u64);
    let mut hasher = Sha256::new();
    hasher.update(&header);
    hasher.update(&out);
    let hash = hasher.finalize();
    let mut chunk = chunk[..4].to_vec();
    chunk.extend_from_slice(&hash[..4]);
    chunk.extend(header);
    chunk.extend(out);
    chunk
}

fn doc() -> AutoCommit {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "a", 1).unwrap();
    doc.commit();
    doc.put(ROOT, "b", "two").unwrap();
    doc.commit();
    doc
}

const MALFORMED: &[&[u8]] = &[&[0x05], &[0x02, 0xff], &[0x7f], &[0x7e, 0x05]];

#[test]
fn the_unchanged_chunks_load() {
    let mut d = doc();
    let saved = d.save_nocompress();
    let same = |old: &[u8]| old.to_vec();
    for spec in [0x01, 0x03, 0x13, 0x40, 0x43] {
        assert_eq!(replace_column(&saved, false, spec, &same), saved);
    }
    let loaded = Automerge::load(&saved).unwrap();
    assert_eq!(
        loaded.get(ROOT, "b").unwrap().unwrap().0.as_str(),
        Some("two")
    );
    let heads = d.get_heads();
    let bundle = d.bundle(heads).unwrap();
    for spec in [0x01, 0x03, 0x13, 0x23] {
        assert_eq!(
            replace_column(bundle.bytes(), false, spec, &same),
            bundle.bytes()
        );
    }
    for spec in [0x15, 0x42] {
        assert_eq!(
            replace_column(bundle.bytes(), true, spec, &same),
            bundle.bytes()
        );
    }
    assert!(Bundle::try_from(bundle.bytes()).is_ok());
}

#[test]
fn malformed_document_change_columns_are_an_error() {
    let saved = doc().save_nocompress();
    // actor, seq, max_op, deps count, deps
    for spec in [0x01, 0x03, 0x13, 0x40, 0x43] {
        for bad in MALFORMED {
            let bytes = replace_column(&saved, false, spec, &|_| bad.to_vec());
            assert!(
                Automerge::load(&bytes).is_err(),
                "column {spec:#x} = {bad:x?}"
            );
        }
    }
}

#[test]
fn malformed_bundle_columns_are_an_error() {
    let mut d = doc();
    let heads = d.get_heads();
    let bundle = d.bundle(heads).unwrap();
    let bundle = bundle.bytes();
    // change columns: actor, seq, start_op, max_op
    for spec in [0x01, 0x03, 0x13, 0x23] {
        for bad in MALFORMED {
            let bytes = replace_column(bundle, false, spec, &|_| bad.to_vec());
            assert!(
                Bundle::try_from(&bytes[..]).is_err(),
                "change column {spec:#x} = {bad:x?}"
            );
        }
    }
    // op columns: key strings, actions
    for spec in [0x15, 0x42] {
        for bad in MALFORMED {
            let bytes = replace_column(bundle, true, spec, &|_| bad.to_vec());
            assert!(
                Bundle::try_from(&bytes[..]).is_err(),
                "op column {spec:#x} = {bad:x?}"
            );
        }
    }
}
