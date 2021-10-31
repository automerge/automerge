
//use std::convert::{TryFrom, TryInto};
use core::ops::Range;
use crate::{ Actor, Op, Change, ChangeHash, AutomergeError };
use sha2::Sha256;
use itertools::Itertools;
use sha2::Digest;
use std::{
    io,
    io::{Read, Write},
    mem,
};

const MAGIC_BYTES: [u8; 4] = [0x85, 0x6f, 0x4a, 0x83];
const PREAMBLE_BYTES: usize = 8;
const HEADER_BYTES: usize = PREAMBLE_BYTES + 1;

const HASH_BYTES: usize = 32;
const BLOCK_TYPE_DOC: u8 = 0;
const BLOCK_TYPE_CHANGE: u8 = 1;
const BLOCK_TYPE_DEFLATE: u8 = 2;
const CHUNK_START: usize = 8;
const HASH_RANGE: Range<usize> = 4..8;

trait Encodable {
    fn encode_with_actors_to_vec(&self, actors: &mut Vec<Actor>) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.encode_with_actors(&mut buf, actors)?;
        Ok(buf)
    }

    fn encode_with_actors<R: Write>(
        &self,
        buf: &mut R,
        _actors: &mut Vec<Actor>,
    ) -> io::Result<usize> {
        self.encode(buf)
    }

    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize>;
}

impl Encodable for String {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        let bytes = self.as_bytes();
        let head = bytes.len().encode(buf)?;
        buf.write_all(bytes)?;
        Ok(head + bytes.len())
    }
}

impl Encodable for Option<String> {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        if let Some(s) = self {
            s.encode(buf)
        } else {
            0.encode(buf)
        }
    }
}

impl Encodable for u64 {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        leb128::write::unsigned(buf, *self)
    }
}

impl Encodable for f64 {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        let bytes = self.to_le_bytes();
        buf.write_all(&bytes)?;
        Ok(bytes.len())
    }
}

impl Encodable for f32 {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        let bytes = self.to_le_bytes();
        buf.write_all(&bytes)?;
        Ok(bytes.len())
    }
}

impl Encodable for i64 {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        leb128::write::signed(buf, *self)
    }
}

impl Encodable for usize {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        (*self as u64).encode(buf)
    }
}

impl Encodable for u32 {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        u64::from(*self).encode(buf)
    }
}

impl Encodable for i32 {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        i64::from(*self).encode(buf)
    }
}

fn encode_changes(changes:&[Change], actors: &[usize]) -> (Vec<u8>, Vec<u8>) {
    unimplemented!()
}

fn encode_doc_ops(doc_ops: &[Op], actors: &[usize]) -> (Vec<u8>, Vec<u8>) {
    unimplemented!()
}

fn get_heads(changes: &[Change]) -> Vec<ChangeHash> {
    unimplemented!()
}

pub fn encode_document(changes: &[Change], doc_ops: &[Op]) -> Result<Vec<u8>, AutomergeError> {
    let mut bytes: Vec<u8> = Vec::new();

    let heads = get_heads(changes);

    // this assumes that all actor_ids referenced are seen in changes.actor_id which is true
    // so long as we have a full history
    let mut actors: Vec<_> = changes
        .iter()
        .map(|c| &c.actor)
        .unique()
        .sorted()
        .cloned()
        .collect();

    let (change_bytes, change_info) = encode_changes(changes, &actors);

    //let doc_ops = group_doc_ops(changes, &actors);

    let (ops_bytes, ops_info) = encode_doc_ops(doc_ops, &mut actors);

    bytes.extend(&MAGIC_BYTES);
    bytes.extend(vec![0, 0, 0, 0]); // we dont know the hash yet so fill in a fake
    bytes.push(BLOCK_TYPE_DOC);

    let mut chunk = Vec::new();

    actors.len().encode(&mut chunk)?;

    for a in &actors {
        a.to_bytes().encode(&mut chunk)?;
    }

    heads.len().encode(&mut chunk)?;
    for head in heads.iter().sorted() {
        chunk.write_all(&head.0).unwrap();
    }

    chunk.extend(change_info);
    chunk.extend(ops_info);

    chunk.extend(change_bytes);
    chunk.extend(ops_bytes);

    leb128::write::unsigned(&mut bytes, chunk.len() as u64).unwrap();

    bytes.extend(&chunk);

    let hash_result = Sha256::digest(&bytes[CHUNK_START..bytes.len()]);

    bytes.splice(HASH_RANGE, hash_result[0..4].iter().copied());

    Ok(bytes)
}
