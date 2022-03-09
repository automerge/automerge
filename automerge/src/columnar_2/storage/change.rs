use std::{borrow::Cow, io::Write};

use crate::{ActorId, ChangeHash};

use super::{parse, ColumnMetadata, Chunk};

#[derive(Clone, Debug)]
pub(crate) struct Change<'a> {
    pub(crate) dependencies: Vec<ChangeHash>,
    pub(crate) actor: ActorId,
    pub(crate) other_actors: Vec<ActorId>,
    pub(crate) seq: u64,
    pub(crate) start_op: u64,
    pub(crate) timestamp: i64,
    pub(crate) message: Option<String>,
    pub(crate) ops_meta: ColumnMetadata,
    pub(crate) ops_data: Cow<'a, [u8]>,
    pub(crate) extra_bytes: Cow<'a, [u8]>,
}

impl<'a> Change<'a> {
    pub(crate) fn parse(input: &'a [u8]) -> parse::ParseResult<Change<'a>> {
        let (i, deps) = parse::length_prefixed(parse::leb128_u64, parse::change_hash)(input)?;
        let (i, actor) = parse::actor_id(i)?;
        let (i, seq) = parse::leb128_u64(i)?;
        let (i, start_op) = parse::leb128_u64(i)?;
        let (i, timestamp) = parse::leb128_i64(i)?;
        let (i, message_len) = parse::leb128_u64(i)?;
        let (i, message) = parse::utf_8(message_len as usize, i)?;
        let (i, other_actors) = parse::length_prefixed(parse::leb128_u64, parse::actor_id)(i)?;
        let (i, ops_meta) = ColumnMetadata::parse(i)?;
        let (i, ops_data) = parse::take_n(ops_meta.total_column_len(), i)?;
        Ok((
            &[],
            Change {
                dependencies: deps,
                actor,
                other_actors,
                seq,
                start_op,
                timestamp,
                message: if message.is_empty() {
                    None
                } else {
                    Some(message)
                },
                ops_meta,
                ops_data: Cow::Borrowed(ops_data),
                extra_bytes: Cow::Borrowed(i),
            },
        ))
    }

    fn byte_len(&self) -> usize {
        (self.dependencies.len() * 32)
            + 8
            + self.actor.to_bytes().len()
            + 24 // seq, start op, timestamp
            + 8
            + self.message.as_ref().map(|m| m.as_bytes().len()).unwrap_or(0_usize)
            + self.other_actors.iter().map(|a| a.to_bytes().len() + 8_usize).sum::<usize>()
            + self.ops_meta.byte_len()
            + self.ops_data.len()
            + self.extra_bytes.len()
    }

    pub(crate) fn write(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.byte_len());
        leb128::write::unsigned(&mut out, self.dependencies.len() as u64).unwrap();
        for dep in &self.dependencies {
            out.write_all(dep.as_bytes()).unwrap();
        }
        length_prefixed_bytes(&self.actor, &mut out);
        leb128::write::unsigned(&mut out, self.seq).unwrap();
        leb128::write::unsigned(&mut out, self.start_op).unwrap();
        leb128::write::signed(&mut out, self.timestamp).unwrap();
        length_prefixed_bytes(self.message.as_ref().map(|m| m.as_bytes()).unwrap_or(&[]), &mut out);
        leb128::write::unsigned(&mut out, self.other_actors.len() as u64).unwrap();
        for actor in self.other_actors.iter() {
            length_prefixed_bytes(&actor, &mut out);
        }
        self.ops_meta.write(&mut out);
        out.write_all(self.ops_data.as_ref()).unwrap();
        out.write_all(self.extra_bytes.as_ref()).unwrap();
        out
    }

    pub(crate) fn hash(&self) -> ChangeHash {
        let this = self.write();
        let chunk = Chunk::new_change(&this);
        chunk.hash()
    }

    pub(crate) fn into_owned(self) -> Change<'static> {
        Change{
            dependencies: self.dependencies,
            actor: self.actor,
            other_actors: self.other_actors,
            seq: self.seq,
            start_op: self.start_op,
            timestamp: self.timestamp,
            message: self.message,
            ops_meta: self.ops_meta,
            ops_data: Cow::Owned(self.ops_data.into_owned()),
            extra_bytes: Cow::Owned(self.extra_bytes.into_owned()),
        }
    }
}

fn length_prefixed_bytes<B: AsRef<[u8]>>(b: B, out: &mut Vec<u8>) -> usize {
    let prefix_len = leb128::write::unsigned(out, b.as_ref().len() as u64).unwrap();
    out.write_all(b.as_ref()).unwrap();
    prefix_len + b.as_ref().len()
}
