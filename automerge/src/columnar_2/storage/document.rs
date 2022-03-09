use std::{borrow::Cow, io::Write};

use super::{column_metadata::ColumnMetadata, parse};

use crate::{ActorId, ChangeHash};

#[derive(Debug)]
pub(crate) struct Document<'a> {
    pub(crate) actors: Vec<ActorId>,
    pub(crate) heads: Vec<ChangeHash>,
    pub(crate) op_metadata: ColumnMetadata,
    pub(crate) op_bytes: Cow<'a, [u8]>,
    pub(crate) change_metadata: ColumnMetadata,
    pub(crate) change_bytes: Cow<'a, [u8]>,
    pub(crate) head_indices: Vec<u64>,
}

impl<'a> Document<'a> {
    pub(crate) fn parse(input: &'a [u8]) -> parse::ParseResult<Document<'a>> {
        let (i, actors) = parse::length_prefixed(parse::leb128_u64, parse::actor_id)(input)?;
        let (i, heads) = parse::length_prefixed(parse::leb128_u64, parse::change_hash)(i)?;
        let (i, change_meta) = ColumnMetadata::parse(i)?;
        let (i, ops_meta) = ColumnMetadata::parse(i)?;
        let (i, change_data) = parse::take_n(change_meta.total_column_len(), i)?;
        let (i, ops_data) = parse::take_n(ops_meta.total_column_len(), i)?;
        let (i, head_indices) = parse::apply_n(heads.len(), parse::leb128_u64)(i)?;
        Ok((
            i,
            Document {
                actors,
                heads,
                op_metadata: ops_meta,
                op_bytes: Cow::Borrowed(ops_data),
                change_metadata: change_meta,
                change_bytes: Cow::Borrowed(change_data),
                head_indices,
            },
        ))
    }

    fn byte_len(&self) -> usize {
        self.actors.iter().map(|a| a.to_bytes().len()).sum::<usize>()
            + (32 * self.heads.len())
            + self.op_metadata.byte_len()
            + self.op_bytes.len()
            + self.change_metadata.byte_len()
            + self.change_bytes.len()
            + (64 * self.head_indices.len())
    }

    pub(crate) fn write(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.byte_len());
        leb128::write::unsigned(&mut out, self.actors.len() as u64).unwrap();
        for actor in &self.actors {
            leb128::write::unsigned(&mut out, actor.to_bytes().len() as u64).unwrap();
            out.write_all(actor.to_bytes()).unwrap()
        }
        leb128::write::unsigned(&mut out, self.heads.len() as u64).unwrap();
        for head in &self.heads {
            out.write_all(head.as_bytes()).unwrap();
        }
        self.change_metadata.write(&mut out);
        self.op_metadata.write(&mut out);
        out.write_all(self.change_bytes.as_ref()).unwrap();
        out.write_all(self.op_bytes.as_ref()).unwrap();
        for index in &self.head_indices {
            leb128::write::unsigned(&mut out, *index).unwrap();
        }
        out
    }
}
