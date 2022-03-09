use std::{borrow::Cow, convert::{TryFrom, TryInto}};

use sha2::{Digest, Sha256};

use crate::ChangeHash;
use super::parse;


const MAGIC_BYTES: [u8; 4] = [0x85, 0x6f, 0x4a, 0x83];

#[derive(Clone, Copy, Debug)]
pub(crate) enum ChunkType {
    Document,
    Change,
    Compressed,
}

impl TryFrom<u8> for ChunkType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Document),
            1 => Ok(Self::Change),
            2 => Ok(Self::Compressed),
            other => Err(other),
        }
    }
}

impl From<ChunkType> for u8 {
    fn from(ct: ChunkType) -> Self {
        match ct {
            ChunkType::Document => 0,
            ChunkType::Change => 1,
            ChunkType::Compressed => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct CheckSum([u8; 4]);

impl CheckSum {
    fn bytes(&self) -> [u8; 4] {
        self.0
    }
}

impl From<[u8; 4]> for CheckSum {
    fn from(raw: [u8; 4]) -> Self {
        CheckSum(raw)
    }
}

impl From<ChangeHash> for CheckSum {
    fn from(h: ChangeHash) -> Self {
        let bytes = h.as_bytes();
        [bytes[0], bytes[1], bytes[2], bytes[3]].into()
    }
}


#[derive(Debug)]
pub(crate) struct Chunk<'a> {
    typ: ChunkType,
    checksum: CheckSum,
    data: Cow<'a, [u8]>,
}

impl<'a> Chunk<'a> {
    pub(crate) fn new_change(data: &'a [u8]) -> Chunk<'a> {
        let hash_result = hash(ChunkType::Change, data);
        Chunk{
            typ: ChunkType::Change,
            checksum: hash_result.into(),
            data: Cow::Borrowed(data),
        }
    }

    pub(crate) fn new_document(data: &'a [u8]) -> Chunk<'a> {
        let hash_result = hash(ChunkType::Document, data);
        Chunk{
            typ: ChunkType::Document,
            checksum: hash_result.into(),
            data: Cow::Borrowed(data),
        }
    }

    pub(crate) fn parse(input: &'a [u8]) -> parse::ParseResult<Chunk<'a>> {
        let (i, magic) = parse::take4(input)?;
        if magic != MAGIC_BYTES {
            return Err(parse::ParseError::Error(
                parse::ErrorKind::InvalidMagicBytes,
            ));
        }
        let (i, checksum_bytes) = parse::take4(i)?;
        let (i, raw_chunk_type) = parse::take1(i)?;
        let chunk_type: ChunkType = raw_chunk_type
            .try_into()
            .map_err(|e| parse::ParseError::Error(parse::ErrorKind::UnknownChunkType(e)))?;
        let (i, chunk_len) = parse::leb128_u64(i)?;
        let (i, data) = parse::take_n(chunk_len as usize, i)?;
        Ok((
            i,
            Chunk {
                typ: chunk_type,
                checksum: checksum_bytes.into(),
                data: Cow::Borrowed(data),
            },
        ))
    }

    fn byte_len(&self) -> usize {
        MAGIC_BYTES.len()
        + 1   // chunk type
        + 4 // checksum
        + 5  //length
        + self.data.len()
    }

    pub(crate) fn write(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.byte_len());
        out.extend(MAGIC_BYTES);
        out.extend(self.checksum.bytes());
        out.push(u8::from(self.typ));
        leb128::write::unsigned(&mut out, self.data.len() as u64).unwrap();
        out.extend(self.data.as_ref());
        out
    }

    pub(crate) fn checksum_valid(&self) -> bool {
        let hash = self.hash();
        let checksum = CheckSum(hash.checksum());
        checksum == self.checksum
    }

    pub(crate) fn hash(&self) -> ChangeHash {
        hash(self.typ, self.data.as_ref())
    }

    pub(crate) fn typ(&self) -> ChunkType {
        self.typ
    }

    pub(crate) fn checksum(&self) -> CheckSum {
        self.checksum
    }

    pub(crate) fn data(&self) -> Cow<'a, [u8]> {
        self.data.clone()
    }
}

fn hash(typ: ChunkType, data: &[u8]) -> ChangeHash {
    let mut out = Vec::new();
    out.push(u8::from(typ));
    leb128::write::unsigned(&mut out, data.len() as u64).unwrap();
    out.extend(data.as_ref());
    let hash_result = Sha256::digest(out);
    let array: [u8; 32] = hash_result.into();
    ChangeHash(array)
}
