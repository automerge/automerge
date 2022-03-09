use std::{
    borrow::Cow,
    convert::{TryFrom, TryInto},
    io::Read,
    ops::Range,
};

use sha2::{Digest, Sha256};

use super::{change::Unverified, parse, Change, Compressed, Document, MAGIC_BYTES};
use crate::{columnar_2::encoding::leb128::ulebsize, ChangeHash};

pub(crate) enum Chunk<'a> {
    Document(Document<'a>),
    Change(Change<'a, Unverified>),
    CompressedChange(Change<'static, Unverified>, Compressed<'a>),
}

impl<'a> Chunk<'a> {
    pub(crate) fn parse(input: &'a [u8]) -> parse::ParseResult<'a, Chunk<'a>> {
        let (_, header) = Header::parse(input)?;
        let (remainder, chunkbytes) = parse::take_n(header.chunk_bytes().len(), input)?;
        let chunk = match header.chunk_type {
            ChunkType::Change => {
                let (remaining, change) = Change::parse_from_header(chunkbytes, header)?;
                if !remaining.is_empty() {
                    return Err(parse::ParseError::Error(parse::ErrorKind::LeftoverData));
                }
                Chunk::Change(change)
            }
            ChunkType::Document => {
                let (remaining, doc) = Document::parse(chunkbytes, header)?;
                if !remaining.is_empty() {
                    return Err(parse::ParseError::Error(parse::ErrorKind::LeftoverData));
                }
                Chunk::Document(doc)
            }
            ChunkType::Compressed => {
                let compressed = &input[header.data_bytes()];
                let mut decoder = flate2::bufread::DeflateDecoder::new(compressed);
                let mut decompressed = Vec::new();
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|_| parse::ParseError::Error(parse::ErrorKind::Deflate))?;
                let inner_header = header.with_data(ChunkType::Change, &decompressed);
                let mut inner_chunk = Vec::with_capacity(inner_header.len() + decompressed.len());
                inner_header.write(&mut inner_chunk);
                inner_chunk.extend(&decompressed);
                let (remaining, change) = Change::parse_from_header(&inner_chunk, inner_header)?;
                if !remaining.is_empty() {
                    return Err(parse::ParseError::Error(parse::ErrorKind::LeftoverData));
                }
                Chunk::CompressedChange(
                    change.into_owned(),
                    Compressed::new(header.checksum, Cow::Borrowed(chunkbytes)),
                )
            }
        };
        Ok((remainder, chunk))
    }

    pub(crate) fn checksum_valid(&self) -> bool {
        match self {
            Self::Document(d) => d.checksum_valid(),
            Self::Change(c) => c.checksum_valid(),
            Self::CompressedChange(change, compressed) => {
                compressed.checksum() == change.checksum() && change.checksum_valid()
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
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
    pub(crate) fn bytes(&self) -> [u8; 4] {
        self.0
    }
}

impl From<[u8; 4]> for CheckSum {
    fn from(raw: [u8; 4]) -> Self {
        CheckSum(raw)
    }
}

impl AsRef<[u8]> for CheckSum {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<ChangeHash> for CheckSum {
    fn from(h: ChangeHash) -> Self {
        let bytes = h.as_bytes();
        [bytes[0], bytes[1], bytes[2], bytes[3]].into()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Header {
    checksum: CheckSum,
    chunk_type: ChunkType,
    data_len: usize,
    header_size: usize,
    hash: ChangeHash,
}

impl Header {
    pub(crate) fn new(chunk_type: ChunkType, data: &[u8]) -> Self {
        let hash = hash(chunk_type, data);
        Self {
            hash,
            checksum: hash.checksum().into(),
            data_len: data.len(),
            header_size: MAGIC_BYTES.len()
                + 4 // checksum
                + 1 // chunk type
                + (ulebsize(data.len() as u64) as usize),
            chunk_type,
        }
    }

    /// Returns a header with the same checksum but with a different chunk type and data length.
    /// This is primarily useful when processing compressed chunks, where the checksum is actually
    /// derived from the uncompressed data.
    pub(crate) fn with_data(&self, chunk_type: ChunkType, data: &[u8]) -> Header {
        let hash = hash(chunk_type, data);
        Self {
            hash,
            checksum: self.checksum,
            data_len: data.len(),
            header_size: MAGIC_BYTES.len()
                + 4 // checksum
                + 1 // chunk type
                + (ulebsize(data.len() as u64) as usize),
            chunk_type,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.header_size
    }

    pub(crate) fn write(&self, out: &mut Vec<u8>) {
        out.extend(MAGIC_BYTES);
        out.extend(self.checksum.bytes());
        out.push(u8::from(self.chunk_type));
        leb128::write::unsigned(out, self.data_len as u64).unwrap();
    }

    pub(crate) fn parse(input: &[u8]) -> parse::ParseResult<'_, Header> {
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
        let header_size = input.len() - i.len();
        let (i, data) = parse::take_n(chunk_len as usize, i)?;
        let hash = hash(chunk_type, data);
        Ok((
            i,
            Header {
                checksum: checksum_bytes.into(),
                chunk_type,
                data_len: data.len() as usize,
                header_size,
                hash,
            },
        ))
    }

    /// The range of the input which corresponds to this chunk
    pub(crate) fn chunk_bytes(&self) -> Range<usize> {
        0..(self.data_len + self.header_size)
    }

    /// The range of the input which corresponds to the data specified by this header
    pub(crate) fn data_bytes(&self) -> Range<usize> {
        self.header_size..(self.header_size + self.data_len)
    }

    pub(crate) fn hash(&self) -> ChangeHash {
        self.hash
    }

    pub(crate) fn checksum_valid(&self) -> bool {
        CheckSum(self.hash.checksum()) == self.checksum
    }

    pub(crate) fn checksum(&self) -> CheckSum {
        self.checksum
    }
}

fn hash(typ: ChunkType, data: &[u8]) -> ChangeHash {
    let mut out = vec![u8::from(typ)];
    leb128::write::unsigned(&mut out, data.len() as u64).unwrap();
    out.extend(data.as_ref());
    let hash_result = Sha256::digest(out);
    let array: [u8; 32] = hash_result.into();
    ChangeHash(array)
}
