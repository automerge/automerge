use std::ops::Range;

use crate::ChangeSignature;

pub(crate) mod bundle;
pub(crate) mod change;
mod chunk;
pub(crate) mod columns;
pub(crate) mod document;
pub(crate) mod load;
pub(crate) mod parse;

pub use bundle::{Bundle, BundleChange, BundleChangeIter};
pub use load::VerificationMode;

pub(crate) use {
    bundle::{BundleMetadata, BundleStorage},
    change::{AsChangeOp, Change, ChangeOp, Compressed, ReadChangeOpError},
    chunk::{CheckSum, Chunk, ChunkType, Header},
    columns::{ColumnSpec, Columns, RawColumn, RawColumns},
    document::{CompressConfig, Document},
};

fn shift_range(range: Range<usize>, by: usize) -> Range<usize> {
    range.start + by..range.end + by
}

pub(crate) const MAGIC_BYTES: [u8; 4] = [0x85, 0x6f, 0x4a, 0x83];

const SIGNATURE_TABLE_MAGIC: [u8; 4] = *b"AMST";
const SIGNATURE_TABLE_VERSION: u8 = 1;

#[derive(Debug, thiserror::Error)]
pub(crate) enum SignatureTableError {
    #[error(transparent)]
    Leb128(#[from] parse::leb128::Error),
    #[error("invalid signature table magic")]
    InvalidMagic,
    #[error("unsupported signature table version {0}")]
    UnsupportedVersion(u8),
    #[error("signature table indexes must be monotonically increasing")]
    NonMonotonicIndex,
}

pub(crate) fn write_signature_table<'a, I>(out: &mut Vec<u8>, signatures: I)
where
    I: IntoIterator<Item = (usize, &'a ChangeSignature)>,
{
    let mut signatures = signatures.into_iter().collect::<Vec<_>>();
    if signatures.is_empty() {
        return;
    }
    signatures.sort_by_key(|(index, _)| *index);

    out.extend_from_slice(&SIGNATURE_TABLE_MAGIC);
    out.push(SIGNATURE_TABLE_VERSION);
    leb128::write::unsigned(out, signatures.len() as u64).unwrap();

    let mut previous = 0_usize;
    for (position, (index, signature)) in signatures.into_iter().enumerate() {
        let delta = if position == 0 {
            index
        } else {
            index
                .checked_sub(previous)
                .expect("signature indexes are sorted above")
        };
        leb128::write::unsigned(out, delta as u64).unwrap();
        leb128::write::unsigned(out, signature.as_bytes().len() as u64).unwrap();
        out.extend_from_slice(signature.as_bytes());
        previous = index;
    }
}

pub(crate) fn parse_signature_table<'a, E>(
    input: parse::Input<'a>,
) -> parse::ParseResult<'a, Vec<(usize, ChangeSignature)>, E>
where
    E: From<SignatureTableError>,
{
    if input.is_empty() {
        return Ok((input, Vec::new()));
    }

    let (i, magic) = parse::take4(input)?;
    if magic != SIGNATURE_TABLE_MAGIC {
        return Err(parse::ParseError::Error(E::from(
            SignatureTableError::InvalidMagic,
        )));
    }
    let (i, version) = parse::take1(i)?;
    if version != SIGNATURE_TABLE_VERSION {
        return Err(parse::ParseError::Error(E::from(
            SignatureTableError::UnsupportedVersion(version),
        )));
    }
    let (mut i, count) = parse::leb128_u64(i).map_err(|e| e.lift())?;
    let mut result = Vec::with_capacity(count as usize);
    let mut previous = 0_usize;
    for position in 0..count as usize {
        let (next, delta) = parse::leb128_u64(i).map_err(|e| e.lift())?;
        let delta = delta as usize;
        let index = if position == 0 {
            delta
        } else {
            previous.checked_add(delta).ok_or_else(|| {
                parse::ParseError::Error(E::from(SignatureTableError::NonMonotonicIndex))
            })?
        };
        if position > 0 && index <= previous {
            return Err(parse::ParseError::Error(E::from(
                SignatureTableError::NonMonotonicIndex,
            )));
        }
        let (next, len) = parse::leb128_u64(next).map_err(|e| e.lift())?;
        let (next, bytes) = parse::take_n(len as usize, next)?;
        result.push((index, ChangeSignature::from(bytes)));
        previous = index;
        i = next;
    }
    Ok((i, result))
}
