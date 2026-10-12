use std::io::Cursor;

use crate::{
    signatures::{Signature, SignatureRef},
    ChangeHash,
};

use super::{ChunkType, Header};

const VERSION: u8 = 0;

/// A chunk carrying detached signatures. Each signature is a target change
/// hash and an opaque, scheme-tagged byte string; a hash may carry several.
#[derive(Clone, Debug)]
pub(crate) struct SignatureChunk {
    header: Header,
    signatures: Vec<Signature>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ParseError {
    #[error("unsupported signature chunk version {0}")]
    UnsupportedVersion(u8),
    #[error("truncated signature chunk")]
    Truncated,
    #[error("invalid signature chunk length")]
    InvalidLength,
    #[error("signature chunk contains trailing bytes")]
    TrailingBytes,
}

impl SignatureChunk {
    /// Encode signatures as one chunk. The output is canonical: signatures are
    /// sorted by hash and then by bytes, and exact duplicates are dropped.
    pub(crate) fn encode<'a>(signatures: impl IntoIterator<Item = SignatureRef<'a>>) -> Vec<u8> {
        let mut signatures: Vec<_> = signatures.into_iter().collect();
        signatures.sort_unstable();
        signatures.dedup();
        let mut grouped: Vec<(ChangeHash, Vec<&[u8]>)> = Vec::new();
        for signature in signatures {
            match grouped.last_mut() {
                Some((last, group)) if *last == signature.change_hash => {
                    group.push(signature.bytes)
                }
                _ => grouped.push((signature.change_hash, vec![signature.bytes])),
            }
        }

        let mut data = vec![VERSION];
        leb128::write::unsigned(&mut data, grouped.len() as u64).unwrap();
        for (hash, group) in grouped {
            data.extend_from_slice(hash.as_bytes());
            leb128::write::unsigned(&mut data, group.len() as u64).unwrap();
            for bytes in group {
                leb128::write::unsigned(&mut data, bytes.len() as u64).unwrap();
                data.extend_from_slice(bytes);
            }
        }

        let header = Header::new(ChunkType::Signatures, &data);
        let mut out = Vec::with_capacity(header.len() + data.len());
        header.write(&mut out);
        out.extend_from_slice(&data);
        out
    }

    pub(crate) fn parse(data: &[u8], header: Header) -> Result<Self, ParseError> {
        let Some((&version, rest)) = data.split_first() else {
            return Err(ParseError::Truncated);
        };
        if version != VERSION {
            return Err(ParseError::UnsupportedVersion(version));
        }

        let mut cursor = Cursor::new(rest);
        let changes = read_uleb(&mut cursor)?;
        let mut signatures = Vec::new();
        for _ in 0..changes {
            let mut raw_hash = [0; 32];
            read_exact(&mut cursor, &mut raw_hash)?;
            let hash = ChangeHash(raw_hash);
            let signature_count = read_uleb(&mut cursor)?;
            for _ in 0..signature_count {
                let len: usize = read_uleb(&mut cursor)?
                    .try_into()
                    .map_err(|_| ParseError::InvalidLength)?;
                signatures.push(Signature {
                    change_hash: hash,
                    bytes: read_vec(&mut cursor, len)?,
                });
            }
        }
        if cursor.position() as usize != rest.len() {
            return Err(ParseError::TrailingBytes);
        }
        Ok(Self { header, signatures })
    }

    pub(crate) fn checksum_valid(&self) -> bool {
        self.header.checksum_valid()
    }

    pub(crate) fn into_signatures(self) -> Vec<Signature> {
        self.signatures
    }
}

fn read_uleb(cursor: &mut Cursor<&[u8]>) -> Result<u64, ParseError> {
    leb128::read::unsigned(cursor).map_err(|_| ParseError::Truncated)
}

fn read_exact(cursor: &mut Cursor<&[u8]>, out: &mut [u8]) -> Result<(), ParseError> {
    use std::io::Read;
    cursor.read_exact(out).map_err(|_| ParseError::Truncated)
}

fn read_vec(cursor: &mut Cursor<&[u8]>, len: usize) -> Result<Vec<u8>, ParseError> {
    let start: usize = cursor
        .position()
        .try_into()
        .map_err(|_| ParseError::InvalidLength)?;
    let end = start.checked_add(len).ok_or(ParseError::InvalidLength)?;
    let value = cursor
        .get_ref()
        .get(start..end)
        .ok_or(ParseError::Truncated)?
        .to_vec();
    cursor.set_position(end as u64);
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{parse, Chunk};

    #[test]
    fn signatures_round_trip_in_canonical_order() {
        let a = ChangeHash([1; 32]);
        let b = ChangeHash([2; 32]);
        let bytes = SignatureChunk::encode([
            SignatureRef {
                change_hash: b,
                bytes: &[1, 2],
            },
            SignatureRef {
                change_hash: a,
                bytes: &[0, 9],
            },
            SignatureRef {
                change_hash: a,
                bytes: &[0, 3],
            },
            SignatureRef {
                change_hash: a,
                bytes: &[0, 9],
            },
        ]);

        let (rest, chunk) = Chunk::parse(parse::Input::new(&bytes)).unwrap();
        assert!(rest.is_empty());
        let Chunk::Signatures(chunk) = chunk else {
            panic!("expected a signature chunk");
        };
        assert!(chunk.checksum_valid());
        assert_eq!(
            chunk.into_signatures(),
            vec![
                Signature {
                    change_hash: a,
                    bytes: vec![0, 3]
                },
                Signature {
                    change_hash: a,
                    bytes: vec![0, 9]
                },
                Signature {
                    change_hash: b,
                    bytes: vec![1, 2]
                },
            ]
        );
    }
}
