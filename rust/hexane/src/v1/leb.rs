//! LEB128 encoding/decoding helpers used by RLE and boolean encodings.

use std::ops::Range;

/// Stack-buffered LEB128 encoding (max 10 bytes, no heap allocation).
#[derive(Clone, Copy)]
pub(crate) struct Leb128Buf {
    pub(crate) buf: [u8; 10],
    pub(crate) len: u8,
}

impl Leb128Buf {
    #[inline]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.buf[..self.len as usize]
    }
}

impl std::ops::Deref for Leb128Buf {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// Owned byte iterator over a `Leb128Buf`. No heap allocation.
pub(crate) struct Leb128Iter {
    buf: [u8; 10],
    pos: u8,
    len: u8,
}

impl Iterator for Leb128Iter {
    type Item = u8;
    #[inline]
    fn next(&mut self) -> Option<u8> {
        if self.pos < self.len {
            let b = self.buf[self.pos as usize];
            self.pos += 1;
            Some(b)
        } else {
            None
        }
    }
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = (self.len - self.pos) as usize;
        (n, Some(n))
    }
}

impl ExactSizeIterator for Leb128Iter {}

impl IntoIterator for Leb128Buf {
    type Item = u8;
    type IntoIter = Leb128Iter;
    #[inline]
    fn into_iter(self) -> Leb128Iter {
        Leb128Iter {
            buf: self.buf,
            pos: 0,
            len: self.len,
        }
    }
}

/// Encode a signed integer as LEB128 into a stack buffer.
#[inline]
pub(crate) fn encode_signed(n: i64) -> Leb128Buf {
    let mut out = Leb128Buf {
        buf: [0u8; 10],
        len: 0,
    };
    let mut val = n;
    loop {
        let mut byte = (val & 0x7f) as u8;
        val >>= 7;
        let more = !((val == 0 && byte & 0x40 == 0) || (val == -1 && byte & 0x40 != 0));
        if more {
            byte |= 0x80;
        }
        out.buf[out.len as usize] = byte;
        out.len += 1;
        if !more {
            break;
        }
    }
    out
}

/// Encode an unsigned integer as LEB128 into a stack buffer.
#[inline]
pub(crate) fn encode_unsigned(n: u64) -> Leb128Buf {
    let mut out = Leb128Buf {
        buf: [0u8; 10],
        len: 0,
    };
    let mut val = n;
    loop {
        let mut byte = (val & 0x7f) as u8;
        val >>= 7;
        if val != 0 {
            byte |= 0x80;
        }
        out.buf[out.len as usize] = byte;
        out.len += 1;
        if val == 0 {
            break;
        }
    }
    out
}

/// Decode one signed LEB128 value from `data`. Returns `(bytes_read, value)`.
pub(crate) fn read_signed(data: &[u8]) -> Option<(usize, i64)> {
    let mut buf = data;
    let start = buf.len();
    let v = leb128::read::signed(&mut buf).ok()?;
    Some((start - buf.len(), v))
}

/// Decode one unsigned LEB128 value from `data`. Returns `(bytes_read, value)`.
pub(crate) fn read_unsigned(data: &[u8]) -> Option<(usize, u64)> {
    let mut buf = data;
    let start = buf.len();
    let v = leb128::read::unsigned(&mut buf).ok()?;
    Some((start - buf.len(), v))
}

/// Decode one unsigned LEB128 as `usize`. Convenience for bool encoding.
pub(crate) fn read_count(data: &[u8]) -> Option<(usize, usize)> {
    let (n, v) = read_unsigned(data)?;
    Some((n, v as usize))
}

/// Encode `n` as unsigned LEB128. Alias for bool encoding compatibility.
#[inline]
pub(crate) fn encode_count(n: usize) -> Leb128Buf {
    encode_unsigned(n as u64)
}

/// Compute the byte range of the signed LEB128 value at `pos` in `buf`.
pub(crate) fn leb_signed_bytes(buf: &[u8], pos: usize) -> Range<usize> {
    let mut tmp = &buf[pos..];
    let start = tmp.len();
    let _ = leb128::read::signed(&mut tmp);
    let n = start - tmp.len();
    pos..pos + n
}

/// Rewrite (or remove) a literal-run count header at `header_pos`.
/// Returns the change in buffer length (can be negative if header shrinks).
pub(crate) fn rewrite_lit_header(buf: &mut Vec<u8>, header_pos: usize, total: usize) -> i64 {
    let len = buf.len();
    let header_bytes = leb_signed_bytes(buf, header_pos);
    if total == 0 {
        buf.splice(header_bytes, []);
    } else {
        buf.splice(header_bytes, encode_signed(-(total as i64)));
    }
    buf.len() as i64 - len as i64
}
