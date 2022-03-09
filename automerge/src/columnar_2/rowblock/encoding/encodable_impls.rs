use super::Encodable;

use std::borrow::Cow;

use smol_str::SmolStr;
use std::io::Write;

/// Encodes bytes without a length prefix
pub(crate) struct RawBytes<'a>(Cow<'a, [u8]>);

impl<'a> From<&'a [u8]> for RawBytes<'a> {
    fn from(r: &'a [u8]) -> Self {
        RawBytes(r.into()) 
    }
}

impl<'a> From<Cow<'a, [u8]>> for RawBytes<'a> {
    fn from(c: Cow<'a, [u8]>) -> Self {
        RawBytes(c)
    }
}

impl<'a> Encodable for RawBytes<'a> {
    fn encode(&self, out: &mut Vec<u8>) -> usize {
        out.write_all(&self.0).unwrap();
        self.0.len() 
    }
}

impl Encodable for SmolStr {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        let bytes = self.as_bytes();
        let len_encoded = bytes.len().encode(buf);
        let data_len = bytes.encode(buf);
        len_encoded + data_len
    }
}

impl<'a> Encodable for Cow<'a, SmolStr> {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        self.as_ref().encode(buf)
    }
}

impl Encodable for String {
    fn encode(&self, buf: &mut Vec<u8>) ->usize {
        let bytes = self.as_bytes();
        let len_encoded = bytes.len().encode(buf);
        let data_len = bytes.encode(buf);
        len_encoded + data_len
    }
}

impl Encodable for Option<String> {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        if let Some(s) = self {
            s.encode(buf)
        } else {
            0.encode(buf)
        }
    }
}

impl<'a> Encodable for Option<Cow<'a, SmolStr>> {
    fn encode(&self, out: &mut Vec<u8>) -> usize {
        if let Some(s) = self {
            SmolStr::encode(s, out)
        } else {
            0.encode(out)
        }
    }
}

impl Encodable for u64 {
    fn encode(&self, buf: &mut Vec<u8>) -> usize{
        leb128::write::unsigned(buf, *self).unwrap()
    }
}

impl Encodable for f64 {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        let bytes = self.to_le_bytes();
        buf.write_all(&bytes).unwrap();
        bytes.len()
    }
}

impl Encodable for f32 {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        let bytes = self.to_le_bytes();
        buf.write_all(&bytes).unwrap();
        bytes.len()
    }
}

impl Encodable for i64 {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        leb128::write::signed(buf, *self).unwrap()
    }
}

impl Encodable for usize {
    fn encode(&self, buf: &mut Vec<u8>) -> usize{
        (*self as u64).encode(buf)
    }
}

impl Encodable for u32 {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        u64::from(*self).encode(buf)
    }
}

impl Encodable for i32 {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        i64::from(*self).encode(buf)
    }
}

impl Encodable for [u8] {
    fn encode(&self, out: &mut Vec<u8>) -> usize {
        out.write(self).unwrap()
    }
}

impl Encodable for &[u8] {
    fn encode(&self, out: &mut Vec<u8>) -> usize {
        out.write(self).unwrap()
    }
}

impl<'a> Encodable for Cow<'a, [u8]> {
    fn encode(&self, out: &mut Vec<u8>) -> usize {
        out.write(self).unwrap()
    }
}

impl Encodable for Vec<u8> {
    fn encode(&self, out: &mut Vec<u8>) -> usize {
        Encodable::encode(&self[..], out)
    }
}
