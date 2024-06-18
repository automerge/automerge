use super::{Encodable, Sink};

use std::borrow::Cow;

use smol_str::SmolStr;

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
    fn encode<S: Sink>(&self, out: &mut S) -> usize {
        out.append(&self.0);
        self.0.len()
    }
}

impl Encodable for SmolStr {
    fn encode<S: Sink>(&self, buf: &mut S) -> usize {
        let bytes = self.as_bytes();
        let len_encoded = bytes.len().encode(buf);
        let data_len = bytes.encode(buf);
        len_encoded + data_len
    }
}

impl<'a> Encodable for Cow<'a, SmolStr> {
    fn encode<S: Sink>(&self, buf: &mut S) -> usize {
        self.as_ref().encode(buf)
    }
}

impl Encodable for String {
    fn encode<S: Sink>(&self, buf: &mut S) -> usize {
        let bytes = self.as_bytes();
        let len_encoded = bytes.len().encode(buf);
        let data_len = bytes.encode(buf);
        len_encoded + data_len
    }
}

impl Encodable for Option<String> {
    fn encode<S: Sink>(&self, buf: &mut S) -> usize {
        if let Some(s) = self {
            s.encode(buf)
        } else {
            0.encode(buf)
        }
    }
}

impl<'a> Encodable for Option<Cow<'a, SmolStr>> {
    fn encode<S: Sink>(&self, out: &mut S) -> usize {
        if let Some(s) = self {
            SmolStr::encode(s, out)
        } else {
            0.encode(out)
        }
    }
}

impl Encodable for f64 {
    fn encode<S: Sink>(&self, buf: &mut S) -> usize {
        let bytes = self.to_le_bytes();
        buf.append(&bytes);
        bytes.len()
    }
}

impl Encodable for f32 {
    fn encode<S: Sink>(&self, buf: &mut S) -> usize {
        let bytes = self.to_le_bytes();
        buf.append(&bytes);
        bytes.len()
    }
}

impl Encodable for usize {
    fn encode<S: Sink>(&self, buf: &mut S) -> usize {
        (*self as u64).encode(buf)
    }
}

impl Encodable for u32 {
    fn encode<S: Sink>(&self, buf: &mut S) -> usize {
        u64::from(*self).encode(buf)
    }
}

impl Encodable for i32 {
    fn encode<S: Sink>(&self, buf: &mut S) -> usize {
        i64::from(*self).encode(buf)
    }
}

impl Encodable for [u8] {
    fn encode<S: Sink>(&self, out: &mut S) -> usize {
        out.append(self);
        self.len()
    }
}

impl Encodable for &[u8] {
    fn encode<S: Sink>(&self, out: &mut S) -> usize {
        out.append(self);
        self.len()
    }
}

impl<'a> Encodable for Cow<'a, [u8]> {
    fn encode<S: Sink>(&self, out: &mut S) -> usize {
        out.append(self);
        self.len()
    }
}

impl Encodable for Vec<u8> {
    fn encode<S: Sink>(&self, out: &mut S) -> usize {
        Encodable::encode(&self[..], out)
    }
}

mod leb128_things {
    use super::{Encodable, Sink};

    impl Encodable for u64 {
        fn encode<S: Sink>(&self, buf: &mut S) -> usize {
            let mut val = *self;
            let mut bytes_written = 0;
            loop {
                let mut byte = low_bits_of_u64(val);
                val >>= 7;
                if val != 0 {
                    // More bytes to come, so set the continuation bit.
                    byte |= CONTINUATION_BIT;
                }

                buf.append(&[byte]);
                bytes_written += 1;

                if val == 0 {
                    return bytes_written;
                }
            }
        }
    }

    impl Encodable for i64 {
        fn encode<S: Sink>(&self, buf: &mut S) -> usize {
            let mut val = *self;
            let mut bytes_written = 0;
            loop {
                let mut byte = val as u8;
                // Keep the sign bit for testing
                val >>= 6;
                let done = val == 0 || val == -1;
                if done {
                    byte &= !CONTINUATION_BIT;
                } else {
                    // Remove the sign bit
                    val >>= 1;
                    // More bytes to come, so set the continuation bit.
                    byte |= CONTINUATION_BIT;
                }

                buf.append(&[byte]);
                bytes_written += 1;

                if done {
                    return bytes_written;
                }
            }
        }
    }

    #[doc(hidden)]
    const CONTINUATION_BIT: u8 = 1 << 7;

    #[inline]
    fn low_bits_of_byte(byte: u8) -> u8 {
        byte & !CONTINUATION_BIT
    }

    #[inline]
    fn low_bits_of_u64(val: u64) -> u8 {
        let byte = val & (u8::MAX as u64);
        low_bits_of_byte(byte as u8)
    }
}
