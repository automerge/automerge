use std::ops;

/// A `Vec<u8>` whose contents have been validated (e.g. during load or encode).
///
/// Stored inside [`super::column::Slab`]. Derefs to [`ValidBytes`], so
/// indexing produces `&ValidBytes` slices that preserve the validation proof.
///
/// The constructors are `pub(crate)` — only load, encode, split, and merge
/// code paths create validated buffers.
#[derive(Clone, Debug, Default)]
pub struct ValidBuf(Vec<u8>);

impl ValidBuf {
    /// Wrap a `Vec<u8>` that has been validated.
    #[inline]
    pub(crate) fn new(data: Vec<u8>) -> Self {
        Self(data)
    }

    /// Mutable access to the underlying `Vec<u8>` for in-place mutations.
    ///
    /// Callers (insert, remove, splice) must preserve validity — i.e. only
    /// write bytes produced by `pack`.
    #[inline]
    pub(crate) fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        &mut self.0
    }
}

impl ops::Deref for ValidBuf {
    type Target = ValidBytes;
    #[inline]
    fn deref(&self) -> &ValidBytes {
        ValidBytes::from_bytes(&self.0)
    }
}

/// Validated byte slice — the `str` to [`ValidBuf`]'s `String`.
///
/// An unsized newtype over `[u8]`. `ValidBuf` derefs to `&ValidBytes`
/// just as `String` derefs to `&str`, so validated slices flow naturally
/// from slab data without explicit conversion.
///
/// `unpack` and `get_null` accept `&ValidBytes` so that the unsafe UTF-8
/// skip in `String::unpack` is statically guaranteed to operate only on
/// validated data.
///
/// Also derefs to `&[u8]` for functions that operate on raw bytes.
#[repr(transparent)]
pub struct ValidBytes([u8]);

impl ValidBytes {
    /// Wrap a byte slice as validated.
    ///
    /// Only usable inside this crate — external callers obtain `&ValidBytes`
    /// from `&ValidBuf` (via `Deref`) or by slicing an existing `&ValidBytes`.
    #[inline]
    pub(crate) fn from_bytes(data: &[u8]) -> &ValidBytes {
        // SAFETY: ValidBytes is #[repr(transparent)] over [u8].
        unsafe { &*(data as *const [u8] as *const ValidBytes) }
    }

    /// Access the underlying bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl ops::Deref for ValidBytes {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl ops::Index<ops::RangeFrom<usize>> for ValidBytes {
    type Output = ValidBytes;
    #[inline]
    fn index(&self, range: ops::RangeFrom<usize>) -> &ValidBytes {
        ValidBytes::from_bytes(&self.0[range])
    }
}

impl ops::Index<ops::Range<usize>> for ValidBytes {
    type Output = ValidBytes;
    #[inline]
    fn index(&self, range: ops::Range<usize>) -> &ValidBytes {
        ValidBytes::from_bytes(&self.0[range])
    }
}

impl ops::Index<ops::RangeTo<usize>> for ValidBytes {
    type Output = ValidBytes;
    #[inline]
    fn index(&self, range: ops::RangeTo<usize>) -> &ValidBytes {
        ValidBytes::from_bytes(&self.0[range])
    }
}

impl ops::Index<ops::RangeFull> for ValidBytes {
    type Output = ValidBytes;
    #[inline]
    fn index(&self, _: ops::RangeFull) -> &ValidBytes {
        self
    }
}
