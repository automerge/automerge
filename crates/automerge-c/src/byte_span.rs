use automerge as am;

/// \struct AMbyteSpan
/// \installed_headerfile
/// \brief A view onto a contiguous sequence of bytes.
#[repr(C)]
#[derive(Eq, PartialEq)]
pub struct AMbyteSpan {
    /// A pointer to an array of bytes.
    /// \attention <b>NEVER CALL `free()` ON \p src!</b>
    /// \warning \p src is only valid until the `AMfree()` function is called
    ///          on the `AMresult` struct that stores the array of bytes to
    ///          which it points.
    pub src: *const u8,
    /// The number of bytes in the array.
    pub count: usize,
}

impl Default for AMbyteSpan {
    fn default() -> Self {
        Self {
            src: std::ptr::null(),
            count: 0,
        }
    }
}

impl From<&am::ActorId> for AMbyteSpan {
    fn from(actor: &am::ActorId) -> Self {
        let slice = actor.to_bytes();
        Self {
            src: slice.as_ptr(),
            count: slice.len(),
        }
    }
}

impl From<&mut am::ActorId> for AMbyteSpan {
    fn from(actor: &mut am::ActorId) -> Self {
        let slice = actor.to_bytes();
        Self {
            src: slice.as_ptr(),
            count: slice.len(),
        }
    }
}

impl From<&am::ChangeHash> for AMbyteSpan {
    fn from(change_hash: &am::ChangeHash) -> Self {
        Self {
            src: change_hash.0.as_ptr(),
            count: change_hash.0.len(),
        }
    }
}

impl From<&[u8]> for AMbyteSpan {
    fn from(slice: &[u8]) -> Self {
        Self {
            src: slice.as_ptr(),
            count: slice.len(),
        }
    }
}
