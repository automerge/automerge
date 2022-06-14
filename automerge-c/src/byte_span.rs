use automerge as am;

use crate::AMchange;

/// \struct AMbyteSpan
/// \brief A contiguous sequence of bytes.
///
#[repr(C)]
pub struct AMbyteSpan {
    /// A pointer to an array of bytes.
    /// \warning \p src is only valid until the `AMfreeResult()` function is called
    ///          on the `AMresult` struct hosting the array of bytes to which
    ///          it points.
    src: *const u8,
    /// The number of bytes in the array.
    count: usize,
}

impl Default for AMbyteSpan {
    fn default() -> Self {
        Self {
            src: std::ptr::null(),
            count: 0,
        }
    }
}

impl From<&AMchange> for AMbyteSpan {
    fn from(change: &AMchange) -> Self {
        let change_hash = &(change.as_ref()).hash;
        change_hash.into()
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

impl From<&Vec<u8>> for AMbyteSpan {
    fn from(v: &Vec<u8>) -> Self {
        Self {
            src: (*v).as_ptr(),
            count: (*v).len(),
        }
    }
}
