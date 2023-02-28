use automerge as am;

use crate::result::{to_result, AMresult};

/// \struct AMsyncHave
/// \installed_headerfile
/// \brief A summary of the changes that the sender of a synchronization
///        message already has.
#[derive(Clone, Eq, PartialEq)]
pub struct AMsyncHave(am::sync::Have);

impl AMsyncHave {
    pub fn new(have: am::sync::Have) -> Self {
        Self(have)
    }
}

impl AsRef<am::sync::Have> for AMsyncHave {
    fn as_ref(&self) -> &am::sync::Have {
        &self.0
    }
}

/// \memberof AMsyncHave
/// \brief Gets the heads of the sender.
///
/// \param[in] sync_have A pointer to an `AMsyncHave` struct.
/// \return A pointer to an `AMresult` struct with `AM_VAL_TYPE_CHANGE_HASH` items.
/// \pre \p sync_have `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// sync_have must be a valid pointer to an AMsyncHave
#[no_mangle]
pub unsafe extern "C" fn AMsyncHaveLastSync(sync_have: *const AMsyncHave) -> *mut AMresult {
    to_result(match sync_have.as_ref() {
        Some(sync_have) => sync_have.as_ref().last_sync.as_slice(),
        None => Default::default(),
    })
}
