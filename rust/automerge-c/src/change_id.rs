use automerge as am;
use std::cell::RefCell;
use std::ops::Deref;

use crate::actor_id::AMactorId;

/// \struct AMchangeId
/// \installed_headerfile
/// \brief A change's unique identifier: the pair of the actor which made it
///        and the 1-based sequence number of the change in that actor's
///        history.
///
/// Unlike a change hash, a change identifier is stable across documents and
/// can be computed without building the hash graph.
#[derive(Eq, PartialEq)]
pub struct AMchangeId {
    body: am::ChangeId,
    c_actor_id: RefCell<Option<AMactorId>>,
}

impl AMchangeId {
    pub fn new(change_id: am::ChangeId) -> Self {
        Self {
            body: change_id,
            c_actor_id: Default::default(),
        }
    }

    fn actor_id(&self) -> *const AMactorId {
        let mut c_actor_id = self.c_actor_id.borrow_mut();
        match c_actor_id.as_mut() {
            None => c_actor_id.insert(AMactorId::new(self.body.actor())) as *const AMactorId,
            Some(value) => value,
        }
    }
}

impl AsRef<am::ChangeId> for AMchangeId {
    fn as_ref(&self) -> &am::ChangeId {
        &self.body
    }
}

impl Deref for AMchangeId {
    type Target = am::ChangeId;

    fn deref(&self) -> &Self::Target {
        &self.body
    }
}

/// \memberof AMchangeId
/// \brief Gets the actor identifier component of a change identifier.
///
/// \param[in] change_id A pointer to an `AMchangeId` struct.
/// \return A pointer to an `AMactorId` struct or `NULL`.
/// \pre \p change_id `!= NULL`
/// \internal
///
/// # Safety
/// change_id must be a valid pointer to an AMchangeId
#[no_mangle]
pub unsafe extern "C" fn AMchangeIdActorId(change_id: *const AMchangeId) -> *const AMactorId {
    if let Some(change_id) = change_id.as_ref() {
        return change_id.actor_id();
    };
    std::ptr::null()
}

/// \memberof AMchangeId
/// \brief Gets the sequence number component of a change identifier.
///
/// \param[in] change_id A pointer to an `AMchangeId` struct.
/// \return A 64-bit unsigned integer.
/// \pre \p change_id `!= NULL`
/// \internal
///
/// # Safety
/// change_id must be a valid pointer to an AMchangeId
#[no_mangle]
pub unsafe extern "C" fn AMchangeIdSeq(change_id: *const AMchangeId) -> u64 {
    if let Some(change_id) = change_id.as_ref() {
        change_id.seq()
    } else {
        u64::MAX
    }
}

/// \memberof AMchangeId
/// \brief Tests the equality of two change identifiers.
///
/// \param[in] change_id1 A pointer to an `AMchangeId` struct.
/// \param[in] change_id2 A pointer to an `AMchangeId` struct.
/// \return `true` if \p change_id1 `==` \p change_id2 and `false` otherwise.
/// \pre \p change_id1 `!= NULL`
/// \pre \p change_id2 `!= NULL`
/// \post `!(`\p change_id1 `&&` \p change_id2 `) -> false`
/// \internal
///
/// #Safety
/// change_id1 must be a valid AMchangeId pointer
/// change_id2 must be a valid AMchangeId pointer
#[no_mangle]
pub unsafe extern "C" fn AMchangeIdEqual(
    change_id1: *const AMchangeId,
    change_id2: *const AMchangeId,
) -> bool {
    match (change_id1.as_ref(), change_id2.as_ref()) {
        (Some(change_id1), Some(change_id2)) => change_id1 == change_id2,
        (None, None) | (None, Some(_)) | (Some(_), None) => false,
    }
}
