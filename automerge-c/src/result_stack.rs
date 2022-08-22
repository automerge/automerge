use crate::result::{AMfree, AMresult, AMresultStatus, AMresultValue, AMstatus, AMvalue};

/// \struct AMresultStack
/// \brief A node in a singly-linked list of result pointers.
#[repr(C)]
pub struct AMresultStack {
    /// A result to be deallocated.
    pub result: *mut AMresult,
    /// The next node in the singly-linked list or `NULL`.
    pub next: *mut AMresultStack,
}

impl AMresultStack {
    pub fn new(result: *mut AMresult, next: *mut AMresultStack) -> Self {
        Self { result, next }
    }
}

/// \memberof AMresultStack
/// \brief Deallocates the storage for a stack of results.
///
/// \param[in,out] stack A pointer to a pointer to an `AMresultStack` struct.
/// \return The number of `AMresult` structs freed.
/// \pre \p stack `!= NULL`.
/// \post `*stack == NULL`.
/// \internal
///
/// # Safety
/// stack must be a valid AMresultStack pointer pointer
#[no_mangle]
pub unsafe extern "C" fn AMfreeStack(stack: *mut *mut AMresultStack) -> usize {
    if stack.is_null() {
        return 0;
    }
    let mut count: usize = 0;
    while !(*stack).is_null() {
        AMfree(AMpop(stack));
        count += 1;
    }
    count
}

/// \memberof AMresultStack
/// \brief Gets the topmost result from the stack after removing it.
///
/// \param[in,out] stack A pointer to a pointer to an `AMresultStack` struct.
/// \return A pointer to an `AMresult` struct or `NULL`.
/// \pre \p stack `!= NULL`.
/// \post `*stack == NULL`.
/// \internal
///
/// # Safety
/// stack must be a valid AMresultStack pointer pointer
#[no_mangle]
pub unsafe extern "C" fn AMpop(stack: *mut *mut AMresultStack) -> *mut AMresult {
    if stack.is_null() || (*stack).is_null() {
        return std::ptr::null_mut();
    }
    let top = Box::from_raw(*stack);
    *stack = top.next;
    let result = top.result;
    drop(top);
    result
}

/// \memberof AMresultStack
/// \brief The prototype of a function to be called when a value matching the
///        given discriminant cannot be extracted from the result at the top of
///        the given stack.
pub type AMpushCallback =
    Option<extern "C" fn(stack: *mut *mut AMresultStack, discriminant: u8) -> ()>;

/// \memberof AMresultStack
/// \brief Pushes the given result onto the given stack and then either extracts
///        a value matching the given discriminant from that result or,
///        failing that, calls the given function and gets a void value instead.
///
/// \param[in,out] stack A pointer to a pointer to an `AMresultStack` struct.
/// \param[in] result A pointer to an `AMresult` struct.
/// \param[in] discriminant An `AMvalue` variant's corresponding enum tag.
/// \param[in] callback A pointer to a function with the same signature as
///                     `AMpushCallback()` or `NULL`.
/// \return An `AMvalue` struct.
/// \pre \p stack `!= NULL`.
/// \pre \p result `!= NULL`.
/// \warning If \p stack `== NULL` then \p result is deallocated in order to
///          prevent a memory leak.
/// \internal
///
/// # Safety
/// stack must be a valid AMresultStack pointer pointer
/// result must be a valid AMresult pointer
#[no_mangle]
pub unsafe extern "C" fn AMpush<'a>(
    stack: *mut *mut AMresultStack,
    result: *mut AMresult,
    discriminant: u8,
    callback: AMpushCallback,
) -> AMvalue<'a> {
    if stack.is_null() {
        // There's no stack to push the result onto so it has to be freed in
        // order to prevent a memory leak.
        AMfree(result);
        if let Some(callback) = callback {
            callback(stack, discriminant);
        }
        return AMvalue::Void;
    } else if result.is_null() {
        if let Some(callback) = callback {
            callback(stack, discriminant);
        }
        return AMvalue::Void;
    }
    // Always push the result onto the stack, even if it's wrong, so that the
    // given callback can retrieve it.
    let node = Box::new(AMresultStack::new(result, *stack));
    let top = Box::into_raw(node);
    *stack = top;
    // Test that the result contains a value.
    match AMresultStatus(result) {
        AMstatus::Ok => {}
        _ => {
            if let Some(callback) = callback {
                callback(stack, discriminant);
            }
            return AMvalue::Void;
        }
    }
    // Test that the result's value matches the given discriminant.
    let value = AMresultValue(result);
    if discriminant != u8::from(&value) {
        if let Some(callback) = callback {
            callback(stack, discriminant);
        }
        return AMvalue::Void;
    }
    value
}
