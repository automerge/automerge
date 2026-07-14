use crate::patches::PatchLog;
use crate::ChangeId;

/// The result of a successful, and committed, transaction.
#[derive(Debug)]
pub struct Success<O> {
    /// The result of the transaction.
    pub result: O,
    /// The id of the change, will be `None` if the transaction did not create any operations
    pub change_id: Option<ChangeId>,
    pub patch_log: PatchLog,
}

/// The result of a failed, and rolled back, transaction.
#[derive(Debug)]
pub struct Failure<E> {
    /// The error returned from the transaction.
    pub error: E,
    /// The number of operations cancelled.
    pub cancelled: usize,
}
