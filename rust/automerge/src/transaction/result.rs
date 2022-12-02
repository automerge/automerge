use crate::ChangeHash;

/// The result of a successful, and committed, transaction.
#[derive(Debug)]
pub struct Success<O, Obs> {
    /// The result of the transaction.
    pub result: O,
    /// The hash of the change, will be `None` if the transaction did not create any operations
    pub hash: Option<ChangeHash>,
    pub op_observer: Obs,
}

/// The result of a failed, and rolled back, transaction.
#[derive(Debug)]
pub struct Failure<E> {
    /// The error returned from the transaction.
    pub error: E,
    /// The number of operations cancelled.
    pub cancelled: usize,
}
