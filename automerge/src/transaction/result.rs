use crate::ChangeHash;

/// The result of a successful, and committed, transaction.
#[derive(Debug)]
pub struct Success<O> {
    /// The result of the transaction.
    pub result: O,
    /// The hash of the change, also the head of the document.
    pub hash: ChangeHash,
}

/// The result of a failed, and rolled back, transaction.
#[derive(Debug)]
pub struct Failure<E> {
    /// The error returned from the transaction.
    pub error: E,
    /// The number of operations cancelled.
    pub cancelled: usize,
}
