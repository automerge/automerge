use crate::ChangeHash;

/// The result of a successful, and committed, transaction.
#[derive(Debug)]
pub struct TransactionSuccess<O> {
    pub(crate) result: O,
    pub(crate) heads: Vec<ChangeHash>,
}

impl<O> TransactionSuccess<O> {
    /// Get the result of the transaction.
    pub fn result(&self) -> &O {
        &self.result
    }

    /// Get the result of the transaction.
    pub fn into_result(self) -> O {
        self.result
    }

    /// Get the new heads of the document after commiting the transaction.
    pub fn heads(&self) -> &[ChangeHash] {
        &self.heads
    }

    /// Get the new heads of the document after commiting the transaction.
    pub fn into_heads(self) -> Vec<ChangeHash> {
        self.heads
    }
}

/// The result of a failed, and rolled back, transaction.
#[derive(Debug)]
pub struct TransactionFailure<E> {
    pub(crate) error: E,
    pub(crate) cancelled: usize,
}

impl<E> TransactionFailure<E> {
    /// Get the error of the transaction.
    pub fn error(&self) -> &E {
        &self.error
    }

    /// Get the error of the transaction.
    pub fn into_error(self) -> E {
        self.error
    }

    /// Get the number of cancelled operations in the transaction.
    pub fn cancelled(&self) -> usize {
        self.cancelled
    }
}
