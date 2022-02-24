mod commit;
mod inner;
mod manual_transaction;
mod result;
mod transactable;

pub use self::commit::CommitOptions;
pub use self::transactable::Transactable;
pub(crate) use inner::TransactionInner;
pub use manual_transaction::Transaction;
pub use result::TransactionFailure;
pub use result::TransactionSuccess;

pub type TransactionResult<O, E> = Result<TransactionSuccess<O>, TransactionFailure<E>>;
