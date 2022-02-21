mod commit;
mod inner;
mod result;
mod transactable;
mod transaction;

pub(crate) use inner::TransactionInner;
pub use result::TransactionFailure;
pub use result::TransactionSuccess;

pub type TransactionResult<O, E> = Result<TransactionSuccess<O>, TransactionFailure<E>>;

pub use self::commit::CommitOptions;
pub use self::transactable::Transactable;
pub use transaction::Transaction;
