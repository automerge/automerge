mod commit;
mod inner;
mod manual_transaction;
mod result;
mod transactable;

pub use self::commit::CommitOptions;
pub use self::transactable::{BlockOrText, Transactable};
pub(crate) use inner::{TransactionArgs, TransactionInner};
pub use manual_transaction::Transaction;
pub use result::Failure;
pub use result::Success;

pub type Result<O, E> = std::result::Result<Success<O>, Failure<E>>;
