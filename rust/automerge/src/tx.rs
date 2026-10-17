mod commit;
mod inner;
mod manual_transaction;
mod owned_transaction;
mod result;
mod transactable;
mod tx_doc;

pub use self::commit::CommitOptions;
pub use self::transactable::{BlockOrText, Transactable};
pub(crate) use inner::{TransactionArgs, TransactionInner};
pub use manual_transaction::Transaction;
pub use owned_transaction::OwnedTransaction;
pub use result::Failure;
pub use result::Success;
pub(crate) use tx_doc::TxDoc;

pub type Result<O, E> = std::result::Result<Success<O>, Failure<E>>;
