#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("a change referenced an actor index we couldn't find")]
    MissingActor,
    #[error("changes out of order")]
    ChangesOutOfOrder,
    #[error("incorrect max op")]
    IncorrectMaxOp,
    #[error("missing ops")]
    MissingOps,
    #[error("missing ops")]
    MissingDep(#[from] crate::change_graph::MissingDep),
}
