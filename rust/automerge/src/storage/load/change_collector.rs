use std::cmp::Ordering;
use std::{
    borrow::Cow,
    collections::{BTreeSet, HashMap},
    num::NonZeroU64,
};

use tracing::instrument;

use crate::change::Change;
use crate::storage::document::ReadChangeError;
use crate::{
    op_set2::{ActorIdx, KeyRef, Op, OpBuilder2, OpSet},
    storage::{
        change::{PredOutOfOrder, Verified},
        convert::ob_as_actor_id,
        Change as StoredChange, ChangeMetadata,
    },
    types::{ChangeHash, ObjId, OpId},
};

use fxhash::FxBuildHasher;

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
