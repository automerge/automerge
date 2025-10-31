use std::{borrow::Cow, io::Write, marker::PhantomData, num::NonZeroU64, ops::Range};

use crate::{convert, ActorId, ChangeHash, ScalarValue};

use super::{parse, shift_range, CheckSum, ChunkType, Header, RawColumns};

mod change_op_columns;
pub(crate) use change_op_columns::ChangeOpsColumns;
pub(crate) use change_op_columns::{ChangeOp, ReadChangeOpError};

mod change_actors;
pub(crate) use change_actors::PredOutOfOrder;
mod compressed;
mod op_with_change_actors;
pub(crate) use compressed::Compressed;

pub(crate) const DEFLATE_MIN_SIZE: usize = 256;

/// Changes present an iterator over the operations encoded in them. Before we have read these
/// changes we don't know if they are valid, so we expose an iterator with items which are
/// `Result`s. However, frequently we know that the changes are valid, this trait is used as a
/// witness that we have verified the operations in a change so we can expose an iterator which
/// does not return `Results`
pub(crate) trait OpReadState {}
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Verified;
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Unverified;
impl OpReadState for Verified {}
impl OpReadState for Unverified {}

/// A `Change` is the result of parsing a change chunk as specified in [1]
///
/// The type parameter to this type represents whether or not operation have been "verified".
/// Operations in a change chunk are stored in a compressed column oriented storage format. In
/// general there is no guarantee that this storage is valid. Therefore we use the `OpReadState`
/// type parameter to distinguish between contexts where we know that the ops are valid and those
/// where we don't. The `Change::verify_ops` method can be used to obtain a verified `Change` which
/// can provide an iterator over `ChangeOp`s directly, rather than over `Result<ChangeOp,
/// ReadChangeOpError>`.
///
/// [1]: https://alexjg.github.io/automerge-storage-docs/#change-chunks
#[derive(Clone, Debug)]
pub(crate) struct Change<'a, O: OpReadState> {
    /// The raw bytes of the entire chunk containing this change, including the header.
    pub(crate) bytes: Cow<'a, [u8]>,
    pub(crate) header: Header,
    pub(crate) dependencies: Vec<ChangeHash>,
    pub(crate) actor: ActorId,
    pub(crate) other_actors: Vec<ActorId>,
    pub(crate) seq: u64,
    pub(crate) start_op: NonZeroU64,
    pub(crate) timestamp: i64,
    pub(crate) message: Option<String>,
    pub(crate) ops_meta: ChangeOpsColumns,
    /// The range in `Self::bytes` where the ops column data is
    pub(crate) ops_data: Range<usize>,
    pub(crate) extra_bytes: Range<usize>,
    pub(crate) num_ops: usize,
    pub(crate) _phantom: PhantomData<O>,
}

impl<O: OpReadState> PartialEq for Change<'_, O> {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum ParseError {
    #[error(transparent)]
    Leb128(#[from] parse::leb128::Error),
    #[error(transparent)]
    InvalidUtf8(#[from] parse::InvalidUtf8),
    #[error("failed to parse change columns: {0}")]
    RawColumns(#[from] crate::storage::columns::raw_column::ParseError),
    #[error("failed to parse header: {0}")]
    Header(#[from] super::chunk::error::Header),
    #[error("change contained compressed columns")]
    CompressedChangeCols,
    #[error("invalid change cols: {0}")]
    InvalidColumns(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl<'a> Change<'a, Unverified> {
    pub(crate) fn parse(
        input: parse::Input<'a>,
    ) -> parse::ParseResult<'a, Change<'a, Unverified>, ParseError> {
        // TODO(alex): check chunk type
        let (i, header) = Header::parse(input)?;
        let parse::Split {
            first: chunk_input,
            remaining,
        } = i.split(header.data_bytes().len());
        let (_, change) = Self::parse_following_header(chunk_input, header)?;
        Ok((remaining, change))
    }

    /// Parse a change chunk. `input` should be the entire chunk, including the header bytes.
    pub(crate) fn parse_following_header(
        input: parse::Input<'a>,
        header: Header,
    ) -> parse::ParseResult<'a, Change<'a, Unverified>, ParseError> {
        let (i, deps) = parse::length_prefixed(parse::change_hash)(input)?;
        let (i, actor) = parse::actor_id(i)?;
        let (i, seq) = parse::leb128_u64(i)?;
        let (i, start_op) = parse::nonzero_leb128_u64(i)?;
        let (i, timestamp) = parse::leb128_i64(i)?;
        let (i, message_len) = parse::leb128_u64(i)?;
        let (i, message) = parse::utf_8(message_len as usize, i)?;
        let (i, other_actors) = parse::length_prefixed(parse::actor_id)(i)?;
        let (i, ops_meta) = RawColumns::parse(i)?;
        let (
            i,
            parse::RangeOf {
                range: ops_data, ..
            },
        ) = parse::range_of(|i| parse::take_n(ops_meta.total_column_len(), i), i)?;

        let (
            _i,
            parse::RangeOf {
                range: extra_bytes, ..
            },
        ) = parse::range_of(parse::take_rest, i)?;

        let ops_meta = ops_meta
            .uncompressed()
            .ok_or(parse::ParseError::Error(ParseError::CompressedChangeCols))?;

        let ops_meta = ChangeOpsColumns::try_from(ops_meta)?;

        Ok((
            parse::Input::empty(),
            Change {
                bytes: input.bytes().into(),
                header,
                dependencies: deps,
                actor,
                other_actors,
                seq,
                start_op,
                timestamp,
                message: if message.is_empty() {
                    None
                } else {
                    Some(message)
                },
                ops_meta,
                ops_data,
                extra_bytes,
                num_ops: 0,
                _phantom: PhantomData,
            },
        ))
    }

    /// Iterate over the ops in this chunk. The iterator will return an error if any of the ops are
    /// malformed.
    pub(crate) fn iter_ops(
        &'a self,
    ) -> impl Iterator<Item = Result<ChangeOp, ReadChangeOpError>> + Clone + 'a {
        self.ops_meta.iter(self.ops_data())
    }

    /// Verify all the ops in this change executing `f` for each one
    ///
    /// `f` will be called for each op in this change, allowing callers to collect additional
    /// information about the ops (e.g. all the actor IDs in the change, or the number of ops)
    ///
    /// # Errors
    /// * If there is an error reading an operation
    pub(crate) fn verify_ops<F: FnMut(ChangeOp)>(
        self,
        mut f: F,
    ) -> Result<Change<'a, Verified>, ReadChangeOpError> {
        let mut num_ops = 0;
        for op in self.iter_ops() {
            f(op?);
            num_ops += 1;
        }
        if u32::try_from(u64::from(self.start_op)).is_err() {
            return Err(ReadChangeOpError::CounterTooLarge);
        }
        Ok(Change {
            bytes: self.bytes,
            header: self.header,
            dependencies: self.dependencies,
            actor: self.actor,
            other_actors: self.other_actors,
            seq: self.seq,
            start_op: self.start_op,
            timestamp: self.timestamp,
            message: self.message,
            ops_meta: self.ops_meta,
            ops_data: self.ops_data,
            extra_bytes: self.extra_bytes,
            num_ops,
            _phantom: PhantomData,
        })
    }
}

impl<'a> Change<'a, Verified> {
    pub(crate) fn len(&self) -> usize {
        self.num_ops
    }

    pub(crate) fn builder() -> ChangeBuilder<Unset, Unset, Unset, Unset> {
        ChangeBuilder::new()
    }

    pub(crate) fn iter_ops(&'a self) -> impl Iterator<Item = ChangeOp> + Clone + 'a {
        // SAFETY: This unwrap is okay because a `Change<'_, Verified>` can only be constructed
        // using either `verify_ops` or `Builder::build`, so we know the ops columns are valid.
        self.ops_meta.iter(self.ops_data()).map(|o| o.unwrap())
    }
}

impl<O: OpReadState> Change<'_, O> {
    pub(crate) fn checksum(&self) -> CheckSum {
        self.header.checksum()
    }

    pub(crate) fn actor(&self) -> &ActorId {
        &self.actor
    }
    pub(crate) fn other_actors(&self) -> &[ActorId] {
        &self.other_actors
    }

    pub(crate) fn start_op(&self) -> NonZeroU64 {
        self.start_op
    }

    pub(crate) fn message(&self) -> &Option<String> {
        &self.message
    }

    pub(crate) fn dependencies(&self) -> &[ChangeHash] {
        &self.dependencies
    }

    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }

    pub(crate) fn timestamp(&self) -> i64 {
        self.timestamp
    }

    pub(crate) fn extra_bytes(&self) -> &[u8] {
        &self.bytes[self.extra_bytes.clone()]
    }

    pub(crate) fn checksum_valid(&self) -> bool {
        self.header.checksum_valid()
    }

    pub(crate) fn body_bytes(&self) -> &[u8] {
        &self.bytes[self.header.len()..]
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn hash(&self) -> ChangeHash {
        self.header.hash()
    }

    pub(crate) fn ops_data(&self) -> &[u8] {
        &self.bytes[self.ops_data.clone()]
    }

    pub(crate) fn into_owned(self) -> Change<'static, O> {
        Change {
            dependencies: self.dependencies,
            bytes: Cow::Owned(self.bytes.into_owned()),
            header: self.header,
            actor: self.actor,
            other_actors: self.other_actors,
            seq: self.seq,
            start_op: self.start_op,
            timestamp: self.timestamp,
            message: self.message,
            ops_meta: self.ops_meta,
            ops_data: self.ops_data,
            num_ops: self.num_ops,
            extra_bytes: self.extra_bytes,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn compress(&self) -> Option<Compressed<'static>> {
        if self.bytes.len() > DEFLATE_MIN_SIZE {
            Some(Compressed::compress(self))
        } else {
            None
        }
    }
}

fn length_prefixed_bytes<B: AsRef<[u8]>>(b: B, out: &mut Vec<u8>) -> usize {
    let prefix_len = leb128::write::unsigned(out, b.as_ref().len() as u64).unwrap();
    out.write_all(b.as_ref()).unwrap();
    prefix_len + b.as_ref().len()
}

// Bunch of type safe builder boilerplate
pub(crate) struct Unset;
pub(crate) struct Set<T> {
    value: T,
}

#[allow(non_camel_case_types)]
pub(crate) struct ChangeBuilder<START_OP, ACTOR, SEQ, TIME> {
    dependencies: Vec<ChangeHash>,
    actor: ACTOR,
    seq: SEQ,
    start_op: START_OP,
    timestamp: TIME,
    message: Option<String>,
    extra_bytes: Option<Vec<u8>>,
}

impl ChangeBuilder<Unset, Unset, Unset, Unset> {
    pub(crate) fn new() -> Self {
        Self {
            dependencies: vec![],
            actor: Unset,
            seq: Unset,
            start_op: Unset,
            timestamp: Unset,
            message: None,
            extra_bytes: None,
        }
    }
}

#[allow(non_camel_case_types)]
impl<START_OP, ACTOR, SEQ, TIME> ChangeBuilder<START_OP, ACTOR, SEQ, TIME> {
    pub(crate) fn with_dependencies(self, mut dependencies: Vec<ChangeHash>) -> Self {
        dependencies.sort_unstable();
        Self {
            dependencies,
            ..self
        }
    }

    pub(crate) fn with_message(self, message: Option<String>) -> Self {
        Self { message, ..self }
    }

    pub(crate) fn with_extra_bytes(self, extra_bytes: Vec<u8>) -> Self {
        Self {
            extra_bytes: Some(extra_bytes),
            ..self
        }
    }
}

#[allow(non_camel_case_types)]
impl<START_OP, ACTOR, TIME> ChangeBuilder<START_OP, ACTOR, Unset, TIME> {
    pub(crate) fn with_seq(self, seq: u64) -> ChangeBuilder<START_OP, ACTOR, Set<u64>, TIME> {
        ChangeBuilder {
            dependencies: self.dependencies,
            actor: self.actor,
            seq: Set { value: seq },
            start_op: self.start_op,
            timestamp: self.timestamp,
            message: self.message,
            extra_bytes: self.extra_bytes,
        }
    }
}

#[allow(non_camel_case_types)]
impl<START_OP, SEQ, TIME> ChangeBuilder<START_OP, Unset, SEQ, TIME> {
    pub(crate) fn with_actor(
        self,
        actor: ActorId,
    ) -> ChangeBuilder<START_OP, Set<ActorId>, SEQ, TIME> {
        ChangeBuilder {
            dependencies: self.dependencies,
            actor: Set { value: actor },
            seq: self.seq,
            start_op: self.start_op,
            timestamp: self.timestamp,
            message: self.message,
            extra_bytes: self.extra_bytes,
        }
    }
}

impl<ACTOR, SEQ, TIME> ChangeBuilder<Unset, ACTOR, SEQ, TIME> {
    pub(crate) fn with_start_op(
        self,
        start_op: NonZeroU64,
    ) -> ChangeBuilder<Set<NonZeroU64>, ACTOR, SEQ, TIME> {
        ChangeBuilder {
            dependencies: self.dependencies,
            actor: self.actor,
            seq: self.seq,
            start_op: Set { value: start_op },
            timestamp: self.timestamp,
            message: self.message,
            extra_bytes: self.extra_bytes,
        }
    }
}

#[allow(non_camel_case_types)]
impl<START_OP, ACTOR, SEQ> ChangeBuilder<START_OP, ACTOR, SEQ, Unset> {
    pub(crate) fn with_timestamp(self, time: i64) -> ChangeBuilder<START_OP, ACTOR, SEQ, Set<i64>> {
        ChangeBuilder {
            dependencies: self.dependencies,
            actor: self.actor,
            seq: self.seq,
            start_op: self.start_op,
            timestamp: Set { value: time },
            message: self.message,
            extra_bytes: self.extra_bytes,
        }
    }
}

/// A row to be encoded as a change op
///
/// The lifetime `'a` is the lifetime of the value and key data types. For types which cannot
/// provide a reference (e.g. because they are decoding from some columnar storage on each
/// iteration) this should be `'static`.
pub(crate) trait AsChangeOp<'a> {
    /// The type of the Actor ID component of the op IDs for this impl. This is typically either
    /// `&'a ActorID` or `usize`
    type ActorId;
    /// The type of the op IDs this impl produces.
    type OpId: convert::OpId<Self::ActorId>;
    /// The type of the predecessor iterator returned by `Self::pred`. This can often be omitted
    type PredIter: Iterator<Item = Self::OpId> + ExactSizeIterator;

    fn obj(&self) -> convert::ObjId<Self::OpId>;
    fn key(&self) -> convert::Key<'a, Self::OpId>;
    fn insert(&self) -> bool;
    fn action(&self) -> u64;
    fn val(&self) -> Cow<'a, ScalarValue>;
    fn pred(&self) -> Self::PredIter;
    fn expand(&self) -> bool;
    fn mark_name(&self) -> Option<Cow<'a, smol_str::SmolStr>>;
}

impl ChangeBuilder<Set<NonZeroU64>, Set<ActorId>, Set<u64>, Set<i64>> {
    pub(crate) fn build<'a, 'b, A, I, O>(
        self,
        ops: I,
    ) -> Result<Change<'static, Verified>, PredOutOfOrder>
    where
        A: AsChangeOp<'a, OpId = O> + 'a + std::fmt::Debug,
        O: convert::OpId<&'a ActorId> + 'a,
        I: Iterator<Item = A> + Clone + 'a + ExactSizeIterator,
    {
        let num_ops = ops.len();
        let mut col_data = Vec::new();
        let actors = change_actors::ChangeActors::new(self.actor.value, ops)?;
        let cols = ChangeOpsColumns::encode(actors.iter(), &mut col_data);

        let (actor, other_actors) = actors.done();

        let mut data = Vec::with_capacity(col_data.len());
        leb128::write::unsigned(&mut data, self.dependencies.len() as u64).unwrap();
        for dep in &self.dependencies {
            data.write_all(dep.as_bytes()).unwrap();
        }
        length_prefixed_bytes(&actor, &mut data);
        leb128::write::unsigned(&mut data, self.seq.value).unwrap();
        leb128::write::unsigned(&mut data, self.start_op.value.into()).unwrap();
        leb128::write::signed(&mut data, self.timestamp.value).unwrap();
        length_prefixed_bytes(
            self.message.as_ref().map(|m| m.as_bytes()).unwrap_or(&[]),
            &mut data,
        );
        leb128::write::unsigned(&mut data, other_actors.len() as u64).unwrap();
        for actor in other_actors.iter() {
            length_prefixed_bytes(actor, &mut data);
        }
        cols.raw_columns().write(&mut data);
        let ops_data_start = data.len();
        let ops_data = ops_data_start..(ops_data_start + col_data.len());

        data.extend(col_data);
        let extra_bytes =
            data.len()..(data.len() + self.extra_bytes.as_ref().map(|e| e.len()).unwrap_or(0));
        if let Some(extra) = self.extra_bytes {
            data.extend(extra);
        }

        let header = Header::new(ChunkType::Change, &data);

        let mut bytes = Vec::with_capacity(header.len() + data.len());
        header.write(&mut bytes);
        bytes.extend(data);

        let ops_data = shift_range(ops_data, header.len());
        let extra_bytes = shift_range(extra_bytes, header.len());

        Ok(Change {
            bytes: Cow::Owned(bytes),
            header,
            dependencies: self.dependencies,
            actor,
            other_actors,
            seq: self.seq.value,
            start_op: self.start_op.value,
            timestamp: self.timestamp.value,
            message: self.message,
            ops_meta: cols,
            ops_data,
            extra_bytes,
            num_ops,
            _phantom: PhantomData,
        })
    }
}
