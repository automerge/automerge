use crate::{
    columnar_2::{
        rowblock::{
            change_op_columns::{ChangeOp, ChangeOpsColumns},
            RowBlock,
        },
        storage::{Change as StoredChange, Chunk, ChunkType},
    },
    types::{ActorId, ChangeHash},
};

#[derive(Clone, Debug)]
pub struct Change {
    stored: StoredChange<'static>,
    hash: ChangeHash,
    len: usize,
}

impl Change {
    pub(crate) fn new(stored: StoredChange<'static>, hash: ChangeHash, len: usize) -> Self {
        Self{
            stored,
            hash,
            len,
        }
    }

    pub fn actor_id(&self) -> &ActorId {
        &self.stored.actor
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn max_op(&self) -> u64 {
        self.stored.start_op + (self.len as u64) - 1
    }

    pub fn start_op(&self) -> u64 {
        self.stored.start_op
    }

    pub fn message(&self) -> Option<&String> {
        self.stored.message.as_ref()
    }

    pub fn deps(&self) -> &[ChangeHash] {
        &self.stored.dependencies
    }

    pub fn hash(&self) -> ChangeHash {
        self.hash
    }

    pub fn seq(&self) -> u64 {
        self.stored.seq
    }

    pub fn timestamp(&self) -> i64 {
        self.stored.timestamp
    }

    pub fn compress(&mut self) {}

    pub fn raw_bytes(&self) -> Vec<u8> {
        let vec = self.stored.write();
        let chunk = Chunk::new_change(&vec);
        chunk.write()
    }

    pub(crate) fn iter_ops<'a>(&'a self) -> impl Iterator<Item= ChangeOp<'a>> {
        let rb = RowBlock::new(self.stored.ops_meta.iter(), self.stored.ops_data.clone()).unwrap();
        let crb: RowBlock<ChangeOpsColumns> = rb.into_change_ops().unwrap();
        let unwrapped = crb.into_iter().map(|r| r.unwrap().into_owned()).collect::<Vec<_>>();
        return OperationIterator{
            inner: unwrapped.into_iter(),
        }
    }

    pub fn extra_bytes(&self) -> &[u8] {
        self.stored.extra_bytes.as_ref()
    }

    // TODO replace all uses of this with TryFrom<&[u8]>
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, LoadError> {
        Self::try_from(&bytes[..])
    }
}

struct OperationIterator<'a> {
    inner: std::vec::IntoIter<ChangeOp<'a>>,
}

impl<'a> Iterator for OperationIterator<'a> {
    type Item = ChangeOp<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl AsRef<StoredChange<'static>> for Change {
    fn as_ref(&self) -> &StoredChange<'static> {
        &self.stored
    }
}

#[derive(thiserror::Error, Debug)]
pub enum LoadError {
    #[error("unable to parse change: {0}")]
    Parse(Box<dyn std::error::Error>),
    #[error("leftover data after parsing")]
    LeftoverData,
    #[error("wrong chunk type")]
    WrongChunkType,
}

impl<'a> TryFrom<&'a [u8]> for Change {
    type Error = LoadError;

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        use crate::columnar_2::rowblock::change_op_columns::ReadChangeOpError;
        let (remaining, chunk) = Chunk::parse(value).map_err(|e| LoadError::Parse(Box::new(e)))?;
        if remaining.len() > 0 {
            return Err(LoadError::LeftoverData);
        }
        match chunk.typ() {
            ChunkType::Change => {
                let chunkbytes = chunk.data();
                let (_, c) = StoredChange::parse(chunkbytes.as_ref())
                    .map_err(|e| LoadError::Parse(Box::new(e)))?;
                let rb = RowBlock::new(c.ops_meta.iter(), c.ops_data.clone()).unwrap();
                let crb: RowBlock<ChangeOpsColumns> = rb.into_change_ops().unwrap();
                let mut iter = crb.into_iter();
                let ops_len = iter
                    .try_fold::<_, _, Result<_, ReadChangeOpError>>(0, |acc, op| {
                        op?;
                        Ok(acc + 1)
                    })
                    .map_err(|e| LoadError::Parse(Box::new(e)))?;
                Ok(Self {
                    stored: c.into_owned(),
                    hash: chunk.hash(),
                    len: ops_len,
                })
            }
            _ => Err(LoadError::WrongChunkType),
        }
    }
}

impl<'a> TryFrom<StoredChange<'a>> for Change {
    type Error = LoadError;

    fn try_from(c: StoredChange) -> Result<Self, Self::Error> {
        use crate::columnar_2::rowblock::change_op_columns::ReadChangeOpError;
        let rb = RowBlock::new(c.ops_meta.iter(), c.ops_data.clone()).unwrap();
        let crb: RowBlock<ChangeOpsColumns> = rb.into_change_ops().unwrap();
        let mut iter = crb.into_iter();
        let ops_len = iter
            .try_fold::<_, _, Result<_, ReadChangeOpError>>(0, |acc, op| {
                op?;
                Ok(acc + 1)
            })
            .map_err(|e| LoadError::Parse(Box::new(e)))?;
        let chunkbytes = c.write();
        let chunk = Chunk::new_change(chunkbytes.as_ref());
        Ok(Self {
            stored: c.into_owned(),
            hash: chunk.hash(),
            len: ops_len,
        })
    }
}
