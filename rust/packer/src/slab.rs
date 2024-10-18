use super::cursor::{ColumnCursor, RunIter, ScanMeta};
use super::pack::PackError;
use std::ops::{Add, AddAssign, Sub, SubAssign};

pub(crate) mod tree;
pub(crate) mod writer;

pub(crate) use super::columndata::normalize_range;
pub(crate) use tree::{HasWeight, SpanTree, SpanTreeIter};
pub use writer::{SlabWriter, WriteOp};

pub type SlabTree = SpanTree<Slab>;
pub(crate) type Iter<'a> = SpanTreeIter<'a, Slab>;

use std::fmt::Debug;
use std::ops::{Index, Range};
use std::sync::Arc;

#[derive(Debug, PartialEq, Clone)]
pub enum Slab {
    External(ReadOnlySlab),
    Owned(OwnedSlab),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReadOnlySlab {
    data: Arc<Vec<u8>>,
    range: Range<usize>,
    len: usize,
    group: usize,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct OwnedSlab {
    data: Arc<Vec<u8>>,
    len: usize,
    group: usize,
    abs: i64,
}

impl Index<Range<usize>> for Slab {
    type Output = [u8];

    fn index(&self, index: Range<usize>) -> &Self::Output {
        match self {
            Self::External(ReadOnlySlab { data, range, .. }) => {
                // FIXME possible to go past range.end
                &data[range.start + index.start..range.start + index.end]
            }
            Self::Owned(OwnedSlab { data, .. }) => &data[index],
        }
    }
}

impl Default for Slab {
    fn default() -> Self {
        Self::Owned(OwnedSlab::default())
    }
}

impl Slab {
    pub(crate) fn new(data: Vec<u8>, len: usize, group: usize, abs: i64) -> Self {
        let data = Arc::new(data);
        Slab::Owned(OwnedSlab {
            data,
            len,
            group,
            abs,
        })
    }

    pub fn abs(&self) -> i64 {
        match self {
            Self::External(ReadOnlySlab { .. }) => 0,
            Self::Owned(OwnedSlab { abs, .. }) => *abs,
        }
    }

    pub fn run_iter<C: ColumnCursor>(&self) -> RunIter<'_, C> {
        RunIter {
            slab: self.as_slice(),
            cursor: C::new(self),
            weight_left: self.weight(),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::External(ReadOnlySlab { data, range, .. }) => &data[range.clone()],
            Self::Owned(OwnedSlab { data, .. }) => data,
        }
    }

    pub fn external<C: ColumnCursor>(
        data: Arc<Vec<u8>>,
        range: Range<usize>,
        m: &ScanMeta,
    ) -> Result<Self, PackError> {
        let index = C::scan(&data.as_ref()[range.clone()], m)?;
        Ok(Slab::External(ReadOnlySlab {
            data,
            range,
            len: index.index(),
            group: index.group(),
        }))
    }

    pub fn byte_len(&self) -> usize {
        match self {
            Self::External(ReadOnlySlab { data, range, .. }) => data[range.clone()].len(),
            Self::Owned(OwnedSlab { data, .. }) => data.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        match self {
            Self::External(ReadOnlySlab { len, .. }) => *len,
            Self::Owned(OwnedSlab { len, .. }) => *len,
        }
    }

    pub fn group(&self) -> usize {
        match self {
            Self::External(ReadOnlySlab { group, .. }) => *group,
            Self::Owned(OwnedSlab { group, .. }) => *group,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub struct SlabWeight {
    pub(crate) pos: usize,
    pub(crate) group: usize,
}

impl HasWeight for Slab {
    type Weight = SlabWeight;

    fn weight(&self) -> SlabWeight {
        SlabWeight {
            pos: self.len(),
            group: self.group(),
        }
    }
}

impl Add for SlabWeight {
    type Output = SlabWeight;

    fn add(self, b: Self) -> Self {
        Self {
            pos: self.pos + b.pos,
            group: self.group + b.group,
        }
    }
}

impl AddAssign<SlabWeight> for SlabWeight {
    fn add_assign(&mut self, other: Self) {
        self.pos += other.pos;
        self.group += other.group;
    }
}
impl SubAssign<SlabWeight> for SlabWeight {
    fn sub_assign(&mut self, other: Self) {
        self.pos -= other.pos;
        self.group = self.group.saturating_sub(other.group);
        //self.group = 0; // FIXME
    }
}

impl Sub for SlabWeight {
    type Output = SlabWeight;
    fn sub(self, b: Self) -> Self {
        Self {
            pos: self.pos - b.pos,
            group: self.group - b.group,
        }
    }
}
