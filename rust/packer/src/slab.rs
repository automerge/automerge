use super::aggregate::{Acc, Agg};
use super::cursor::{ColumnCursor, HasAcc, HasPos, RunIter, ScanMeta};
use super::pack::{PackError, Packable};
use super::Cow;

pub mod tree;
pub(crate) mod writer;

pub(crate) use super::columndata::normalize_range;
pub use tree::{SpanTree, SpanTreeIter, SpanWeight};
pub use writer::{SlabWriter, WriteOp};

pub type SlabTree<W> = SpanTree<Slab, W>;

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
    acc: Acc,
    min: Agg,
    max: Agg,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct OwnedSlab {
    data: Arc<Vec<u8>>,
    len: usize,
    acc: Acc,
    min: Agg,
    max: Agg,
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
    pub(crate) fn new(data: Vec<u8>, len: usize, acc: Acc, abs: i64) -> Self {
        let data = Arc::new(data);
        Slab::Owned(OwnedSlab {
            data,
            len,
            acc,
            abs,
            min: Agg::default(),
            max: Agg::default(),
        })
    }

    pub fn set_min_max(&mut self, new_min: Agg, new_max: Agg) {
        match self {
            Self::External(ReadOnlySlab { min, max, .. }) => {
                *min = new_min;
                *max = new_max;
            }
            Self::Owned(OwnedSlab { min, max, .. }) => {
                *min = new_min;
                *max = new_max;
            }
        }
    }

    pub fn abs(&self) -> i64 {
        match self {
            Self::External(ReadOnlySlab { .. }) => 0,
            Self::Owned(OwnedSlab { abs, .. }) => *abs,
        }
    }

    pub fn max(&self) -> Agg {
        match self {
            Self::External(ReadOnlySlab { max, .. }) => *max,
            Self::Owned(OwnedSlab { max, .. }) => *max,
        }
    }

    pub fn min(&self) -> Agg {
        match self {
            Self::External(ReadOnlySlab { min, .. }) => *min,
            Self::Owned(OwnedSlab { min, .. }) => *min,
        }
    }

    pub fn first_value<C: ColumnCursor>(&self) -> Option<Cow<'_, C::Item>> {
        let mut cursor = C::new(self);
        let run = cursor.next(self.as_slice())?;
        run.value
    }

    pub fn run_iter<C: ColumnCursor>(&self) -> RunIter<'_, C> {
        let weight = <C::SlabIndex>::alloc(self);
        RunIter {
            slab: self.as_slice(),
            cursor: C::new(self),
            pos_left: weight.pos(),
            acc_left: weight.acc(),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(OwnedSlab { data, .. }) => data,
            Self::External(ReadOnlySlab { data, range, .. }) => &data[range.clone()],
        }
    }

    pub fn external<C: ColumnCursor>(
        data: Arc<Vec<u8>>,
        range: Range<usize>,
        m: &ScanMeta,
    ) -> Result<Self, PackError> {
        let mut cursor = C::empty();
        let bytes = &data.as_ref()[range.clone()];
        while let Some(val) = cursor.try_next(bytes)? {
            C::Item::validate(val.value.as_deref(), m)?;
        }
        Ok(Slab::External(ReadOnlySlab {
            data,
            range,
            len: cursor.index(),
            acc: cursor.acc(),
            min: cursor.min(),
            max: cursor.max(),
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

    pub fn acc(&self) -> Acc {
        match self {
            Self::External(ReadOnlySlab { acc, .. }) => *acc,
            Self::Owned(OwnedSlab { acc, .. }) => *acc,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub struct SlabWeight {
    pub(crate) pos: usize,
    pub(crate) acc: Acc,
    pub(crate) min: Agg,
    pub(crate) max: Agg,
}

impl SpanWeight<Slab> for SlabWeight {
    fn alloc(span: &Slab) -> Self {
        SlabWeight {
            pos: span.len(),
            acc: span.acc(),
            min: span.min(),
            max: span.max(),
        }
    }

    fn and(self, b: &Self) -> Self {
        Self {
            pos: self.pos + b.pos,
            acc: self.acc + b.acc,
            max: self.max.maximize(b.max),
            min: self.min.minimize(b.min),
        }
    }

    fn union(&mut self, other: &Self) {
        self.pos += other.pos;
        self.acc += other.acc;
        self.max = self.max.maximize(other.max);
        self.min = self.min.minimize(other.min);
    }

    fn maybe_sub(&mut self, other: &Self) -> bool {
        let max_ok = other.max.is_none() || self.max > other.max;
        let min_ok = other.min.is_none() || self.min.is_some() && self.min < other.min;
        if max_ok && min_ok {
            self.pos -= other.pos;
            self.acc -= other.acc;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn test_maybe_sub() {
        let baseline = SlabWeight {
            pos: 100,
            acc: Acc::from(100),
            min: Agg::from(2),
            max: Agg::from(20),
        };

        let max_eq = SlabWeight {
            pos: 50,
            acc: Acc::from(50),
            min: Agg::from(3),
            max: Agg::from(20),
        };
        let max_gr = SlabWeight {
            pos: 50,
            acc: Acc::from(50),
            min: Agg::from(3),
            max: Agg::from(21),
        };
        let max_lt = SlabWeight {
            pos: 50,
            acc: Acc::from(50),
            min: Agg::from(3),
            max: Agg::from(19),
        };
        let max_none = SlabWeight {
            pos: 50,
            acc: Acc::from(50),
            min: Agg::from(3),
            max: Agg::from(0),
        };
        assert!(!baseline.clone().maybe_sub(&max_eq));
        assert!(!baseline.clone().maybe_sub(&max_gr));
        assert!(baseline.clone().maybe_sub(&max_lt));
        assert!(baseline.clone().maybe_sub(&max_none));

        let min_eq = SlabWeight {
            pos: 50,
            acc: Acc::from(50),
            min: Agg::from(2),
            max: Agg::from(19),
        };
        let min_gr = SlabWeight {
            pos: 50,
            acc: Acc::from(50),
            min: Agg::from(3),
            max: Agg::from(19),
        };
        let min_lt = SlabWeight {
            pos: 50,
            acc: Acc::from(50),
            min: Agg::from(1),
            max: Agg::from(19),
        };
        let min_none = SlabWeight {
            pos: 50,
            acc: Acc::from(50),
            min: Agg::from(0),
            max: Agg::from(19),
        };
        assert!(!baseline.clone().maybe_sub(&min_eq));
        assert!(baseline.clone().maybe_sub(&min_gr));
        assert!(!baseline.clone().maybe_sub(&min_lt));
        assert!(baseline.clone().maybe_sub(&min_none));
    }
}
