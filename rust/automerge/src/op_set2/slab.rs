use super::{ColumnCursor, PackError, Packable, Run};

use std::borrow::Borrow;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) enum Slab {
    External(ReadOnlySlab),
    Owned(WritableSlab),
}

#[derive(Debug, Clone)]
pub(crate) struct ReadOnlySlab {
    data: Arc<Vec<u8>>,
    range: Range<usize>,
    len: usize,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct WritableSlab {
    data: Vec<u8>,
    len: usize,
}

impl Default for Slab {
    fn default() -> Self {
        Self::Owned(WritableSlab::default())
    }
}

#[derive(Debug)]
pub(crate) struct SlabIter<'a, C: ColumnCursor> {
    slab: &'a Slab,
    cursor: C,
    state: Option<Run<'a, C::Item>>,
}

impl<'a, C: ColumnCursor> Iterator for SlabIter<'a, C> {
    type Item = Option<<C::Item as Packable>::Unpacked<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut state = None;
        std::mem::swap(&mut state, &mut self.state);
        match state {
            Some(run) => {
                let (value, next_state) = self.cursor.pop(run);
                self.state = next_state;
                Some(value)
            }
            None => {
                if let Some((run, cursor)) = self.cursor.next(self.slab.as_ref()) {
                    self.cursor = cursor;
                    let (value, next_state) = self.cursor.pop(run);
                    self.state = next_state;
                    Some(value)
                } else {
                    self.state = None;
                    None
                }
            }
        }
    }
}

impl WritableSlab {
    pub(crate) fn new(bytes: &[u8], len: usize) -> Self {
        WritableSlab {
            data: bytes.to_vec(),
            len,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn add_len(&mut self, len: usize) {
        self.len += len
    }

    pub(crate) fn append_i64(&mut self, element: i64) {
        let _ = i64::pack(&mut self.data, &element);
    }

    pub(crate) fn append_usize(&mut self, element: usize) {
        let _ = usize::pack(&mut self.data, &element);
    }

    pub(crate) fn append<D: Packable + ?Sized, M: Borrow<D>>(&mut self, element: M) {
        let _ = D::pack(&mut self.data, element.borrow());
    }

    pub(crate) fn append_bytes(&mut self, bytes: &[u8]) {
        self.data.extend(bytes);
    }
}

impl Slab {
    pub(crate) fn iter<'a, C: ColumnCursor>(&'a self) -> SlabIter<'a, C> {
        SlabIter {
            slab: self,
            cursor: C::default(),
            state: None,
        }
    }

    pub(crate) fn as_ref(&self) -> &[u8] {
        match self {
            Self::External(ReadOnlySlab { data, range, .. }) => &data[range.clone()],
            Self::Owned(WritableSlab { data, .. }) => &data,
        }
    }

    pub(crate) fn get_mut(&mut self) -> Option<&mut Vec<u8>> {
        match self {
            Self::External(_) => None,
            Self::Owned(WritableSlab { data, .. }) => Some(data),
        }
    }

    pub(crate) fn external<C: ColumnCursor>(
        data: Arc<Vec<u8>>,
        range: Range<usize>,
    ) -> Result<Self, PackError> {
        let index = C::scan(&data.as_ref()[range.clone()])?;
        Ok(Slab::External(ReadOnlySlab {
            data,
            range,
            len: index.index(),
        }))
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::External(ReadOnlySlab { len, .. }) => *len,
            Self::Owned(WritableSlab { len, .. }) => *len,
        }
    }
}
