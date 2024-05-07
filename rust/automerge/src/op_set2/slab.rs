use super::{ColumnCursor, PackError, Packable, Run};

use std::borrow::Borrow;
use std::fmt::Debug;
use std::ops::{Index, Range};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) enum Slab {
    External(ReadOnlySlab),
    Owned(OwnedSlab),
}

#[derive(Debug, Clone)]
pub(crate) struct ReadOnlySlab {
    data: Arc<Vec<u8>>,
    range: Range<usize>,
    len: usize,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct OwnedSlab {
    data: Vec<u8>,
    len: usize,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct WritableSlab {
    data: Vec<u8>,
    done: Vec<Slab>,
    len: usize,
}

impl Index<Range<usize>> for Slab {
    type Output = [u8];

    fn index(&self, index: Range<usize>) -> &Self::Output {
        match self {
            Self::External(ReadOnlySlab { data, range, .. }) => {
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

#[derive(Copy, Clone, Debug)]
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
            done: vec![],
            len,
        }
    }

    pub(crate) fn bytes_left(&self, max: usize) -> usize {
        max.saturating_sub(self.data.len())
    }

    pub(crate) fn next_slab(&mut self) {
        let mut data = Vec::new();
        let len = self.len;
        std::mem::swap(&mut data, &mut self.data);
        self.len = 0;
        let slab = OwnedSlab { data, len };
        self.done.push(Slab::Owned(slab));
    }

    pub(crate) fn finish(mut self) -> Vec<Slab> {
        let slab = OwnedSlab {
            data: self.data,
            len: self.len,
        };
        if slab.data.len() > 0 {
            self.done.push(Slab::Owned(slab));
        }
        self.done
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
            Self::Owned(OwnedSlab { data, .. }) => &data,
        }
    }

    pub(crate) fn get_mut(&mut self) -> Option<&mut Vec<u8>> {
        match self {
            Self::External(_) => None,
            Self::Owned(OwnedSlab { data, .. }) => Some(data),
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
            Self::Owned(OwnedSlab { len, .. }) => *len,
        }
    }
}
