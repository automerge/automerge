use super::cursor::{ColumnCursor, Encoder, Run, ScanMeta};
use super::pack::PackError;
use super::slab::{self, Slab, SlabWriter};

use std::fmt::Debug;
use std::ops::Range;

#[derive(Debug, Default, Clone, Copy)]
pub struct RawCursorInternal<const B: usize> {
    offset: usize,
}

pub type RawCursor = RawCursorInternal<4096>;

impl<const B: usize> ColumnCursor for RawCursorInternal<B> {
    type Item = [u8];
    type State<'a> = ();
    type PostState<'a> = &'a [u8];
    type Export = u8;

    fn empty() -> Self {
        Self::default()
    }

    fn write<'a>(
        writer: &mut SlabWriter<'a>,
        slab: &'a Slab,
        _state: Self::State<'a>,
    ) -> Self::State<'a> {
        let len = slab.len();
        writer.flush_before(slab, 0..len, 0, len, 0);
    }

    fn finish<'a>(_slab: &'a Slab, _out: &mut SlabWriter<'a>, _cursor: Self) {}

    fn finalize_state<'a>(
        _slab: &'a Slab,
        out: &mut SlabWriter<'a>,
        _state: (),
        post: Self::PostState<'a>,
        _cursor: Self,
    ) -> Option<Self> {
        out.flush_bytes(post, post.len());
        None
    }

    fn flush_state<'a>(_out: &mut SlabWriter<'a>, _state: Self::State<'a>) {}

    fn copy_between<'a>(
        _slab: &'a Slab,
        _out: &mut SlabWriter<'a>,
        _c0: Self,
        _c1: Self,
        _run: Run<'a, [u8]>,
        _size: usize,
    ) -> Self::State<'a> {
        // only called from write and we override that
    }

    fn append_chunk<'a>(
        _state: &mut Self::State<'a>,
        slab: &mut SlabWriter<'a>,
        run: Run<'a, [u8]>,
    ) -> usize {
        let mut len = 0;
        for _ in 0..run.count {
            if let Some(i) = run.value {
                len += i.len();
                slab.flush_bytes(i, i.len());
            }
        }
        len
    }

    fn encode(index: usize, del: usize, slab: &Slab, cap: usize) -> Encoder<'_, Self> {
        let state = ();
        let cursor = Self { offset: index };

        // everything before...
        let mut current = SlabWriter::new(B, cap + 4);
        current.flush_bytes(&slab.as_slice()[0..index], index);

        let post;
        let deleted;
        if index + del < slab.as_slice().len() {
            // everything after
            post = &slab.as_slice()[(index + del)..];
            deleted = del;
        } else {
            // nothing left
            post = &[];
            deleted = slab.as_slice().len() - index;
        }
        let overflow = del - deleted;
        let group = 0;

        Encoder {
            slab,
            current,
            post,
            group,
            state,
            deleted,
            overflow,
            cursor,
        }
    }

    fn export_splice<'a, I>(data: &mut Vec<Self::Export>, range: Range<usize>, values: I)
    where
        I: Iterator<Item = Option<&'a [u8]>>,
    {
        let mut total: Vec<u8> = vec![];
        for bytes in values.flatten() {
            total.extend(bytes);
        }
        data.splice(range, total);
    }

    fn scan(data: &[u8], _m: &ScanMeta) -> Result<Self, PackError> {
        Ok(Self { offset: data.len() })
    }

    // dont think this has any real use
    // this column will always use get_slice
    fn try_next<'a>(
        &self,
        slab: &'a [u8],
    ) -> Result<Option<(Run<'a, Self::Item>, Self)>, PackError> {
        let next_offset = self.offset + 1;
        if next_offset > slab.len() {
            //return Err(PackError::IndexOutOfRange(self.offset));
            return Ok(None);
        }
        let data = &slab[self.offset..next_offset];
        Ok(Some((
            Run {
                count: 1,
                value: Some(data),
            },
            Self {
                offset: next_offset,
            },
        )))
    }

    fn index(&self) -> usize {
        self.offset
    }
}

#[derive(Debug, Clone, Default)]
pub struct RawReader<'a> {
    pub(crate) slabs: slab::tree::SpanTreeIter<'a, Slab>,
    pub(crate) current: Option<(&'a Slab, usize)>,
}

impl<'a> RawReader<'a> {
    pub fn empty() -> RawReader<'static> {
        RawReader {
            slabs: slab::Iter::default(),
            current: None,
        }
    }

    /// Read a slice out of a set of slabs
    ///
    /// Returns an error if:
    /// * The read would cross a slab boundary
    /// * The read would go past the end of the data
    pub fn read_next(&mut self, length: usize) -> Result<&'a [u8], ReadRawError> {
        let (slab, offset) = match self.current.take() {
            Some(state) => state,
            None => {
                if let Some(slab) = self.slabs.next() {
                    (slab, 0)
                } else {
                    return Err(ReadRawError::EndOfData);
                }
            }
        };
        if offset + length > slab.len() {
            return Err(ReadRawError::CrossBoundary);
        }
        let result = slab[offset..offset + length].as_ref();
        let new_offset = offset + length;
        if new_offset == slab.len() {
            self.current = None;
        } else {
            self.current = Some((slab, new_offset));
        }
        Ok(result)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReadRawError {
    #[error("attempted to read across slab boundaries")]
    CrossBoundary,
    #[error("attempted to read past end of data")]
    EndOfData,
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::columndata::ColumnData;
    use super::super::test::ColExport;
    use super::*;

    #[test]
    fn column_data_raw_splice() {
        let mut col1: ColumnData<RawCursorInternal<6>> = ColumnData::new();
        col1.splice(0, 0, vec![vec![1, 1, 1]]);
        assert_eq!(col1.test_dump(), vec![vec![ColExport::Raw(vec![1, 1, 1])]]);
        col1.splice(0, 0, vec![vec![2, 2, 2]]);
        assert_eq!(
            col1.test_dump(),
            vec![vec![ColExport::Raw(vec![2, 2, 2, 1, 1, 1])]]
        );
        col1.splice(3, 0, vec![vec![3, 3, 3]]);
        assert_eq!(
            col1.test_dump(),
            vec![
                vec![ColExport::Raw(vec![2, 2, 2, 3, 3, 3])],
                vec![ColExport::Raw(vec![1, 1, 1])],
            ]
        );
        col1.splice(3, 0, vec![vec![4, 4, 4]]);
        assert_eq!(
            col1.test_dump(),
            vec![
                vec![ColExport::Raw(vec![2, 2, 2, 4, 4, 4])],
                vec![ColExport::Raw(vec![3, 3, 3])],
                vec![ColExport::Raw(vec![1, 1, 1])],
            ]
        );
        col1.splice::<Vec<u8>>(3, 1, vec![]);
        assert_eq!(
            col1.test_dump(),
            vec![
                vec![ColExport::Raw(vec![2, 2, 2, 4, 4])],
                vec![ColExport::Raw(vec![3, 3, 3])],
                vec![ColExport::Raw(vec![1, 1, 1])],
            ]
        );
        col1.splice(3, 2, vec![vec![5, 5, 5, 5, 5, 5], vec![6, 6, 6]]);
        assert_eq!(
            col1.test_dump(),
            vec![
                vec![ColExport::Raw(vec![2, 2, 2, 5, 5, 5, 5, 5, 5])],
                vec![ColExport::Raw(vec![6, 6, 6])],
                vec![ColExport::Raw(vec![3, 3, 3])],
                vec![ColExport::Raw(vec![1, 1, 1])],
            ]
        );
    }

    #[test]
    fn raw_reader() {
        let mut col: ColumnData<RawCursorInternal<6>> = ColumnData::new();
        // stuff it with sets of 3 bytes
        for n in 0..=255 {
            col.splice(0, 0, vec![vec![n, n, n]]);
        }
        // single reader - read all;
        let mut reader = col.raw_reader(0);
        for m in (0..=255).rev() {
            let val = reader.read_next(3).unwrap();
            assert_eq!(&[m, m, m], val);
        }
        // many readers w offset;
        for m in (0..=255).rev() {
            let offset = (255 - m as usize) * 3;
            let val = col.raw_reader(offset).read_next(3).unwrap();
            assert_eq!(&[m, m, m], val);
        }
    }
}
