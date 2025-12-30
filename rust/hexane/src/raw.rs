use super::aggregate::Acc;
use super::columndata::ColumnData;
use super::cursor::{ColumnCursor, HasPos, Run, ScanMeta};
use super::encoder::{Encoder, SpliceEncoder};
use super::pack::PackError;
use super::slab::{self, Slab, SlabTree, SlabWeight, SlabWriter, SpanWeight};
use super::Cow;

use std::fmt::Debug;
use std::ops::Range;

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct RawCursorInternal<const B: usize> {
    offset: usize,
}

pub type RawCursor = RawCursorInternal<4096>;

impl<const B: usize> ColumnCursor for RawCursorInternal<B> {
    type Item = [u8];
    type State<'a> = ();
    type PostState<'a> = Range<usize>; //&'a [u8];
    type Export = u8;
    type SlabIndex = SlabWeight;

    fn empty() -> Self {
        Self::default()
    }

    fn finish<'a>(_slab: &'a Slab, _writer: &mut SlabWriter<'a, [u8]>, _cursor: Self) {}

    fn finalize_state<'a>(
        slab: &'a Slab,
        encoder: &mut Encoder<'a, Self>,
        post: Self::PostState<'a>,
        _cursor: Self,
    ) -> Option<Self> {
        let len = post.end - post.start;
        encoder
            .writer
            .copy(slab.as_slice(), post, 0, len, Acc::new(), None);
        None
    }

    fn copy_between<'a>(
        _slab: &'a [u8],
        _writer: &mut SlabWriter<'a, [u8]>,
        _c0: Self,
        _c1: Self,
        _run: Run<'a, [u8]>,
        _size: usize,
    ) -> Self::State<'a> {
        // only called from write and we override that
    }

    fn slab_size() -> usize {
        B
    }

    fn splice_encoder(index: usize, del: usize, slab: &Slab) -> SpliceEncoder<'_, Self> {
        let state = ();
        let cursor = Self { offset: index };
        let bytes = slab.as_slice();

        // everything before...
        let mut current = SlabWriter::new(B, false);
        current.copy(bytes, 0..index, 0, index, Acc::new(), None);

        let post;
        let deleted;
        if index + del < bytes.len() {
            // everything after
            post = (index + del)..(bytes.len());
            deleted = del;
        } else {
            // nothing left
            post = 0..0;
            deleted = bytes.len() - index;
        }
        let overflow = del - deleted;
        let acc = Acc::new();

        SpliceEncoder {
            encoder: Encoder::init(current, state),
            slab,
            post,
            acc,
            deleted,
            overflow,
            cursor,
        }
    }

    fn export_splice<'a, I>(data: &mut Vec<Self::Export>, range: Range<usize>, values: I)
    where
        I: Iterator<Item = Option<Cow<'a, [u8]>>>,
    {
        let mut total: Vec<u8> = vec![];
        for bytes in values.flatten() {
            total.extend_from_slice(&bytes);
        }
        data.splice(range, total);
    }

    fn try_next<'a>(&mut self, slab: &'a [u8]) -> Result<Option<Run<'a, Self::Item>>, PackError> {
        let next_offset = self.offset + 1;
        if next_offset > slab.len() {
            return Ok(None);
        }
        let data = &slab[self.offset..next_offset];
        self.offset = next_offset;
        Ok(Some(Run {
            count: 1,
            value: Some(Cow::Borrowed(data)),
        }))
    }

    fn try_again<'a>(&self, slab: &'a [u8]) -> Result<Option<Run<'a, Self::Item>>, PackError> {
        if self.offset == 0 {
            Ok(None)
        } else {
            let data = &slab[(self.offset - 1)..self.offset];
            Ok(Some(Run {
                count: 1,
                value: Some(Cow::Borrowed(data)),
            }))
        }
    }

    fn index(&self) -> usize {
        self.offset
    }

    fn offset(&self) -> usize {
        self.offset
    }

    fn load_with(data: &[u8], _m: &ScanMeta) -> Result<ColumnData<Self>, PackError> {
        let len = data.len();
        let slab = Slab::new(data.to_vec(), len, Acc::default(), 0);
        Ok(ColumnData::init(len, SlabTree::load([slab])))
    }
}

#[derive(Debug, Clone, Default)]
pub struct RawReader<'a, T: SpanWeight<Slab> + HasPos> {
    pub(crate) pos: usize,
    pub(crate) slabs: slab::tree::SpanTreeIter<'a, Slab, T>,
    pub(crate) current: Option<(&'a Slab, usize)>,
}

impl<'a, T: SpanWeight<Slab> + HasPos> RawReader<'a, T> {
    pub fn empty() -> RawReader<'static, T> {
        RawReader {
            pos: 0,
            slabs: slab::SpanTreeIter::default(),
            current: None,
        }
    }

    /// Read a slice out of a set of slabs
    ///
    /// Returns an error if:
    /// * The read would cross a slab boundary
    /// * The read would go past the end of the data
    pub fn read_next(&mut self, length: usize) -> Result<&'a [u8], ReadRawError> {
        if length == 0 {
            return Ok(&[]);
        }
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
        self.pos += length;
        Ok(result)
    }

    pub fn seek_to(&mut self, advance: usize) {
        if let Some(slabs) = self.slabs.span_tree() {
            let cursor = slabs.get_where_or_last(|acc, next| advance < acc.pos() + next.pos());
            let current = Some((cursor.element, advance - cursor.weight.pos()));
            let slabs = slab::SpanTreeIter::new(slabs, cursor);
            let pos = advance;
            *self = RawReader {
                pos,
                slabs,
                current,
            }
        }
    }

    pub fn suspend(&self) -> usize {
        self.pos
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
        col1.splice::<Vec<u8>, _>(3, 1, vec![]);
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
