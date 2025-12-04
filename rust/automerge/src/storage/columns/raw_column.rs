use std::collections::BTreeMap;
use std::{io::Read, marker::PhantomData, ops::Range};

use crate::storage::parse;

use super::{compression, ColumnSpec};

/// This is a "raw" column in the sense that it is just the column specification[1] and range. This
/// is in contrast to [`super::Column`] which is aware of composite columns such as value columns[2] and
/// group columns[3].
///
/// `RawColumn` is generally an intermediary object which is parsed into a [`super::Column`].
///
/// The type parameter `T` is a witness to whether this column is compressed. If `T:
/// compression::Uncompressed` then we have proved that this column is not compressed, otherwise it
/// may be compressed.
///
/// [1]: https://alexjg.github.io/automerge-storage-docs/#column-specifications
/// [2]: https://alexjg.github.io/automerge-storage-docs/#raw-value-columns
/// [3]: https://alexjg.github.io/automerge-storage-docs/#group-columns
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RawColumn<T: compression::ColumnCompression> {
    spec: ColumnSpec,
    /// The location of the data in the column data block. Note that this range starts at the
    /// beginning of the column data block - i.e. the `data` attribute of the first column in the
    /// column data block will be 0 - not at the start of the chunk.
    data: Range<usize>,
    _phantom: PhantomData<T>,
}

impl RawColumn<compression::Uncompressed> {
    pub(crate) fn new(spec: ColumnSpec, data: Range<usize>) -> Self {
        Self {
            spec: ColumnSpec::new(spec.id(), spec.col_type(), false),
            data,
            _phantom: PhantomData,
        }
    }
}

impl<T: compression::ColumnCompression> RawColumn<T> {
    pub(crate) fn spec(&self) -> ColumnSpec {
        self.spec
    }

    pub(crate) fn data(&self) -> Range<usize> {
        self.data.clone()
    }

    fn compress(&self, input: &[u8], out: &mut Vec<u8>, threshold: usize) -> (ColumnSpec, usize) {
        let (spec, len) = if self.data.len() < threshold || self.spec.deflate() {
            out.extend(&input[self.data.clone()]);
            (self.spec, self.data.len())
        } else {
            let mut deflater = flate2::bufread::DeflateEncoder::new(
                &input[self.data.clone()],
                flate2::Compression::default(),
            );
            //This unwrap should be okay as we're reading and writing to in memory buffers
            (self.spec.deflated(), deflater.read_to_end(out).unwrap())
        };
        (spec, len)
    }

    pub(crate) fn uncompressed(&self) -> Option<RawColumn<compression::Uncompressed>> {
        if self.spec.deflate() {
            None
        } else {
            Some(RawColumn {
                spec: self.spec,
                data: self.data.clone(),
                _phantom: PhantomData,
            })
        }
    }

    fn decompress(
        &self,
        input: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(ColumnSpec, usize), ParseError> {
        let len = if self.spec.deflate() {
            let mut inflater = flate2::bufread::DeflateDecoder::new(&input[self.data.clone()]);
            inflater.read_to_end(out).map_err(ParseError::Deflate)?
        } else {
            out.extend(&input[self.data.clone()]);
            self.data.len()
        };
        Ok((self.spec.inflated(), len))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RawColumns<T: compression::ColumnCompression>(pub(crate) Vec<RawColumn<T>>);

impl<T: compression::ColumnCompression> RawColumns<T> {
    /// Returns `Some` if no column in this set of columns is marked as compressed
    pub(crate) fn uncompressed(&self) -> Option<RawColumns<compression::Uncompressed>> {
        let mut result = Vec::with_capacity(self.0.len());
        for col in &self.0 {
            if let Some(uncomp) = col.uncompressed() {
                result.push(uncomp);
            } else {
                return None;
            }
        }
        Some(RawColumns(result))
    }

    /// Write each column in `input` represented by `self` into `out`, possibly compressing.
    ///
    /// # Returns
    /// The `RawColumns` corresponding to the data written to `out`
    ///
    /// # Panics
    /// * If any of the ranges in `self` is outside the bounds of `input`
    pub(crate) fn compress(
        &self,
        input: &[u8],
        out: &mut Vec<u8>,
        threshold: usize,
    ) -> RawColumns<compression::Unknown> {
        let mut result = Vec::with_capacity(self.0.len());
        let mut start = 0;
        for col in &self.0 {
            let (spec, len) = col.compress(input, out, threshold);
            result.push(RawColumn {
                spec,
                data: start..(start + len),
                _phantom: PhantomData::<compression::Unknown>,
            });
            start += len;
        }
        RawColumns(result)
    }

    /// Read each column from `input` and write to `out`, decompressing any compressed columns
    ///
    /// # Returns
    /// The `RawColumns` corresponding to the data written to `out`
    ///
    /// # Panics
    /// * If any of the ranges in `self` is outside the bounds of `input`
    pub(crate) fn uncompress(
        &self,
        input: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<RawColumns<compression::Uncompressed>, ParseError> {
        let mut result = Vec::with_capacity(self.0.len());
        let mut start = 0;
        for col in &self.0 {
            let (spec, len) = if let Some(decomp) = col.uncompressed() {
                out.extend(&input[decomp.data.clone()]);
                (decomp.spec, decomp.data.len())
            } else {
                col.decompress(input, out)?
            };
            result.push(RawColumn {
                spec,
                data: start..(start + len),
                _phantom: PhantomData::<compression::Uncompressed>,
            });
            start += len;
        }
        Ok(RawColumns(result))
    }
}

impl<T: compression::ColumnCompression> FromIterator<RawColumn<T>> for RawColumns<T> {
    fn from_iter<U: IntoIterator<Item = RawColumn<T>>>(iter: U) -> Self {
        Self(iter.into_iter().filter(|c| !c.data.is_empty()).collect())
    }
}

impl FromIterator<(ColumnSpec, Range<usize>)> for RawColumns<compression::Unknown> {
    fn from_iter<T: IntoIterator<Item = (ColumnSpec, Range<usize>)>>(iter: T) -> Self {
        Self(
            iter.into_iter()
                .filter_map(|(spec, data)| {
                    if data.is_empty() {
                        None
                    } else {
                        Some(RawColumn {
                            spec,
                            data,
                            _phantom: PhantomData,
                        })
                    }
                })
                .collect(),
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ParseError {
    #[error("columns were not in normalized order")]
    NotInNormalOrder,
    #[error(transparent)]
    Leb128(#[from] parse::leb128::Error),
    #[error(transparent)]
    Deflate(#[from] std::io::Error),
}

impl RawColumns<compression::Unknown> {
    pub(crate) fn parse<E>(input: parse::Input<'_>) -> parse::ParseResult<'_, Self, E>
    where
        E: From<ParseError>,
    {
        let i = input;
        let (i, num_columns) = parse::leb128_u64(i).map_err(|e| e.lift())?;
        let (i, specs_and_lens) = parse::apply_n(
            num_columns as usize,
            parse::tuple2(
                parse::map(parse::leb128_u32, ColumnSpec::from),
                parse::leb128_u64,
            ),
        )(i)
        .map_err(|e| e.lift())?;
        let columns: Vec<RawColumn<compression::Unknown>> = specs_and_lens
            .into_iter()
            .scan(0_usize, |offset, (spec, len)| {
                // Note: we use a saturating add here as len was passed over the network
                // and so could be anything. If the addition does every saturate we would
                // expect parsing to fail later (but at least it won't panic!).
                let end = offset.saturating_add(len as usize);
                let data = *offset..end;
                *offset = end;
                Some(RawColumn {
                    spec,
                    data,
                    _phantom: PhantomData,
                })
            })
            .collect::<Vec<_>>();
        if !are_normal_sorted(&columns) {
            return Err(parse::ParseError::Error(
                ParseError::NotInNormalOrder.into(),
            ));
        }
        Ok((i, RawColumns(columns)))
    }
}

impl<T: compression::ColumnCompression> RawColumns<T> {
    pub(crate) fn write(&self, out: &mut Vec<u8>) -> usize {
        let mut written = leb128::write::unsigned(out, self.0.len() as u64).unwrap();
        for col in &self.0 {
            written += leb128::write::unsigned(out, u32::from(col.spec) as u64).unwrap();
            written += leb128::write::unsigned(out, col.data.len() as u64).unwrap();
        }
        written
    }

    pub(crate) fn total_column_len(&self) -> usize {
        self.0.iter().map(|c| c.data.len()).sum()
    }

    pub(crate) fn as_map(&self) -> BTreeMap<ColumnSpec, Range<usize>> {
        self.0.iter().map(|c| (c.spec(), c.data())).collect()
    }

    pub(crate) fn bytes<'a>(&self, c: ColumnSpec, data: &'a [u8]) -> &'a [u8] {
        debug_assert!(self.0.iter().map(|i| i.spec()).is_sorted());
        if let Ok(index) = self.0.binary_search_by(|col| col.spec().cmp(&c)) {
            &data[self.0[index].data().clone()]
        } else {
            &[]
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &RawColumn<T>> + '_ {
        self.0.iter()
    }
}

fn are_normal_sorted<T: compression::ColumnCompression>(cols: &[RawColumn<T>]) -> bool {
    if cols.len() > 1 {
        for (i, col) in cols[1..].iter().enumerate() {
            if col.spec.normalize() < cols[i].spec.normalize() {
                return false;
            }
        }
    }
    true
}
