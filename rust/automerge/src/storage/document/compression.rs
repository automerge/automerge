use std::{borrow::Cow, convert::Infallible, ops::Range};

use crate::storage::{
    columns::{compression, raw_column},
    shift_range, ChunkType, Header, RawColumns,
};

pub(super) struct Args<'a, T: compression::ColumnCompression, DirArgs> {
    /// The original data of the entire document chunk (compressed or uncompressed)
    pub(super) original: Cow<'a, [u8]>,
    /// The number of bytes in the original before the beginning of the change column metadata
    pub(super) prefix: usize,
    /// The offset in the original after the end of the ops column data
    pub(super) suffix: usize,
    /// The column data for the changes
    pub(super) changes: Cols<T>,
    /// The column data for the ops
    pub(super) ops: Cols<T>,
    /// Additional arguments specific to the direction (compression or uncompression)
    pub(super) extra_args: DirArgs,
}

pub(super) struct CompressArgs {
    pub(super) threshold: usize,
    pub(super) original_header_len: usize,
}

/// Compress a document chunk returning the compressed bytes
pub(super) fn compress(args: Args<'_, compression::Uncompressed, CompressArgs>) -> Vec<u8> {
    let header_len = args.extra_args.original_header_len;
    let threshold = args.extra_args.threshold;
    // Wrap in a closure so we can use `?` in the construction but still force the compiler
    // to check that the error type is `Infallible`
    let result: Result<_, Infallible> = (|| {
        Ok(Compression::<Compressing, _>::new(
            args,
            Compressing {
                threshold,
                header_len,
            },
        )
        .changes()?
        .ops()?
        .write_data()
        .finish())
    })();
    // We just checked the error is `Infallible` so unwrap is fine
    result.unwrap()
}

pub(super) fn decompress<'a>(
    args: Args<'a, compression::Unknown, ()>,
) -> Result<Decompressed<'a>, raw_column::ParseError> {
    match (
        args.changes.raw_columns.uncompressed(),
        args.ops.raw_columns.uncompressed(),
    ) {
        (Some(changes), Some(ops)) => Ok(Decompressed {
            changes,
            ops,
            compressed: None,
            uncompressed: args.original,
            change_bytes: args.changes.data,
            op_bytes: args.ops.data,
        }),
        _ => Ok(
            Compression::<'a, Decompressing, _>::new(args, Decompressing)
                .changes()?
                .ops()?
                .write_data()
                .finish(),
        ),
    }
}

pub(super) struct Decompressed<'a> {
    /// The original compressed data, if there was any
    pub(super) compressed: Option<Cow<'a, [u8]>>,
    /// The final uncompressed data
    pub(super) uncompressed: Cow<'a, [u8]>,
    /// The ops column metadata
    pub(super) ops: RawColumns<compression::Uncompressed>,
    /// The change column metadata
    pub(super) changes: RawColumns<compression::Uncompressed>,
    /// The location of the change column data in the uncompressed data
    pub(super) change_bytes: Range<usize>,
    /// The location of the op column data in the uncompressed data
    pub(super) op_bytes: Range<usize>,
}

struct Compression<'a, D: Direction, S: CompressionState> {
    args: Args<'a, D::In, D::Args>,
    state: S,
    direction: D,
}

/// Some columns in the original data
pub(super) struct Cols<T: compression::ColumnCompression> {
    /// The metadata for these columns
    pub(super) raw_columns: RawColumns<T>,
    /// The location in the original chunk of the data for these columns
    pub(super) data: Range<usize>,
}

// Compression and decompression involve almost the same steps in either direction. This trait
// encapsulates that.
trait Direction: std::fmt::Debug {
    type Out: compression::ColumnCompression;
    type In: compression::ColumnCompression;
    type Error;
    type Args;

    /// This method represents the (de)compression process for a direction. The arguments are:
    ///
    /// * cols - The columns we are processing
    /// * input - the entire document chunk
    /// * out - the vector to place the processed columns in
    /// * meta_out - the vector to place processed column metadata in
    fn process(
        &self,
        cols: &Cols<Self::In>,
        input: &[u8],
        out: &mut Vec<u8>,
        meta_out: &mut Vec<u8>,
    ) -> Result<Cols<Self::Out>, Self::Error>;
}
#[derive(Debug)]
struct Compressing {
    threshold: usize,
    header_len: usize,
}

impl Direction for Compressing {
    type Error = Infallible;
    type Out = compression::Unknown;
    type In = compression::Uncompressed;
    type Args = CompressArgs;

    fn process(
        &self,
        cols: &Cols<Self::In>,
        input: &[u8],
        out: &mut Vec<u8>,
        meta_out: &mut Vec<u8>,
    ) -> Result<Cols<Self::Out>, Self::Error> {
        let start = out.len();
        let raw_columns = cols
            .raw_columns
            .compress(&input[cols.data.clone()], out, self.threshold);
        raw_columns.write(meta_out);
        Ok(Cols {
            data: start..out.len(),
            raw_columns,
        })
    }
}

#[derive(Debug)]
struct Decompressing;

impl Direction for Decompressing {
    type Error = raw_column::ParseError;
    type Out = compression::Uncompressed;
    type In = compression::Unknown;
    type Args = ();

    fn process(
        &self,
        cols: &Cols<Self::In>,
        input: &[u8],
        out: &mut Vec<u8>,
        meta_out: &mut Vec<u8>,
    ) -> Result<Cols<Self::Out>, raw_column::ParseError> {
        let start = out.len();
        let raw_columns = cols
            .raw_columns
            .uncompress(&input[cols.data.clone()], out)?;
        raw_columns.write(meta_out);
        Ok(Cols {
            data: start..out.len(),
            raw_columns,
        })
    }
}

// Somewhat absurdly I (alex) kept getting the order of writing ops and changes wrong as well as
// the order that column metadata vs data should be written in. This is a type state to get the
// compiler to enforce that things are done in the right order.
trait CompressionState {}
impl CompressionState for Starting {}
impl<D: Direction> CompressionState for Changes<D> {}
impl<D: Direction> CompressionState for ChangesAndOps<D> {}
impl<D: Direction> CompressionState for Finished<D> {}

/// We haven't done any processing yet
struct Starting {
    /// The vector to write column data to
    data_out: Vec<u8>,
    /// The vector to write column metadata to
    meta_out: Vec<u8>,
}

/// We've processed the changes columns
struct Changes<D: Direction> {
    /// The `Cols` for the processed change columns
    change_cols: Cols<D::Out>,
    /// The vector to write column metadata to
    meta_out: Vec<u8>,
    /// The vector to write column data to
    data_out: Vec<u8>,
}

/// We've processed the ops columns
struct ChangesAndOps<D: Direction> {
    /// The `Cols` for the processed change columns
    change_cols: Cols<D::Out>,
    /// The `Cols` for the processed op columns
    ops_cols: Cols<D::Out>,
    /// The vector to write column metadata to
    meta_out: Vec<u8>,
    /// The vector to write column data to
    data_out: Vec<u8>,
}

/// We've written the column metadata and the op metadata for changes and ops to the output buffer
/// and added the prefix and suffix from the args.
struct Finished<D: Direction> {
    /// The `Cols` for the processed change columns
    change_cols: Cols<D::Out>,
    /// The `Cols` for the processed op columns
    ops_cols: Cols<D::Out>,
    /// The start of the change column metadata in the processed chunk
    data_start: usize,
    /// The processed chunk
    out: Vec<u8>,
}

impl<'a, D: Direction> Compression<'a, D, Starting> {
    fn new(args: Args<'a, D::In, D::Args>, direction: D) -> Compression<'a, D, Starting> {
        let mut meta_out = Vec::with_capacity(args.original.len() * 2);
        meta_out.extend(&args.original[..args.prefix]);
        Compression {
            args,
            direction,
            state: Starting {
                meta_out,
                data_out: Vec::new(),
            },
        }
    }
}

impl<'a, D: Direction> Compression<'a, D, Starting> {
    fn changes(self) -> Result<Compression<'a, D, Changes<D>>, D::Error> {
        let Starting {
            mut data_out,
            mut meta_out,
        } = self.state;
        let change_cols = self.direction.process(
            &self.args.changes,
            &self.args.original,
            &mut data_out,
            &mut meta_out,
        )?;
        Ok(Compression {
            args: self.args,
            direction: self.direction,
            state: Changes {
                change_cols,
                meta_out,
                data_out,
            },
        })
    }
}

impl<'a, D: Direction> Compression<'a, D, Changes<D>> {
    fn ops(self) -> Result<Compression<'a, D, ChangesAndOps<D>>, D::Error> {
        let Changes {
            change_cols,
            mut meta_out,
            mut data_out,
        } = self.state;
        let ops_cols = self.direction.process(
            &self.args.ops,
            &self.args.original,
            &mut data_out,
            &mut meta_out,
        )?;
        Ok(Compression {
            args: self.args,
            direction: self.direction,
            state: ChangesAndOps {
                change_cols,
                ops_cols,
                meta_out,
                data_out,
            },
        })
    }
}

impl<'a, D: Direction> Compression<'a, D, ChangesAndOps<D>> {
    fn write_data(self) -> Compression<'a, D, Finished<D>> {
        let ChangesAndOps {
            data_out,
            mut meta_out,
            change_cols,
            ops_cols,
        } = self.state;
        let data_start = meta_out.len();
        meta_out.extend(&data_out);
        meta_out.extend(&self.args.original[self.args.suffix..]);
        Compression {
            args: self.args,
            direction: self.direction,
            state: Finished {
                ops_cols,
                change_cols,
                out: meta_out,
                data_start,
            },
        }
    }
}

impl<'a> Compression<'a, Decompressing, Finished<Decompressing>> {
    fn finish(self) -> Decompressed<'a> {
        let Finished {
            change_cols,
            ops_cols,
            data_start,
            out,
        } = self.state;
        Decompressed {
            ops: ops_cols.raw_columns,
            changes: change_cols.raw_columns,
            uncompressed: Cow::Owned(out),
            compressed: Some(self.args.original),
            change_bytes: shift_range(change_cols.data, data_start),
            op_bytes: shift_range(ops_cols.data, data_start),
        }
    }
}

impl<'a> Compression<'a, Compressing, Finished<Compressing>> {
    fn finish(self) -> Vec<u8> {
        let Finished { out, .. } = self.state;
        let headerless = &out[self.direction.header_len..];
        let header = Header::new(ChunkType::Document, headerless);
        let mut result = Vec::with_capacity(header.len() + out.len());
        header.write(&mut result);
        result.extend(headerless);
        result
    }
}
