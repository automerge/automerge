use std::ops::Range;

use super::{super::ColumnSpec, parse};

#[derive(Clone, Debug)]
pub(crate) struct Column {
    spec: ColumnSpec,
    data: Range<usize>,
}

#[derive(Clone, Debug)]
pub(crate) struct ColumnMetadata(Vec<Column>);

impl FromIterator<Column> for ColumnMetadata {
    fn from_iter<T: IntoIterator<Item = Column>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl FromIterator<(ColumnSpec, Range<usize>)> for ColumnMetadata {
    fn from_iter<T: IntoIterator<Item = (ColumnSpec, Range<usize>)>>(iter: T) -> Self {
        Self(iter.into_iter().map(|(spec, data)| Column{spec, data}).collect())
    }
}

impl ColumnMetadata {
    pub(crate) fn parse(input: &[u8]) -> parse::ParseResult<ColumnMetadata> {
        let i = input;
        let (i, num_columns) = parse::leb128_u64(i)?;
        let (i, specs_and_lens) = parse::apply_n(
            num_columns as usize,
            parse::tuple2(
                parse::map(parse::leb128_u32, ColumnSpec::from),
                parse::leb128_u64,
            ),
        )(i)?;
        let columns = specs_and_lens
            .into_iter()
            .scan(0_usize, |offset, (spec, len)| {
                let end = *offset + len as usize;
                let data = *offset..end;
                *offset = end;
                Some(Column { spec, data })
            })
            .collect::<Vec<_>>();
        if !are_normal_sorted(&columns) {
            return Err(parse::ParseError::Error(
                parse::ErrorKind::InvalidColumnMetadataSort,
            ));
        }
        Ok((i, ColumnMetadata(columns)))
    }

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

    pub(crate) fn iter(&self) -> impl Iterator<Item = (ColumnSpec, Range<usize>)> + '_ {
        self.0.iter().map(|c| (c.spec, c.data.clone()))
    }

    pub(crate) fn byte_len(&self) -> usize {
        self.0.len() * 16
    }
}

fn are_normal_sorted(cols: &[Column]) -> bool {
    if cols.len() > 1 {
        for (i, col) in cols[1..].iter().enumerate() {
            if col.spec.normalize() < cols[i].spec.normalize() {
                return false;
            }
        }
    }
    true
}
