//! `PrefixColumn::load_iter` returned a `PrefixColumnLoadIter<'a, T>`,
//! whose codec is the default, so a column with another codec was decoded
//! as LEB128.
#![cfg(feature = "bijou64")]

use hexane::{Bijou64, LoadOpts, PrefixColumn};

#[test]
fn load_iter_decodes_with_the_columns_codec() {
    // values of more than one byte, which the codecs encode differently
    let values: Vec<u64> = (0..1000).map(|i| i * 1000).collect();
    let bytes = PrefixColumn::<u64, Bijou64>::from_values(values.clone()).save();
    let col = PrefixColumn::<u64, Bijou64>::load_iter(&bytes, LoadOpts::new())
        .finalize()
        .unwrap();
    assert_eq!(col.to_vec(), values);
}
