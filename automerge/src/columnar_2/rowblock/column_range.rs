use std::{borrow::Cow, ops::Range};

use smol_str::SmolStr;

use super::encoding::{
    BooleanDecoder, BooleanEncoder, DeltaDecoder, DeltaEncoder, RawDecoder, RawEncoder,
    RleDecoder, RleEncoder,
};

macro_rules! make_col_range({$name: ident, $decoder_name: ident$(<$($dparam: tt),+>)?, $encoder_name: ident$(<$($eparam: tt),+>)?} => {
    #[derive(Clone, Debug)]
    pub(crate) struct $name(Range<usize>);

    impl $name {
        pub(crate) fn decoder<'a>(&self, data: &'a[u8]) -> $decoder_name $(<$($dparam,)+>)* {
            $decoder_name::from(Cow::Borrowed(&data[self.0.clone()]))
        }

        pub(crate) fn encoder<'a>(&self, output: &'a mut Vec<u8>) -> $encoder_name $(<$($eparam,)+>)* {
            $encoder_name::from(output)
        }

        pub(crate) fn len(&self) -> usize {
            self.0.len()
        }

        pub(crate) fn is_empty(&self) -> bool {
            self.0.is_empty()
        }
    }

    impl AsRef<Range<usize>> for $name {
        fn as_ref(&self) -> &Range<usize> {
            &self.0
        }
    }

    impl From<Range<usize>> for $name {
        fn from(r: Range<usize>) -> $name {
            $name(r)
        }
    }

    impl From<$name> for Range<usize> {
        fn from(r: $name) -> Range<usize> {
            r.0
        }
    }
});

make_col_range!(ActorRange, RleDecoder<'a, u64>, RleEncoder<'a, u64>);
make_col_range!(RleIntRange, RleDecoder<'a, u64>, RleEncoder<'a, u64>);
make_col_range!(DeltaIntRange, DeltaDecoder<'a>, DeltaEncoder<'a>);
make_col_range!(
    RleStringRange,
    RleDecoder<'a, SmolStr>,
    RleEncoder<'a, SmolStr>
);
make_col_range!(BooleanRange, BooleanDecoder<'a>, BooleanEncoder<'a>);
make_col_range!(RawRange, RawDecoder<'a>, RawEncoder<'a>);
