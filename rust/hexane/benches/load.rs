//! Column load benchmark across encoding shapes (the streaming
//! load_iter-backed loader).

use divan::{black_box, Bencher};
use hexane::{Column, LoadOpts, PrefixColumn};

fn main() {
    divan::main();
}

const N: usize = 100_000;

fn saved_literals_u64() -> Vec<u8> {
    Column::<u64>::from_values((0..N as u64).collect()).save()
}

fn saved_repeats_u64() -> Vec<u8> {
    Column::<u64>::from_values((0..N as u64).map(|i| i / 1000).collect()).save()
}

fn saved_mixed_nullable() -> Vec<u8> {
    Column::<Option<u64>>::from_values(
        (0..N as u64)
            .map(|i| if i % 5 == 0 { None } else { Some(i / 7) })
            .collect(),
    )
    .save()
}

fn saved_strings() -> Vec<u8> {
    Column::<String>::from_values(
        (0..N / 10)
            .map(|i| {
                if i % 3 == 0 {
                    "repeated".to_string()
                } else {
                    format!("item_{i:06}")
                }
            })
            .collect(),
    )
    .save()
}

fn saved_bool() -> Vec<u8> {
    Column::<bool>::from_values((0..N).map(|i| (i / 17) % 2 == 0).collect()).save()
}

fn saved_prefix_u32() -> Vec<u8> {
    PrefixColumn::<u32>::from_values((0..N).map(|i| (i % 4) as u32).collect()).save()
}

macro_rules! bench_pair {
    ($name:ident, $ty:ty, $col:ty, $gen:ident) => {
        mod $name {
            use super::*;

            #[divan::bench]
            fn load_with(bencher: Bencher) {
                let data = $gen();
                bencher.bench_local(|| black_box(<$col>::load_with(&data, LoadOpts::new().into())));
            }
        }
    };
}

bench_pair!(literals_u64, u64, Column::<u64>, saved_literals_u64);
bench_pair!(repeats_u64, u64, Column::<u64>, saved_repeats_u64);
bench_pair!(
    nullable_u64,
    Option<u64>,
    Column::<Option<u64>>,
    saved_mixed_nullable
);
bench_pair!(strings, String, Column::<String>, saved_strings);
bench_pair!(bools, bool, Column::<bool>, saved_bool);
bench_pair!(prefix_u32, u32, PrefixColumn::<u32>, saved_prefix_u32);
