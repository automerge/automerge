#![feature(test)]

extern crate test;

use packer::*;
use rand::prelude::*;
use test::Bencher;

const MIN: u64 = 1;

#[bench]
fn seek_10_large(b: &mut Bencher) {
    seek_n(b, 10, u32::MAX as u64);
}

#[bench]
fn seek_10_small(b: &mut Bencher) {
    seek_n(b, 10, MIN);
}

#[bench]
fn seek_100_large(b: &mut Bencher) {
    seek_n(b, 100, u32::MAX as u64);
}

#[bench]
fn seek_100_small(b: &mut Bencher) {
    seek_n(b, 100, MIN);
}

#[bench]
fn seek_1000_large(b: &mut Bencher) {
    seek_n(b, 1000, u32::MAX as u64);
}

#[bench]
fn seek_1000_small(b: &mut Bencher) {
    seek_n(b, 1000, MIN);
}

#[bench]
fn seek_10000_large(b: &mut Bencher) {
    seek_n(b, 10000, u32::MAX as u64);
}

#[bench]
fn seek_10000_small(b: &mut Bencher) {
    seek_n(b, 10000, MIN);
}

fn seek_n(b: &mut Bencher, n: usize, max: u64) {
    let mut rng = rand::thread_rng();
    let mut col: ColumnData<IntCursor> = ColumnData::new();
    let values: Vec<u64> = (0..n).map(|_| rng.gen::<u64>() % max).collect();
    col.splice(0, 0, values);
    println!(
        "COL bytes={} vs {}",
        col.byte_len(),
        n * std::mem::size_of::<u64>()
    );
    b.iter(|| {
        let pos = rng.gen::<usize>() % col.len();
        col.get(pos);
    });
}
