/*
#![feature(test)]

extern crate test;

use packer::*;
use rand::prelude::*;
use test::Bencher;

const MIN: u64 = 1;

#[bench]
fn insert_10_large(b: &mut Bencher) {
    insert_n(b, 10, u32::MAX as u64);
}

#[bench]
fn insert_10_small(b: &mut Bencher) {
    insert_n(b, 10, MIN);
}

#[bench]
fn insert_100_large(b: &mut Bencher) {
    insert_n(b, 100, u32::MAX as u64);
}

#[bench]
fn insert_100_small(b: &mut Bencher) {
    insert_n(b, 100, MIN);
}

#[bench]
fn insert_1000_large(b: &mut Bencher) {
    insert_n(b, 1000, u32::MAX as u64);
}

#[bench]
fn insert_1000_small(b: &mut Bencher) {
    insert_n(b, 1000, MIN);
}

#[bench]
fn insert_10000_large(b: &mut Bencher) {
    insert_n(b, 10000, u32::MAX as u64);
}

#[bench]
fn insert_10000_small(b: &mut Bencher) {
    insert_n(b, 10000, MIN);
}

fn insert_n(b: &mut Bencher, n: usize, max: u64) {
    let mut rng = rand::thread_rng();
    let mut col: ColumnData<IntCursor> = ColumnData::new();
    for _ in 0..n {
        let val: u64 = rng.gen();
        let val = val % max;
        let pos: usize = rng.gen();
        let pos = pos % (col.len() + 1);
        col.splice(pos, 0, vec![val as u64]);
    }
    b.iter(|| {
        let val: u64 = rng.gen();
        let val = val % max;
        let pos: usize = rng.gen();
        let pos = pos % (col.len() + 1);
        col.splice(pos, 0, vec![val as u64]);
    });
}
*/
