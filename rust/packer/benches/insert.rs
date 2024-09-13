#![feature(test)]

extern crate test;

use rand::prelude::*;
use packer::*;
use test::Bencher;

#[bench]
fn insert_10_large(b: &mut Bencher) {
  insert_large_n(b, 10, u32::MAX as u64);
}

#[bench]
fn insert_10_small(b: &mut Bencher) {
  insert_large_n(b, 10, 100);
}

#[bench]
fn insert_100_large(b: &mut Bencher) {
  insert_large_n(b, 100, u32::MAX as u64);
}

#[bench]
fn insert_100_small(b: &mut Bencher) {
  insert_large_n(b, 100, 100);
}

#[bench]
fn insert_1000_large(b: &mut Bencher) {
  insert_large_n(b, 1000, u32::MAX as u64);
}

#[bench]
fn insert_1000_small(b: &mut Bencher) {
  insert_large_n(b, 1000, 100);
}

fn insert_large_n(b: &mut Bencher, n: usize, max: u64) {
    let mut rng = rand::thread_rng();
    b.iter(||  {
        let mut col : ColumnData<IntCursor> = ColumnData::new();
        for _ in 0..n {
          let val: u64 = rng.gen();
          let val = val % max;
          let pos: usize = rng.gen();
          let pos = pos % (col.len() + 1);
          col.splice(pos, 0, vec![val as u64]);
        }
    });
}
