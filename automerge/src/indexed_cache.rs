
extern crate hex;
extern crate web_sys;
extern crate uuid;

use std::ops::Index;
use std::collections::HashMap;
use itertools::Itertools;
use std::hash::Hash;

#[derive(Debug, Clone)]
pub(crate) struct IndexedCache<T> {
    pub cache: Vec<T>,
    lookup: HashMap<T, usize>,
}

impl<T> IndexedCache<T>
where
    T: Clone + Eq + Hash + Ord,
{
    pub fn new() -> Self {
        IndexedCache {
            cache: Default::default(),
            lookup: Default::default(),
        }
    }

    pub fn from(cache: Vec<T>) -> Self {
        let lookup = cache
            .iter()
            .enumerate()
            .map(|(i, v)| (v.clone(), i))
            .collect();
        IndexedCache { cache, lookup }
    }

    pub fn cache(&mut self, item: T) -> usize {
        if let Some(n) = self.lookup.get(&item) {
            *n
        } else {
            let n = self.cache.len();
            self.cache.push(item.clone());
            self.lookup.insert(item, n);
            n
        }
    }

    pub fn lookup(&self, item: T) -> Option<usize> {
        self.lookup.get(&item).cloned()
    }

    pub fn get(&self, index: usize) -> &T {
        &self.cache[index]
    }

    pub fn encode_index(&self) -> Vec<usize> {
        let sorted = self.sorted();
        self.cache.iter().map(|a| sorted.iter().position(|r| r == a).unwrap()).collect()
    }

    pub fn decode_index(&self) -> Vec<usize> {
        let sorted = self.sorted();
        sorted.iter().map(|a| self.cache.iter().position(|r| r == a).unwrap()).collect()
    }

    pub fn sorted(&self) -> Vec<T> {
        self.cache.iter().sorted().cloned().collect()
    }
}

impl<T> Index<usize> for IndexedCache<T> {
    type Output = T;
    fn index(&self, i: usize) -> &T {
        &self.cache[i]
    }
}

