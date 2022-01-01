use itertools::Itertools;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Index;

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

    pub fn lookup(&self, item: &T) -> Option<usize> {
        self.lookup.get(item).cloned()
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn get(&self, index: usize) -> &T {
        &self.cache[index]
    }

    pub fn sorted(&self) -> IndexedCache<T> {
        let mut sorted = Self::new();
        self.cache.iter().sorted().cloned().for_each(|item| {
            let n = sorted.cache.len();
            sorted.cache.push(item.clone());
            sorted.lookup.insert(item, n);
        });
        sorted
    }

    pub fn encode_index(&self) -> Vec<usize> {
        let sorted: Vec<_> = self.cache.iter().sorted().cloned().collect();
        self.cache
            .iter()
            .map(|a| sorted.iter().position(|r| r == a).unwrap())
            .collect()
    }
}

impl<T> IntoIterator for IndexedCache<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.cache.into_iter()
    }
}

impl<T> Index<usize> for IndexedCache<T> {
    type Output = T;
    fn index(&self, i: usize) -> &T {
        &self.cache[i]
    }
}
