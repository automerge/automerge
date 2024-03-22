use itertools::Itertools;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Index;

#[derive(Debug, Clone)]
pub(crate) struct IndexedCache<T> {
    pub(crate) cache: Vec<T>,
    lookup: HashMap<T, usize>,
}

impl<T> PartialEq for IndexedCache<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.cache == other.cache
    }
}

impl<T> IndexedCache<T>
where
    T: Clone + Eq + Hash + Ord,
{
    pub(crate) fn new() -> Self {
        IndexedCache {
            cache: Default::default(),
            lookup: Default::default(),
        }
    }

    pub(crate) fn cache(&mut self, item: T) -> usize {
        if let Some(n) = self.lookup.get(&item) {
            *n
        } else {
            let n = self.cache.len();
            self.cache.push(item.clone());
            self.lookup.insert(item, n);
            n
        }
    }

    pub(crate) fn lookup(&self, item: &T) -> Option<usize> {
        self.lookup.get(item).cloned()
    }

    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.cache.len()
    }

    pub(crate) fn get(&self, index: usize) -> &T {
        &self.cache[index]
    }

    pub(crate) fn safe_get(&self, index: usize) -> Option<&T> {
        self.cache.get(index)
    }

    #[allow(dead_code)]
    pub(crate) fn sorted(&self) -> IndexedCache<T> {
        let mut sorted = Self::new();
        self.cache.iter().sorted().cloned().for_each(|item| {
            let n = sorted.cache.len();
            sorted.cache.push(item.clone());
            sorted.lookup.insert(item, n);
        });
        sorted
    }

    /// Create a vector from positions in this index to positions in an equivalent sorted index
    ///
    /// This is useful primarily when encoding an `IndexedCache<ActorId>` in the document format.
    /// In this case we encode the actors in sorted order in the document and all ops reference the
    /// offset into this sorted actor array. But the `IndexedCache<ActorId>` we have in the
    /// application does not contain actors in sorted order because we add them as we encounter
    /// them, so we must map from the actor IDs in the application to the actor IDs in the document
    /// format
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let idx: IndexedCache<String> = IndexedCache::new();
    /// let first_idx = idx.cache("b"); // first_idx is `0`
    /// let second_idx = idx.cache("a"); // second_idx i `1`
    /// let encoded = idx.encode_index();
    /// // first_idx (0) maps to `1` whilst second_idx (1) maps to `0` because "a" < "b"
    /// assert_eq!(encoded, vec![1,0])
    /// ```
    pub(crate) fn encode_index(&self) -> Vec<usize> {
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

impl<A: Hash + Eq + Clone> FromIterator<A> for IndexedCache<A> {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        let mut cache = Vec::new();
        let mut lookup = HashMap::new();
        for (index, elem) in iter.into_iter().enumerate() {
            cache.push(elem.clone());
            lookup.insert(elem, index);
        }
        Self { cache, lookup }
    }
}
