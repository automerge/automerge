#![allow(dead_code)]

use crate::error::AutomergeError;
use im_rc::HashMap;
use rand::rngs::ThreadRng;
use rand::Rng;
use std::cmp::{max, min};
use std::fmt::Debug;
use std::hash::Hash;
use std::iter::Iterator;
use std::ops::AddAssign;

#[derive(Debug, Clone, PartialEq)]
struct Tower<K>
where
    K: Clone + Debug + PartialEq,
{
    next: Vec<Link<K>>,
    prev: Vec<Link<K>>,
    level: usize,
    is_head: bool,
}

#[derive(Debug, Clone, PartialEq)]
struct Node<K, V>
where
    K: Clone + Debug + PartialEq,
    V: Clone + Debug + PartialEq,
{
    tower: Tower<K>,
    key: K,
    value: V,
}

#[derive(Debug, Clone, PartialEq)]
struct Link<K>
where
    K: Clone + Debug + PartialEq,
{
    key: Option<K>,
    count: usize,
}

impl<K> AddAssign for Link<K>
where
    K: Clone + Debug + PartialEq,
{
    fn add_assign(&mut self, other: Self) {
        *self = Self {
            key: other.key.clone(),
            count: self.count + other.count,
        };
    }
}

impl<K> Tower<K>
where
    K: Debug + Clone + PartialEq,
{
    fn successor(&self) -> &Option<K> {
        if self.next.is_empty() {
            &None
        } else {
            &self.next[0].key //.as_ref()
        }
    }

    fn remove_after(&mut self, from_level: usize, removed_level: usize, links: &[Link<K>]) {
        for (level, item) in links.iter().enumerate().take(self.level).skip(from_level) {
            if level < removed_level {
                self.next[level] = item.clone();
            } else {
                self.next[level].count -= 1;
            }
        }
    }

    fn remove_before(&mut self, from_level: usize, removed_level: usize, links: &[Link<K>]) {
        for (level, item) in links.iter().enumerate().take(self.level).skip(from_level) {
            if level < removed_level {
                self.prev[level] = item.clone();
            } else {
                self.prev[level].count -= 1;
            }
        }
    }

    fn insert_after(
        &mut self,
        new_key: &K,
        new_level: usize,
        from_level: usize,
        distance: usize,
    ) -> Result<(), AutomergeError> {
        if new_level > self.level && !self.is_head {
            Err(AutomergeError::SkipListError(
                "Cannot increase the level of a non-head node".to_string(),
            ))
        } else {
            self.level = max(self.level, new_level);

            for level in from_level..self.level {
                if level < new_level {
                    let link = Link {
                        key: Some(new_key.clone()),
                        count: distance,
                    };
                    if self.next.len() == level {
                        self.next.push(link)
                    } else {
                        self.next[level] = link
                    }
                } else {
                    self.next[level].count += 1;
                }
            }

            Ok(())
        }
    }

    fn insert_before(
        &mut self,
        new_key: &K,
        new_level: usize,
        from_level: usize,
        distance: usize,
    ) -> Result<(), AutomergeError> {
        if new_level > self.level {
            Err(AutomergeError::SkipListError(
                "Cannot increase the level on insert-before".to_string(),
            ))
        } else {
            for level in from_level..self.level {
                if level < new_level {
                    self.prev[level] = Link {
                        key: Some(new_key.clone()),
                        count: distance,
                    };
                } else {
                    self.prev[level].count += 1;
                }
            }
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VecOrderedMap<K, V>
where
    K: Clone + Debug + Hash + PartialEq + Eq,
    V: Clone + Debug + PartialEq,
{
    keys: Vec<K>,
    values: HashMap<K, V>,
}

impl<K, V> VecOrderedMap<K, V>
where
    K: Clone + Debug + Hash + PartialEq + Eq,
    V: Clone + Debug + PartialEq,
{
    pub fn new() -> VecOrderedMap<K, V> {
        VecOrderedMap {
            keys: Vec::new(),
            values: HashMap::new(),
        }
    }
}

pub trait OrderedMap<K, V>
where
    K: Clone + Debug + Hash + PartialEq + Eq,
    V: Clone + Debug + PartialEq,
{
    fn index_of(&self, key: &K) -> Option<usize>;
    fn remove_key(&mut self, key: &K) -> Option<usize>;
    fn insert_index(&mut self, index: usize, key: K, val: V) -> Option<&K>;
    fn remove_index(&mut self, index: usize) -> Option<(K, V)>;
    fn key_of(&self, index: usize) -> Option<&K>;
    fn to_vec(&self) -> Vec<K>;
}

impl<K, V> OrderedMap<K, V> for SkipList<K, V>
where
    K: Clone + Debug + Hash + PartialEq + Eq,
    V: Clone + Debug + PartialEq,
{
    fn remove_index(&mut self, index: usize) -> Option<(K, V)> {
        if let Some(key) = self.key_of(index).cloned() {
            let value = self._remove_key(&key).unwrap();
            Some((key, value))
        } else {
            None
        }
    }

    fn remove_key(&mut self, key: &K) -> Option<usize> {
        let index = self.index_of(key);
        if index.is_some() {
            self._remove_key(key).unwrap();
        }
        index
    }

    fn to_vec(&self) -> Vec<K> {
        self.iter().map(|(k, _)| k).cloned().collect()
    }

    fn key_of(&self, index: usize) -> Option<&K> {
        if index >= self.len {
            return None;
        }
        let target = index + 1;
        let mut node = &self.head;
        let mut level = node.level - 1;
        let mut count = 0;
        loop {
            while count + node.next[level].count > target {
                level -= 1
            }
            count += node.next[level].count;
            let k: &Option<K> = &node.next[level].key;
            if count == target {
                return k.as_ref();
            }
            node = self.get_tower(k).unwrap(); // panic is correct
        }
    }

    fn index_of(&self, key: &K) -> Option<usize> {
        if !self.nodes.contains_key(&key) {
            return None;
        }

        let mut count = 0;
        let mut k = key.clone();
        loop {
            if let Some(node) = self.nodes.get(&k) {
                let link = &node.tower.prev[node.tower.level - 1];
                count += link.count;
                if let Some(key) = &link.key {
                    k = key.clone();
                } else {
                    break;
                }
            } else {
                return None;
            }
        }
        Some(count - 1)
    }

    fn insert_index(&mut self, index: usize, key: K, value: V) -> Option<&K> {
        if index == 0 {
            self.insert_head(key, value).unwrap();
            None
        } else {
            let suc = self.key_of(index - 1).unwrap().clone();
            self.insert_after(&suc, key, value).unwrap();
            self.key_of(index - 1) // FIXME
        }
    }
}

impl<K, V> OrderedMap<K, V> for VecOrderedMap<K, V>
where
    K: Clone + Debug + Hash + PartialEq + Eq,
    V: Clone + Debug + PartialEq,
{
    fn remove_index(&mut self, index: usize) -> Option<(K, V)> {
        if self.keys.len() > index {
            let k = self.keys.remove(index);
            let v = self.values.remove(&k).unwrap();
            Some((k, v))
        } else {
            None
        }
    }

    fn to_vec(&self) -> Vec<K> {
        self.keys.clone()
    }

    fn key_of(&self, index: usize) -> Option<&K> {
        self.keys.get(index)
    }

    fn index_of(&self, key: &K) -> Option<usize> {
        self.keys.iter().position(|o| o == key)
    }

    fn insert_index(&mut self, index: usize, key: K, value: V) -> Option<&K> {
        self.keys.insert(index, key.clone());
        self.values.insert(key, value);
        if index == 0 {
            None
        } else {
            self.keys.get(index - 1)
        }
    }

    fn remove_key(&mut self, key: &K) -> Option<usize> {
        if let Some(index) = self.keys.iter().position(|o| o == key) {
            self.keys.remove(index);
            Some(index)
        } else {
            None
        }
    }
}

impl<K, V> Default for SkipList<K, V>
where
    K: Clone + Debug + Hash + PartialEq + Eq,
    V: Clone + Debug + PartialEq,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Default for VecOrderedMap<K, V>
where
    K: Clone + Debug + Hash + PartialEq + Eq,
    V: Clone + Debug + PartialEq,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, K, V> IntoIterator for &'a VecOrderedMap<K, V>
where
    K: Clone + Debug + Hash + PartialEq + Eq,
    V: Clone + Debug + PartialEq,
{
    type Item = &'a K;
    type IntoIter = std::slice::Iter<'a, K>;

    fn into_iter(self) -> std::slice::Iter<'a, K> {
        self.keys.as_slice().iter()
    }
}

impl<'a, K, V> IntoIterator for &'a SkipList<K, V>
where
    K: Clone + Debug + Hash + PartialEq + Eq,
    V: Clone + Debug + PartialEq,
{
    type Item = &'a K;
    type IntoIter = SkipKeyIterator<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        SkipKeyIterator {
            id: self.head.successor(),
            nodes: &self.nodes,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SkipList<K, V>
where
    K: Clone + Debug + Hash + PartialEq + Eq,
    V: Clone + Debug + PartialEq,
{
    nodes: HashMap<K, Node<K, V>>,
    head: Tower<K>,
    rng: ThreadRng,
    pub len: usize,
}

impl<K, V> PartialEq for SkipList<K, V>
where
    K: Clone + Debug + Hash + PartialEq + Eq,
    V: Clone + Debug + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.nodes.eq(&other.nodes)
    }
}

impl<K, V> SkipList<K, V>
where
    K: Clone + Debug + Hash + PartialEq + Eq,
    V: Clone + Debug + PartialEq,
{
    pub fn new() -> SkipList<K, V> {
        let nodes = HashMap::new();
        let head = Tower {
            next: Vec::new(),
            prev: Vec::new(),
            level: 1,
            is_head: true,
        };
        let len = 0;
        let rng = rand::thread_rng();
        SkipList {
            nodes,
            head,
            len,
            rng,
        }
    }

    fn _remove_key(&mut self, key: &K) -> Result<V, AutomergeError> {
        let removed = self.nodes.remove(key).ok_or_else(|| {
            AutomergeError::SkipListError(
                "The given key cannot be removed because it does not exist".to_string(),
            )
        })?;
        let max_level = self.head.level;
        let mut pre = self.predecessors(&removed.tower.prev[0].key, max_level)?;
        let mut suc = self.successors(&removed.tower.next[0].key, max_level)?;

        for i in 0..max_level {
            let distance = pre[i].count + suc[i].count - 1;
            pre[i].count = distance;
            suc[i].count = distance;
        }

        self.len -= 1;
        let mut pre_level = 0;
        let mut suc_level = 0;

        for level in 1..(max_level + 1) {
            let update_level = min(level, removed.tower.level);
            if level == max_level
                || pre.get(level).map(|l| &l.key) != pre.get(pre_level).map(|l| &l.key)
            {
                self.get_tower_mut(&pre[pre_level].key)?.remove_after(
                    pre_level,
                    update_level,
                    &suc,
                );
                pre_level = level;
            }
            if suc[suc_level].key.is_some()
                && (level == max_level
                    || suc.get(level).map(|l| &l.key) != suc.get(suc_level).map(|l| &l.key))
            {
                self.get_tower_mut(&suc[suc_level].key)?.remove_before(
                    suc_level,
                    update_level,
                    &pre,
                );
                suc_level = level;
            }
        }
        Ok(removed.value)
    }

    fn set(&mut self, key: &K, value: V) -> Result<(), AutomergeError> {
        let mut node = self
            .nodes
            .get_mut(&key)
            .ok_or_else(|| AutomergeError::SkipListError("Set index out of bounds".to_string()))?;
        node.value = value;
        Ok(())
    }

    fn get(&self, key: &K) -> Option<&V> {
        self.nodes.get(&key).map(|n| &n.value)
    }

    fn get_tower(&self, key: &Option<K>) -> Result<&Tower<K>, AutomergeError> {
        if let Some(ref k) = key {
            self.nodes
                .get(k)
                .map(|n| &n.tower)
                .ok_or_else(|| AutomergeError::SkipListError("Key not found".to_string()))
        } else {
            Ok(&self.head)
        }
    }

    fn get_tower_mut(&mut self, key: &Option<K>) -> Result<&mut Tower<K>, AutomergeError> {
        if let Some(ref k) = key {
            self.nodes
                .get_mut(k)
                .map(|n| &mut n.tower)
                .ok_or_else(|| AutomergeError::SkipListError("Key not found".to_string()))
        } else {
            Ok(&mut self.head)
        }
    }

    fn predecessors(
        &self,
        predecessor: &Option<K>,
        max_level: usize,
    ) -> Result<Vec<Link<K>>, AutomergeError> {
        let mut pre = vec![Link {
            key: predecessor.clone(),
            count: 1,
        }];

        for level in 1..max_level {
            let mut link = pre[level - 1].clone();
            while link.key.is_some() {
                let node = self.get_tower(&link.key)?;
                if node.level > level {
                    break;
                }
                if node.level < level {
                    return Err(AutomergeError::SkipListError(
                        "Level lower than expected".to_string(),
                    ));
                }
                link += node.prev[level - 1].clone();
            }
            pre.push(link);
        }
        Ok(pre)
    }

    fn successors(
        &self,
        successor: &Option<K>,
        max_level: usize,
    ) -> Result<Vec<Link<K>>, AutomergeError> {
        let mut suc = vec![Link {
            key: successor.clone(),
            count: 1,
        }];

        for level in 1..max_level {
            let mut link = suc[level - 1].clone();
            while link.key.is_some() {
                let tower = self.get_tower(&link.key)?;
                if tower.level > level {
                    break;
                }
                if tower.level < level {
                    return Err(AutomergeError::SkipListError(
                        "Level lower than expected".to_string(),
                    ));
                }
                link += tower.next[level - 1].clone();
            }
            suc.push(link);
        }
        Ok(suc)
    }

    pub fn iter(&self) -> SkipKeyValIterator<K, V> {
        SkipKeyValIterator {
            id: self.head.successor(),
            nodes: &self.nodes,
        }
    }

    pub fn insert_head(&mut self, key: K, value: V) -> Result<(), AutomergeError> {
        self._insert_after(&None, key, value)
    }

    pub fn insert_after(
        &mut self,
        predecessor: &K,
        key: K,
        value: V,
    ) -> Result<(), AutomergeError> {
        self._insert_after(&Some(predecessor.clone()), key, value)
    }

    fn _insert_after(
        &mut self,
        predecessor: &Option<K>,
        key: K,
        value: V,
    ) -> Result<(), AutomergeError> {
        if self.nodes.contains_key(&key) {
            return Err(AutomergeError::SkipListError("DuplicateKey".to_string()));
        }

        let new_level = self.random_level();
        let max_level = max(new_level, self.head.level);
        let successor = self.get_tower(predecessor)?.successor();
        let mut pre = self.predecessors(predecessor, max_level)?;
        let mut suc = self.successors(successor, max_level)?;

        self.len += 1;

        let mut pre_level = 0;
        let mut suc_level = 0;
        for level in 1..(max_level + 1) {
            let update_level = min(level, new_level);
            if level == max_level
                || pre.get(level).map(|l| &l.key) != pre.get(pre_level).map(|l| &l.key)
            {
                self.get_tower_mut(&pre[pre_level].key)?.insert_after(
                    &key,
                    update_level,
                    pre_level,
                    pre[pre_level].count,
                )?;
                pre_level = level;
            }
            if suc[suc_level].key.is_some()
                && (level == max_level
                    || suc.get(level).map(|l| &l.key) != suc.get(suc_level).map(|l| &l.key))
            {
                self.get_tower_mut(&suc[suc_level].key)?.insert_before(
                    &key,
                    update_level,
                    suc_level,
                    suc[suc_level].count,
                )?;
                suc_level = level;
            }
        }

        pre.truncate(new_level);
        suc.truncate(new_level);
        self.nodes.insert(
            key.clone(),
            Node {
                key,
                value,
                tower: Tower {
                    level: new_level,
                    prev: pre,
                    next: suc,
                    is_head: false,
                },
            },
        );
        Ok(())
    }

    // Returns a random number from the geometric distribution with p = 0.75.
    // That is, returns k with probability p * (1 - p)^(k - 1).
    // For example, returns 1 with probability 3/4, returns 2 with probability 3/16,
    // returns 3 with probability 3/64, and so on.

    fn random_level(&mut self) -> usize {
        // Create random number between 0 and 2^32 - 1
        // Count leading zeros in that 32-bit number
        let rand: u32 = self.rng.gen();
        let mut level = 1;
        while rand < 1 << (32 - 2 * level) && level < 16 {
            level += 1
        }
        level
    }
}

pub(crate) struct SkipKeyValIterator<'a, K, V>
where
    K: Debug + Clone + PartialEq,
    V: Debug + Clone + PartialEq,
{
    id: &'a Option<K>,
    nodes: &'a HashMap<K, Node<K, V>>,
}

pub(crate) struct SkipKeyIterator<'a, K, V>
where
    K: Debug + Clone + PartialEq,
    V: Debug + Clone + PartialEq,
{
    id: &'a Option<K>,
    nodes: &'a HashMap<K, Node<K, V>>,
}

impl<'a, K, V> Iterator for SkipKeyValIterator<'a, K, V>
where
    K: Debug + Clone + Hash + PartialEq + Eq,
    V: Debug + Clone + PartialEq,
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<(&'a K, &'a V)> {
        match &self.id {
            None => None,
            Some(ref key) => {
                if let Some(ref node) = &self.nodes.get(key) {
                    self.id = node.tower.successor();
                    Some((key, &node.value))
                } else {
                    panic!("iter::next hit a dead end")
                }
            }
        }
    }
}

impl<'a, K, V> Iterator for SkipKeyIterator<'a, K, V>
where
    K: Debug + Clone + Hash + PartialEq + Eq,
    V: Debug + Clone + PartialEq,
{
    type Item = &'a K;

    fn next(&mut self) -> Option<&'a K> {
        match &self.id {
            None => None,
            Some(ref key) => {
                if let Some(ref node) = &self.nodes.get(key) {
                    self.id = node.tower.successor();
                    Some(key)
                } else {
                    panic!("iter::next hit a dead end")
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Delta<K>
where
    K: Clone + Debug + Hash + PartialEq + Eq,
{
    index: isize,
    key: Option<K>,
}

// this is an experiment to if I can change request processing
// index lookups by not mutating the skip list
// throuput was quite signifigant actually - about 1.5x over in the
// mass edit perf test
// ideally we can speed up the skip list enough to not need this
// also this could perform worse if the ops per change were huge
// eg.. 10,000 changes with 10 ops each vs 10 changes with 10,000 ops each

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OrdDelta<'a, K, V>
where
    K: Clone + Debug + Hash + PartialEq + Eq,
    V: Clone + Debug + PartialEq,
{
    list: Option<&'a SkipList<K, V>>,
    delta: Vec<Delta<K>>,
}

impl<'a, K, V> OrdDelta<'a, K, V>
where
    K: Clone + Debug + Hash + PartialEq + Eq,
    V: Clone + Debug + PartialEq,
{
    pub fn new(list: Option<&'a SkipList<K, V>>) -> OrdDelta<'a, K, V> {
        OrdDelta {
            list,
            delta: Vec::new(),
        }
    }

    pub fn insert_index(&mut self, index: usize, key: K) {
        let index = index as isize;
        let delta = Delta {
            index,
            key: Some(key),
        };
        for i in 0..self.delta.len() {
            if self.delta[i].index >= index {
                self.delta.iter_mut().skip(i).for_each(|d| d.index += 1);
                self.delta.insert(i, delta);
                return;
            }
        }
        self.delta.push(delta);
    }

    pub fn key_of(&self, index: usize) -> Option<K> {
        let index = index as isize;
        let mut acc: isize = 0;
        for i in 0..self.delta.len() {
            match &self.delta[i] {
                Delta {
                    index: j,
                    key: Some(key),
                } => {
                    if j == &index {
                        return Some(key.clone());
                    }
                    if j > &index {
                        break;
                    }
                    acc += 1;
                }
                Delta {
                    index: j,
                    key: None,
                } => {
                    if j > &index {
                        break;
                    }
                    acc -= 1;
                }
            }
        }
        self.list
            .and_then(|l| l.key_of((index as isize - acc) as usize).cloned())
    }

    pub fn remove_index(&mut self, index: usize) -> Option<K> {
        let index = index as isize;
        let delta = Delta { index, key: None };
        for i in 0..self.delta.len() {
            if self.delta[i].index == index && self.delta[i].key.is_some() {
                let old_insert = self.delta.remove(i);
                self.delta.iter_mut().skip(i).for_each(|d| d.index -= 1);
                return old_insert.key;
            }
            if self.delta[i].index > index {
                let key = self.key_of(index as usize);
                self.delta.iter_mut().skip(i).for_each(|d| d.index -= 1);
                self.delta.insert(i, delta);
                return key;
            }
        }
        let key = self.key_of(index as usize);
        self.delta.push(delta);
        key
    }
}

// get(n)
// insert(n)
// len()
// remove(n)
// get_index_for(T)
// insert_after_(i,K,V)

#[cfg(test)]
mod tests {
    use super::*;
    //use std::str::FromStr;

    #[test]
    fn test_index_of() -> Result<(), AutomergeError> {
        let mut s = SkipList::<&str, u32>::new();

        // should return None on an empty list
        assert_eq!(s.index_of(&"foo"), None);

        // should return None for a nonexistent key
        s.insert_head("foo", 10)?;
        assert_eq!(s.index_of(&"baz"), None);

        // should return 0 for the first list element
        assert_eq!(s.index_of(&"foo"), Some(0));

        // should return length-1 for the last list element
        s.insert_after(&"foo", "bar", 20)?;
        s.insert_after(&"bar", "baz", 30)?;
        assert_eq!(s.index_of(&"baz"), Some(s.len - 1));

        // should adjust based on removed elements
        s.remove_key(&"foo");
        assert_eq!(s.index_of(&"bar"), Some(0));
        assert_eq!(s.index_of(&"baz"), Some(1));
        s.remove_key(&"bar");
        assert_eq!(s.index_of(&"baz"), Some(0));
        Ok(())
    }

    #[test]
    fn test_len() -> Result<(), AutomergeError> {
        let mut s = SkipList::<&str, u32>::new();

        //should be 0 for an empty list
        assert_eq!(s.len, 0);

        // should increase by 1 for every insertion
        s.insert_head("a3", 3)?;
        s.insert_head("a2", 2)?;
        s.insert_head("a1", 1)?;
        assert_eq!(s.len, 3);

        //should decrease by 1 for every removal
        s.remove_key(&"a2");
        assert_eq!(s.len, 2);
        Ok(())
    }

    #[test]
    fn test_key_of() -> Result<(), AutomergeError> {
        let mut s = SkipList::<&str, u32>::new();

        // should return None on an empty list
        assert_eq!(s.key_of(0), None);

        // should return None for an index past the end of the list
        s.insert_head("a3", 3)?;
        s.insert_head("a2", 2)?;
        s.insert_head("a1", 1)?;
        assert_eq!(s.key_of(10), None);

        // should return the first key for index 0
        assert_eq!(s.key_of(0), Some(&"a1"));

        // should return the last key for index -1
        // assert_eq!(s.key_of(-1), Some("a3"));

        // should return the last key for index length-1
        assert_eq!(s.key_of(s.len - 1), Some(&"a3"));

        // should not count removed elements
        s.remove_key(&"a1");
        s.remove_key(&"a3");
        assert_eq!(s.key_of(0), Some(&"a2"));

        Ok(())
    }

    #[test]
    fn test_get() -> Result<(), AutomergeError> {
        let mut s = SkipList::<&str, &str>::new();

        // should return None for a nonexistent key
        assert_eq!(s.get(&"key4"), None);

        // should return the inserted value when present
        s.insert_head("key3", "value3")?;
        s.insert_head("key2", "value2")?;
        s.insert_head("key1", "value1")?;

        assert_eq!(s.get(&"key1"), Some(&"value1"));
        assert_eq!(s.get(&"key3"), Some(&"value3"));
        Ok(())
    }

    #[test]
    fn test_set() -> Result<(), AutomergeError> {
        let mut s = SkipList::<&str, &str>::new();

        // should error when setting a nonexistent key
        assert_eq!(s.set(&"hello", "world").is_err(), true);

        // should update the value for an existing key
        s.insert_head("key2", "value2")?;
        s.insert_head("key1", "value1")?;

        assert_eq!(s.get(&"key1"), Some(&"value1"));
        assert_eq!(s.get(&"key2"), Some(&"value2"));

        s.set(&"key2", "updated_value")?;

        assert_eq!(s.get(&"key1"), Some(&"value1"));
        assert_eq!(s.get(&"key2"), Some(&"updated_value"));

        Ok(())
    }

    #[test]
    fn test_insert_index() -> Result<(), AutomergeError> {
        let mut s = SkipList::<&str, &str>::new();

        // should insert the new key-value pair at the given index
        s.insert_head("aaa", "AAA")?;
        s.insert_after(&"aaa", "ccc", "CCC")?;
        s.insert_index(1, "bbb", "BBB");
        assert_eq!(s.index_of(&"aaa"), Some(0));
        assert_eq!(s.index_of(&"bbb"), Some(1));
        assert_eq!(s.index_of(&"ccc"), Some(2));

        // should insert at the head if the index is zero
        s.insert_index(0, "a", "aa");
        assert_eq!(s.key_of(0), Some(&"a"));
        Ok(())
    }

    #[test]
    fn test_remove_index() -> Result<(), AutomergeError> {
        let mut s = SkipList::<&str, &str>::new();

        // should remove the value at the given index
        s.insert_head("ccc", "CCC")?;
        s.insert_head("bbb", "BBB")?;
        s.insert_head("aaa", "AAA")?;
        s.remove_index(1);
        assert_eq!(s.index_of(&"aaa"), Some(0));
        assert_eq!(s.index_of(&"bbb"), None);
        assert_eq!(s.index_of(&"ccc"), Some(1));

        // should raise an error if the given index is out of bounds
        assert_eq!(s.remove_index(100), None);
        Ok(())
    }

    #[test]
    fn test_remove_key_big() -> Result<(), AutomergeError> {
        let mut s = SkipList::<String, u32>::new();
        for i in 0..10000 {
            let j = 9999 - i;
            s.insert_head(format!("a{}", j), j)?;
        }

        assert_eq!(s.index_of(&"a20".to_string()), Some(20));
        assert_eq!(s.index_of(&"a500".to_string()), Some(500));
        assert_eq!(s.index_of(&"a1000".to_string()), Some(1000));

        for i in 0..5000 {
            let j = (4999 - i) * 2 + 1;
            s.remove_index(j);
        }

        assert_eq!(s.index_of(&"a4000".to_string()), Some(2000));
        assert_eq!(s.index_of(&"a1000".to_string()), Some(500));
        assert_eq!(s.index_of(&"a500".to_string()), Some(250));
        assert_eq!(s.index_of(&"a20".to_string()), Some(10));

        Ok(())
    }

    #[test]
    fn test_remove_key() -> Result<(), AutomergeError> {
        let mut s = SkipList::<&str, u32>::new();
        s.insert_head("a20", 20)?;
        s.insert_head("a19", 19)?;
        s.insert_head("a18", 18)?;
        s.insert_head("a17", 17)?;
        s.insert_head("a16", 16)?;
        s.insert_head("a15", 15)?;
        s.insert_head("a14", 14)?;
        s.insert_head("a13", 13)?;
        s.insert_head("a12", 12)?;
        s.insert_head("a11", 11)?;
        s.insert_head("a10", 10)?;
        s.insert_head("a9", 9)?;
        s.insert_head("a8", 8)?;
        s.insert_head("a7", 7)?;
        s.insert_head("a6", 6)?;
        s.insert_head("a5", 5)?;
        s.insert_head("a4", 4)?;
        s.insert_head("a3", 3)?;
        s.insert_head("a2", 2)?;
        s.insert_head("a1", 1)?;
        s.insert_head("a0", 0)?;

        assert_eq!(s.index_of(&"a20"), Some(20));

        s.remove_key(&"a1");
        s.remove_key(&"a3");
        s.remove_key(&"a5");
        s.remove_key(&"a7");
        s.remove_key(&"a9");
        s.remove_key(&"a11");
        s.remove_key(&"a13");
        s.remove_key(&"a15");
        s.remove_key(&"a17");
        s.remove_key(&"a19");

        assert_eq!(s.index_of(&"a20"), Some(10));
        assert_eq!(s.index_of(&"a10"), Some(5));
        Ok(())
    }

    #[test]
    fn test_iter1() -> Result<(), AutomergeError> {
        let mut s = SkipList::<String, u32>::new();
        assert_eq!(s.len, 0);
        let e1 = "10@actor1".to_string();
        let e2 = "11@actor1".to_string();
        let e3 = "12@actor2".to_string();
        s.insert_head(e1.clone(), 10)?;
        assert_eq!(s.to_vec(), vec![e1.clone()]);
        s.insert_after(&e1, e2.clone(), 20).unwrap();
        assert_eq!(s.to_vec(), vec![e1.clone(), e2.clone()]);
        s.insert_after(&e1, e3.clone(), 15).unwrap();
        assert_eq!(s.to_vec(), vec![e1.clone(), e3.clone(), e2.clone()]);
        Ok(())
    }
}
