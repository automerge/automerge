#![allow(dead_code)]

use fxhash::FxBuildHasher;
//use im::HashMap;
//use std::sync::Arc;
use std::collections::HashMap;
//use rand::rngs::ThreadRng;
use rand::prelude::*;
//use rand::Rng;
use std::cmp::{max, min};
use std::fmt::Debug;
use std::hash::Hash;
use std::iter::Iterator;
use std::mem;
use std::ops::AddAssign;

#[derive(Debug, Copy, Clone, PartialEq)]
struct Link<K>
where
    K: Clone + Copy + Debug + PartialEq,
{
    key: Option<K>,
    count: usize,
}

#[derive(Debug, Clone, PartialEq)]
struct LinkLevel<K>
where
    K: Copy + Clone + Debug + PartialEq,
{
    next: Link<K>,
    prev: Link<K>,
}

#[derive(Debug, Clone, PartialEq)]
struct Node<K>
where
    K: Copy + Clone + Debug + PartialEq,
{
    level: usize,
    links: Vec<LinkLevel<K>>,
    // IDEA: can I make this an unsized array??
    // IDEA - Node could be Node(Vec<K>)
}

impl<K> AddAssign for Link<K>
where
    K: Copy + Clone + Debug + PartialEq,
{
    fn add_assign(&mut self, other: Self) {
        self.key = other.key;
        self.count += other.count;
    }
}

impl<K> Node<K>
where
    K: Debug + Copy + Clone + PartialEq,
{
    fn successor(&self) -> Option<&K> {
        if self.links.is_empty() {
            None
        } else {
            self.links[0].next.key.as_ref()
        }
    }

    fn remove_node_after(&mut self, from_level: usize, removed_level: usize, links: &[Link<K>]) {
        for (level, link) in links.iter().enumerate().take(self.level).skip(from_level) {
            if level < removed_level {
                self.links[level].next = *link;
            } else {
                self.links[level].next.count -= 1;
            }
        }
    }

    fn remove_node_before(&mut self, from_level: usize, removed_level: usize, links: &[Link<K>]) {
        for (level, link) in links.iter().enumerate().take(self.level).skip(from_level) {
            if level < removed_level {
                self.links[level].prev = *link;
            } else {
                self.links[level].prev.count -= 1;
            }
        }
    }

    fn insert_node_after(
        &mut self,
        new_key: &K,
        new_level: usize,
        from_level: usize,
        distance: usize,
        is_head: bool,
    ) {
        if new_level > self.level && !is_head {
            panic!("Cannot increase the level of a non-head node")
        }
        self.level = max(self.level, new_level);

        for level in from_level..self.level {
            if level < new_level {
                let next = Link {
                    key: Some(*new_key),
                    count: distance,
                };
                let prev = Link {
                    key: None,
                    count: 0,
                };
                if self.links.len() == level {
                    self.links.push(LinkLevel { next, prev });
                } else {
                    self.links[level].next = next;
                }
            } else {
                self.links[level].next.count += 1;
            }
        }
    }

    fn insert_node_before(
        &mut self,
        new_key: &K,
        new_level: usize,
        from_level: usize,
        distance: usize,
    ) {
        if new_level > self.level {
            panic!("Cannot increase the level on insert_node_before")
        }
        for level in from_level..self.level {
            if level < new_level {
                self.links[level].prev = Link {
                    key: Some(*new_key),
                    count: distance,
                };
            } else {
                self.links[level].prev.count += 1;
            }
        }
    }
}

pub(crate) trait OrderedSet<K>
where
    K: Clone + Debug + Hash + PartialEq + Eq,
{
    fn index_of(&self, key: &K) -> Option<usize>;
    fn remove_key(&mut self, key: &K) -> Option<usize>;
    fn insert_index(&mut self, index: usize, key: K) -> bool;
    fn remove_index(&mut self, index: usize) -> Option<K>;
    fn key_of(&self, index: usize) -> Option<&K>;
}

impl<K> OrderedSet<K> for SkipList<K>
where
    K: Copy + Clone + Debug + Hash + PartialEq + Eq,
{
    fn remove_index(&mut self, index: usize) -> Option<K> {
        let key = self.key_of(index).cloned();
        if let Some(ref k) = &key {
            self.remove(k);
        }
        key
    }

    fn remove_key(&mut self, key: &K) -> Option<usize> {
        let index = self.index_of(key);
        if index.is_some() {
            self.remove(key);
        }
        index
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
            while count + node.links[level].next.count > target {
                level -= 1
            }
            count += node.links[level].next.count;
            let k = node.links[level].next.key.as_ref();
            if count == target {
                return k;
            }
            node = self.get_node(k)
        }
    }

    fn index_of(&self, key: &K) -> Option<usize> {
        let mut count = 0;
        let mut key = key;
        loop {
            if let Some(node) = self.nodes.get(key) {
                let link = &node.links[node.level - 1].prev;
                count += link.count;
                if let Some(ref k) = &link.key {
                    key = k;
                } else {
                    break;
                }
            } else {
                return None;
            }
        }
        Some(count - 1)
    }

    fn insert_index(&mut self, index: usize, key: K) -> bool {
        if index == 0 {
            self.insert_head(key)
        } else {
            self.key_of(index - 1)
                .cloned()
                .map(|suc| self.insert_after(&suc, key))
                .unwrap_or(false)
        }
    }
}

impl<K> Default for SkipList<K>
where
    K: Copy + Clone + Debug + Hash + PartialEq + Eq,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, K> IntoIterator for &'a SkipList<K>
where
    K: Copy + Clone + Debug + Hash + PartialEq + Eq,
{
    type Item = &'a K;
    type IntoIter = SkipIterator<'a, K>;

    fn into_iter(self) -> Self::IntoIter {
        SkipIterator {
            id: self.head.successor(),
            nodes: &self.nodes,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SkipList<K>
where
    K: Copy + Clone + Debug + Hash + PartialEq + Eq,
{
    nodes: HashMap<K, Node<K>, FxBuildHasher>,
    head: Node<K>,
    rng: ThreadRng,
    pub(crate) len: usize,
}

impl<K> PartialEq for SkipList<K>
where
    K: Copy + Clone + Debug + Hash + PartialEq + Eq,
{
    fn eq(&self, other: &Self) -> bool {
        self.nodes.eq(&other.nodes)
    }
}

impl<K> SkipList<K>
where
    K: Copy + Clone + Debug + Hash + PartialEq + Eq,
{
    pub(crate) fn new() -> SkipList<K> {
        let nodes = HashMap::default();
        let head = Node {
            links: Vec::new(),
            level: 1,
        };
        let len = 0;
        let rng = rand::rng();
        SkipList {
            nodes,
            head,
            len,
            rng,
        }
    }

    fn remove(&mut self, key: &K) {
        let removed = self
            .nodes
            .remove(key)
            .unwrap_or_else(|| panic!("The given key cannot be removed because it does not exist"));

        let max_level = self.head.level;
        let mut pre = self.predecessors(removed.links[0].prev.key.as_ref(), max_level);
        let mut suc = self.successors(removed.links[0].next.key.as_ref(), max_level);

        for i in 0..max_level {
            let distance = pre[i].count + suc[i].count - 1;
            pre[i].count = distance;
            suc[i].count = distance;
        }

        self.len -= 1;
        let mut pre_level = 0;
        let mut suc_level = 0;

        for level in 1..(max_level + 1) {
            let update_level = min(level, removed.level);
            if level == max_level
                || pre.get(level).map(|l| &l.key) != pre.get(pre_level).map(|l| &l.key)
            {
                self.get_node_mut(pre[pre_level].key.as_ref())
                    .remove_node_after(pre_level, update_level, &suc);
                pre_level = level;
            }
            if suc[suc_level].key.is_some()
                && (level == max_level
                    || suc.get(level).map(|l| &l.key) != suc.get(suc_level).map(|l| &l.key))
            {
                self.get_node_mut(suc[suc_level].key.as_ref())
                    .remove_node_before(suc_level, update_level, &pre);
                suc_level = level;
            }
        }
    }

    fn get_node(&self, key: Option<&K>) -> &Node<K> {
        if let Some(ref k) = key {
            self.nodes
                .get(k)
                .unwrap_or_else(|| panic!("get_node - missing key {:?}", key))
        } else {
            &self.head
        }
    }

    fn get_node_mut(&mut self, key: Option<&K>) -> &mut Node<K> {
        if let Some(ref k) = key {
            self.nodes
                .get_mut(k)
                .unwrap_or_else(|| panic!("get_node - missing key {:?}", key))
        } else {
            &mut self.head
        }
    }

    // IDEA: Can i merge the successors and predecessors into a singlue unified function
    // so we dont need to zip the results?
    fn predecessors(&self, predecessor: Option<&K>, max_level: usize) -> Vec<Link<K>> {
        let mut pre = Vec::with_capacity(max_level);
        pre.push(Link {
            key: predecessor.cloned(),
            count: 1,
        });

        for level in 1..max_level {
            let mut link = pre[level - 1];
            while link.key.is_some() {
                let node = self.get_node(link.key.as_ref());
                if node.level > level {
                    break;
                }
                if node.level < level {
                    panic!("Level lower than expected");
                }
                link += node.links[level - 1].prev;
            }
            pre.push(link);
        }
        pre
    }

    fn successors(&self, successor: Option<&K>, max_level: usize) -> Vec<Link<K>> {
        let mut suc = Vec::with_capacity(max_level);
        suc.push(Link {
            key: successor.cloned(),
            count: 1,
        });

        for level in 1..max_level {
            let mut link = suc[level - 1];
            while link.key.is_some() {
                let node = self.get_node(link.key.as_ref());
                if node.level > level {
                    break;
                }
                if node.level < level {
                    panic!("Level lower than expected");
                }
                link += node.links[level - 1].next;
            }
            suc.push(link);
        }
        suc
    }

    pub(crate) fn splice<I>(&mut self, pos: usize, keys: I) -> bool where I: IntoIterator<Item = K> {
      let mut prev = if pos == 0 {
        None
      } else {
        self.key_of(pos - 1).cloned()
      };
      for k in keys.into_iter() {
        self.insert(prev.as_ref(), k.clone());
        prev = Some(k);
      }
      true
    }

    pub(crate) fn insert_head(&mut self, key: K) -> bool {
        self.insert(None, key)
    }

    pub(crate) fn push(&mut self, key: K) -> bool {
        if self.len == 0 {
          self.insert(None, key)
        } else {
            self.key_of(self.len - 1)
                .cloned()
                .map(|suc| self.insert_after(&suc, key))
                .unwrap_or(false)
        }
    }

    pub(crate) fn insert_after(&mut self, predecessor: &K, key: K) -> bool {
        self.insert(Some(predecessor), key)
    }

    fn insert(&mut self, predecessor: Option<&K>, key: K) -> bool {
        if self.nodes.contains_key(&key) {
            return false;
        }

        let new_level = self.random_level();
        let max_level = max(new_level, self.head.level);
        let successor = self.get_node(predecessor).successor();
        let mut pre = self.predecessors(predecessor, max_level);
        let mut suc = self.successors(successor, max_level);

        self.len += 1;

        let mut pre_level = 0;
        let mut suc_level = 0;
        for level in 1..(max_level + 1) {
            let update_level = min(level, new_level);
            if level == max_level
                || pre.get(level).map(|l| &l.key) != pre.get(pre_level).map(|l| &l.key)
            {
                self.get_node_mut(pre[pre_level].key.as_ref())
                    .insert_node_after(
                        &key,
                        update_level,
                        pre_level,
                        pre[pre_level].count,
                        pre[pre_level].key.is_none(),
                    );
                pre_level = level;
            }
            if suc[suc_level].key.is_some()
                && (level == max_level
                    || suc.get(level).map(|l| &l.key) != suc.get(suc_level).map(|l| &l.key))
            {
                self.get_node_mut(suc[suc_level].key.as_ref())
                    .insert_node_before(&key, update_level, suc_level, suc[suc_level].count);
                suc_level = level;
            }
        }

        pre.truncate(new_level);
        suc.truncate(new_level);
        let links = pre
            .into_iter()
            .zip(suc.into_iter())
            .map(|(prev, next)| LinkLevel { prev, next })
            .collect();
        self.nodes.insert(
            key,
            Node {
                level: new_level,
                links,
            },
        );
        true
    }

    // Returns a random number from the geometric distribution with p = 0.75.
    // That is, returns k with probability p * (1 - p)^(k - 1).
    // For example, returns 1 with probability 3/4, returns 2 with probability 3/16,
    // returns 3 with probability 3/64, and so on.

    fn random_level(&mut self) -> usize {
        // Create random number between 0 and 2^32 - 1
        // Count leading zeros in that 32-bit number
        let rand: u32 = self.rng.gen();
        //let rand: u32 = rand::thread_rng().gen();

        let mut level = 1;
        while rand < 1 << (32 - 2 * level) && level < 16 {
            level += 1
        }
        level
    }
}

pub(crate) struct SkipIterator<'a, K>
where
    K: Debug + Copy + Clone + PartialEq,
{
    id: Option<&'a K>,
    nodes: &'a HashMap<K, Node<K>, FxBuildHasher>,
}

impl<'a, K> Iterator for SkipIterator<'a, K>
where
    K: Debug + Copy + Clone + Hash + PartialEq + Eq,
{
    type Item = &'a K;

    fn next(&mut self) -> Option<&'a K> {
        let mut successor = match self.id {
            None => None,
            Some(ref key) => self.nodes.get(key).and_then(|n| n.successor()),
        };
        mem::swap(&mut successor, &mut self.id);
        successor
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_of() {
        let mut s = SkipList::<&str>::new();

        // should return None on an empty list
        assert_eq!(s.index_of(&"foo"), None);

        // should return None for a nonexistent key
        s.insert_head("foo");
        assert_eq!(s.index_of(&"baz"), None);

        // should return 0 for the first list element
        assert_eq!(s.index_of(&"foo"), Some(0));

        // should return length-1 for the last list element
        s.insert_after(&"foo", "bar");
        s.insert_after(&"bar", "baz");
        assert_eq!(s.index_of(&"baz"), Some(s.len - 1));

        // should adjust based on removed elements
        s.remove_key(&"foo");
        assert_eq!(s.index_of(&"bar"), Some(0));
        assert_eq!(s.index_of(&"baz"), Some(1));
        s.remove_key(&"bar");
        assert_eq!(s.index_of(&"baz"), Some(0));
    }

    #[test]
    fn test_len() {
        let mut s = SkipList::<&str>::new();

        //should be 0 for an empty list
        assert_eq!(s.len, 0);

        // should increase by 1 for every insertion
        s.insert_head("a3");
        s.insert_head("a2");
        s.insert_head("a1");
        assert_eq!(s.len, 3);

        //should decrease by 1 for every removal
        s.remove_key(&"a2");
        assert_eq!(s.len, 2);
    }

    #[test]
    fn test_key_of() {
        let mut s = SkipList::<&str>::new();

        // should return None on an empty list
        assert_eq!(s.key_of(0), None);

        // should return None for an index past the end of the list
        s.insert_head("a3");
        s.insert_head("a2");
        s.insert_head("a1");
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
    }

    #[test]
    fn test_insert_index() {
        let mut s = SkipList::<&str>::new();

        // should insert the new key-value pair at the given index
        s.insert_head("aaa");
        s.insert_after(&"aaa", "ccc");
        s.insert_index(1, "bbb");
        assert_eq!(s.index_of(&"aaa"), Some(0));
        assert_eq!(s.index_of(&"bbb"), Some(1));
        assert_eq!(s.index_of(&"ccc"), Some(2));

        // should insert at the head if the index is zero
        s.insert_index(0, "a");
        assert_eq!(s.key_of(0), Some(&"a"));
    }

    #[test]
    fn test_remove_index() {
        let mut s = SkipList::<&str>::new();

        // should remove the value at the given index
        s.insert_head("ccc");
        s.insert_head("bbb");
        s.insert_head("aaa");
        s.remove_index(1);
        assert_eq!(s.index_of(&"aaa"), Some(0));
        assert_eq!(s.index_of(&"bbb"), None);
        assert_eq!(s.index_of(&"ccc"), Some(1));

        // should raise an error if the given index is out of bounds
        assert_eq!(s.remove_index(100), None);
    }

    #[test]
    fn test_remove_key_big() {
        let mut data = vec![];
        for i in 0..10000 {
            let j = 9999 - i;
            data.push(format!("a{}", j));
        }
        {
            let mut s = SkipList::<&str>::new();
            for i in 0..10000 {
                s.insert_head(&data[i]);
            }

            assert_eq!(s.index_of(&"a20"), Some(20));
            assert_eq!(s.index_of(&"a500"), Some(500));
            assert_eq!(s.index_of(&"a1000"), Some(1000));

            for i in 0..5000 {
                let j = (4999 - i) * 2 + 1;
                s.remove_index(j);
            }

            assert_eq!(s.index_of(&"a4000"), Some(2000));
            assert_eq!(s.index_of(&"a1000"), Some(500));
            assert_eq!(s.index_of(&"a500"), Some(250));
            assert_eq!(s.index_of(&"a20"), Some(10));
        }
    }

    #[test]
    fn test_remove_key() {
        let mut s = SkipList::<&str>::new();
        s.insert_head("a20");
        s.insert_head("a19");
        s.insert_head("a18");
        s.insert_head("a17");
        s.insert_head("a16");
        s.insert_head("a15");
        s.insert_head("a14");
        s.insert_head("a13");
        s.insert_head("a12");
        s.insert_head("a11");
        s.insert_head("a10");
        s.insert_head("a9");
        s.insert_head("a8");
        s.insert_head("a7");
        s.insert_head("a6");
        s.insert_head("a5");
        s.insert_head("a4");
        s.insert_head("a3");
        s.insert_head("a2");
        s.insert_head("a1");
        s.insert_head("a0");

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
    }
}
