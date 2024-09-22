use std::cmp::{min, Ordering};
use std::fmt::Debug;
use std::mem;
use std::ops::RangeBounds;

use super::normalize_range;

pub trait HasWidth: Debug + Clone {
    fn width(&self) -> usize;
}

pub(crate) const B: usize = 16;

#[derive(Clone, Debug)]
pub struct SpanTree<T: HasWidth> {
    root_node: Option<TreeNode<T>>,
}

#[derive(Clone, Debug, PartialEq)]
struct TreeNode<T: HasWidth> {
    elements: Vec<T>,
    children: Vec<TreeNode<T>>,
    length: usize,
    width: usize,
}

impl<T: HasWidth> SpanTree<T> {
    /// Construct a new, empty, sequence.
    pub fn new() -> Self {
        Self { root_node: None }
    }

    pub fn new2(element: T) -> Self {
        let mut t = Self::new();
        t.push(element);
        t
    }

    pub fn width(&self) -> usize {
        self.root_node.as_ref().map_or(0, |n| n.width())
    }

    /// Get the length of the sequence.
    pub fn len(&self) -> usize {
        self.root_node.as_ref().map_or(0, |n| n.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn to_vec(&self) -> Vec<T> {
        self.iter().cloned().collect()
    }

    /// Create an iterator through the sequence.
    pub fn iter(&self) -> SpanTreeIter<'_, T> {
        SpanTreeIter {
            inner: Some(self),
            index: 0,
        }
    }

    pub fn splice<R, I>(&mut self, range: R, values: I)
    where
        R: RangeBounds<usize>,
        I: IntoIterator<Item = T>,
    {
        let mut values = values.into_iter();
        let (start, end) = normalize_range(range);
        let end = min(end, self.len());
        let mut index = start;
        let mut to_delete = end - start;
        // when possible replace a value, dont delete then insert
        // to prevent unnessarry tree shuffling
        while to_delete > 0 {
            if let Some(val) = values.next() {
                self.replace(index, val);
                index += 1;
            } else {
                self.remove(index);
            }
            to_delete -= 1;
        }
        for val in values {
            self.insert(index, val);
            index += 1;
        }
    }

    /// Insert the `element` into the sequence at `index`.
    ///
    /// # Panics
    ///
    /// Panics if `index > len`.
    pub fn insert(&mut self, index: usize, element: T) {
        let old_len = self.len();
        if let Some(root) = self.root_node.as_mut() {
            #[cfg(debug_assertions)]
            root.check();

            if root.is_full() {
                let original_len = root.len();
                let new_root = TreeNode::new();

                // move new_root to root position
                let old_root = mem::replace(root, new_root);

                root.length += old_root.len();
                root.width += old_root.width();
                root.children.push(old_root);
                root.split_child(0);

                assert_eq!(original_len, root.len());

                // after splitting the root has one element and two children, find which child the
                // index is in
                let first_child_len = root.children[0].len();
                let (child, insertion_index) = if first_child_len < index {
                    (&mut root.children[1], index - (first_child_len + 1))
                } else {
                    (&mut root.children[0], index)
                };
                root.length += 1;
                root.width += element.width();
                child.insert_into_non_full_node(insertion_index, element)
            } else {
                root.insert_into_non_full_node(index, element)
            }
        } else {
            let width = element.width();
            self.root_node = Some(TreeNode {
                elements: vec![element],
                children: Vec::new(),
                length: 1,
                width,
            })
        }
        assert_eq!(self.len(), old_len + 1, "{:#?}", self);
    }

    /// Push the `element` onto the back of the sequence.
    pub fn push(&mut self, element: T) {
        let l = self.len();
        self.insert(l, element)
    }

    /// Get the `element` at `index` in the sequence.
    pub fn get(&self, index: usize) -> Option<&T> {
        self.root_node.as_ref().and_then(|n| n.get(index))
    }

    /// Get the `element` at `index` in the sequence.
    pub fn get_at_width(&self, pos: usize) -> Option<(usize, usize, &T)> {
        self.root_node
            .as_ref()
            .and_then(|n| n.get_at_width(0, 0, pos))
    }

    /// Removes the element at `index` from the sequence.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    pub fn remove(&mut self, index: usize) -> T {
        if let Some(root) = self.root_node.as_mut() {
            #[cfg(debug_assertions)]
            let len = root.check();
            let old = root.remove(index);

            if root.elements.is_empty() {
                if root.is_leaf() {
                    self.root_node = None;
                } else {
                    self.root_node = Some(root.children.remove(0));
                }
            }

            #[cfg(debug_assertions)]
            debug_assert_eq!(len, self.root_node.as_ref().map_or(0, |r| r.check()) + 1);
            old
        } else {
            panic!("remove from empty tree")
        }
    }

    pub fn replace(&mut self, index: usize, element: T) -> T {
        if let Some(root) = self.root_node.as_mut() {
            #[cfg(debug_assertions)]
            let len = root.check();
            let old = root.replace(index, element);

            #[cfg(debug_assertions)]
            debug_assert_eq!(len, self.root_node.as_ref().map_or(0, |r| r.check()));
            old
        } else {
            panic!("remove from empty tree")
        }
    }
}

impl<T: HasWidth> TreeNode<T> {
    fn new() -> Self {
        Self {
            elements: Vec::new(),
            children: Vec::new(),
            length: 0,
            width: 0,
        }
    }

    fn width(&self) -> usize {
        self.width
    }

    fn len(&self) -> usize {
        self.length
    }

    fn recompute_len(&mut self) {
        self.length = self.elements.len() + self.children.iter().map(|c| c.len()).sum::<usize>();
        self.width = self.elements.iter().map(|c| c.width()).sum::<usize>()
            + self.children.iter().map(|c| c.width()).sum::<usize>();
    }

    fn is_leaf(&self) -> bool {
        self.children.is_empty()
    }

    fn is_full(&self) -> bool {
        self.elements.len() >= 2 * B - 1
    }

    /// Returns the child index and the given index adjusted for the cumulative index before that
    /// child.
    fn find_child_index(&self, index: usize) -> (usize, usize) {
        let mut cumulative_len = 0;
        for (child_index, child) in self.children.iter().enumerate() {
            if cumulative_len + child.len() >= index {
                return (child_index, index - cumulative_len);
            } else {
                cumulative_len += child.len() + 1;
            }
        }
        panic!("index not found in node")
    }

    fn insert_into_non_full_node(&mut self, index: usize, element: T) {
        assert!(!self.is_full());
        if self.is_leaf() {
            self.length += 1;
            self.width += element.width();
            self.elements.insert(index, element);
        } else {
            let (child_index, sub_index) = self.find_child_index(index);
            let child = &mut self.children[child_index];

            self.length += 1;
            self.width += element.width();
            if child.is_full() {
                self.split_child(child_index);

                // child structure has changed so we need to find the index again
                let (child_index, sub_index) = self.find_child_index(index);
                let child = &mut self.children[child_index];
                child.insert_into_non_full_node(sub_index, element);
            } else {
                child.insert_into_non_full_node(sub_index, element);
            }
        }
    }

    // A utility function to split the child `full_child_index` of this node
    // Note that `full_child_index` must be full when this function is called.
    fn split_child(&mut self, full_child_index: usize) {
        let original_len_self = self.len();

        // Create a new node which is going to store (B-1) keys
        // of the full child.
        let mut successor_sibling = TreeNode::new();

        let full_child = &mut self.children[full_child_index];
        let original_len = full_child.len();
        assert!(full_child.is_full());

        successor_sibling.elements = full_child.elements.split_off(B);

        if !full_child.is_leaf() {
            successor_sibling.children = full_child.children.split_off(B);
        }

        let middle = full_child.elements.pop().unwrap();

        full_child.recompute_len();

        /*
                successor_sibling.length = successor_sibling.elements.len()
                    + successor_sibling
                        .children
                        .iter()
                        .map(|c| c.len())
                        .sum::<usize>();
        */

        successor_sibling.recompute_len();

        let z_len = successor_sibling.len();

        let full_child_len = full_child.len();

        self.children
            .insert(full_child_index + 1, successor_sibling);

        self.elements.insert(full_child_index, middle);

        assert_eq!(full_child_len + z_len + 1, original_len, "{:#?}", self);

        assert_eq!(original_len_self, self.len());
    }

    fn remove_from_leaf(&mut self, index: usize) -> T {
        let s = self.elements.remove(index);
        self.length -= 1;
        self.width -= s.width();
        s
    }

    fn remove_element_from_non_leaf(&mut self, index: usize, element_index: usize) -> T {
        let result = if self.children[element_index].elements.len() >= B {
            let total_index = self.cumulative_index(element_index);
            // recursively delete index - 1 in predecessor_node
            let predecessor = self.children[element_index].remove(index - 1 - total_index);
            // replace element with that one
            mem::replace(&mut self.elements[element_index], predecessor)
        } else if self.children[element_index + 1].elements.len() >= B {
            // recursively delete index + 1 in successor_node
            let total_index = self.cumulative_index(element_index + 1);
            let successor = self.children[element_index + 1].remove(index + 1 - total_index);
            // replace element with that one
            mem::replace(&mut self.elements[element_index], successor)
        } else {
            let middle_element = self.elements.remove(element_index);
            let successor_child = self.children.remove(element_index + 1);
            self.children[element_index].merge(middle_element, successor_child);

            let total_index = self.cumulative_index(element_index);
            self.children[element_index].remove(index - total_index)
        };
        self.length -= 1;
        self.width -= result.width();
        result
    }

    fn cumulative_index(&self, child_index: usize) -> usize {
        self.children[0..child_index]
            .iter()
            .map(|c| c.len() + 1)
            .sum()
    }

    fn remove_from_internal_child(&mut self, index: usize, mut child_index: usize) -> T {
        if self.children[child_index].elements.len() < B
            && if child_index > 0 {
                self.children[child_index - 1].elements.len() < B
            } else {
                true
            }
            && if child_index + 1 < self.children.len() {
                self.children[child_index + 1].elements.len() < B
            } else {
                true
            }
        {
            // if the child and its immediate siblings have B-1 elements merge the child
            // with one sibling, moving an element from this node into the new merged node
            // to be the median

            if child_index > 0 {
                let middle = self.elements.remove(child_index - 1);

                // use the predessor sibling
                let successor = self.children.remove(child_index);
                child_index -= 1;

                self.children[child_index].merge(middle, successor);
            } else {
                let middle = self.elements.remove(child_index);

                // use the sucessor sibling
                let successor = self.children.remove(child_index + 1);

                self.children[child_index].merge(middle, successor);
            }
        } else if self.children[child_index].elements.len() < B {
            if child_index > 0
                && self
                    .children
                    .get(child_index - 1)
                    .map_or(false, |c| c.elements.len() >= B)
            {
                let last_element = self.children[child_index - 1].elements.pop().unwrap();
                assert!(!self.children[child_index - 1].elements.is_empty());
                self.children[child_index - 1].length -= 1;

                let parent_element =
                    mem::replace(&mut self.elements[child_index - 1], last_element);

                self.children[child_index]
                    .elements
                    .insert(0, parent_element);
                self.children[child_index].length += 1;

                if let Some(last_child) = self.children[child_index - 1].children.pop() {
                    self.children[child_index - 1].length -= last_child.len();
                    self.children[child_index].length += last_child.len();
                    self.children[child_index].children.insert(0, last_child);
                }
            } else if self
                .children
                .get(child_index + 1)
                .map_or(false, |c| c.elements.len() >= B)
            {
                let first_element = self.children[child_index + 1].elements.remove(0);
                self.children[child_index + 1].length -= 1;

                assert!(!self.children[child_index + 1].elements.is_empty());

                let parent_element = mem::replace(&mut self.elements[child_index], first_element);

                self.children[child_index].length += 1;
                self.children[child_index].elements.push(parent_element);

                if !self.children[child_index + 1].is_leaf() {
                    let first_child = self.children[child_index + 1].children.remove(0);
                    self.children[child_index + 1].length -= first_child.len();
                    self.children[child_index].length += first_child.len();

                    self.children[child_index].children.push(first_child);
                }
            }
        }
        let total_index = self.cumulative_index(child_index);
        let v = self.children[child_index].remove(index - total_index);
        self.length -= 1;
        self.width -= v.width();
        v
    }

    fn check(&self) -> usize {
        let l = self.elements.len() + self.children.iter().map(|c| c.check()).sum::<usize>();
        assert_eq!(self.len(), l, "{:#?}", self);

        l
    }

    fn remove(&mut self, index: usize) -> T {
        let original_len = self.len();
        if self.is_leaf() {
            let v = self.remove_from_leaf(index);
            assert_eq!(original_len, self.len() + 1);
            debug_assert_eq!(self.check(), self.len());
            v
        } else {
            let mut total_index = 0;
            for (child_index, child) in self.children.iter().enumerate() {
                match (total_index + child.len()).cmp(&index) {
                    Ordering::Less => {
                        // should be later on in the loop
                        total_index += child.len() + 1;
                        continue;
                    }
                    Ordering::Equal => {
                        let v = self.remove_element_from_non_leaf(
                            index,
                            min(child_index, self.elements.len() - 1),
                        );
                        assert_eq!(original_len, self.len() + 1);
                        debug_assert_eq!(self.check(), self.len());
                        return v;
                    }
                    Ordering::Greater => {
                        let v = self.remove_from_internal_child(index, child_index);
                        assert_eq!(original_len, self.len() + 1);
                        debug_assert_eq!(self.check(), self.len());
                        return v;
                    }
                }
            }
            panic!(
                "index not found to remove {} {} {} {}",
                index,
                total_index,
                self.len(),
                self.check()
            );
        }
    }

    fn replace(&mut self, index: usize, element: T) -> T {
        let original_len = self.len();
        if self.is_leaf() {
            self.width += element.width();
            let v = mem::replace(&mut self.elements[index], element);
            self.width -= v.width();
            debug_assert_eq!(self.check(), self.len());
            v
        } else {
            let mut total_index = 0;
            for (child_index, child) in self.children.iter().enumerate() {
                match (total_index + child.len()).cmp(&index) {
                    Ordering::Less => {
                        // should be later on in the loop
                        total_index += child.len() + 1;
                        continue;
                    }
                    Ordering::Equal => {
                        let child_index = min(child_index, self.elements.len() - 1);
                        self.width += element.width();
                        let v = mem::replace(&mut self.elements[child_index], element);
                        self.width -= v.width();
                        debug_assert_eq!(self.check(), self.len());
                        return v;
                    }
                    Ordering::Greater => {
                        self.width += element.width();
                        let v = self.children[child_index].replace(index - total_index, element);
                        self.width -= v.width();
                        assert_eq!(original_len, self.len());
                        debug_assert_eq!(self.check(), self.len());
                        return v;
                    }
                }
            }
            panic!(
                "index not found to remove {} {} {} {}",
                index,
                total_index,
                self.len(),
                self.check()
            );
        }
    }

    fn merge(&mut self, middle: T, successor_sibling: TreeNode<T>) {
        self.length += successor_sibling.length + 1;
        self.width += successor_sibling.width + middle.width();
        self.elements.push(middle);
        self.elements.extend(successor_sibling.elements);
        self.children.extend(successor_sibling.children);
        assert!(self.is_full());
    }

    fn get_at_width(
        &self,
        mut index: usize,
        mut sub_index: usize,
        pos: usize,
    ) -> Option<(usize, usize, &T)> {
        if self.is_leaf() {
            let iter = self.elements.iter().peekable();
            for e in iter {
                if pos - sub_index < e.width() {
                    return Some((index, pos - sub_index, e));
                }
                index += 1;
                sub_index += e.width()
            }
        } else {
            for i in 0..self.children.len() {
                if let Some(child) = self.children.get(i) {
                    if pos - sub_index < child.width() {
                        return child.get_at_width(index, sub_index, pos);
                    }
                    index += child.len();
                    sub_index += child.width();
                }
                if let Some(e) = self.elements.get(i) {
                    if pos - sub_index < e.width() {
                        return Some((index, pos - sub_index, e));
                    }
                    index += 1;
                    sub_index += e.width();
                }
            }
        }
        None
    }

    fn get(&self, index: usize) -> Option<&T> {
        if self.is_leaf() {
            return self.elements.get(index);
        } else {
            let mut cumulative_len = 0;
            for (child_index, child) in self.children.iter().enumerate() {
                match (cumulative_len + child.len()).cmp(&index) {
                    Ordering::Less => {
                        cumulative_len += child.len() + 1;
                    }
                    Ordering::Equal => return self.elements.get(child_index),
                    Ordering::Greater => {
                        return child.get(index - cumulative_len);
                    }
                }
            }
        }
        None
    }
}

impl<T: HasWidth> Default for SpanTree<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, T: HasWidth> IntoIterator for &'a SpanTree<T> {
    type Item = &'a T;

    type IntoIter = SpanTreeIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        SpanTreeIter {
            inner: Some(self),
            index: 0,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SpanTreeIter<'a, T: HasWidth> {
    inner: Option<&'a SpanTree<T>>,
    index: usize,
}

impl<'a, T: HasWidth> Copy for SpanTreeIter<'a, T> {}

impl<'a, T: HasWidth> Iterator for SpanTreeIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.index += 1;
        self.inner.as_ref().and_then(|i| i.get(self.index - 1))
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.index += n + 1;
        self.inner.as_ref().and_then(|i| i.get(self.index - 1))
    }
}

/*
#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn push_back() {
        let mut t = SequenceTree::new();

        t.push(1);
        t.push(2);
        t.push(3);
        t.push(4);
        t.push(5);
        t.push(6);
        t.push(8);
        t.push(100);
    }

    #[test]
    fn insert() {
        let mut t = SequenceTree::new();

        t.insert(0, 1);
        t.insert(1, 1);
        t.insert(0, 1);
        t.insert(0, 1);
        t.insert(0, 1);
        t.insert(3, 1);
        t.insert(4, 1);
    }

    #[test]
    fn insert_book() {
        let mut t = SequenceTree::new();

        for i in 0..100 {
            t.insert(i % 2, ());
        }
    }

    #[test]
    fn insert_book_vec() {
        let mut t = SequenceTree::new();
        let mut v = Vec::new();

        for i in 0..100 {
            t.insert(i % 3, ());
            v.insert(i % 3, ());

            assert_eq!(v, t.iter().copied().collect::<Vec<_>>())
        }
    }

    fn arb_indices() -> impl Strategy<Value = Vec<usize>> {
        proptest::collection::vec(any::<usize>(), 0..1000).prop_map(|v| {
            let mut len = 0;
            v.into_iter()
                .map(|i| {
                    len += 1;
                    i % len
                })
                .collect::<Vec<_>>()
        })
    }

    proptest! {

        #[test]
        fn proptest_insert(indices in arb_indices()) {
            let mut t = SpanTree::<usize>::new();
            let mut v = Vec::new();

            for i in indices{
                if i <= v.len() {
                    t.insert(i % 3, i);
                    v.insert(i % 3, i);
                } else {
                    return Err(proptest::test_runner::TestCaseError::reject("index out of bounds"))
                }

                assert_eq!(v, t.iter().copied().collect::<Vec<_>>())
            }
        }

    }

    proptest! {

        // This is a really slow test due to all the copying of the Vecs (i.e. not due to the
        // sequencetree) so we only do a few runs
        #![proptest_config(ProptestConfig::with_cases(20))]
        #[test]
        fn proptest_remove(inserts in arb_indices(), removes in arb_indices()) {
            let mut t = SpanTree::<usize>::new();
            let mut v = Vec::new();

            for i in inserts {
                if i <= v.len() {
                    t.insert(i , i);
                    v.insert(i , i);
                } else {
                    return Err(proptest::test_runner::TestCaseError::reject("index out of bounds"))
                }

                assert_eq!(v, t.iter().copied().collect::<Vec<_>>())
            }

            for i in removes {
                if i < v.len() {
                    let tr = t.remove(i);
                    let vr = v.remove(i);
                    assert_eq!(tr, vr);
                } else {
                    return Err(proptest::test_runner::TestCaseError::reject("index out of bounds"))
                }

                assert_eq!(v, t.iter().copied().collect::<Vec<_>>())
            }
        }

    }
}
*/

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::ops::Range;

    #[derive(Clone, Debug, PartialEq)]
    struct TestWidth {
        index: usize,
        width: usize,
    }

    impl HasWidth for TestWidth {
        fn width(&self) -> usize {
            self.width
        }
    }

    impl HasWidth for usize {
        fn width(&self) -> usize {
            *self
        }
    }

    #[test]
    fn test_basic_get_at_width() {
        let mut tree = SpanTree::<usize>::default();

        tree.push(10);
        tree.push(20);
        tree.push(30);
        tree.push(40);
        tree.push(50);

        assert_eq!(tree.width(), 150);
        assert_eq!(tree.get(0), Some(&10));
        assert_eq!(tree.get(1), Some(&20));
        assert_eq!(tree.get(2), Some(&30));
        assert_eq!(tree.get(3), Some(&40));
        assert_eq!(tree.get(4), Some(&50));
        assert_eq!(tree.get_at_width(0), Some((0, 0, &10)));
        assert_eq!(tree.get_at_width(5), Some((0, 5, &10)));
        assert_eq!(tree.get_at_width(6), Some((0, 6, &10)));
        assert_eq!(tree.get_at_width(10), Some((1, 0, &20)));
        assert_eq!(tree.get_at_width(15), Some((1, 5, &20)));
        assert_eq!(tree.get_at_width(16), Some((1, 6, &20)));
        assert_eq!(tree.get_at_width(29), Some((1, 19, &20)));
        assert_eq!(tree.get_at_width(30), Some((2, 0, &30)));
        assert_eq!(tree.get_at_width(40), Some((2, 10, &30)));
        assert_eq!(tree.get_at_width(50), Some((2, 20, &30)));
        assert_eq!(tree.get_at_width(60), Some((3, 0, &40)));
        assert_eq!(tree.get_at_width(200), None);
    }

    #[test]
    fn test_advanced_get_at_width() {
        let mut tree = SpanTree::<TestWidth>::default();
        const MAX: usize = 1000;
        for index in 0..MAX {
            tree.push(TestWidth { index, width: 10 });
        }
        assert_eq!(tree.width(), MAX * 10);
        for i in 0..MAX * 10 {
            let index = i / 10;
            let width = 10;
            let n = i % 10;
            assert_eq!(
                tree.get_at_width(i),
                Some((index, n, &TestWidth { index, width }))
            );
        }

        tree.replace(
            10,
            TestWidth {
                index: 10,
                width: 100,
            },
        );
        assert_eq!(tree.width(), MAX * 10 + 90);

        assert_eq!(
            tree.get_at_width(201),
            Some((
                11,
                1,
                &TestWidth {
                    index: 11,
                    width: 10
                }
            ))
        );

        tree.replace(
            20,
            TestWidth {
                index: 10,
                width: 1000,
            },
        );
        assert_eq!(tree.width(), MAX * 10 + 90 + 990);

        let result = tree.iter().map(|t| t.width()).sum::<usize>();
        assert_eq!(tree.width(), result);
    }

    fn test_splice(
        tree: &mut SpanTree<usize>,
        vec: &mut Vec<usize>,
        range: Range<usize>,
        values: Vec<usize>,
    ) {
        vec.splice(range.clone(), values.clone());
        tree.splice(range, values);
        assert_eq!(&tree.to_vec(), vec);
    }

    #[test]
    fn test_tree_splice() {
        let mut tree = SpanTree::<usize>::default();
        let mut base = vec![];
        test_splice(&mut tree, &mut base, 0..0, vec![1, 2, 3]);
        test_splice(&mut tree, &mut base, 1..2, vec![7]);
        test_splice(&mut tree, &mut base, 1..2, vec![10, 11, 12, 13, 14]);
        test_splice(&mut tree, &mut base, 2..5, vec![50, 60, 70]);
    }
}
