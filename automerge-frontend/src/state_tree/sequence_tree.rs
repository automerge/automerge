use std::fmt::Debug;

use automerge_protocol::OpId;

const T: usize = 5;

#[derive(Clone, Debug, PartialEq)]
pub struct SequenceTree<T> {
    root_node: Option<SequenceTreeNode<T>>,
}

#[derive(Clone, Debug, PartialEq)]
struct SequenceTreeNode<T> {
    elements: Vec<(OpId, T)>,
    children: Vec<SequenceTreeNode<T>>,
}

impl<T> SequenceTree<T>
where
    T: Clone + Debug,
{
    pub fn new() -> Self {
        Self { root_node: None }
    }

    pub fn len(&self) -> usize {
        self.root_node.as_ref().map_or(0, |n| n.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn insert(&mut self, mut index: usize, opid: OpId, element: T) {
        if let Some(root) = self.root_node.as_mut() {
            if root.elements.len() == 2 * T - 1 {
                let new_root = SequenceTreeNode {
                    elements: Vec::new(),
                    children: Vec::new(),
                };

                // move new_root to root position
                let old_root = std::mem::replace(root, new_root);

                root.children.push(old_root);
                root.split_child(0);

                let mut i = 0;
                if root.children[0].len() < index {
                    i += 1;
                    index -= root.children[0].len() + 1
                }
                root.children[i].insert_non_full(index, opid, element)
            } else {
                root.insert_non_full(index, opid, element)
            }
        } else {
            self.root_node = Some(SequenceTreeNode {
                elements: vec![(opid, element)],
                children: Vec::new(),
            })
        }
    }

    pub fn push_back(&mut self, opid: OpId, element: T) {
        let l = self.len();
        self.insert(l, opid, element)
    }

    pub fn get(&self, index: usize) -> Option<(OpId, &T)> {
        self.root_node.as_ref().and_then(|n| n.get(index))
    }

    pub fn get_mut(&mut self, index: usize) -> Option<(OpId, &mut T)> {
        self.root_node.as_mut().and_then(|n| n.get_mut(index))
    }

    pub fn remove(&mut self, index: usize) -> T {
        if let Some(root) = self.root_node.as_mut() {
            let old = root.remove(index);

            if root.elements.is_empty() {
                if root.children.is_empty() {
                    self.root_node = None;
                } else {
                    self.root_node = Some(root.children[0].clone());
                }
            }

            old
        } else {
            panic!("remove from empty tree")
        }
    }

    pub fn set(&mut self, index: usize, element: T) -> T {
        self.root_node.as_mut().unwrap().set(index, element)
    }
}

impl<T> SequenceTreeNode<T>
where
    T: Clone + Debug,
{
    pub fn len(&self) -> usize {
        self.elements.len() + self.children.iter().map(|c| c.len()).sum::<usize>()
    }

    fn insert_non_full(&mut self, index: usize, opid: OpId, element: T) {
        if self.children.is_empty() {
            // leaf

            self.elements.insert(index, (opid, element));
        } else {
            // not a leaf

            let mut i = 0;
            for (child_index, c) in self.children.iter_mut().enumerate() {
                if i + c.len() > index {
                    // insert into c
                    if c.elements.len() == 2 * T - 1 {
                        self.split_child(child_index);

                        let mut cumulative_len = 0;
                        for c in self.children.iter_mut() {
                            cumulative_len += c.len();
                            if cumulative_len > index {
                                c.insert_non_full(index - i, opid, element);
                                break;
                            }
                        }
                    } else {
                        c.insert_non_full(index - i, opid, element);
                    }
                    break;
                } else {
                    i += c.len() + 1
                }
            }
        }
    }

    // A utility function to split the child y of this node
    // Note that y must be full when this function is called
    fn split_child(&mut self, i: usize) {
        // Create a new node which is going to store (t-1) keys
        // of y
        let mut z = SequenceTreeNode {
            elements: Vec::new(),
            children: Vec::new(),
        };

        let y = &mut self.children[i];
        dbg!(&y);
        z.elements = y.elements.split_off(T);
        if !y.children.is_empty() {
            z.children = y.children.split_off(T);
        }

        let middle = y.elements.remove(T - 1);

        self.children.insert(i + 1, z);

        self.elements.insert(i, middle);
    }

    fn remove_from_leaf(&mut self, index: usize) -> T {
        self.elements.remove(index).1
    }

    fn remove_from_non_leaf(&mut self, index: usize) -> T {
        todo!()
    }

    pub fn remove(&mut self, index: usize) -> T {
        let mut total_index = 0;
        for (ci, child) in self.children.iter().enumerate() {
            if total_index + child.len() > index {
                // in that child
                todo!("remove in a child")
            } else if total_index + child.len() == index {
                // in this node
                if self.children.is_empty() {
                    return self.remove_from_leaf(ci);
                } else {
                    todo!("delete internal key")
                }
            } else {
                // should be later on in the loop
                total_index += child.len();
                continue;
            }
        }
        panic!("index not found to remove")
    }

    pub fn set(&mut self, mut index: usize, element: T) -> T {
        let mut i = 0;
        if self.children.is_empty() {
            let (_, old_element) = self.elements.get_mut(i).unwrap();
            std::mem::replace(old_element, element)
        } else {
            for c in &mut self.children {
                let c_len = c.len();
                if index < c_len {
                    return c.set(index, element);
                } else if index == c_len {
                    let (_, old_element) = self.elements.get_mut(i).unwrap();
                    return std::mem::replace(old_element, element);
                } else {
                    index -= c_len;
                    i += 1;
                }
            }
            panic!("Invalid index to set")
        }
    }

    pub fn get(&self, mut index: usize) -> Option<(OpId, &T)> {
        let mut i = 0;
        if self.children.is_empty() {
            return self.elements.get(index).map(|(o, t)| (o.clone(), t));
        } else {
            for c in &self.children {
                let c_len = c.len();
                if index < c_len {
                    return c.get(index);
                } else if index == c_len {
                    return self.elements.get(i).map(|(o, t)| (o.clone(), t));
                } else {
                    index -= c_len;
                    i += 1;
                }
            }
        }
        None
    }

    pub fn get_mut(&mut self, mut index: usize) -> Option<(OpId, &mut T)> {
        let mut i = 0;
        if self.children.is_empty() {
            return self.elements.get_mut(index).map(|(o, t)| (o.clone(), t));
        } else {
            for c in &mut self.children {
                let c_len = c.len();
                if index < c_len {
                    return c.get_mut(index);
                } else if index == c_len {
                    return self.elements.get_mut(i).map(|(o, t)| (o.clone(), t));
                } else {
                    index -= c_len;
                    i += 1;
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use automerge_protocol::ActorId;

    use super::*;

    #[test]
    fn push_back() {
        let mut t = SequenceTree::new();
        let actor = ActorId::random();

        t.push_back(actor.op_id_at(1), ());
        t.push_back(actor.op_id_at(2), ());
        t.push_back(actor.op_id_at(3), ());
        t.push_back(actor.op_id_at(4), ());
        t.push_back(actor.op_id_at(5), ());
        t.push_back(actor.op_id_at(6), ());
        t.push_back(actor.op_id_at(8), ());
        t.push_back(actor.op_id_at(100), ());
    }

    #[test]
    fn insert() {
        let mut t = SequenceTree::new();
        let actor = ActorId::random();

        t.insert(0, actor.op_id_at(1), ());
        t.insert(1, actor.op_id_at(1), ());
        t.insert(0, actor.op_id_at(1), ());
        t.insert(0, actor.op_id_at(1), ());
        t.insert(0, actor.op_id_at(1), ());
        t.insert(3, actor.op_id_at(1), ());
        t.insert(4, actor.op_id_at(1), ());
    }
}
