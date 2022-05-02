use crate::types::Op;

use super::{OpTreeInternal, OpTreeNode};

#[derive(Clone)]
pub(crate) enum OpTreeIter<'a> {
    Empty,
    NonEmpty {
        // A stack of (OpTreeNode, index) where `index` is the index in the elements of the optree node
        // at which we descended into a child
        ancestors: Vec<(&'a OpTreeNode, usize)>,
        current: &'a OpTreeNode,
        index: usize,
        tree: &'a OpTreeInternal,
    },
}

impl<'a> OpTreeIter<'a> {
    pub(crate) fn new(tree: &'a OpTreeInternal) -> OpTreeIter<'a> {
        tree.root_node
            .as_ref()
            .map(|root| OpTreeIter::NonEmpty {
                // This is a guess at the average depth of an OpTree
                ancestors: Vec::with_capacity(6),
                current: root,
                index: 0,
                tree,
            })
            .unwrap_or(OpTreeIter::Empty)
    }
}

impl<'a> Iterator for OpTreeIter<'a> {
    type Item = &'a Op;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            OpTreeIter::Empty => None,
            OpTreeIter::NonEmpty {
                ancestors,
                current,
                index,
                ..
            } => {
                if current.is_leaf() {
                    // If we're in a leaf node and we haven't exhausted it yet we just return the elements
                    // of the leaf node
                    if *index < current.len() {
                        let result = &current.elements[*index];
                        *index += 1;
                        Some(result)
                    } else {
                        // We've exhausted the leaf node, we must find the nearest non-exhausted parent (lol)
                        let (parent, parent_index) = loop {
                            if let Some((parent, parent_index)) = ancestors.pop() {
                                // We've exhausted this parent
                                if parent_index >= parent.elements.len() {
                                    continue;
                                } else {
                                    // This parent still has elements to process, let's use it!
                                    break (parent, parent_index);
                                }
                            } else {
                                // No parents left, we're done
                                return None;
                            }
                        };
                        // if we've finished the elements in a leaf node and there's a parent node then we
                        // return the element from the parent node which is one after the index at which we
                        // descended into the child
                        *index = parent_index + 1;
                        *current = parent;
                        let result = &current.elements[parent_index];
                        Some(result)
                    }
                } else {
                    // If we're in a non-leaf node then the last iteration returned an element from the
                    // current nodes `elements`, so we must now descend into a leaf child
                    ancestors.push((current, *index));
                    loop {
                        let child = &current.children[*index];
                        *index = 0;
                        if !child.is_leaf() {
                            ancestors.push((child, 0));
                            *current = child
                        } else {
                            *current = child;
                            break;
                        }
                    }
                    self.next()
                }
            }
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        match self {
            Self::Empty => None,
            Self::NonEmpty { tree, .. } => tree.get(n),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::OpTreeInternal;
    use crate::types::{Key, Op, OpId, OpType, ScalarValue};
    use proptest::prelude::*;

    #[derive(Debug, Clone)]
    enum Action {
        Insert(usize, Op),
        Delete(usize),
    }

    fn op(counter: u64) -> Op {
        Op {
            action: OpType::Put(ScalarValue::Uint(counter)),
            id: OpId(counter, 0),
            key: Key::Map(0),
            succ: Vec::new(),
            pred: Vec::new(),
            insert: false,
        }
    }

    /// A model for a property based test of the OpTreeIter. We generate a set of actions, each
    /// action pertaining to a `model` - which is just a `Vec<Op>`. As we generate each action we
    /// apply it to the model and record the action we took. In the property test we replay the
    /// same actions against an `OpTree` and check that the iterator returns the same result as the
    /// `model`.
    #[derive(Debug, Clone)]
    struct Model {
        actions: Vec<Action>,
        model: Vec<Op>,
    }

    impl Model {
        fn insert(&self, index: usize, next_op_counter: u64) -> Self {
            let mut actions = self.actions.clone();
            let op = op(next_op_counter);
            actions.push(Action::Insert(index, op.clone()));
            let mut model = self.model.clone();
            model.insert(index, op);
            Self { actions, model }
        }

        fn delete(&self, index: usize) -> Self {
            let mut actions = self.actions.clone();
            actions.push(Action::Delete(index));
            let mut model = self.model.clone();
            model.remove(index);
            Self { actions, model }
        }

        fn next(self, next_op_counter: u64) -> impl Strategy<Value = Model> {
            if self.model.is_empty() {
                Just(self.insert(0, next_op_counter)).boxed()
            } else {
                // Note that we have to feed `self` through the `prop_flat_map` using `Just` to
                // appease the borrow checker, this is annoying because it does obscure the meaning
                // of the code heere which is basically "decide whether the next action should be
                // insert, if it is insert choose an index between 0..model.len() + 1 and generate
                // an op to insert, otherwise choose an index between 0..model.len() and generate a
                // delete action".
                //
                // 95% chance of inserting to make sure we deal with large lists
                (proptest::bool::weighted(0.95), Just(self))
                    .prop_flat_map(move |(insert, model)| {
                        if insert {
                            (0..model.model.len() + 1, Just(model))
                                .prop_map(move |(index, model)| {
                                    model.insert(index, next_op_counter)
                                })
                                .boxed()
                        } else {
                            ((0..model.model.len()), Just(model))
                                .prop_map(move |(index, model)| model.delete(index))
                                .boxed()
                        }
                    })
                    .boxed()
            }
        }
    }

    fn scenario() -> impl Strategy<Value = Model> {
        (0_u64..150).prop_flat_map(|num_steps| {
            let mut strat = Just((
                0,
                Model {
                    actions: Vec::new(),
                    model: Vec::new(),
                },
            ))
            .boxed();
            for _ in 0..num_steps {
                strat = strat
                    // Note the counter, which we feed through each `prop_flat_map`, incrementing
                    // it by one each time. This mean that the generated ops have ascending (but
                    // not necessarily consecutive because not every `Action` is an `Insert`)
                    // counters. This makes it easier to debug failures - if we just used a random
                    // counter it would be much harder to see where things are out of order.
                    .prop_flat_map(|(counter, model)| {
                        let next_counter = counter + 1;
                        model.next(counter).prop_map(move |m| (next_counter, m))
                    })
                    .boxed();
            }
            strat.prop_map(|(_, model)| model)
        })
    }

    proptest! {
        #[test]
        fn optree_iter_proptest(Model{actions, model} in scenario()) {
            let mut optree = OpTreeInternal::new();
            for action in actions {
                match action {
                    Action::Insert(index, op) => optree.insert(index, op),
                    Action::Delete(index) => { optree.remove(index); },
                }
            }
            let iter = super::OpTreeIter::new(&optree);
            let iterated = iter.cloned().collect::<Vec<_>>();
            assert_eq!(model, iterated)
        }
    }
}
