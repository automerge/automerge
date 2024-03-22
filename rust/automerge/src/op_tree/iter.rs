use std::cmp::Ordering;

use crate::op_set::OpIdx;

use super::{Op, OpTreeInternal, OpTreeNode};

#[derive(Clone)]
pub(crate) struct OpTreeIter<'a>(Inner<'a>);

impl<'a> OpTreeIter<'a> {
    pub(crate) fn new(tree: &'a OpTreeInternal) -> OpTreeIter<'a> {
        Self(
            tree.root_node
                .as_ref()
                .map(|root| Inner::NonEmpty {
                    // This is a guess at the average depth of an OpTree
                    ancestors: Vec::with_capacity(6),
                    current: NodeIter {
                        node: root,
                        index: 0,
                    },
                    cumulative_index: 0,
                    root_node: root,
                })
                .unwrap_or(Inner::Empty),
        )
    }
}

impl<'a> Iterator for OpTreeIter<'a> {
    type Item = OpIdx;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.0.nth(n)
    }
}

#[derive(Clone)]
enum Inner<'a> {
    Empty,
    NonEmpty {
        // A stack of nodes in the optree which we have descended in to to get to the current
        // element.
        ancestors: Vec<NodeIter<'a>>,
        current: NodeIter<'a>,
        // How far through the whole optree we are
        cumulative_index: usize,
        root_node: &'a OpTreeNode,
    },
}

/// A node in the op tree which we are iterating over
#[derive(Clone)]
struct NodeIter<'a> {
    /// The node itself
    node: &'a OpTreeNode,
    /// The index of the next element we will pull from the node. This means something different
    /// depending on whether the node is a leaf node or not. If the node is a leaf node then this
    /// index is the index in `node.elements` which will be returned on the next call to `next()`.
    /// If the node is not an internal node then this index is the index of `children` which we are
    /// currently iterating as well as being the index of the next element of `elements` which we
    /// will return once we have finished iterating over the child node.
    index: usize,
}

impl<'a> Iterator for Inner<'a> {
    type Item = OpIdx;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Inner::Empty => None,
            Inner::NonEmpty {
                ancestors,
                current,
                cumulative_index,
                ..
            } => {
                if current.node.is_leaf() {
                    // If we're in a leaf node and we haven't exhausted it yet we just return the elements
                    // of the leaf node
                    if current.index < current.node.len() {
                        let result = current.node.elements[current.index];
                        current.index += 1;
                        *cumulative_index += 1;
                        Some(result)
                    } else {
                        // We've exhausted the leaf node, we must find the nearest non-exhausted parent (lol)
                        let node_iter = loop {
                            if let Some(
                                node_iter @ NodeIter {
                                    node: parent,
                                    index: parent_index,
                                },
                            ) = ancestors.pop()
                            {
                                // We've exhausted this parent
                                if parent_index >= parent.elements.len() {
                                    continue;
                                } else {
                                    // This parent still has elements to process, let's use it!
                                    break node_iter;
                                }
                            } else {
                                // No parents left, we're done
                                return None;
                            }
                        };
                        // if we've finished the elements in a leaf node and there's a parent node then we
                        // return the element from the parent node which is one after the index at which we
                        // descended into the child
                        *current = node_iter;
                        let result = current.node.elements[current.index];
                        current.index += 1;
                        *cumulative_index += 1;
                        Some(result)
                    }
                } else {
                    // If we're in a non-leaf node then the last iteration returned an element from the
                    // current nodes `elements`, so we must now descend into a leaf child
                    ancestors.push(current.clone());
                    loop {
                        let child = &current.node.children[current.index];
                        current.index = 0;
                        if !child.is_leaf() {
                            ancestors.push(NodeIter {
                                node: child,
                                index: 0,
                            });
                            current.node = child
                        } else {
                            current.node = child;
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
            Self::NonEmpty {
                root_node,
                cumulative_index,
                current,
                ancestors,
                ..
            } => {
                // Make sure that we don't rewind when calling nth more than once
                if n < *cumulative_index {
                    None
                } else if n >= root_node.len() {
                    *cumulative_index = root_node.len() - 1;
                    None
                } else {
                    // rather than trying to go back up through the ancestors to find the right
                    // node we just start at the root.
                    *current = NodeIter {
                        node: root_node,
                        index: n,
                    };
                    *cumulative_index = 0;
                    ancestors.clear();
                    while !current.node.is_leaf() {
                        for (child_index, child) in current.node.children.iter().enumerate() {
                            match (*cumulative_index + child.len()).cmp(&n) {
                                Ordering::Less => {
                                    *cumulative_index += child.len() + 1;
                                    current.index = child_index + 1;
                                }
                                Ordering::Equal => {
                                    *cumulative_index += child.len() + 1;
                                    current.index = child_index + 1;
                                    return Some(current.node.elements[child_index]);
                                }
                                Ordering::Greater => {
                                    current.index = child_index;
                                    let old = std::mem::replace(
                                        current,
                                        NodeIter {
                                            node: child,
                                            index: 0,
                                        },
                                    );
                                    ancestors.push(old);
                                    break;
                                }
                            }
                        }
                    }
                    // we're in a leaf node and we kept track of the cumulative index as we went,
                    let index_in_this_node = n.saturating_sub(*cumulative_index);
                    current.index = index_in_this_node + 1;
                    Some(current.node.elements[index_in_this_node])
                }
            }
        }
    }
}

pub(crate) struct OpTreeOpIter<'a> {
    iter: OpTreeIter<'a>,
    osd: &'a crate::op_set::OpSetData,
}

impl<'a> OpTreeOpIter<'a> {
    pub(crate) fn new(iter: OpTreeIter<'a>, osd: &'a crate::op_set::OpSetData) -> Self {
        Self { iter, osd }
    }
}

impl<'a> Iterator for OpTreeOpIter<'a> {
    type Item = Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|idx| idx.as_op(self.osd))
    }
}

#[cfg(test)]
mod tests {
    use super::super::OpTreeInternal;
    use crate::op_set::{OpIdx, OpSetData};
    use crate::types::{Key, ObjType, OpBuilder, OpId, OpType, ScalarValue, ROOT};
    use proptest::prelude::*;

    #[derive(Clone)]
    enum Action {
        Insert(usize, OpIdx),
        Delete(usize),
    }

    impl std::fmt::Debug for Action {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Insert(index, ..) => write!(f, "Insert({})", index),
                Self::Delete(index) => write!(f, "Delete({})", index),
            }
        }
    }

    // A struct which impls Debug by only printing the counters of the IDs of the  ops it wraps.
    // This is useful because the only difference between the ops that we generate is the counter
    // of their IDs. Wrapping a Vec<OpBuilder> in DebugOps will result in output from assert! etc. which
    // only shows the counters. For example, the output of a failing assert_eq! like this
    //
    //     assert_eq!(DebugOps(&ops1), DebugOps(&ops2))
    //
    // Might look like this
    //
    //     left: `[0,1,2,3]
    //     right: `[0,1,2,3,4]
    //
    // i.e. all the other details of the ops are elided
    #[derive(PartialEq)]
    struct DebugOps<'a>(&'a [OpIdx]);

    impl<'a> std::fmt::Debug for DebugOps<'a> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "[")?;
            for (index, op) in self.0.iter().enumerate() {
                if index < self.0.len() - 1 {
                    write!(f, "{:?},", op)?;
                } else {
                    write!(f, "{:?}]", op)?
                }
            }
            Ok(())
        }
    }

    fn op(counter: u64, osd: &mut OpSetData) -> OpIdx {
        let op = OpBuilder {
            action: OpType::Put(ScalarValue::Uint(counter)),
            id: OpId::new(counter, 0),
            key: Key::Map(0),
            insert: false,
        };
        osd.push(ROOT.into(), op)
    }

    /// A model for a property based test of the OpTreeIter. We generate a set of actions, each
    /// action pertaining to a `model` - which is just a `Vec<OpBuilder>`. As we generate each action we
    /// apply it to the model and record the action we took. In the property test we replay the
    /// same actions against an `OpTree` and check that the iterator returns the same result as the
    /// `model`.
    #[derive(Clone)]
    struct Model {
        actions: Vec<Action>,
        model: Vec<OpIdx>,
        osd: OpSetData,
    }

    impl std::fmt::Debug for Model {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Model")
                .field("actions", &self.actions)
                .field("model", &DebugOps(&self.model))
                .finish()
        }
    }

    impl Model {
        fn insert(&self, index: usize, next_op_counter: u64) -> Self {
            let mut actions = self.actions.clone();
            let mut osd = OpSetData::default();
            let op = op(next_op_counter, &mut osd);
            actions.push(Action::Insert(index, op));
            let mut model = self.model.clone();
            model.insert(index, op);
            Self {
                actions,
                model,
                osd,
            }
        }

        fn delete(&self, index: usize) -> Self {
            let mut actions = self.actions.clone();
            actions.push(Action::Delete(index));
            let mut model = self.model.clone();
            let osd = self.osd.clone();
            model.remove(index);
            Self {
                actions,
                model,
                osd,
            }
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

    fn model() -> impl Strategy<Value = Model> {
        (0_u64..150).prop_flat_map(|num_steps| {
            let mut strat = Just((
                0,
                Model {
                    actions: Vec::new(),
                    model: Vec::new(),
                    osd: OpSetData::default(),
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

    fn make_optree(actions: &[Action], osd: &OpSetData) -> super::OpTreeInternal {
        let mut optree = OpTreeInternal::new(ObjType::List);
        for action in actions {
            match action {
                Action::Insert(index, idx) => {
                    optree.insert(*index, *idx, osd);
                }
                Action::Delete(index) => {
                    optree.remove(*index, osd);
                }
            }
        }
        optree
    }

    /// A model for calls to `nth`. `NthModel::n` is guarnateed to be in `(0..model.len())`
    #[derive(Clone)]
    struct NthModel {
        model: Vec<OpIdx>,
        actions: Vec<Action>,
        osd: OpSetData,
        n: usize,
    }

    impl std::fmt::Debug for NthModel {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Model")
                .field("actions", &self.actions)
                .field("model", &DebugOps(&self.model))
                .field("n", &self.n)
                .finish()
        }
    }

    fn nth_model() -> impl Strategy<Value = NthModel> {
        model().prop_flat_map(|model| {
            if model.model.is_empty() {
                Just(NthModel {
                    model: model.model,
                    actions: model.actions,
                    osd: model.osd,
                    n: 0,
                })
                .boxed()
            } else {
                (0..model.model.len(), Just(model))
                    .prop_map(|(index, model)| NthModel {
                        model: model.model,
                        actions: model.actions,
                        osd: model.osd,
                        n: index,
                    })
                    .boxed()
            }
        })
    }

    proptest! {
        #[test]
        fn optree_iter_proptest(model in model()) {
            let optree = make_optree(&model.actions, &model.osd);
            let iter = super::OpTreeIter::new(&optree);
            let iterated = iter.collect::<Vec<_>>();
            assert_eq!(DebugOps(&model.model), DebugOps(&iterated))
        }

        #[test]
        fn optree_iter_nth(model in nth_model()) {
            let optree = make_optree(&model.actions, &model.osd);
            let mut iter = super::OpTreeIter::new(&optree);
            let mut model_iter = model.model.iter();
            assert_eq!(model_iter.nth(model.n).cloned(), iter.nth(model.n));

            let tail = iter.collect::<Vec<_>>();
            let expected_tail = model_iter.cloned().collect::<Vec<_>>();
            assert_eq!(DebugOps(tail.as_slice()), DebugOps(expected_tail.as_slice()));
        }
    }
}
