use std::cmp::Ordering;

use crate::types::Op;

use super::{OpTreeInternal, OpTreeNode};

#[derive(Clone)]
pub(crate) struct OpTreeIter<'a>(Inner<'a>);

impl<'a> Default for OpTreeIter<'a> {
    fn default() -> Self {
        OpTreeIter(Inner::Empty)
    }
}

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
                    ops: &tree.ops,
                })
                .unwrap_or(Inner::Empty),
        )
    }
}

impl<'a> Iterator for OpTreeIter<'a> {
    type Item = &'a Op;

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
        ops: &'a [Op],
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
    type Item = &'a Op;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Inner::Empty => None,
            Inner::NonEmpty {
                ancestors,
                ops,
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
                        Some(&ops[result])
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
                        Some(&ops[result])
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
                ops,
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
                                    return Some(&ops[current.node.elements[child_index]]);
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
                    Some(&ops[current.node.elements[index_in_this_node]])
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::OpTreeInternal;
    use crate::types::{Key, Op, OpId, OpIds, OpType, ScalarValue};
    use proptest::prelude::*;

    #[derive(Clone)]
    enum Action {
        Insert(usize, Op),
        Delete(usize),
        Overwrite(usize, Op),
    }

    impl std::fmt::Debug for Action {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Insert(index, ..) => write!(f, "Insert({})", index),
                Self::Delete(index) => write!(f, "Delete({})", index),
                Self::Overwrite(index, ..) => write!(f, "Overwrite({})", index),
            }
        }
    }

    // A struct which impls Debug by only printing the counters of the IDs of the  ops it wraps.
    // This is useful because the only difference between the ops that we generate is the counter
    // of their IDs. Wrapping a Vec<Op> in DebugOps will result in output from assert! etc. which
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
    struct DebugOps<'a>(&'a [Op]);

    impl<'a> std::fmt::Debug for DebugOps<'a> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let mut table = prettytable::Table::new();
            table.set_format(*prettytable::format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
            table.set_titles(prettytable::row!["Counter", "Preds", "Succ",]);
            for (_index, op) in self.0.iter().enumerate() {
                let preds = op.pred.iter().map(|o| o.counter()).collect::<Vec<_>>();
                let succ = op.succ.iter().map(|o| o.counter()).collect::<Vec<_>>();
                table.add_row(prettytable::row![
                    op.id.counter(),
                    format!("{:?}", preds),
                    format!("{:?}", succ)
                ]);
            }
            let mut out = Vec::new();
            table.print(&mut out).unwrap();
            write!(f, "\n{}\n", String::from_utf8(out).unwrap())?;
            Ok(())
        }
    }

    fn op(counter: u64, key: Key) -> Op {
        op_with_pred(counter, key, &[])
    }

    fn op_with_pred(counter: u64, key: Key, pred: &[u64]) -> Op {
        Op {
            action: OpType::Put(ScalarValue::Uint(counter)),
            id: OpId::new(counter, 0),
            key,
            pred: OpIds::new(pred.iter().map(|c| OpId::new(*c, 0)), |a, b| {
                a.counter().cmp(&b.counter())
            }),
            succ: Default::default(),
            insert: false,
        }
    }

    /// A model for a property based test of the OpTreeIter. We generate a set of actions, each
    /// action pertaining to a `model` - which is just a `Vec<Op>`. As we generate each action we
    /// apply it to the model and record the action we took. In the property test we replay the
    /// same actions against an `OpTree` and check that the iterator returns the same result as the
    /// `model`.
    #[derive(Clone)]
    struct Model {
        actions: Vec<Action>,
        model: Vec<Op>,
        last_counter: u64,
        last_map_key: usize,
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
        fn insert(&self, index: usize) -> Self {
            let mut actions = self.actions.clone();
            let next_op_key = self.last_counter + 1;
            let next_map_key = self.last_map_key + 1;
            let op = op(next_op_key, Key::Map(next_map_key));
            actions.push(Action::Insert(index, op.clone()));
            let mut model = self.model.clone();
            model.insert(index, op);
            Self {
                actions,
                model,
                last_counter: next_op_key,
                last_map_key: next_map_key,
            }
        }

        fn overwrite(&self, index: usize) -> Self {
            let mut actions = self.actions.clone();
            let to_overwrite = &self.model[index];
            let next_op_counter = self.last_counter + 1;
            let op = op_with_pred(
                next_op_counter,
                to_overwrite.key,
                &[to_overwrite.id.counter()],
            );

            let mut model = self.model.clone();
            model[index]
                .succ
                .add(op.id, |a, b| a.counter().cmp(&b.counter()));
            model.insert(index + 1, op.clone());

            actions.push(Action::Overwrite(index, op));
            Self {
                actions,
                model,
                last_counter: next_op_counter,
                last_map_key: self.last_map_key,
            }
        }

        fn delete(&self, index: usize) -> Self {
            let mut actions = self.actions.clone();
            actions.push(Action::Delete(index));
            let mut model = self.model.clone();
            model.remove(index);
            Self {
                actions,
                model,
                last_counter: self.last_counter,
                last_map_key: self.last_map_key,
            }
        }

        fn next(self) -> impl Strategy<Value = Model> {
            if self.model.is_empty() {
                Just(self.insert(0)).boxed()
            } else {
                // Note that we have to feed `self` through the `prop_flat_map` using `Just` to
                // appease the borrow checker, this is annoying because it does obscure the meaning
                // of the code heere which is basically "decide whether the next action should be
                // insert, if it is insert choose an index between 0..model.len() + 1 and generate
                // an op to insert, otherwise choose an index between 0..model.len() and generate a
                // delete action".
                //
                // 95% chance of inserting to make sure we deal with large lists
                #[derive(Clone, Debug)]
                enum ActionKind {
                    Insert,
                    Delete,
                    Overwrite,
                }
                (
                    Just(self),
                    prop_oneof![
                        50 => Just(ActionKind::Insert),
                        40 => Just(ActionKind::Overwrite),
                        10 => Just(ActionKind::Delete),
                    ],
                )
                    .prop_flat_map(move |(model, action)| match action {
                        ActionKind::Insert => (0..model.model.len() + 1, Just(model))
                            .prop_map(move |(index, model)| model.insert(index))
                            .boxed(),
                        ActionKind::Delete => ((0..model.model.len()), Just(model))
                            .prop_map(move |(index, model)| model.delete(index))
                            .boxed(),
                        ActionKind::Overwrite => {
                            // Indices of visible ops
                            let visible = model
                                .model
                                .iter()
                                .enumerate()
                                .filter_map(|(i, op)| if op.visible() { Some(i) } else { None })
                                .collect::<Vec<_>>();
                            if visible.is_empty() {
                                Just(model).boxed()
                            } else {
                                ((0..visible.len(), Just(model)).prop_map(move |(index, model)| {
                                    model.overwrite(visible[index])
                                }))
                                .boxed()
                            }
                        }
                    })
                    .boxed()
            }
        }
    }

    fn model() -> impl Strategy<Value = Model> {
        (0_u64..150).prop_flat_map(|num_steps| {
            let mut strat = Just(Model {
                actions: Vec::new(),
                model: Vec::new(),
                last_counter: 0,
                last_map_key: 0,
            })
            .boxed();
            for _ in 0..num_steps {
                strat = strat.prop_flat_map(|model| model.next()).boxed();
            }
            strat
        })
    }

    fn make_optree(actions: &[Action]) -> super::OpTreeInternal {
        let mut optree = OpTreeInternal::new();
        for action in actions {
            match action {
                Action::Insert(index, op) => optree.insert(*index, op.clone()),
                Action::Delete(index) => {
                    optree.remove(*index);
                }
                Action::Overwrite(index, op) => {
                    optree.update(*index, |old_op| {
                        old_op.add_succ(op, |a, b| a.counter().cmp(&b.counter()));
                    });
                    optree.insert(index + 1, op.clone());
                }
            }
        }
        optree
    }

    fn nth_model() -> impl Strategy<Value = (Model, usize)> {
        model().prop_flat_map(|model| {
            if model.model.is_empty() {
                Just((model, 0)).boxed()
            } else {
                (0..model.model.len(), Just(model))
                    .prop_map(|(index, model)| (model, index))
                    .boxed()
            }
        })
    }

    proptest! {
        #[test]
        fn optree_iter_proptest(model in model()) {
            let optree = make_optree(&model.actions);
            let iter = super::OpTreeIter::new(&optree);
            let iterated = iter.cloned().collect::<Vec<_>>();
            assert_eq!(DebugOps(&model.model), DebugOps(&iterated))
        }

        #[test]
        fn optree_iter_nth((model, n) in nth_model()) {
            let optree = make_optree(&model.actions);
            let mut iter = super::OpTreeIter::new(&optree);
            let mut model_iter = model.model.iter();
            assert_eq!(model_iter.nth(n), iter.nth(n));

            let tail = iter.cloned().collect::<Vec<_>>();
            let expected_tail = model_iter.cloned().collect::<Vec<_>>();
            assert_eq!(DebugOps(tail.as_slice()), DebugOps(expected_tail.as_slice()));
        }

        #[test]
        fn optree_top_ops(model in model()) {
            let optree = make_optree(&model.actions);
            let top = optree.top_ops(None).map(|o| o.op.clone()).collect::<Vec<_>>();
            let expected = model.model.into_iter().filter(|op| op.visible()).collect::<Vec<_>>();
            assert_eq!(DebugOps(&expected), DebugOps(&top))
        }
    }
}
