use std::collections::{HashMap, HashSet};

use crate::{
    op_set2::{ActorIdx, Op, OpSet},
    types::OpId,
};

use super::CommitRangeClocks;

pub(super) struct ReifiedDeletes<'a> {
    op_set: &'a OpSet,
    range_clocks: &'a [CommitRangeClocks],
    actor_mapping: HashMap<ActorIdx, usize>,
    pub(super) preds: HashMap<OpId, Vec<OpId>>,
    pub(super) reified_deletes: Vec<Op<'a>>,
}

impl<'a> ReifiedDeletes<'a> {
    pub(super) fn new(opset: &'a OpSet, ranges: &'a [CommitRangeClocks]) -> Self {
        // We have to translate between predecessor and successor operations.
        // In the OpSet every operation can be marked with "successors" which
        // are the IDs of operations which override the current operation. For
        // example, a delete operation is represented as just a successor on
        // the operation it deletes. Likewise, a "set" operation which replaces
        // an existing value(s) is added to the succesor set of the overwritten
        // values.
        //
        // We can't use this representation in the bundle format because the targets
        // of the operations might not appear in the bundle. Imagine a delete operation
        // which targets an operation created before the bundle start.
        //
        // To accomodate such operations we encode operations which target an operation
        // outside of the bundle directly, with a "predecessor" column set to the
        // target operation. This is the same as the encoding of the "change" chunk.
        //
        // These are the scenarios we need to handle:
        //
        // * A delete operation which targets an operation outside of the bundle.
        //   In this case we need to create a new delete operation
        // * A set, insert, or make operation which targets an operation outside
        //   of the bundle. In this case we need to make sure we include the target
        //   operation as part of the "predecessors" column for the operation
        //
        // We can't distinguish between these two cases looking solely at the successors
        // of an operation in the OpSet because delete operations are implicit - i.e. if we
        // see an operation ID in the successor of some operation in the OpSet we only
        // know it is a delete operation by checking that no operation with that ID appears
        // anywhere else in the OpSet.

        let mut seen_actors = HashSet::new();
        let mut preds = HashMap::<OpId, Vec<_>>::new();
        // A map from OpId to the operations which the OpId is a successor of
        let mut out_of_bundle_preds = HashMap::<OpId, Vec<_>>::new();

        for op in opset.iter() {
            let op_is_in_bundle_range = ranges.iter().any(|r| r.covers(&op.id));

            if op_is_in_bundle_range {
                seen_actors.insert(op.id.actoridx());
                if let Some(actor) = op.key.actor() {
                    seen_actors.insert(actor);
                }
            }

            for succ in op.succ() {
                let succ_is_in_bundle_range = ranges.iter().any(|r| r.covers(&succ));
                if succ_is_in_bundle_range {
                    seen_actors.insert(succ.actoridx());
                    if !op_is_in_bundle_range {
                        // This succ op is in the bundle, but the op it succeeds is
                        // not. There are two possibilities now: either we will encounter
                        // a set, insert, or make op later which corresponds to this
                        // succ - or the succ is a delete operation. In the latter case
                        // we need the object ID and key to reify the delete op, so we
                        // hold on to it here.
                        out_of_bundle_preds.entry(succ).or_default().push((
                            op.id,
                            op.obj,
                            op.key.clone(),
                        ));
                    }
                }
            }

            // Remove this op so that by the time we've finished iterating over the
            // ops the only ops remaining in out_of_bundle_preds are delete operations
            if let Some(succeeds) = out_of_bundle_preds.remove(&op.id) {
                for (target_id, _, _) in succeeds {
                    preds.entry(op.id).or_default().push(target_id);
                }
            }
        }

        // Add the deletes to the preds map so that we can encode them later
        for (op_id, predecessors) in &out_of_bundle_preds {
            // If we have predecessors then we know this is a delete operation
            if !predecessors.is_empty() {
                preds
                    .entry(*op_id)
                    .or_default()
                    .extend(predecessors.iter().map(|(pred_id, _, _)| *pred_id));
            }
        }

        // Now take the remaining operations - which we know must be deletes - and encode
        // them as explicit delete operations. We want to produce them in document order
        // so we obtain them by iterating over the document ops again
        let reified_deletes = opset
            .iter()
            .filter_map(|o| {
                out_of_bundle_preds.get(&o.id).and_then(|ps| {
                    if let Some((opid, obj, key)) = ps.iter().next() {
                        Some((o, obj, key, ps))
                    } else {
                        // If we don't have an object and key then we can't create a delete operation
                        return None;
                    }
                })
            })
            .map(|(op, obj, key, predecessors)| {
                debug_assert_eq!(
                    predecessors
                        .iter()
                        .map(|(_, obj, key)| (obj, key))
                        .collect::<HashSet::<_>>()
                        .len(),
                    1,
                    "All predecessors of a delete operation should have the same object and key"
                );
                Op::del(op.id, *obj, key.clone())
            })
            .collect();

        let mut actors = seen_actors.into_iter().collect::<Vec<_>>();
        actors.sort();
        let actor_mapping = actors
            .into_iter()
            .enumerate()
            .map(|(i, actor_idx)| (actor_idx, i))
            .collect::<HashMap<_, _>>();

        Self {
            preds,
            reified_deletes,
            actor_mapping,
            op_set: opset,
            range_clocks: ranges,
        }
    }

    pub(super) fn iter<'b>(&'b self) -> impl Iterator<Item = Op<'a>> + Clone + 'b {
        std::iter::from_fn(|| {
            let mut ops = self
                .op_set
                .iter()
                .filter(|op| self.range_clocks.iter().any(|r| r.covers(&op.id)))
                .peekable();
            let mut deletes = self.reified_deletes.iter().cloned().peekable();

            match (ops.peek(), deletes.peek()) {
                (Some(op), Some(delete)) => {
                    if op.key < delete.key {
                        ops.next()
                    } else if op.id < delete.id {
                        ops.next()
                    } else {
                        deletes.next()
                    }
                }
                (Some(_), None) => ops.next(),
                (None, Some(_)) => deletes.next(),
                (None, None) => None,
            }
        })
    }

    pub(super) fn map_actor(&self, actor: &ActorIdx) -> ActorIdx {
        ActorIdx(*self.actor_mapping.get(&actor).unwrap() as u32)
    }
}
