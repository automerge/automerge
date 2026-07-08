use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

use crate::trace::{
    ActorSpec, MarkExpand, Metadata, Trace, VmHeadRef, VmHydrated, VmInstr, VmObjRef,
    VmObserveMode, VmOp, VmValue,
};

pub const MAX_VM_INSTRUCTIONS: usize = 256;

impl Trace {
    pub fn generate(seed: u64, steps: usize) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut builder = VmBuilder::default();

        // Give generated traces a small amount of useful initial structure, but
        // keep it in the same VM representation as mutated traces.
        let list_key = builder.allocate_object(ObjKind::List);
        let text_key = builder.allocate_object(ObjKind::Text);
        let mut instructions = vec![VmInstr::Change {
            doc: 0,
            actor: 0,
            ops: vec![
                VmOp::Put {
                    obj: VmObjRef::Root,
                    key: 2,
                    value: VmValue::Uint { slot: seed as u8 },
                },
                VmOp::MakeList {
                    obj: VmObjRef::Root,
                    key: list_key,
                },
                VmOp::MakeText {
                    obj: VmObjRef::Root,
                    key: text_key,
                },
            ],
        }];

        for _ in 0..steps {
            instructions.push(builder.random_instr(&mut rng));
            if rng.random_range(0..100) < 15 {
                instructions.push(VmInstr::SaveLoad { doc: 0 });
            }
        }
        instructions.push(VmInstr::SaveLoad { doc: 0 });

        Self {
            version: 1,
            metadata: Metadata {
                seed: Some(seed),
                parent: None,
                reason: Some("generated".to_string()),
            },
            actors: vec![ActorSpec::new(0), ActorSpec::new(1), ActorSpec::new(2)],
            steps: instructions,
        }
    }
}

pub fn normalize_trace(trace: &mut Trace) {
    if trace.steps.len() > MAX_VM_INSTRUCTIONS {
        trace.steps.truncate(MAX_VM_INSTRUCTIONS);
    }
}

pub fn mutate(input: &Trace, rng: &mut StdRng) -> Trace {
    let mut trace = input.clone();
    trace.metadata.reason = Some("mutated".to_string());

    ensure_actor_count(&mut trace, 3);

    let rounds = mutation_rounds(rng);
    for _ in 0..rounds {
        mutate_once(&mut trace, rng);
        repair_trace(&mut trace, rng);
        trim_trace(&mut trace, rng);
    }

    trace
}

pub fn mutation_batch(
    input: &Trace,
    rng: &mut StdRng,
    effort: usize,
    comparison_u8_values: &[u8],
) -> MutationBatch {
    MutationBatchBuilder::new(input, rng, effort, comparison_u8_values).finish()
}

pub fn prefix_extension_batch(input: &Trace, rng: &mut StdRng, effort: usize) -> MutationBatch {
    let max_plans = effort.saturating_mul(32).clamp(16, 1024);
    let mut plans = Vec::with_capacity(max_plans);
    let remaining_capacity = MAX_VM_INSTRUCTIONS.saturating_sub(input.steps.len());
    if remaining_capacity != 0 {
        for site in collect_map_alignment_sites(input).into_iter().take(64) {
            plans.push(MutationPlan::AppendConcurrentMapPuts {
                obj: site.obj.clone(),
                key: site.key,
            });
            plans.push(MutationPlan::AppendTripleMapPuts {
                obj: site.obj.clone(),
                key: site.key,
            });
            plans.push(MutationPlan::AppendMapDeleteAfterConflict {
                obj: site.obj.clone(),
                key: site.key,
            });
            plans.push(MutationPlan::AppendMapDeleteExpose {
                obj: site.obj.clone(),
                key: site.key,
            });
            plans.push(MutationPlan::AppendFreshConcurrentSeq {
                parent: site.obj.clone(),
                key: site.key,
                kind: SeqKind::List,
            });
            plans.push(MutationPlan::AppendFreshStagedSeqConflict {
                parent: site.obj.clone(),
                key: site.key,
                kind: SeqKind::List,
            });
            plans.push(MutationPlan::AppendFreshConcurrentSeq {
                parent: site.obj.clone(),
                key: site.key,
                kind: SeqKind::Text,
            });
            plans.push(MutationPlan::AppendFreshStagedSeqConflict {
                parent: site.obj,
                key: site.key,
                kind: SeqKind::Text,
            });
            if plans.len() >= max_plans {
                break;
            }
        }
        if plans.len() < max_plans {
            for site in collect_seq_alignment_sites(input).into_iter().take(64) {
                plans.push(MutationPlan::AppendConcurrentSeqInserts {
                    obj: site.obj.clone(),
                    index: site.index,
                    kind: site.kind,
                });
                plans.push(MutationPlan::AppendConcurrentSeqInserts {
                    obj: site.obj.clone(),
                    index: 0,
                    kind: site.kind,
                });
                plans.push(MutationPlan::AppendSeqDeleteAfterConcurrentInsert {
                    obj: site.obj.clone(),
                    index: site.index,
                    kind: site.kind,
                });
                plans.push(MutationPlan::AppendSeqDeleteExpose {
                    obj: site.obj.clone(),
                    index: site.index,
                    kind: site.kind,
                });
                plans.push(MutationPlan::AppendStagedSeqConflict {
                    obj: site.obj,
                    index: site.index,
                    kind: site.kind,
                });
                if plans.len() >= max_plans {
                    break;
                }
            }
        }
        while plans.len() < max_plans {
            let extra_steps = rng.random_range(1..=remaining_capacity.min(64));
            plans.push(MutationPlan::AppendGenerated { extra_steps });
        }
    }
    plans.shuffle(rng);
    MutationBatch {
        source: input.clone(),
        plans,
        cursor: 0,
        random_remaining: 0,
    }
}

pub struct MutationBatch {
    source: Trace,
    plans: Vec<MutationPlan>,
    cursor: usize,
    random_remaining: usize,
}

impl MutationBatch {
    pub fn next(&mut self, rng: &mut StdRng) -> Option<Trace> {
        if let Some(plan) = self.plans.get(self.cursor).cloned() {
            self.cursor += 1;
            return Some(apply_plan(&self.source, plan, rng));
        }
        if self.random_remaining > 0 {
            let index = self.random_remaining;
            self.random_remaining -= 1;
            let mut trace = mutate(&self.source, rng);
            trace.metadata.reason = Some(format!("lazy random mutation sample {index}"));
            return Some(trace);
        }
        None
    }

    pub fn is_empty(&self) -> bool {
        self.cursor >= self.plans.len() && self.random_remaining == 0
    }
}

#[derive(Clone)]
enum MutationPlan {
    Position {
        position: Position,
        variant: usize,
    },
    CopyU8 {
        dest: usize,
        value: u8,
    },
    CopyObj {
        dest: usize,
        value: VmObjRef,
    },
    CopyHead {
        dest: usize,
        value: VmHeadRef,
    },
    AlignMapTuple {
        dest: usize,
        obj: VmObjRef,
        key: u8,
    },
    AlignTwoMapTuples {
        first: usize,
        second: usize,
        obj: VmObjRef,
        key: u8,
    },
    AlignSeqTuple {
        dest: usize,
        obj: VmObjRef,
        index: u8,
    },
    AlignTwoSeqTuples {
        first: usize,
        second: usize,
        obj: VmObjRef,
        index: u8,
    },
    ConstructorSwap {
        op_index: usize,
        variant: usize,
    },
    AppendGenerated {
        extra_steps: usize,
    },
    AppendConcurrentMapPuts {
        obj: VmObjRef,
        key: u8,
    },
    AppendTripleMapPuts {
        obj: VmObjRef,
        key: u8,
    },
    AppendMapDeleteAfterConflict {
        obj: VmObjRef,
        key: u8,
    },
    AppendMapDeleteExpose {
        obj: VmObjRef,
        key: u8,
    },
    AppendConcurrentSeqInserts {
        obj: VmObjRef,
        index: u8,
        kind: SeqKind,
    },
    AppendSeqDeleteAfterConcurrentInsert {
        obj: VmObjRef,
        index: u8,
        kind: SeqKind,
    },
    AppendSeqDeleteExpose {
        obj: VmObjRef,
        index: u8,
        kind: SeqKind,
    },
    AppendFreshConcurrentSeq {
        parent: VmObjRef,
        key: u8,
        kind: SeqKind,
    },
    AppendStagedSeqConflict {
        obj: VmObjRef,
        index: u8,
        kind: SeqKind,
    },
    AppendFreshStagedSeqConflict {
        parent: VmObjRef,
        key: u8,
        kind: SeqKind,
    },
}

struct MutationBatchBuilder<'a, 'rng> {
    input: &'a Trace,
    rng: &'rng mut StdRng,
    max_plans: usize,
    comparison_u8_values: &'a [u8],
    plans: Vec<MutationPlan>,
}

impl<'a, 'rng> MutationBatchBuilder<'a, 'rng> {
    fn new(
        input: &'a Trace,
        rng: &'rng mut StdRng,
        effort: usize,
        comparison_u8_values: &'a [u8],
    ) -> Self {
        Self {
            input,
            rng,
            max_plans: effort.saturating_mul(64).clamp(32, 4096),
            comparison_u8_values,
            plans: Vec::new(),
        }
    }

    fn finish(mut self) -> MutationBatch {
        self.enumerate_positions();
        self.enumerate_typed_field_copies();
        self.enumerate_multi_field_alignments();
        self.enumerate_constructor_swaps();
        self.plans.shuffle(self.rng);
        let random_remaining = (self.max_plans / 64).max(1);
        MutationBatch {
            source: self.input.clone(),
            plans: self.plans,
            cursor: 0,
            random_remaining,
        }
    }

    fn is_full(&self) -> bool {
        self.plans.len() >= self.max_plans
    }

    fn push_plan(&mut self, plan: MutationPlan) {
        if !self.is_full() {
            self.plans.push(plan);
        }
    }

    fn enumerate_positions(&mut self) {
        let ctx = TraceContext::new(self.input);
        for position in trace_positions(self.input) {
            if self.is_full() {
                return;
            }
            let variants = position_variant_count(self.input, &position, &ctx).min(8);
            for variant in 0..variants {
                self.push_plan(MutationPlan::Position {
                    position: position.clone(),
                    variant,
                });
            }
        }
    }

    fn enumerate_typed_field_copies(&mut self) {
        let mut u8_values = collect_u8_fields(self.input);
        u8_values.extend_from_slice(self.comparison_u8_values);
        let u8_values = unique_u8s(u8_values);
        for dest in 0..count_u8_fields(self.input) {
            for value in &u8_values {
                self.push_plan(MutationPlan::CopyU8 {
                    dest,
                    value: *value,
                });
                if self.is_full() {
                    return;
                }
            }
        }

        let obj_refs = dedup_preserving(collect_obj_refs(self.input));
        for dest in 0..count_obj_refs(self.input) {
            for value in &obj_refs {
                self.push_plan(MutationPlan::CopyObj {
                    dest,
                    value: value.clone(),
                });
                if self.is_full() {
                    return;
                }
            }
        }

        let head_refs = dedup_preserving(collect_head_refs(self.input));
        for dest in 0..count_head_refs(self.input) {
            for value in &head_refs {
                self.push_plan(MutationPlan::CopyHead {
                    dest,
                    value: value.clone(),
                });
                if self.is_full() {
                    return;
                }
            }
        }
    }

    fn enumerate_multi_field_alignments(&mut self) {
        let map_sites = collect_map_alignment_sites(self.input);
        for source in &map_sites {
            for dest in &map_sites {
                if source.op_index == dest.op_index {
                    continue;
                }
                self.push_plan(MutationPlan::AlignMapTuple {
                    dest: dest.op_index,
                    obj: source.obj.clone(),
                    key: source.key,
                });
                if self.is_full() {
                    return;
                }
            }
        }
        for source in &map_sites {
            for first in &map_sites {
                if first.op_index == source.op_index {
                    continue;
                }
                for second in &map_sites {
                    if second.op_index == source.op_index || second.op_index <= first.op_index {
                        continue;
                    }
                    self.push_plan(MutationPlan::AlignTwoMapTuples {
                        first: first.op_index,
                        second: second.op_index,
                        obj: source.obj.clone(),
                        key: source.key,
                    });
                    if self.is_full() {
                        return;
                    }
                }
            }
        }

        let seq_sites = collect_seq_alignment_sites(self.input);
        for source in &seq_sites {
            for dest in &seq_sites {
                if source.op_index == dest.op_index {
                    continue;
                }
                self.push_plan(MutationPlan::AlignSeqTuple {
                    dest: dest.op_index,
                    obj: source.obj.clone(),
                    index: source.index,
                });
                if self.is_full() {
                    return;
                }
            }
        }
        for source in &seq_sites {
            for first in &seq_sites {
                if first.op_index == source.op_index {
                    continue;
                }
                for second in &seq_sites {
                    if second.op_index == source.op_index || second.op_index <= first.op_index {
                        continue;
                    }
                    self.push_plan(MutationPlan::AlignTwoSeqTuples {
                        first: first.op_index,
                        second: second.op_index,
                        obj: source.obj.clone(),
                        index: source.index,
                    });
                    if self.is_full() {
                        return;
                    }
                }
            }
        }
    }

    fn enumerate_constructor_swaps(&mut self) {
        for op_index in 0..count_ops(self.input) {
            let Some(op) = nth_op(self.input, op_index) else {
                continue;
            };
            for variant in 0..constructor_swap_variant_count(op) {
                self.push_plan(MutationPlan::ConstructorSwap { op_index, variant });
                if self.is_full() {
                    return;
                }
            }
        }
    }
}

fn apply_plan(source: &Trace, plan: MutationPlan, rng: &mut StdRng) -> Trace {
    let reason = plan.reason();
    let (mut trace, preserve_prefix) = match plan {
        MutationPlan::Position { position, variant } => {
            let ctx = TraceContext::new(source);
            (
                apply_position_variant(source, &position, &ctx, variant, rng),
                false,
            )
        }
        MutationPlan::CopyU8 { dest, value } => {
            let mut trace = source.clone();
            let mut seen = 0;
            set_nth_u8_field(&mut trace, dest, value, &mut seen);
            (trace, false)
        }
        MutationPlan::CopyObj { dest, value } => {
            let mut trace = source.clone();
            let mut seen = 0;
            set_nth_obj_ref(&mut trace, dest, value, &mut seen);
            (trace, false)
        }
        MutationPlan::CopyHead { dest, value } => {
            let mut trace = source.clone();
            let mut seen = 0;
            set_nth_head_ref(&mut trace, dest, value, &mut seen);
            (trace, false)
        }
        MutationPlan::AlignMapTuple { dest, obj, key } => {
            let mut trace = source.clone();
            align_nth_map_tuple(&mut trace, dest, obj, key);
            (trace, false)
        }
        MutationPlan::AlignTwoMapTuples {
            first,
            second,
            obj,
            key,
        } => {
            let mut trace = source.clone();
            align_nth_map_tuple(&mut trace, first, obj.clone(), key);
            align_nth_map_tuple(&mut trace, second, obj, key);
            (trace, false)
        }
        MutationPlan::AlignSeqTuple { dest, obj, index } => {
            let mut trace = source.clone();
            align_nth_seq_tuple(&mut trace, dest, obj, index);
            (trace, false)
        }
        MutationPlan::AlignTwoSeqTuples {
            first,
            second,
            obj,
            index,
        } => {
            let mut trace = source.clone();
            align_nth_seq_tuple(&mut trace, first, obj.clone(), index);
            align_nth_seq_tuple(&mut trace, second, obj, index);
            (trace, false)
        }
        MutationPlan::ConstructorSwap { op_index, variant } => {
            let mut trace = source.clone();
            if let Some(op) = nth_op(source, op_index) {
                if let Some(replacement) =
                    constructor_swap_variants(op, rng).into_iter().nth(variant)
                {
                    set_nth_op(&mut trace, op_index, replacement);
                }
            }
            (trace, false)
        }
        MutationPlan::AppendGenerated { extra_steps } => {
            (append_generated_suffix(source, extra_steps, rng), true)
        }
        MutationPlan::AppendConcurrentMapPuts { obj, key } => (
            append_concurrent_map_puts(source, obj, key, 2, false, rng),
            true,
        ),
        MutationPlan::AppendTripleMapPuts { obj, key } => (
            append_concurrent_map_puts(source, obj, key, 3, false, rng),
            true,
        ),
        MutationPlan::AppendMapDeleteAfterConflict { obj, key } => (
            append_concurrent_map_puts(source, obj, key, 2, true, rng),
            true,
        ),
        MutationPlan::AppendMapDeleteExpose { obj, key } => {
            (append_map_delete_expose(source, obj, key, rng), true)
        }
        MutationPlan::AppendConcurrentSeqInserts { obj, index, kind } => (
            append_concurrent_seq_inserts(source, obj, index, kind, false, rng),
            true,
        ),
        MutationPlan::AppendSeqDeleteAfterConcurrentInsert { obj, index, kind } => (
            append_concurrent_seq_inserts(source, obj, index, kind, true, rng),
            true,
        ),
        MutationPlan::AppendSeqDeleteExpose { obj, index, kind } => (
            append_seq_delete_expose(source, obj, index, kind, rng),
            true,
        ),
        MutationPlan::AppendFreshConcurrentSeq { parent, key, kind } => (
            append_fresh_concurrent_seq(source, parent, key, kind, rng),
            true,
        ),
        MutationPlan::AppendStagedSeqConflict { obj, index, kind } => (
            append_staged_seq_conflict(source, obj, index, kind, rng),
            true,
        ),
        MutationPlan::AppendFreshStagedSeqConflict { parent, key, kind } => (
            append_fresh_staged_seq_conflict(source, parent, key, kind, rng),
            true,
        ),
    };
    trace.metadata = Metadata {
        seed: trace.metadata.seed,
        parent: trace.metadata.parent.clone(),
        reason: Some(reason),
    };
    ensure_actor_count(&mut trace, 3);
    if !preserve_prefix {
        repair_trace(&mut trace, rng);
        trim_trace(&mut trace, rng);
    }
    trace
}

impl MutationPlan {
    fn reason(&self) -> String {
        match self {
            Self::Position { position, variant } => {
                format!("lazy position {position:?} variant {variant}")
            }
            Self::CopyU8 { dest, value } => format!("lazy copy u8 {value} to field {dest}"),
            Self::CopyObj { dest, .. } => format!("lazy copy obj ref to field {dest}"),
            Self::CopyHead { dest, .. } => format!("lazy copy head ref to field {dest}"),
            Self::AlignMapTuple { dest, key, .. } => {
                format!("lazy align map tuple key {key} to op {dest}")
            }
            Self::AlignTwoMapTuples {
                first, second, key, ..
            } => format!("lazy align map tuple key {key} to ops {first},{second}"),
            Self::AlignSeqTuple { dest, index, .. } => {
                format!("lazy align seq tuple index {index} to op {dest}")
            }
            Self::AlignTwoSeqTuples {
                first,
                second,
                index,
                ..
            } => format!("lazy align seq tuple index {index} to ops {first},{second}"),
            Self::ConstructorSwap { op_index, variant } => {
                format!("lazy constructor swap op {op_index} variant {variant}")
            }
            Self::AppendGenerated { extra_steps } => {
                format!("lazy prefix-preserving append {extra_steps} steps")
            }
            Self::AppendConcurrentMapPuts { key, .. } => {
                format!("lazy append concurrent map puts key {key}")
            }
            Self::AppendTripleMapPuts { key, .. } => {
                format!("lazy append triple map puts key {key}")
            }
            Self::AppendMapDeleteAfterConflict { key, .. } => {
                format!("lazy append map delete after conflict key {key}")
            }
            Self::AppendMapDeleteExpose { key, .. } => {
                format!("lazy append map delete expose key {key}")
            }
            Self::AppendConcurrentSeqInserts { index, .. } => {
                format!("lazy append concurrent seq inserts index {index}")
            }
            Self::AppendSeqDeleteAfterConcurrentInsert { index, .. } => {
                format!("lazy append seq delete after concurrent insert index {index}")
            }
            Self::AppendSeqDeleteExpose { index, .. } => {
                format!("lazy append seq delete expose index {index}")
            }
            Self::AppendFreshConcurrentSeq { key, .. } => {
                format!("lazy append fresh concurrent sequence key {key}")
            }
            Self::AppendStagedSeqConflict { index, .. } => {
                format!("lazy append staged sequence conflict index {index}")
            }
            Self::AppendFreshStagedSeqConflict { key, .. } => {
                format!("lazy append fresh staged sequence conflict key {key}")
            }
        }
    }
}

fn append_generated_suffix(source: &Trace, extra_steps: usize, rng: &mut StdRng) -> Trace {
    let mut trace = source.clone();
    ensure_actor_count(&mut trace, 3);
    let available = MAX_VM_INSTRUCTIONS.saturating_sub(trace.steps.len());
    let extra_steps = extra_steps.min(available);
    let mut builder = VmBuilder::from_instructions(&trace.steps);
    for _ in 0..extra_steps {
        let instr = builder.random_instr(rng);
        builder.observe_instr(&instr);
        trace.steps.push(instr);
    }
    trace
}

fn append_map_delete_expose(source: &Trace, obj: VmObjRef, key: u8, rng: &mut StdRng) -> Trace {
    let mut trace = source.clone();
    ensure_actor_count(&mut trace, 3);
    push_if_capacity(&mut trace, VmInstr::Fork { from: 0, to: 1 });
    push_if_capacity(&mut trace, VmInstr::Fork { from: 0, to: 2 });
    for doc in [1u8, 2] {
        push_if_capacity(
            &mut trace,
            VmInstr::Change {
                doc,
                actor: doc,
                ops: vec![VmOp::Put {
                    obj: obj.clone(),
                    key,
                    value: random_vm_value(rng),
                }],
            },
        );
    }
    push_if_capacity(&mut trace, VmInstr::Merge { into: 0, from: 1 });
    push_if_capacity(&mut trace, VmInstr::Merge { into: 0, from: 2 });
    push_if_capacity(
        &mut trace,
        VmInstr::Change {
            doc: 2,
            actor: 2,
            ops: vec![VmOp::Delete {
                obj: obj.clone(),
                key,
            }],
        },
    );
    push_if_capacity(&mut trace, VmInstr::Merge { into: 0, from: 2 });
    push_if_capacity(
        &mut trace,
        VmInstr::Observe {
            doc: 0,
            object: obj,
            mode: VmObserveMode::MapGets,
            head: VmHeadRef::Current,
            budget: 8,
        },
    );
    trace
}

fn append_concurrent_map_puts(
    source: &Trace,
    obj: VmObjRef,
    key: u8,
    writers: usize,
    delete_after_merge: bool,
    rng: &mut StdRng,
) -> Trace {
    let mut trace = source.clone();
    ensure_actor_count(&mut trace, 3);
    let writers = writers.clamp(2, 3);
    push_if_capacity(&mut trace, VmInstr::Fork { from: 0, to: 1 });
    if writers >= 3 {
        push_if_capacity(&mut trace, VmInstr::Fork { from: 0, to: 2 });
    }
    for doc in 0..writers {
        push_if_capacity(
            &mut trace,
            VmInstr::Change {
                doc: doc as u8,
                actor: doc as u8,
                ops: vec![VmOp::Put {
                    obj: obj.clone(),
                    key,
                    value: random_vm_value(rng),
                }],
            },
        );
    }
    push_if_capacity(&mut trace, VmInstr::Merge { into: 0, from: 1 });
    if writers >= 3 {
        push_if_capacity(&mut trace, VmInstr::Merge { into: 0, from: 2 });
    }
    if delete_after_merge {
        push_if_capacity(
            &mut trace,
            VmInstr::Change {
                doc: 0,
                actor: 0,
                ops: vec![VmOp::Delete {
                    obj: obj.clone(),
                    key,
                }],
            },
        );
    }
    push_if_capacity(&mut trace, VmInstr::SaveHeads { doc: 0, slot: 7 });
    push_if_capacity(
        &mut trace,
        VmInstr::Observe {
            doc: 0,
            object: obj.clone(),
            mode: VmObserveMode::MapGets,
            head: VmHeadRef::Current,
            budget: 8,
        },
    );
    push_if_capacity(
        &mut trace,
        VmInstr::Observe {
            doc: 0,
            object: obj,
            mode: VmObserveMode::MapGets,
            head: VmHeadRef::Slot { slot: 7 },
            budget: 8,
        },
    );
    trace
}

fn append_fresh_concurrent_seq(
    source: &Trace,
    parent: VmObjRef,
    key: u8,
    kind: SeqKind,
    rng: &mut StdRng,
) -> Trace {
    let mut trace = source.clone();
    ensure_actor_count(&mut trace, 3);
    push_if_capacity(
        &mut trace,
        VmInstr::Change {
            doc: 0,
            actor: 0,
            ops: vec![match kind {
                SeqKind::List => VmOp::MakeList { obj: parent, key },
                SeqKind::Text => VmOp::MakeText { obj: parent, key },
            }],
        },
    );
    let obj = VmObjRef::Recent { back: 0 };
    push_if_capacity(&mut trace, VmInstr::Fork { from: 0, to: 1 });
    for doc in 0..2 {
        push_if_capacity(
            &mut trace,
            VmInstr::Change {
                doc: doc as u8,
                actor: doc as u8,
                ops: vec![seq_insert_op(obj.clone(), 0, kind, rng)],
            },
        );
    }
    push_if_capacity(&mut trace, VmInstr::Merge { into: 0, from: 1 });
    push_if_capacity(
        &mut trace,
        VmInstr::Observe {
            doc: 0,
            object: obj,
            mode: match kind {
                SeqKind::List => VmObserveMode::Ranges,
                SeqKind::Text => VmObserveMode::Text,
            },
            head: VmHeadRef::Current,
            budget: 8,
        },
    );
    trace
}

fn append_seq_delete_expose(
    source: &Trace,
    obj: VmObjRef,
    index: u8,
    kind: SeqKind,
    rng: &mut StdRng,
) -> Trace {
    let mut trace = source.clone();
    ensure_actor_count(&mut trace, 3);
    push_if_capacity(&mut trace, VmInstr::Fork { from: 0, to: 1 });
    push_if_capacity(&mut trace, VmInstr::Fork { from: 0, to: 2 });
    for doc in [1u8, 2] {
        push_if_capacity(
            &mut trace,
            VmInstr::Change {
                doc,
                actor: doc,
                ops: vec![seq_update_op(obj.clone(), index, kind, rng)],
            },
        );
    }
    push_if_capacity(&mut trace, VmInstr::Merge { into: 0, from: 1 });
    push_if_capacity(&mut trace, VmInstr::Merge { into: 0, from: 2 });
    push_if_capacity(
        &mut trace,
        VmInstr::Change {
            doc: 2,
            actor: 2,
            ops: vec![match kind {
                SeqKind::List => VmOp::DeleteSeq {
                    obj: obj.clone(),
                    index,
                },
                SeqKind::Text => VmOp::SpliceText {
                    obj: obj.clone(),
                    index,
                    delete: 1,
                    value: rng.random(),
                },
            }],
        },
    );
    push_if_capacity(&mut trace, VmInstr::Merge { into: 0, from: 2 });
    push_if_capacity(
        &mut trace,
        VmInstr::Observe {
            doc: 0,
            object: obj,
            mode: match kind {
                SeqKind::List => VmObserveMode::Ranges,
                SeqKind::Text => VmObserveMode::Text,
            },
            head: VmHeadRef::Current,
            budget: 8,
        },
    );
    trace
}

fn append_fresh_staged_seq_conflict(
    source: &Trace,
    parent: VmObjRef,
    key: u8,
    kind: SeqKind,
    rng: &mut StdRng,
) -> Trace {
    let mut trace = source.clone();
    ensure_actor_count(&mut trace, 4);
    push_if_capacity(
        &mut trace,
        VmInstr::Change {
            doc: 0,
            actor: 0,
            ops: vec![match kind {
                SeqKind::List => VmOp::MakeList { obj: parent, key },
                SeqKind::Text => VmOp::MakeText { obj: parent, key },
            }],
        },
    );
    append_staged_seq_conflict_to_trace(&mut trace, VmObjRef::Recent { back: 0 }, 0, kind, rng);
    trace
}

fn append_staged_seq_conflict(
    source: &Trace,
    obj: VmObjRef,
    index: u8,
    kind: SeqKind,
    rng: &mut StdRng,
) -> Trace {
    let mut trace = source.clone();
    ensure_actor_count(&mut trace, 4);
    append_staged_seq_conflict_to_trace(&mut trace, obj, index, kind, rng);
    trace
}

fn append_staged_seq_conflict_to_trace(
    trace: &mut Trace,
    obj: VmObjRef,
    index: u8,
    kind: SeqKind,
    rng: &mut StdRng,
) {
    // Stage a history where the final target document receives both the insert
    // and concurrent updates to that newly inserted element in one merge. This
    // is the shape that exercises Automerge's sequence insert untangling path.
    push_if_capacity(trace, VmInstr::Fork { from: 0, to: 1 });
    push_if_capacity(
        trace,
        VmInstr::Change {
            doc: 1,
            actor: 1,
            ops: vec![seq_insert_op(obj.clone(), index, kind, rng)],
        },
    );
    push_if_capacity(trace, VmInstr::Fork { from: 1, to: 2 });
    push_if_capacity(trace, VmInstr::Fork { from: 1, to: 3 });
    for doc in [2u8, 3] {
        push_if_capacity(
            trace,
            VmInstr::Change {
                doc,
                actor: doc,
                ops: vec![seq_update_op(obj.clone(), index, kind, rng)],
            },
        );
    }
    push_if_capacity(trace, VmInstr::Merge { into: 1, from: 2 });
    push_if_capacity(trace, VmInstr::Merge { into: 1, from: 3 });
    push_if_capacity(trace, VmInstr::Merge { into: 0, from: 1 });
    push_if_capacity(
        trace,
        VmInstr::Observe {
            doc: 0,
            object: obj,
            mode: match kind {
                SeqKind::List => VmObserveMode::Ranges,
                SeqKind::Text => VmObserveMode::Text,
            },
            head: VmHeadRef::Current,
            budget: 8,
        },
    );
}

fn append_concurrent_seq_inserts(
    source: &Trace,
    obj: VmObjRef,
    index: u8,
    kind: SeqKind,
    delete_after_merge: bool,
    rng: &mut StdRng,
) -> Trace {
    let mut trace = source.clone();
    ensure_actor_count(&mut trace, 3);
    push_if_capacity(&mut trace, VmInstr::Fork { from: 0, to: 1 });
    for doc in 0..2 {
        push_if_capacity(
            &mut trace,
            VmInstr::Change {
                doc: doc as u8,
                actor: doc as u8,
                ops: vec![seq_insert_op(obj.clone(), index, kind, rng)],
            },
        );
    }
    push_if_capacity(&mut trace, VmInstr::Merge { into: 0, from: 1 });
    if delete_after_merge {
        push_if_capacity(
            &mut trace,
            VmInstr::Change {
                doc: 0,
                actor: 0,
                ops: vec![match kind {
                    SeqKind::List => VmOp::DeleteSeq {
                        obj: obj.clone(),
                        index,
                    },
                    SeqKind::Text => VmOp::SpliceText {
                        obj: obj.clone(),
                        index,
                        delete: 1,
                        value: rng.random(),
                    },
                }],
            },
        );
    }
    push_if_capacity(
        &mut trace,
        VmInstr::Observe {
            doc: 0,
            object: obj,
            mode: match kind {
                SeqKind::List => VmObserveMode::Ranges,
                SeqKind::Text => VmObserveMode::Text,
            },
            head: VmHeadRef::Current,
            budget: 8,
        },
    );
    trace
}

fn seq_insert_op(obj: VmObjRef, index: u8, kind: SeqKind, rng: &mut StdRng) -> VmOp {
    match kind {
        SeqKind::List => VmOp::Insert {
            obj,
            index,
            value: random_vm_value(rng),
        },
        SeqKind::Text => VmOp::SpliceText {
            obj,
            index,
            delete: 0,
            value: rng.random(),
        },
    }
}

fn seq_update_op(obj: VmObjRef, index: u8, kind: SeqKind, rng: &mut StdRng) -> VmOp {
    match kind {
        SeqKind::List => VmOp::PutSeq {
            obj,
            index,
            value: random_vm_value(rng),
        },
        // Text doesn't have a direct put-at-index operation in the VM. Replacing
        // the character is the closest generic operation and still constructs a
        // staged concurrent sequence history for text objects.
        SeqKind::Text => VmOp::SpliceText {
            obj,
            index,
            delete: 1,
            value: rng.random(),
        },
    }
}

fn push_if_capacity(trace: &mut Trace, instr: VmInstr) {
    if trace.steps.len() < MAX_VM_INSTRUCTIONS {
        trace.steps.push(instr);
    }
}

fn position_variant_count(input: &Trace, position: &Position, ctx: &TraceContext) -> usize {
    match position {
        Position::Instr { step } => instr_variant_count(input, *step, ctx),
        Position::Op { step, op } => nth_op_at(input, *step, *op)
            .map(constructor_swap_variant_count)
            .unwrap_or(0),
        Position::OpObj { .. } => ctx.obj_refs.len(),
        Position::OpValue { .. } => ctx.values.len(),
        Position::OpHydrated { .. } => ctx.hydrated.len(),
        Position::OpMarkExpand { .. } => all_mark_expands().len(),
        Position::ObserveMode { .. } => all_observe_modes().len(),
        Position::ObserveHead { .. } | Position::DiffBefore { .. } | Position::DiffAfter { .. } => {
            ctx.head_refs.len()
        }
    }
}

/// Must agree with the variant list built by [`instr_variants`]: six fixed
/// replacement instructions, three more when another document exists, one
/// random change, plus a committed and a rolled-back transaction.
fn instr_variant_count(input: &Trace, step: usize, ctx: &TraceContext) -> usize {
    let Some(instr) = input.steps.get(step) else {
        return 0;
    };
    let doc = instr_doc(instr).unwrap_or(0);
    let has_other_doc = ctx.docs.iter().any(|candidate| *candidate != doc);
    6 + if has_other_doc { 3 } else { 0 } + 1 + 2
}

fn constructor_swap_variant_count(op: &VmOp) -> usize {
    match op {
        VmOp::Put { .. } => 3,
        VmOp::Delete { .. } => 1,
        VmOp::Insert { .. } | VmOp::PutSeq { .. } => 4,
        VmOp::SpliceList { .. } => 3,
        VmOp::DeleteSeq { .. } => 1,
        VmOp::SpliceText { .. } => 3,
        VmOp::Mark { .. } => 2,
        VmOp::Unmark { .. } => 1,
        VmOp::MakeMap { .. } => 1,
        VmOp::MakeList { .. } | VmOp::MakeText { .. } => 1,
        VmOp::Increment { .. } => 1,
        VmOp::UpdateText { .. } => 1,
        VmOp::UpdateObject { .. } => 1,
        VmOp::BatchCreate { .. } => 2,
    }
}

#[derive(Clone, Debug)]
enum Position {
    Instr { step: usize },
    Op { step: usize, op: usize },
    OpObj { step: usize, op: usize },
    OpValue { step: usize, op: usize },
    OpHydrated { step: usize, op: usize },
    OpMarkExpand { step: usize, op: usize },
    ObserveMode { step: usize },
    ObserveHead { step: usize },
    DiffBefore { step: usize },
    DiffAfter { step: usize },
}

#[derive(Clone, Debug)]
struct TraceContext {
    docs: Vec<u8>,
    actors: Vec<u8>,
    obj_refs: Vec<VmObjRef>,
    head_refs: Vec<VmHeadRef>,
    values: Vec<VmValue>,
    hydrated: Vec<VmHydrated>,
}

impl TraceContext {
    fn new(trace: &Trace) -> Self {
        let mut docs = referenced_docs(&trace.steps);
        docs.push(0);
        docs.sort_unstable();
        docs.dedup();
        let mut actors = trace
            .steps
            .iter()
            .filter_map(|instr| match instr {
                VmInstr::Change { actor, .. } | VmInstr::Transact { actor, .. } => Some(*actor),
                _ => None,
            })
            .collect::<Vec<_>>();
        actors.extend([0, 1, 2]);
        actors.sort_unstable();
        actors.dedup();
        let mut obj_refs = dedup_preserving(collect_obj_refs(trace));
        if !obj_refs.iter().any(|obj| matches!(obj, VmObjRef::Root)) {
            obj_refs.push(VmObjRef::Root);
        }
        let mut head_refs = dedup_preserving(collect_head_refs(trace));
        head_refs.extend([VmHeadRef::Empty, VmHeadRef::Current]);
        head_refs = dedup_preserving(head_refs);
        let mut values = collect_values(trace);
        values.extend([
            VmValue::Null,
            VmValue::Bool { slot: 0 },
            VmValue::Uint { slot: 0 },
        ]);
        values = dedup_preserving(values);
        let mut hydrated = collect_hydrated_values(trace);
        hydrated.extend([
            VmHydrated::Scalar {
                value: VmValue::Null,
            },
            VmHydrated::Map { seed: 0, depth: 1 },
            VmHydrated::List { seed: 0, depth: 1 },
            VmHydrated::Text { slot: 0 },
        ]);
        hydrated = dedup_preserving(hydrated);
        Self {
            docs,
            actors,
            obj_refs,
            head_refs,
            values,
            hydrated,
        }
    }
}

fn trace_positions(trace: &Trace) -> Vec<Position> {
    let mut positions = Vec::new();
    for (step, instr) in trace.steps.iter().enumerate() {
        positions.push(Position::Instr { step });
        match instr {
            VmInstr::Change { ops, .. } | VmInstr::Transact { ops, .. } => {
                for (op, vm_op) in ops.iter().enumerate() {
                    positions.push(Position::Op { step, op });
                    positions.push(Position::OpObj { step, op });
                    match vm_op {
                        VmOp::Put { .. }
                        | VmOp::Insert { .. }
                        | VmOp::PutSeq { .. }
                        | VmOp::Mark { .. } => positions.push(Position::OpValue { step, op }),
                        VmOp::UpdateObject { .. } | VmOp::BatchCreate { .. } => {
                            positions.push(Position::OpHydrated { step, op })
                        }
                        _ => {}
                    }
                    if matches!(vm_op, VmOp::Mark { .. } | VmOp::Unmark { .. }) {
                        positions.push(Position::OpMarkExpand { step, op });
                    }
                }
            }
            VmInstr::Observe { .. } => {
                positions.push(Position::ObserveMode { step });
                positions.push(Position::ObserveHead { step });
            }
            VmInstr::DiffRange { .. } => {
                positions.push(Position::DiffBefore { step });
                positions.push(Position::DiffAfter { step });
            }
            _ => {}
        }
    }
    positions
}

/// Apply variant number `variant` of the mutation at `position`, cloning the
/// input trace exactly once. Out-of-range variants return the input unchanged.
fn apply_position_variant(
    input: &Trace,
    position: &Position,
    ctx: &TraceContext,
    variant: usize,
    rng: &mut StdRng,
) -> Trace {
    let mut trace = input.clone();
    match *position {
        Position::Instr { step } => {
            let mut variants = instr_variants(input, step, ctx, rng);
            if variant < variants.len() {
                trace.steps[step] = variants.swap_remove(variant);
            }
        }
        Position::Op { step, op } => {
            if let Some(vm_op) = nth_op_at(input, step, op) {
                let mut variants = constructor_swap_variants(vm_op, rng);
                if variant < variants.len() {
                    set_op_at(&mut trace, step, op, variants.swap_remove(variant));
                }
            }
        }
        Position::OpObj { step, op } => {
            if let Some(obj) = ctx.obj_refs.get(variant) {
                set_op_obj_at(&mut trace, step, op, obj.clone());
            }
        }
        Position::OpValue { step, op } => {
            if let Some(value) = ctx.values.get(variant) {
                set_op_value_at(&mut trace, step, op, value.clone());
            }
        }
        Position::OpHydrated { step, op } => {
            if let Some(value) = ctx.hydrated.get(variant) {
                set_op_hydrated_at(&mut trace, step, op, value.clone());
            }
        }
        Position::OpMarkExpand { step, op } => {
            if let Some(expand) = all_mark_expands().get(variant) {
                set_op_mark_expand_at(&mut trace, step, op, *expand);
            }
        }
        Position::ObserveMode { step } => {
            if let (Some(mode), Some(VmInstr::Observe { mode: field, .. })) =
                (all_observe_modes().get(variant), trace.steps.get_mut(step))
            {
                *field = *mode;
            }
        }
        Position::ObserveHead { step } => {
            if let (Some(head), Some(VmInstr::Observe { head: field, .. })) =
                (ctx.head_refs.get(variant), trace.steps.get_mut(step))
            {
                *field = head.clone();
            }
        }
        Position::DiffBefore { step } => {
            if let (Some(head), Some(VmInstr::DiffRange { before, .. })) =
                (ctx.head_refs.get(variant), trace.steps.get_mut(step))
            {
                *before = head.clone();
            }
        }
        Position::DiffAfter { step } => {
            if let (Some(head), Some(VmInstr::DiffRange { after, .. })) =
                (ctx.head_refs.get(variant), trace.steps.get_mut(step))
            {
                *after = head.clone();
            }
        }
    }
    trace
}

/// Replacement instructions for the instruction at `step`. The list length
/// must agree with [`instr_variant_count`].
fn instr_variants(
    input: &Trace,
    step: usize,
    ctx: &TraceContext,
    rng: &mut StdRng,
) -> Vec<VmInstr> {
    let Some(instr) = input.steps.get(step) else {
        return Vec::new();
    };
    let doc = instr_doc(instr).unwrap_or(0);
    let actor = ctx.actors.first().copied().unwrap_or(0);
    let obj = ctx.obj_refs.first().cloned().unwrap_or(VmObjRef::Root);
    let head = ctx.head_refs.first().cloned().unwrap_or(VmHeadRef::Current);
    let mut variants = vec![
        VmInstr::SaveLoad { doc },
        VmInstr::UpdateDiffCursor { doc },
        VmInstr::ResetDiffCursor { doc },
        VmInstr::DiffIncremental { doc },
        VmInstr::Observe {
            doc,
            object: obj,
            mode: VmObserveMode::MapGets,
            head,
            budget: 32,
        },
        VmInstr::SaveHeads { doc, slot: 0 },
    ];
    if let Some(other) = ctx.docs.iter().copied().find(|candidate| *candidate != doc) {
        variants.push(VmInstr::Fork {
            from: doc,
            to: other,
        });
        variants.push(VmInstr::Merge {
            into: doc,
            from: other,
        });
        variants.push(VmInstr::Sync {
            left: doc,
            right: other,
            rounds: 8,
        });
    }
    let mut builder = VmBuilder::from_instructions(&input.steps[..step]);
    variants.push(VmInstr::Change {
        doc,
        actor,
        ops: vec![builder.random_op(rng)],
    });
    // Re-run the instruction's own ops (or one random op) through an explicit
    // transaction, once committed and once rolled back.
    let tx_ops = match instr {
        VmInstr::Change { ops, .. } | VmInstr::Transact { ops, .. } => ops.clone(),
        _ => vec![builder.random_op(rng)],
    };
    variants.push(VmInstr::Transact {
        doc,
        actor,
        ops: tx_ops.clone(),
        commit: true,
    });
    variants.push(VmInstr::Transact {
        doc,
        actor,
        ops: tx_ops,
        commit: false,
    });
    variants
}

fn instr_doc(instr: &VmInstr) -> Option<u8> {
    match instr {
        VmInstr::Fork { from, .. } => Some(*from),
        VmInstr::Merge { into, .. } => Some(*into),
        VmInstr::Change { doc, .. }
        | VmInstr::Transact { doc, .. }
        | VmInstr::SaveLoad { doc }
        | VmInstr::Observe { doc, .. }
        | VmInstr::SaveHeads { doc, .. }
        | VmInstr::DiffRange { doc, .. }
        | VmInstr::UpdateDiffCursor { doc }
        | VmInstr::ResetDiffCursor { doc }
        | VmInstr::DiffIncremental { doc } => Some(*doc),
        VmInstr::Sync { left, .. } => Some(*left),
    }
}

fn nth_op_at(trace: &Trace, step: usize, op: usize) -> Option<&VmOp> {
    match trace.steps.get(step)? {
        VmInstr::Change { ops, .. } | VmInstr::Transact { ops, .. } => ops.get(op),
        _ => None,
    }
}

fn set_op_at(trace: &mut Trace, step: usize, op: usize, replacement: VmOp) -> bool {
    match trace.steps.get_mut(step) {
        Some(VmInstr::Change { ops, .. }) | Some(VmInstr::Transact { ops, .. }) => ops
            .get_mut(op)
            .map(|target| *target = replacement)
            .is_some(),
        _ => false,
    }
}

fn set_op_obj_at(trace: &mut Trace, step: usize, op: usize, replacement: VmObjRef) -> bool {
    let Some(VmInstr::Change { ops, .. } | VmInstr::Transact { ops, .. }) =
        trace.steps.get_mut(step)
    else {
        return false;
    };
    let Some(op) = ops.get_mut(op) else {
        return false;
    };
    match op {
        VmOp::Put { obj, .. }
        | VmOp::MakeMap { obj, .. }
        | VmOp::MakeList { obj, .. }
        | VmOp::MakeText { obj, .. }
        | VmOp::Insert { obj, .. }
        | VmOp::PutSeq { obj, .. }
        | VmOp::SpliceList { obj, .. }
        | VmOp::SpliceText { obj, .. }
        | VmOp::UpdateText { obj, .. }
        | VmOp::Increment { obj, .. }
        | VmOp::Mark { obj, .. }
        | VmOp::Unmark { obj, .. }
        | VmOp::Delete { obj, .. }
        | VmOp::DeleteSeq { obj, .. }
        | VmOp::UpdateObject { obj, .. }
        | VmOp::BatchCreate { obj, .. } => {
            *obj = replacement;
            true
        }
    }
}

fn set_op_value_at(trace: &mut Trace, step: usize, op: usize, replacement: VmValue) -> bool {
    let Some(VmInstr::Change { ops, .. } | VmInstr::Transact { ops, .. }) =
        trace.steps.get_mut(step)
    else {
        return false;
    };
    let Some(op) = ops.get_mut(op) else {
        return false;
    };
    match op {
        VmOp::Put { value, .. }
        | VmOp::Insert { value, .. }
        | VmOp::PutSeq { value, .. }
        | VmOp::Mark { value, .. } => {
            *value = replacement;
            true
        }
        _ => false,
    }
}

fn set_op_hydrated_at(trace: &mut Trace, step: usize, op: usize, replacement: VmHydrated) -> bool {
    let Some(VmInstr::Change { ops, .. } | VmInstr::Transact { ops, .. }) =
        trace.steps.get_mut(step)
    else {
        return false;
    };
    let Some(op) = ops.get_mut(op) else {
        return false;
    };
    match op {
        VmOp::UpdateObject { value, .. } | VmOp::BatchCreate { value, .. } => {
            *value = replacement;
            true
        }
        _ => false,
    }
}

fn set_op_mark_expand_at(
    trace: &mut Trace,
    step: usize,
    op: usize,
    replacement: MarkExpand,
) -> bool {
    let Some(VmInstr::Change { ops, .. } | VmInstr::Transact { ops, .. }) =
        trace.steps.get_mut(step)
    else {
        return false;
    };
    let Some(op) = ops.get_mut(op) else {
        return false;
    };
    match op {
        VmOp::Mark { expand, .. } | VmOp::Unmark { expand, .. } => {
            *expand = replacement;
            true
        }
        _ => false,
    }
}

fn all_observe_modes() -> Vec<VmObserveMode> {
    vec![
        VmObserveMode::Shallow,
        VmObserveMode::Hydrate,
        VmObserveMode::Ranges,
        VmObserveMode::Historical,
        VmObserveMode::MapGets,
        VmObserveMode::Text,
        VmObserveMode::Cursors,
        VmObserveMode::Marks,
    ]
}

fn all_mark_expands() -> Vec<MarkExpand> {
    vec![
        MarkExpand::Before,
        MarkExpand::After,
        MarkExpand::Both,
        MarkExpand::None,
    ]
}

fn unique_u8s(mut values: Vec<u8>) -> Vec<u8> {
    values.sort_unstable();
    values.dedup();
    values
}

fn dedup_preserving<T: PartialEq>(values: Vec<T>) -> Vec<T> {
    let mut unique = Vec::new();
    for value in values {
        if !unique.contains(&value) {
            unique.push(value);
        }
    }
    unique
}

#[derive(Clone)]
struct MapAlignmentSite {
    op_index: usize,
    obj: VmObjRef,
    key: u8,
}

#[derive(Clone, Copy)]
enum SeqKind {
    List,
    Text,
}

#[derive(Clone)]
struct SeqAlignmentSite {
    op_index: usize,
    obj: VmObjRef,
    index: u8,
    kind: SeqKind,
}

fn collect_map_alignment_sites(trace: &Trace) -> Vec<MapAlignmentSite> {
    let mut sites = Vec::new();
    for op_index in 0..count_ops(trace) {
        let Some(op) = nth_op(trace, op_index) else {
            continue;
        };
        let Some((obj, key)) = map_tuple(op) else {
            continue;
        };
        sites.push(MapAlignmentSite { op_index, obj, key });
    }
    sites
}

fn collect_seq_alignment_sites(trace: &Trace) -> Vec<SeqAlignmentSite> {
    let mut sites = Vec::new();
    for op_index in 0..count_ops(trace) {
        let Some(op) = nth_op(trace, op_index) else {
            continue;
        };
        let Some((obj, index, kind)) = seq_tuple(op) else {
            continue;
        };
        sites.push(SeqAlignmentSite {
            op_index,
            obj,
            index,
            kind,
        });
    }
    sites
}

fn map_tuple(op: &VmOp) -> Option<(VmObjRef, u8)> {
    match op {
        VmOp::Put { obj, key, .. }
        | VmOp::MakeMap { obj, key }
        | VmOp::MakeList { obj, key }
        | VmOp::MakeText { obj, key }
        | VmOp::Increment { obj, key, .. }
        | VmOp::Delete { obj, key }
        | VmOp::BatchCreate { obj, key, .. } => Some((obj.clone(), *key)),
        _ => None,
    }
}

fn seq_tuple(op: &VmOp) -> Option<(VmObjRef, u8, SeqKind)> {
    match op {
        // VmBuilder allocates MakeList/MakeText keys equal to the object slot
        // the runner will assign, so the key doubles as a slot reference to the
        // created sequence.
        VmOp::MakeList { key, .. } => Some((VmObjRef::Slot { slot: *key }, 0, SeqKind::List)),
        VmOp::MakeText { key, .. } => Some((VmObjRef::Slot { slot: *key }, 0, SeqKind::Text)),
        VmOp::Insert { obj, index, .. }
        | VmOp::PutSeq { obj, index, .. }
        | VmOp::SpliceList { obj, index, .. }
        | VmOp::DeleteSeq { obj, index } => Some((obj.clone(), *index, SeqKind::List)),
        VmOp::SpliceText { obj, index, .. } => Some((obj.clone(), *index, SeqKind::Text)),
        _ => None,
    }
}

fn align_nth_map_tuple(trace: &mut Trace, target: usize, obj: VmObjRef, key: u8) -> bool {
    let Some(op) = nth_op(trace, target).cloned() else {
        return false;
    };
    let replacement = match op {
        VmOp::Put { value, .. } => VmOp::Put { obj, key, value },
        VmOp::MakeMap { .. } => VmOp::MakeMap { obj, key },
        VmOp::MakeList { .. } => VmOp::MakeList { obj, key },
        VmOp::MakeText { .. } => VmOp::MakeText { obj, key },
        VmOp::Increment { value, .. } => VmOp::Increment { obj, key, value },
        VmOp::Delete { .. } => VmOp::Delete { obj, key },
        VmOp::BatchCreate { value, .. } => VmOp::BatchCreate { obj, key, value },
        other => other,
    };
    set_nth_op(trace, target, replacement)
}

fn align_nth_seq_tuple(trace: &mut Trace, target: usize, obj: VmObjRef, index: u8) -> bool {
    let Some(op) = nth_op(trace, target).cloned() else {
        return false;
    };
    let replacement = match op {
        VmOp::Insert { value, .. } => VmOp::Insert { obj, index, value },
        VmOp::PutSeq { value, .. } => VmOp::PutSeq { obj, index, value },
        VmOp::SpliceList { delete, values, .. } => VmOp::SpliceList {
            obj,
            index,
            delete,
            values,
        },
        VmOp::SpliceText { delete, value, .. } => VmOp::SpliceText {
            obj,
            index,
            delete,
            value,
        },
        VmOp::DeleteSeq { .. } => VmOp::DeleteSeq { obj, index },
        other => other,
    };
    set_nth_op(trace, target, replacement)
}

fn count_ops(trace: &Trace) -> usize {
    trace
        .steps
        .iter()
        .map(|instr| match instr {
            VmInstr::Change { ops, .. } | VmInstr::Transact { ops, .. } => ops.len(),
            _ => 0,
        })
        .sum()
}

fn nth_op(trace: &Trace, target: usize) -> Option<&VmOp> {
    let mut seen = 0;
    for instr in &trace.steps {
        if let VmInstr::Change { ops, .. } | VmInstr::Transact { ops, .. } = instr {
            for op in ops {
                if seen == target {
                    return Some(op);
                }
                seen += 1;
            }
        }
    }
    None
}

fn set_nth_op(trace: &mut Trace, target: usize, replacement: VmOp) -> bool {
    let mut seen = 0;
    for instr in &mut trace.steps {
        if let VmInstr::Change { ops, .. } | VmInstr::Transact { ops, .. } = instr {
            for op in ops {
                if seen == target {
                    *op = replacement;
                    return true;
                }
                seen += 1;
            }
        }
    }
    false
}

fn constructor_swap_variants(op: &VmOp, rng: &mut StdRng) -> Vec<VmOp> {
    match op {
        VmOp::Put { obj, key, value } => vec![
            VmOp::Delete {
                obj: obj.clone(),
                key: *key,
            },
            VmOp::Increment {
                obj: obj.clone(),
                key: *key,
                value: rng.random(),
            },
            VmOp::Put {
                obj: obj.clone(),
                key: *key,
                value: value.clone(),
            },
        ],
        VmOp::Delete { obj, key } => vec![VmOp::Put {
            obj: obj.clone(),
            key: *key,
            value: random_vm_value(rng),
        }],
        VmOp::Insert { obj, index, value } | VmOp::PutSeq { obj, index, value } => vec![
            VmOp::DeleteSeq {
                obj: obj.clone(),
                index: *index,
            },
            VmOp::PutSeq {
                obj: obj.clone(),
                index: *index,
                value: value.clone(),
            },
            VmOp::SpliceList {
                obj: obj.clone(),
                index: *index,
                delete: 1,
                values: vec![value.clone()],
            },
            VmOp::Insert {
                obj: obj.clone(),
                index: *index,
                value: value.clone(),
            },
        ],
        VmOp::SpliceList {
            obj,
            index,
            delete,
            values,
        } => vec![
            VmOp::DeleteSeq {
                obj: obj.clone(),
                index: *index,
            },
            VmOp::PutSeq {
                obj: obj.clone(),
                index: *index,
                value: values
                    .first()
                    .cloned()
                    .unwrap_or_else(|| random_vm_value(rng)),
            },
            VmOp::SpliceList {
                obj: obj.clone(),
                index: *index,
                delete: *delete,
                values: values.clone(),
            },
        ],
        VmOp::DeleteSeq { obj, index } => vec![VmOp::PutSeq {
            obj: obj.clone(),
            index: *index,
            value: random_vm_value(rng),
        }],
        VmOp::SpliceText {
            obj,
            index,
            delete,
            value,
        } => vec![
            VmOp::Mark {
                obj: obj.clone(),
                start: *index,
                end: index.saturating_add(1),
                name: rng.random(),
                value: VmValue::Null,
                expand: random_mark_expand(rng),
            },
            VmOp::Unmark {
                obj: obj.clone(),
                start: *index,
                end: index.saturating_add(1),
                name: rng.random(),
                expand: random_mark_expand(rng),
            },
            VmOp::SpliceText {
                obj: obj.clone(),
                index: *index,
                delete: *delete,
                value: *value,
            },
        ],
        VmOp::Mark {
            obj,
            start,
            end,
            name,
            value,
            expand,
        } => vec![
            VmOp::Unmark {
                obj: obj.clone(),
                start: *start,
                end: *end,
                name: *name,
                expand: *expand,
            },
            VmOp::Mark {
                obj: obj.clone(),
                start: *start,
                end: *end,
                name: *name,
                value: value.clone(),
                expand: *expand,
            },
        ],
        VmOp::Unmark {
            obj,
            start,
            end,
            name,
            expand,
        } => vec![VmOp::Mark {
            obj: obj.clone(),
            start: *start,
            end: *end,
            name: *name,
            value: random_vm_value(rng),
            expand: *expand,
        }],
        VmOp::MakeMap { obj, key } => vec![VmOp::Put {
            obj: obj.clone(),
            key: *key,
            value: random_vm_value(rng),
        }],
        VmOp::MakeList { obj, key } | VmOp::MakeText { obj, key } => vec![VmOp::Delete {
            obj: obj.clone(),
            key: *key,
        }],
        VmOp::Increment { obj, key, .. } => vec![VmOp::Put {
            obj: obj.clone(),
            key: *key,
            value: VmValue::Counter { slot: rng.random() },
        }],
        VmOp::UpdateText { obj, value } => vec![VmOp::SpliceText {
            obj: obj.clone(),
            index: 0,
            delete: 0,
            value: *value,
        }],
        VmOp::UpdateObject { obj, value } => vec![VmOp::BatchCreate {
            obj: obj.clone(),
            key: rng.random(),
            value: value.clone(),
        }],
        VmOp::BatchCreate { obj, key, value } => vec![
            VmOp::UpdateObject {
                obj: obj.clone(),
                value: value.clone(),
            },
            VmOp::BatchCreate {
                obj: obj.clone(),
                key: *key,
                value: value.clone(),
            },
        ],
    }
}

fn mutation_rounds(rng: &mut StdRng) -> usize {
    match rng.random_range(0..100) {
        0..=64 => 1,
        65..=84 => 2,
        85..=94 => 4,
        95..=98 => 8,
        _ => 16,
    }
}

fn mutate_once(trace: &mut Trace, rng: &mut StdRng) {
    match rng.random_range(0..100) {
        // Add small semantic state-space expansions. These are not saved seeds
        // or copied genes; they are grammar-level transitions that create the
        // concurrency, observation, and deletion states that single-field
        // mutation reaches only rarely.
        // The VM is now the primary mutation substrate. Most mutations either
        // add a fresh VM program or modify an existing one.
        0..=34 => insert_vm_program(trace, rng),
        35..=64 => mutate_existing_vm_or_insert(trace, rng),
        // Copy typed fields between existing subexpressions. This gives the
        // search a way to satisfy equality constraints such as same key, index,
        // object, or saved-head slot without relying on blind byte mutation.
        65..=79 => copy_compatible_field(trace, rng),
        80..=89 => swap_op_constructor_reusing_fields(trace, rng),
        // Keep a small amount of whole-step structure mutation so old corpus
        // traces and checkpoint prefixes can still be rearranged/shrunk without
        // copying arbitrary chunks around as genes.
        90..=93 => delete_step(trace, rng),
        94..=96 => duplicate_step(trace, rng),
        97..=98 => swap_adjacent_steps(trace, rng),
        _ => append_vm_instruction_or_insert(trace, rng),
    }
}

fn trim_trace(trace: &mut Trace, rng: &mut StdRng) {
    while trace.steps.len() > MAX_VM_INSTRUCTIONS {
        let index = rng.random_range(0..trace.steps.len());
        trace.steps.remove(index);
    }
}

fn delete_step(trace: &mut Trace, rng: &mut StdRng) {
    if trace.steps.len() > 1 {
        let index = rng.random_range(0..trace.steps.len());
        trace.steps.remove(index);
    }
}

fn duplicate_step(trace: &mut Trace, rng: &mut StdRng) {
    if trace.steps.is_empty() {
        return;
    }
    let index = rng.random_range(0..trace.steps.len());
    let step = trace.steps[index].clone();
    trace.steps.insert(index + 1, step);
}

fn swap_adjacent_steps(trace: &mut Trace, rng: &mut StdRng) {
    if trace.steps.len() > 1 {
        let index = rng.random_range(0..trace.steps.len() - 1);
        trace.steps.swap(index, index + 1);
    }
}

fn insert_vm_program(trace: &mut Trace, rng: &mut StdRng) {
    let index = rng.random_range(0..=trace.steps.len());
    trace.steps.splice(index..index, random_vm_program(rng));
}

fn mutate_existing_vm_or_insert(trace: &mut Trace, rng: &mut StdRng) {
    if trace.steps.is_empty() {
        insert_vm_program(trace, rng);
        return;
    }

    match rng.random_range(0..100) {
        0..=24 => insert_vm_instruction(&mut trace.steps, rng),
        25..=39 => delete_vm_instruction(&mut trace.steps, rng),
        40..=54 => duplicate_vm_instruction(&mut trace.steps, rng),
        55..=64 => swap_vm_instructions(&mut trace.steps, rng),
        65..=84 => mutate_vm_instruction(&mut trace.steps, rng),
        _ => splice_vm_program(&mut trace.steps, rng),
    }
}

fn append_vm_instruction_or_insert(trace: &mut Trace, rng: &mut StdRng) {
    let instr = VmBuilder::from_instructions(&trace.steps).random_instr(rng);
    trace.steps.push(instr);
}

fn insert_vm_instruction(instructions: &mut Vec<VmInstr>, rng: &mut StdRng) {
    let index = rng.random_range(0..=instructions.len());
    let instr = VmBuilder::from_instructions(instructions).random_instr(rng);
    instructions.insert(index, instr);
}

fn delete_vm_instruction(instructions: &mut Vec<VmInstr>, rng: &mut StdRng) {
    if !instructions.is_empty() {
        let index = rng.random_range(0..instructions.len());
        instructions.remove(index);
    }
}

fn duplicate_vm_instruction(instructions: &mut Vec<VmInstr>, rng: &mut StdRng) {
    if !instructions.is_empty() {
        let index = rng.random_range(0..instructions.len());
        let instr = instructions[index].clone();
        instructions.insert(index + 1, instr);
    }
}

fn swap_vm_instructions(instructions: &mut [VmInstr], rng: &mut StdRng) {
    if instructions.len() > 1 {
        let index = rng.random_range(0..instructions.len() - 1);
        instructions.swap(index, index + 1);
    }
}

fn splice_vm_program(instructions: &mut Vec<VmInstr>, rng: &mut StdRng) {
    let index = rng.random_range(0..=instructions.len());
    let mut program = random_vm_program(rng);
    if !program.is_empty() && rng.random_range(0..100) < 70 {
        let keep = rng.random_range(1..=program.len().min(8));
        program.truncate(keep);
    }
    instructions.splice(index..index, program);
}

fn mutate_vm_instruction(instructions: &mut [VmInstr], rng: &mut StdRng) {
    if instructions.is_empty() {
        return;
    }

    let index = rng.random_range(0..instructions.len());
    match &mut instructions[index] {
        VmInstr::Fork { from, to } => mutate_u8_pair(from, to, rng),
        VmInstr::Merge { into, from } => mutate_u8_pair(into, from, rng),
        VmInstr::Change { doc, actor, ops } => match rng.random_range(0..4) {
            0 => *doc = mutate_byte(*doc, rng),
            1 => *actor = mutate_byte(*actor, rng),
            2 if !ops.is_empty() => mutate_vm_op(ops, rng),
            _ => ops.push(VmBuilder::default().random_op(rng)),
        },
        VmInstr::Transact {
            doc,
            actor,
            ops,
            commit,
        } => match rng.random_range(0..5) {
            0 => *doc = mutate_byte(*doc, rng),
            1 => *actor = mutate_byte(*actor, rng),
            2 => *commit = !*commit,
            3 if !ops.is_empty() => mutate_vm_op(ops, rng),
            _ => ops.push(VmBuilder::default().random_op(rng)),
        },
        VmInstr::SaveLoad { doc }
        | VmInstr::UpdateDiffCursor { doc }
        | VmInstr::ResetDiffCursor { doc }
        | VmInstr::DiffIncremental { doc } => *doc = mutate_byte(*doc, rng),
        VmInstr::Observe {
            doc,
            object,
            mode,
            head,
            budget,
        } => match rng.random_range(0..5) {
            0 => *doc = mutate_byte(*doc, rng),
            1 => *object = random_vm_obj(rng, 16),
            2 => *mode = random_observe_mode(rng),
            3 => *head = random_vm_head(rng),
            _ => *budget = mutate_byte(*budget, rng).clamp(1, 32),
        },
        VmInstr::SaveHeads { doc, slot } => mutate_u8_pair(doc, slot, rng),
        VmInstr::DiffRange { doc, before, after } => match rng.random_range(0..3) {
            0 => *doc = mutate_byte(*doc, rng),
            1 => *before = random_vm_head(rng),
            _ => *after = random_vm_head(rng),
        },
        VmInstr::Sync {
            left,
            right,
            rounds,
        } => match rng.random_range(0..3) {
            0 => *left = mutate_byte(*left, rng),
            1 => *right = mutate_byte(*right, rng),
            _ => *rounds = mutate_byte(*rounds, rng).max(1),
        },
    }
}

fn mutate_vm_op(ops: &mut [VmOp], rng: &mut StdRng) {
    let index = rng.random_range(0..ops.len());
    match &mut ops[index] {
        VmOp::Put { obj, key, value } => mutate_obj_key_value(obj, key, value, rng),
        VmOp::MakeMap { obj, key } | VmOp::MakeList { obj, key } | VmOp::MakeText { obj, key } => {
            mutate_obj_key(obj, key, rng)
        }
        VmOp::Insert { obj, index, value } | VmOp::PutSeq { obj, index, value } => {
            mutate_obj_index_value(obj, index, value, rng)
        }
        VmOp::SpliceList {
            obj,
            index,
            delete,
            values,
        } => match rng.random_range(0..5) {
            0 => *obj = random_vm_obj(rng, 16),
            1 => *index = mutate_byte(*index, rng),
            2 => *delete = mutate_byte(*delete, rng),
            3 if !values.is_empty() => {
                let value_index = rng.random_range(0..values.len());
                values[value_index] = random_vm_value(rng);
            }
            _ => values.push(random_vm_value(rng)),
        },
        VmOp::SpliceText {
            obj,
            index,
            delete,
            value,
        } => match rng.random_range(0..4) {
            0 => *obj = random_vm_obj(rng, 16),
            1 => *index = mutate_byte(*index, rng),
            2 => *delete = mutate_byte(*delete, rng),
            _ => *value = mutate_byte(*value, rng),
        },
        VmOp::UpdateText { obj, value } => match rng.random_range(0..2) {
            0 => *obj = random_vm_obj(rng, 16),
            _ => *value = mutate_byte(*value, rng),
        },
        VmOp::Increment { obj, key, value } => match rng.random_range(0..3) {
            0 => *obj = random_vm_obj(rng, 16),
            1 => *key = mutate_byte(*key, rng),
            _ => *value = value.wrapping_add(rng.random_range(-8..=8)),
        },
        VmOp::Mark {
            obj,
            start,
            end,
            name,
            value,
            expand,
        } => match rng.random_range(0..6) {
            0 => *obj = random_vm_obj(rng, 16),
            1 => *start = mutate_byte(*start, rng),
            2 => *end = mutate_byte(*end, rng),
            3 => *name = mutate_byte(*name, rng),
            4 => *value = random_vm_value(rng),
            _ => *expand = random_mark_expand(rng),
        },
        VmOp::Unmark {
            obj,
            start,
            end,
            name,
            expand,
        } => match rng.random_range(0..5) {
            0 => *obj = random_vm_obj(rng, 16),
            1 => *start = mutate_byte(*start, rng),
            2 => *end = mutate_byte(*end, rng),
            3 => *name = mutate_byte(*name, rng),
            _ => *expand = random_mark_expand(rng),
        },
        VmOp::Delete { obj, key } => mutate_obj_key(obj, key, rng),
        VmOp::DeleteSeq { obj, index } => match rng.random_range(0..2) {
            0 => *obj = random_vm_obj(rng, 16),
            _ => *index = mutate_byte(*index, rng),
        },
        VmOp::UpdateObject { obj, value } => match rng.random_range(0..2) {
            0 => *obj = random_vm_obj(rng, 16),
            _ => *value = random_vm_hydrated(rng),
        },
        VmOp::BatchCreate { obj, key, value } => match rng.random_range(0..3) {
            0 => *obj = random_vm_obj(rng, 16),
            1 => *key = mutate_byte(*key, rng),
            _ => *value = random_vm_hydrated(rng),
        },
    }
}

fn mutate_obj_key_value(obj: &mut VmObjRef, key: &mut u8, value: &mut VmValue, rng: &mut StdRng) {
    match rng.random_range(0..3) {
        0 => *obj = random_vm_obj(rng, 16),
        1 => *key = mutate_byte(*key, rng),
        _ => *value = random_vm_value(rng),
    }
}

fn mutate_obj_key(obj: &mut VmObjRef, key: &mut u8, rng: &mut StdRng) {
    if rng.random_range(0..2) == 0 {
        *obj = random_vm_obj(rng, 16);
    } else {
        *key = mutate_byte(*key, rng);
    }
}

fn mutate_obj_index_value(
    obj: &mut VmObjRef,
    index: &mut u8,
    value: &mut VmValue,
    rng: &mut StdRng,
) {
    match rng.random_range(0..3) {
        0 => *obj = random_vm_obj(rng, 16),
        1 => *index = mutate_byte(*index, rng),
        _ => *value = random_vm_value(rng),
    }
}

fn mutate_u8_pair(left: &mut u8, right: &mut u8, rng: &mut StdRng) {
    if rng.random_range(0..2) == 0 {
        *left = mutate_byte(*left, rng);
    } else {
        *right = mutate_byte(*right, rng);
    }
}

fn mutate_byte(value: u8, rng: &mut StdRng) -> u8 {
    match rng.random_range(0..4) {
        0 => value.wrapping_add(1),
        1 => value.wrapping_sub(1),
        2 => value ^ (1 << rng.random_range(0..8)),
        _ => rng.random(),
    }
}

fn random_vm_program(rng: &mut StdRng) -> Vec<VmInstr> {
    let instruction_count = rng.random_range(8..=40);
    let mut builder = VmBuilder::default();
    (0..instruction_count)
        .map(|_| builder.random_instr(rng))
        .collect()
}

fn referenced_docs(instructions: &[VmInstr]) -> Vec<u8> {
    let mut docs = Vec::new();
    for instr in instructions {
        match instr {
            VmInstr::Fork { from, to }
            | VmInstr::Merge {
                into: from,
                from: to,
            } => {
                docs.push(*from);
                docs.push(*to);
            }
            VmInstr::Change { doc, .. }
            | VmInstr::Transact { doc, .. }
            | VmInstr::SaveLoad { doc }
            | VmInstr::Observe { doc, .. }
            | VmInstr::SaveHeads { doc, .. }
            | VmInstr::DiffRange { doc, .. }
            | VmInstr::UpdateDiffCursor { doc }
            | VmInstr::ResetDiffCursor { doc }
            | VmInstr::DiffIncremental { doc } => docs.push(*doc),
            VmInstr::Sync { left, right, .. } => {
                docs.push(*left);
                docs.push(*right);
            }
        }
    }
    docs
}

fn rebase_instr(instr: &mut VmInstr, prefix: &VmBuilder, rng: &mut StdRng) {
    match instr {
        VmInstr::Fork { from, .. } => *from = (*from).min(prefix.docs),
        VmInstr::Merge { into, from }
        | VmInstr::Sync {
            left: into,
            right: from,
            ..
        } => {
            *into = (*into).min(prefix.docs);
            *from = (*from).min(prefix.docs.max(1));
        }
        VmInstr::Change { doc, ops, .. } | VmInstr::Transact { doc, ops, .. } => {
            *doc = (*doc).min(prefix.docs.max(1));
            for op in ops {
                rebase_op(op, prefix, rng);
            }
        }
        VmInstr::Observe {
            doc, object, head, ..
        } => {
            *doc = (*doc).min(prefix.docs.max(1));
            *object = rebase_obj_ref(object, prefix, ObjNeed::Any, rng);
            if !has_saved_head(prefix) && matches!(head, VmHeadRef::Slot { .. }) {
                *head = VmHeadRef::Current;
            }
        }
        VmInstr::SaveLoad { doc }
        | VmInstr::SaveHeads { doc, .. }
        | VmInstr::DiffRange { doc, .. }
        | VmInstr::UpdateDiffCursor { doc }
        | VmInstr::ResetDiffCursor { doc }
        | VmInstr::DiffIncremental { doc } => *doc = (*doc).min(prefix.docs.max(1)),
    }
}

fn rebase_op(op: &mut VmOp, prefix: &VmBuilder, rng: &mut StdRng) {
    match op {
        VmOp::Put { obj, .. }
        | VmOp::MakeMap { obj, .. }
        | VmOp::MakeList { obj, .. }
        | VmOp::MakeText { obj, .. }
        | VmOp::Increment { obj, .. }
        | VmOp::Delete { obj, .. }
        | VmOp::BatchCreate { obj, .. } => {
            *obj = rebase_obj_ref(obj, prefix, ObjNeed::Map, rng);
        }
        VmOp::Insert { obj, .. }
        | VmOp::PutSeq { obj, .. }
        | VmOp::SpliceList { obj, .. }
        | VmOp::DeleteSeq { obj, .. } => {
            *obj = rebase_obj_ref(obj, prefix, ObjNeed::List, rng);
        }
        VmOp::SpliceText { obj, .. }
        | VmOp::UpdateText { obj, .. }
        | VmOp::Mark { obj, .. }
        | VmOp::Unmark { obj, .. } => {
            *obj = rebase_obj_ref(obj, prefix, ObjNeed::Text, rng);
        }
        VmOp::UpdateObject { obj, .. } => {
            *obj = rebase_obj_ref(obj, prefix, ObjNeed::Any, rng);
        }
    }
}

#[derive(Clone, Copy)]
enum ObjNeed {
    Any,
    Map,
    List,
    Text,
}

fn rebase_obj_ref(obj: &VmObjRef, prefix: &VmBuilder, need: ObjNeed, rng: &mut StdRng) -> VmObjRef {
    if obj_ref_plausible(obj, prefix) {
        return obj.clone();
    }
    match need {
        ObjNeed::Any => prefix.random_any_ref(rng),
        ObjNeed::Map => prefix.random_map_ref(rng),
        ObjNeed::List => prefix.random_list_ref(rng),
        ObjNeed::Text => prefix.random_text_ref(rng),
    }
}

fn obj_ref_plausible(obj: &VmObjRef, prefix: &VmBuilder) -> bool {
    match obj {
        VmObjRef::Root => true,
        VmObjRef::Slot { slot } => *slot < prefix.objects,
        VmObjRef::Recent { back } => *back <= prefix.objects,
        VmObjRef::Invalid { .. } => false,
    }
}

fn has_saved_head(prefix: &VmBuilder) -> bool {
    prefix.saved_heads > 0
}

fn copy_compatible_field(trace: &mut Trace, rng: &mut StdRng) {
    match rng.random_range(0..3) {
        0 => copy_u8_field(trace, rng),
        1 => copy_obj_ref_field(trace, rng),
        _ => copy_head_ref_field(trace, rng),
    }
}

fn copy_u8_field(trace: &mut Trace, rng: &mut StdRng) {
    let values = collect_u8_fields(trace);
    let dests = count_u8_fields(trace);
    if values.is_empty() || dests == 0 {
        return;
    }
    let value = values[rng.random_range(0..values.len())];
    let dest = rng.random_range(0..dests);
    let mut seen = 0;
    set_nth_u8_field(trace, dest, value, &mut seen);
}

fn collect_u8_fields(trace: &Trace) -> Vec<u8> {
    let mut values = Vec::new();
    for instr in &trace.steps {
        collect_instr_u8(instr, &mut values);
    }
    values
}

fn collect_instr_u8(instr: &VmInstr, values: &mut Vec<u8>) {
    match instr {
        VmInstr::Fork { from, to }
        | VmInstr::Merge {
            into: from,
            from: to,
        } => {
            values.push(*from);
            values.push(*to);
        }
        VmInstr::Change { doc, actor, ops }
        | VmInstr::Transact {
            doc, actor, ops, ..
        } => {
            values.push(*doc);
            values.push(*actor);
            for op in ops {
                collect_op_u8(op, values);
            }
        }
        VmInstr::SaveLoad { doc }
        | VmInstr::UpdateDiffCursor { doc }
        | VmInstr::ResetDiffCursor { doc }
        | VmInstr::DiffIncremental { doc } => values.push(*doc),
        VmInstr::Observe { doc, budget, .. } => {
            values.push(*doc);
            values.push(*budget);
        }
        VmInstr::SaveHeads { doc, slot } => {
            values.push(*doc);
            values.push(*slot);
        }
        VmInstr::DiffRange { doc, before, after } => {
            values.push(*doc);
            collect_head_u8(before, values);
            collect_head_u8(after, values);
        }
        VmInstr::Sync {
            left,
            right,
            rounds,
        } => {
            values.push(*left);
            values.push(*right);
            values.push(*rounds);
        }
    }
}

fn collect_op_u8(op: &VmOp, values: &mut Vec<u8>) {
    match op {
        VmOp::Put { obj, key, value } => {
            collect_obj_u8(obj, values);
            values.push(*key);
            collect_value_u8(value, values);
        }
        VmOp::MakeMap { obj, key } | VmOp::MakeList { obj, key } | VmOp::MakeText { obj, key } => {
            collect_obj_u8(obj, values);
            values.push(*key);
        }
        VmOp::Insert { obj, index, value } | VmOp::PutSeq { obj, index, value } => {
            collect_obj_u8(obj, values);
            values.push(*index);
            collect_value_u8(value, values);
        }
        VmOp::SpliceList {
            obj,
            index,
            delete,
            values: op_values,
        } => {
            collect_obj_u8(obj, values);
            values.push(*index);
            values.push(*delete);
            for value in op_values {
                collect_value_u8(value, values);
            }
        }
        VmOp::SpliceText {
            obj,
            index,
            delete,
            value,
        } => {
            collect_obj_u8(obj, values);
            values.push(*index);
            values.push(*delete);
            values.push(*value);
        }
        VmOp::UpdateText { obj, value } => {
            collect_obj_u8(obj, values);
            values.push(*value);
        }
        VmOp::Increment { obj, key, value } => {
            collect_obj_u8(obj, values);
            values.push(*key);
            values.push(*value as u8);
        }
        VmOp::Mark {
            obj,
            start,
            end,
            name,
            value,
            ..
        } => {
            collect_obj_u8(obj, values);
            values.extend([*start, *end, *name]);
            collect_value_u8(value, values);
        }
        VmOp::Unmark {
            obj,
            start,
            end,
            name,
            ..
        } => {
            collect_obj_u8(obj, values);
            values.extend([*start, *end, *name]);
        }
        VmOp::Delete { obj, key } => {
            collect_obj_u8(obj, values);
            values.push(*key);
        }
        VmOp::DeleteSeq { obj, index } => {
            collect_obj_u8(obj, values);
            values.push(*index);
        }
        VmOp::UpdateObject { obj, value } => {
            collect_obj_u8(obj, values);
            collect_hydrated_u8(value, values);
        }
        VmOp::BatchCreate { obj, key, value } => {
            collect_obj_u8(obj, values);
            values.push(*key);
            collect_hydrated_u8(value, values);
        }
    }
}

fn collect_obj_u8(obj: &VmObjRef, values: &mut Vec<u8>) {
    match obj {
        VmObjRef::Root => {}
        VmObjRef::Slot { slot } | VmObjRef::Invalid { slot } => values.push(*slot),
        VmObjRef::Recent { back } => values.push(*back),
    }
}

fn collect_head_u8(head: &VmHeadRef, values: &mut Vec<u8>) {
    if let VmHeadRef::Slot { slot } = head {
        values.push(*slot);
    }
}

fn collect_value_u8(value: &VmValue, values: &mut Vec<u8>) {
    match value {
        VmValue::Null => {}
        VmValue::Bool { slot }
        | VmValue::Int { slot }
        | VmValue::Uint { slot }
        | VmValue::Str { slot }
        | VmValue::Counter { slot } => values.push(*slot),
    }
}

fn collect_hydrated_u8(value: &VmHydrated, values: &mut Vec<u8>) {
    match value {
        VmHydrated::Scalar { value } => collect_value_u8(value, values),
        VmHydrated::Map { seed, depth } | VmHydrated::List { seed, depth } => {
            values.push(*seed);
            values.push(*depth);
        }
        VmHydrated::Text { slot } => values.push(*slot),
    }
}

fn count_u8_fields(trace: &Trace) -> usize {
    collect_u8_fields(trace).len()
}

fn set_nth_u8_field(trace: &mut Trace, target: usize, value: u8, seen: &mut usize) -> bool {
    for instr in &mut trace.steps {
        if set_instr_u8(instr, target, value, seen) {
            return true;
        }
    }
    false
}

fn maybe_set_u8(field: &mut u8, target: usize, value: u8, seen: &mut usize) -> bool {
    if *seen == target {
        *field = value;
        true
    } else {
        *seen += 1;
        false
    }
}

fn maybe_set_i8(field: &mut i8, target: usize, value: u8, seen: &mut usize) -> bool {
    if *seen == target {
        *field = value as i8;
        true
    } else {
        *seen += 1;
        false
    }
}

fn set_instr_u8(instr: &mut VmInstr, target: usize, value: u8, seen: &mut usize) -> bool {
    match instr {
        VmInstr::Fork { from, to }
        | VmInstr::Merge {
            into: from,
            from: to,
        } => maybe_set_u8(from, target, value, seen) || maybe_set_u8(to, target, value, seen),
        VmInstr::Change { doc, actor, ops }
        | VmInstr::Transact {
            doc, actor, ops, ..
        } => {
            maybe_set_u8(doc, target, value, seen)
                || maybe_set_u8(actor, target, value, seen)
                || ops.iter_mut().any(|op| set_op_u8(op, target, value, seen))
        }
        VmInstr::SaveLoad { doc }
        | VmInstr::UpdateDiffCursor { doc }
        | VmInstr::ResetDiffCursor { doc }
        | VmInstr::DiffIncremental { doc } => maybe_set_u8(doc, target, value, seen),
        VmInstr::Observe { doc, budget, .. } => {
            maybe_set_u8(doc, target, value, seen) || maybe_set_u8(budget, target, value, seen)
        }
        VmInstr::SaveHeads { doc, slot } => {
            maybe_set_u8(doc, target, value, seen) || maybe_set_u8(slot, target, value, seen)
        }
        VmInstr::DiffRange { doc, before, after } => {
            maybe_set_u8(doc, target, value, seen)
                || set_head_u8(before, target, value, seen)
                || set_head_u8(after, target, value, seen)
        }
        VmInstr::Sync {
            left,
            right,
            rounds,
        } => {
            maybe_set_u8(left, target, value, seen)
                || maybe_set_u8(right, target, value, seen)
                || maybe_set_u8(rounds, target, value, seen)
        }
    }
}

fn set_op_u8(op: &mut VmOp, target: usize, value: u8, seen: &mut usize) -> bool {
    match op {
        VmOp::Put { obj, key, value: v } => {
            set_obj_u8(obj, target, value, seen)
                || maybe_set_u8(key, target, value, seen)
                || set_value_u8(v, target, value, seen)
        }
        VmOp::MakeMap { obj, key } | VmOp::MakeList { obj, key } | VmOp::MakeText { obj, key } => {
            set_obj_u8(obj, target, value, seen) || maybe_set_u8(key, target, value, seen)
        }
        VmOp::Insert {
            obj,
            index,
            value: v,
        }
        | VmOp::PutSeq {
            obj,
            index,
            value: v,
        } => {
            set_obj_u8(obj, target, value, seen)
                || maybe_set_u8(index, target, value, seen)
                || set_value_u8(v, target, value, seen)
        }
        VmOp::SpliceList {
            obj,
            index,
            delete,
            values,
        } => {
            set_obj_u8(obj, target, value, seen)
                || maybe_set_u8(index, target, value, seen)
                || maybe_set_u8(delete, target, value, seen)
                || values
                    .iter_mut()
                    .any(|v| set_value_u8(v, target, value, seen))
        }
        VmOp::SpliceText {
            obj,
            index,
            delete,
            value: v,
        } => {
            set_obj_u8(obj, target, value, seen)
                || maybe_set_u8(index, target, value, seen)
                || maybe_set_u8(delete, target, value, seen)
                || maybe_set_u8(v, target, value, seen)
        }
        VmOp::UpdateText { obj, value: v } => {
            set_obj_u8(obj, target, value, seen) || maybe_set_u8(v, target, value, seen)
        }
        VmOp::Increment { obj, key, value: v } => {
            set_obj_u8(obj, target, value, seen)
                || maybe_set_u8(key, target, value, seen)
                || maybe_set_i8(v, target, value, seen)
        }
        VmOp::Mark {
            obj,
            start,
            end,
            name,
            value: v,
            ..
        } => {
            set_obj_u8(obj, target, value, seen)
                || maybe_set_u8(start, target, value, seen)
                || maybe_set_u8(end, target, value, seen)
                || maybe_set_u8(name, target, value, seen)
                || set_value_u8(v, target, value, seen)
        }
        VmOp::Unmark {
            obj,
            start,
            end,
            name,
            ..
        } => {
            set_obj_u8(obj, target, value, seen)
                || maybe_set_u8(start, target, value, seen)
                || maybe_set_u8(end, target, value, seen)
                || maybe_set_u8(name, target, value, seen)
        }
        VmOp::Delete { obj, key } => {
            set_obj_u8(obj, target, value, seen) || maybe_set_u8(key, target, value, seen)
        }
        VmOp::DeleteSeq { obj, index } => {
            set_obj_u8(obj, target, value, seen) || maybe_set_u8(index, target, value, seen)
        }
        VmOp::UpdateObject { obj, value: v } => {
            set_obj_u8(obj, target, value, seen) || set_hydrated_u8(v, target, value, seen)
        }
        VmOp::BatchCreate { obj, key, value: v } => {
            set_obj_u8(obj, target, value, seen)
                || maybe_set_u8(key, target, value, seen)
                || set_hydrated_u8(v, target, value, seen)
        }
    }
}

fn set_obj_u8(obj: &mut VmObjRef, target: usize, value: u8, seen: &mut usize) -> bool {
    match obj {
        VmObjRef::Root => false,
        VmObjRef::Slot { slot } | VmObjRef::Invalid { slot } => {
            maybe_set_u8(slot, target, value, seen)
        }
        VmObjRef::Recent { back } => maybe_set_u8(back, target, value, seen),
    }
}

fn set_head_u8(head: &mut VmHeadRef, target: usize, value: u8, seen: &mut usize) -> bool {
    if let VmHeadRef::Slot { slot } = head {
        maybe_set_u8(slot, target, value, seen)
    } else {
        false
    }
}

fn set_value_u8(value: &mut VmValue, target: usize, new_value: u8, seen: &mut usize) -> bool {
    match value {
        VmValue::Null => false,
        VmValue::Bool { slot }
        | VmValue::Int { slot }
        | VmValue::Uint { slot }
        | VmValue::Str { slot }
        | VmValue::Counter { slot } => maybe_set_u8(slot, target, new_value, seen),
    }
}

fn set_hydrated_u8(value: &mut VmHydrated, target: usize, new_value: u8, seen: &mut usize) -> bool {
    match value {
        VmHydrated::Scalar { value } => set_value_u8(value, target, new_value, seen),
        VmHydrated::Map { seed, depth } | VmHydrated::List { seed, depth } => {
            maybe_set_u8(seed, target, new_value, seen)
                || maybe_set_u8(depth, target, new_value, seen)
        }
        VmHydrated::Text { slot } => maybe_set_u8(slot, target, new_value, seen),
    }
}

fn copy_obj_ref_field(trace: &mut Trace, rng: &mut StdRng) {
    let values = collect_obj_refs(trace);
    let dests = count_obj_refs(trace);
    if values.is_empty() || dests == 0 {
        return;
    }
    let value = values[rng.random_range(0..values.len())].clone();
    let dest = rng.random_range(0..dests);
    let mut seen = 0;
    set_nth_obj_ref(trace, dest, value, &mut seen);
}

fn collect_obj_refs(trace: &Trace) -> Vec<VmObjRef> {
    let mut refs = Vec::new();
    for instr in &trace.steps {
        match instr {
            VmInstr::Change { ops, .. } => {
                for op in ops {
                    collect_op_obj_refs(op, &mut refs);
                }
            }
            VmInstr::Observe { object, .. } => refs.push(object.clone()),
            _ => {}
        }
    }
    refs
}

fn collect_op_obj_refs(op: &VmOp, refs: &mut Vec<VmObjRef>) {
    match op {
        VmOp::Put { obj, .. }
        | VmOp::MakeMap { obj, .. }
        | VmOp::MakeList { obj, .. }
        | VmOp::MakeText { obj, .. }
        | VmOp::Insert { obj, .. }
        | VmOp::PutSeq { obj, .. }
        | VmOp::SpliceList { obj, .. }
        | VmOp::SpliceText { obj, .. }
        | VmOp::UpdateText { obj, .. }
        | VmOp::Increment { obj, .. }
        | VmOp::Mark { obj, .. }
        | VmOp::Unmark { obj, .. }
        | VmOp::Delete { obj, .. }
        | VmOp::DeleteSeq { obj, .. }
        | VmOp::UpdateObject { obj, .. }
        | VmOp::BatchCreate { obj, .. } => refs.push(obj.clone()),
    }
}

fn collect_values(trace: &Trace) -> Vec<VmValue> {
    let mut values = Vec::new();
    for instr in &trace.steps {
        if let VmInstr::Change { ops, .. } | VmInstr::Transact { ops, .. } = instr {
            for op in ops {
                collect_op_values(op, &mut values);
            }
        }
    }
    values
}

fn collect_op_values(op: &VmOp, values: &mut Vec<VmValue>) {
    match op {
        VmOp::Put { value, .. }
        | VmOp::Insert { value, .. }
        | VmOp::PutSeq { value, .. }
        | VmOp::Mark { value, .. } => values.push(value.clone()),
        VmOp::SpliceList {
            values: op_values, ..
        } => values.extend(op_values.iter().cloned()),
        VmOp::UpdateObject { value, .. } | VmOp::BatchCreate { value, .. } => {
            collect_hydrated_values_inner(value, values)
        }
        _ => {}
    }
}

fn collect_hydrated_values(trace: &Trace) -> Vec<VmHydrated> {
    let mut values = Vec::new();
    for instr in &trace.steps {
        if let VmInstr::Change { ops, .. } | VmInstr::Transact { ops, .. } = instr {
            for op in ops {
                match op {
                    VmOp::UpdateObject { value, .. } | VmOp::BatchCreate { value, .. } => {
                        values.push(value.clone())
                    }
                    _ => {}
                }
            }
        }
    }
    values
}

fn collect_hydrated_values_inner(value: &VmHydrated, values: &mut Vec<VmValue>) {
    if let VmHydrated::Scalar { value } = value {
        values.push(value.clone());
    }
}

fn count_obj_refs(trace: &Trace) -> usize {
    collect_obj_refs(trace).len()
}

fn set_nth_obj_ref(trace: &mut Trace, target: usize, value: VmObjRef, seen: &mut usize) -> bool {
    for instr in &mut trace.steps {
        match instr {
            VmInstr::Change { ops, .. } => {
                if ops
                    .iter_mut()
                    .any(|op| set_op_obj_ref(op, target, value.clone(), seen))
                {
                    return true;
                }
            }
            VmInstr::Observe { object, .. } => {
                if maybe_set_obj_ref(object, target, value.clone(), seen) {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

fn set_op_obj_ref(op: &mut VmOp, target: usize, value: VmObjRef, seen: &mut usize) -> bool {
    match op {
        VmOp::Put { obj, .. }
        | VmOp::MakeMap { obj, .. }
        | VmOp::MakeList { obj, .. }
        | VmOp::MakeText { obj, .. }
        | VmOp::Insert { obj, .. }
        | VmOp::PutSeq { obj, .. }
        | VmOp::SpliceList { obj, .. }
        | VmOp::SpliceText { obj, .. }
        | VmOp::UpdateText { obj, .. }
        | VmOp::Increment { obj, .. }
        | VmOp::Mark { obj, .. }
        | VmOp::Unmark { obj, .. }
        | VmOp::Delete { obj, .. }
        | VmOp::DeleteSeq { obj, .. }
        | VmOp::UpdateObject { obj, .. }
        | VmOp::BatchCreate { obj, .. } => maybe_set_obj_ref(obj, target, value, seen),
    }
}

fn maybe_set_obj_ref(
    field: &mut VmObjRef,
    target: usize,
    value: VmObjRef,
    seen: &mut usize,
) -> bool {
    if *seen == target {
        *field = value;
        true
    } else {
        *seen += 1;
        false
    }
}

fn copy_head_ref_field(trace: &mut Trace, rng: &mut StdRng) {
    let values = collect_head_refs(trace);
    let dests = count_head_refs(trace);
    if values.is_empty() || dests == 0 {
        return;
    }
    let value = values[rng.random_range(0..values.len())].clone();
    let dest = rng.random_range(0..dests);
    let mut seen = 0;
    set_nth_head_ref(trace, dest, value, &mut seen);
}

fn collect_head_refs(trace: &Trace) -> Vec<VmHeadRef> {
    let mut refs = Vec::new();
    for instr in &trace.steps {
        match instr {
            VmInstr::Observe { head, .. } => refs.push(head.clone()),
            VmInstr::DiffRange { before, after, .. } => {
                refs.push(before.clone());
                refs.push(after.clone());
            }
            _ => {}
        }
    }
    refs
}

fn count_head_refs(trace: &Trace) -> usize {
    collect_head_refs(trace).len()
}

fn set_nth_head_ref(trace: &mut Trace, target: usize, value: VmHeadRef, seen: &mut usize) -> bool {
    for instr in &mut trace.steps {
        match instr {
            VmInstr::Observe { head, .. } => {
                if maybe_set_head_ref(head, target, value.clone(), seen) {
                    return true;
                }
            }
            VmInstr::DiffRange { before, after, .. } => {
                if maybe_set_head_ref(before, target, value.clone(), seen)
                    || maybe_set_head_ref(after, target, value.clone(), seen)
                {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

fn maybe_set_head_ref(
    field: &mut VmHeadRef,
    target: usize,
    value: VmHeadRef,
    seen: &mut usize,
) -> bool {
    if *seen == target {
        *field = value;
        true
    } else {
        *seen += 1;
        false
    }
}

fn swap_op_constructor_reusing_fields(trace: &mut Trace, rng: &mut StdRng) {
    let change_indices = trace
        .steps
        .iter()
        .enumerate()
        .filter_map(|(index, instr)| match instr {
            VmInstr::Change { ops, .. } | VmInstr::Transact { ops, .. } if !ops.is_empty() => {
                Some(index)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    if change_indices.is_empty() {
        return;
    }
    let instr_index = change_indices[rng.random_range(0..change_indices.len())];
    let (VmInstr::Change { ops, .. } | VmInstr::Transact { ops, .. }) =
        &mut trace.steps[instr_index]
    else {
        return;
    };
    let op_index = rng.random_range(0..ops.len());
    let mut variants = constructor_swap_variants(&ops[op_index], rng);
    let variant = rng.random_range(0..variants.len());
    ops[op_index] = variants.swap_remove(variant);
}

fn repair_trace(trace: &mut Trace, rng: &mut StdRng) {
    ensure_actor_count(trace, 3);
    let mut builder = VmBuilder::default();
    let mut repaired = Vec::with_capacity(trace.steps.len());
    for mut instr in trace.steps.drain(..) {
        rebase_instr(&mut instr, &builder, rng);
        builder.observe_instr(&instr);
        repaired.push(instr);
    }
    trace.steps = repaired;
}

#[derive(Clone, Default)]
struct VmBuilder {
    /// Highest document slot known to have been created.
    docs: u8,
    /// Number of non-root object slots expected to exist.
    objects: u8,
    maps: Vec<u8>,
    lists: Vec<u8>,
    texts: Vec<u8>,
    saved_heads: u8,
}

impl VmBuilder {
    fn from_instructions(instructions: &[VmInstr]) -> Self {
        let mut builder = Self::default();
        for instr in instructions {
            match instr {
                VmInstr::Fork { to, .. } => builder.docs = builder.docs.max(*to),
                VmInstr::Merge { into, from } => builder.docs = builder.docs.max(*into).max(*from),
                VmInstr::Change { doc, ops, .. } => {
                    builder.docs = builder.docs.max(*doc);
                    for op in ops {
                        builder.observe_op(op);
                    }
                }
                VmInstr::Transact {
                    doc, ops, commit, ..
                } => {
                    builder.docs = builder.docs.max(*doc);
                    // Rolled-back transactions create no objects.
                    if *commit {
                        for op in ops {
                            builder.observe_op(op);
                        }
                    }
                }
                VmInstr::SaveLoad { doc }
                | VmInstr::DiffRange { doc, .. }
                | VmInstr::UpdateDiffCursor { doc }
                | VmInstr::ResetDiffCursor { doc }
                | VmInstr::DiffIncremental { doc } => builder.docs = builder.docs.max(*doc),
                VmInstr::SaveHeads { doc, slot } => {
                    builder.docs = builder.docs.max(*doc);
                    builder.saved_heads = builder.saved_heads.max(slot.saturating_add(1));
                }
                VmInstr::Observe { doc, .. } => builder.docs = builder.docs.max(*doc),
                VmInstr::Sync { left, right, .. } => {
                    builder.docs = builder.docs.max(*left).max(*right);
                }
            }
        }
        builder
    }

    fn random_instr(&mut self, rng: &mut StdRng) -> VmInstr {
        if self.objects == 0 || rng.random_range(0..100) < 55 {
            return self.random_change(rng);
        }

        match rng.random_range(0..11) {
            0 => {
                self.docs = self.docs.max(1);
                VmInstr::Fork { from: 0, to: 1 }
            }
            1 => VmInstr::Merge {
                into: 0,
                from: rng.random_range(0..=self.docs.max(1)),
            },
            2 => VmInstr::SaveLoad { doc: self.doc(rng) },
            3 => VmInstr::Observe {
                doc: self.doc(rng),
                object: self.random_any_ref(rng),
                mode: random_observe_mode(rng),
                head: random_vm_head(rng),
                budget: rng.random_range(1..=8),
            },
            4 => VmInstr::SaveHeads {
                doc: self.doc(rng),
                slot: rng.random_range(0..8),
            },
            5 => VmInstr::DiffRange {
                doc: self.doc(rng),
                before: random_vm_head(rng),
                after: random_vm_head(rng),
            },
            6 => VmInstr::UpdateDiffCursor { doc: self.doc(rng) },
            7 => VmInstr::ResetDiffCursor { doc: self.doc(rng) },
            8 => VmInstr::DiffIncremental { doc: self.doc(rng) },
            9 if self.docs >= 1 => VmInstr::Sync {
                left: 0,
                right: 1,
                rounds: rng.random_range(1..=16),
            },
            _ => self.random_change(rng),
        }
    }

    fn random_change(&mut self, rng: &mut StdRng) -> VmInstr {
        let op_count = rng.random_range(1..=4);
        let doc = self.doc(rng);
        let actor = rng.random_range(0..3);
        if rng.random_range(0..100) < 15 {
            let commit = rng.random_range(0..2) == 0;
            // Generate ops through a scratch builder when rolling back so the
            // main model does not register objects the rollback discards.
            let ops = if commit {
                (0..op_count).map(|_| self.random_op(rng)).collect()
            } else {
                let mut scratch = self.clone();
                (0..op_count).map(|_| scratch.random_op(rng)).collect()
            };
            return VmInstr::Transact {
                doc,
                actor,
                ops,
                commit,
            };
        }
        let ops = (0..op_count).map(|_| self.random_op(rng)).collect();
        VmInstr::Change { doc, actor, ops }
    }

    fn random_op(&mut self, rng: &mut StdRng) -> VmOp {
        let mut choices = vec![0, 1, 2, 3, 9, 12, 14, 15];
        if !self.lists.is_empty() {
            choices.extend([4, 5, 6, 13]);
        }
        if !self.texts.is_empty() {
            // Text splices are relatively cheap and build large histories. Bias
            // toward them over update_text, which pays text-diff costs.
            choices.extend([7, 7, 8, 10, 11]);
        }

        match choices[rng.random_range(0..choices.len())] {
            0 => VmOp::Put {
                obj: self.random_map_ref(rng),
                key: rng.random(),
                value: random_vm_value(rng),
            },
            1 => {
                let obj = self.random_map_ref(rng);
                let slot = self.allocate_object(ObjKind::Map);
                VmOp::MakeMap { obj, key: slot }
            }
            2 => {
                let obj = self.random_map_ref(rng);
                let slot = self.allocate_object(ObjKind::List);
                VmOp::MakeList { obj, key: slot }
            }
            3 => {
                let obj = self.random_map_ref(rng);
                let slot = self.allocate_object(ObjKind::Text);
                VmOp::MakeText { obj, key: slot }
            }
            4 => VmOp::Insert {
                obj: self.random_list_ref(rng),
                index: rng.random(),
                value: random_vm_value(rng),
            },
            5 => VmOp::PutSeq {
                obj: self.random_list_ref(rng),
                index: rng.random(),
                value: random_vm_value(rng),
            },
            6 => VmOp::SpliceList {
                obj: self.random_list_ref(rng),
                index: rng.random(),
                delete: rng.random_range(0..4),
                values: vec![random_vm_value(rng), random_vm_value(rng)],
            },
            7 => VmOp::SpliceText {
                obj: self.random_text_ref(rng),
                index: rng.random(),
                delete: rng.random_range(0..4),
                value: rng.random(),
            },
            8 => VmOp::UpdateText {
                obj: self.random_text_ref(rng),
                value: rng.random(),
            },
            9 => VmOp::Increment {
                obj: self.random_map_ref(rng),
                key: rng.random(),
                value: rng.random(),
            },
            10 => VmOp::Mark {
                obj: self.random_text_ref(rng),
                start: rng.random(),
                end: rng.random(),
                name: rng.random(),
                value: random_vm_value(rng),
                expand: random_mark_expand(rng),
            },
            11 => VmOp::Unmark {
                obj: self.random_text_ref(rng),
                start: rng.random(),
                end: rng.random(),
                name: rng.random(),
                expand: random_mark_expand(rng),
            },
            12 => VmOp::Delete {
                obj: self.random_map_ref(rng),
                key: rng.random(),
            },
            13 => VmOp::DeleteSeq {
                obj: self.random_list_ref(rng),
                index: rng.random(),
            },
            14 => VmOp::UpdateObject {
                obj: self.random_any_ref(rng),
                value: random_vm_hydrated(rng),
            },
            _ => {
                let obj = self.random_map_ref(rng);
                let value = random_vm_hydrated(rng);
                self.allocate_hydrated_root(&value);
                VmOp::BatchCreate {
                    obj,
                    key: rng.random(),
                    value,
                }
            }
        }
    }

    fn doc(&self, rng: &mut StdRng) -> u8 {
        rng.random_range(0..=self.docs)
    }

    fn observe_instr(&mut self, instr: &VmInstr) {
        match instr {
            VmInstr::Fork { to, .. } => self.docs = self.docs.max(*to),
            VmInstr::Merge { into, from } => self.docs = self.docs.max(*into).max(*from),
            VmInstr::Change { doc, ops, .. } => {
                self.docs = self.docs.max(*doc);
                for op in ops {
                    self.observe_op(op);
                }
            }
            VmInstr::Transact {
                doc, ops, commit, ..
            } => {
                self.docs = self.docs.max(*doc);
                if *commit {
                    for op in ops {
                        self.observe_op(op);
                    }
                }
            }
            VmInstr::SaveLoad { doc }
            | VmInstr::DiffRange { doc, .. }
            | VmInstr::UpdateDiffCursor { doc }
            | VmInstr::ResetDiffCursor { doc }
            | VmInstr::DiffIncremental { doc }
            | VmInstr::Observe { doc, .. } => self.docs = self.docs.max(*doc),
            VmInstr::SaveHeads { doc, slot } => {
                self.docs = self.docs.max(*doc);
                self.saved_heads = self.saved_heads.max(slot.saturating_add(1));
            }
            VmInstr::Sync { left, right, .. } => {
                self.docs = self.docs.max(*left).max(*right);
            }
        }
    }

    fn observe_op(&mut self, op: &VmOp) {
        match op {
            VmOp::MakeMap { key, .. } => self.register_slot(*key, ObjKind::Map),
            VmOp::MakeList { key, .. } => self.register_slot(*key, ObjKind::List),
            VmOp::MakeText { key, .. } => self.register_slot(*key, ObjKind::Text),
            VmOp::BatchCreate { value, .. } => self.allocate_hydrated_root(value),
            _ => {}
        }
    }

    fn allocate_object(&mut self, kind: ObjKind) -> u8 {
        let slot = self.objects;
        self.objects = self.objects.saturating_add(1);
        self.register_slot(slot, kind);
        slot
    }

    fn allocate_hydrated_root(&mut self, value: &VmHydrated) {
        match value {
            VmHydrated::Map { .. } => {
                self.allocate_object(ObjKind::Map);
            }
            VmHydrated::List { .. } => {
                self.allocate_object(ObjKind::List);
            }
            VmHydrated::Text { .. } => {
                self.allocate_object(ObjKind::Text);
            }
            VmHydrated::Scalar { .. } => {}
        }
    }

    fn register_slot(&mut self, slot: u8, kind: ObjKind) {
        match kind {
            ObjKind::Map => push_unique(&mut self.maps, slot),
            ObjKind::List => push_unique(&mut self.lists, slot),
            ObjKind::Text => push_unique(&mut self.texts, slot),
        }
    }

    fn random_map_ref(&self, rng: &mut StdRng) -> VmObjRef {
        if self.maps.is_empty() || rng.random_range(0..100) < 55 {
            VmObjRef::Root
        } else {
            VmObjRef::Slot {
                slot: self.maps[rng.random_range(0..self.maps.len())],
            }
        }
    }

    fn random_list_ref(&self, rng: &mut StdRng) -> VmObjRef {
        random_typed_ref(rng, &self.lists).unwrap_or(VmObjRef::Root)
    }

    fn random_text_ref(&self, rng: &mut StdRng) -> VmObjRef {
        random_typed_ref(rng, &self.texts).unwrap_or(VmObjRef::Root)
    }

    fn random_any_ref(&self, rng: &mut StdRng) -> VmObjRef {
        random_vm_obj(rng, self.objects)
    }
}

#[derive(Clone, Copy)]
enum ObjKind {
    Map,
    List,
    Text,
}

fn push_unique(values: &mut Vec<u8>, value: u8) {
    if !values.contains(&value) {
        values.push(value);
    }
}

fn random_typed_ref(rng: &mut StdRng, slots: &[u8]) -> Option<VmObjRef> {
    if slots.is_empty() {
        None
    } else if rng.random_range(0..100) < 85 {
        Some(VmObjRef::Slot {
            slot: slots[rng.random_range(0..slots.len())],
        })
    } else {
        Some(VmObjRef::Recent {
            back: rng.random_range(0..=slots.len().min(8) as u8),
        })
    }
}

fn random_vm_obj(rng: &mut StdRng, objects: u8) -> VmObjRef {
    match rng.random_range(0..10) {
        0..=2 => VmObjRef::Root,
        3..=6 => VmObjRef::Slot {
            slot: rng.random_range(0..=objects.max(1)),
        },
        7..=8 => VmObjRef::Recent {
            back: rng.random_range(0..=objects.min(8)),
        },
        _ => VmObjRef::Invalid { slot: rng.random() },
    }
}

fn random_vm_value(rng: &mut StdRng) -> VmValue {
    match rng.random_range(0..6) {
        0 => VmValue::Null,
        1 => VmValue::Bool { slot: rng.random() },
        2 => VmValue::Int { slot: rng.random() },
        3 => VmValue::Uint { slot: rng.random() },
        4 => VmValue::Counter { slot: rng.random() },
        _ => VmValue::Str { slot: rng.random() },
    }
}

fn random_vm_hydrated(rng: &mut StdRng) -> VmHydrated {
    match rng.random_range(0..4) {
        0 => VmHydrated::Scalar {
            value: random_vm_value(rng),
        },
        1 => VmHydrated::Map {
            seed: rng.random(),
            depth: rng.random_range(1..=4),
        },
        2 => VmHydrated::List {
            seed: rng.random(),
            depth: rng.random_range(1..=4),
        },
        _ => VmHydrated::Text { slot: rng.random() },
    }
}

fn random_observe_mode(rng: &mut StdRng) -> VmObserveMode {
    match rng.random_range(0..8) {
        0 => VmObserveMode::Shallow,
        1 => VmObserveMode::Hydrate,
        2 => VmObserveMode::Ranges,
        3 => VmObserveMode::Historical,
        4 => VmObserveMode::MapGets,
        5 => VmObserveMode::Text,
        6 => VmObserveMode::Cursors,
        _ => VmObserveMode::Marks,
    }
}

fn random_vm_head(rng: &mut StdRng) -> VmHeadRef {
    match rng.random_range(0..3) {
        0 => VmHeadRef::Empty,
        1 => VmHeadRef::Current,
        _ => VmHeadRef::Slot {
            slot: rng.random_range(0..8),
        },
    }
}

fn random_mark_expand(rng: &mut StdRng) -> MarkExpand {
    match rng.random_range(0..4) {
        0 => MarkExpand::Before,
        1 => MarkExpand::After,
        2 => MarkExpand::Both,
        _ => MarkExpand::None,
    }
}

fn ensure_actor_count(trace: &mut Trace, count: usize) {
    while trace.actors.len() < count {
        trace.actors.push(ActorSpec::new(trace.actors.len()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The typed-field copy plans address fields by ordinal, so the collect_*
    /// and set_nth_* visitors must walk fields in exactly the same order. Use a
    /// generated trace (which contains a mix of instruction and op kinds) to
    /// check that setting the nth u8 field changes the nth collected value.
    #[test]
    fn u8_field_visitors_stay_aligned() {
        let trace = Trace::generate(42, 200);
        let fields = collect_u8_fields(&trace);
        assert_eq!(fields.len(), count_u8_fields(&trace));
        assert!(!fields.is_empty());

        for target in 0..fields.len() {
            let mut mutated = trace.clone();
            let mut seen = 0;
            let sentinel = fields[target].wrapping_add(1);
            assert!(set_nth_u8_field(&mut mutated, target, sentinel, &mut seen));
            let mutated_fields = collect_u8_fields(&mutated);
            assert_eq!(mutated_fields.len(), fields.len());
            for (index, (before, after)) in fields.iter().zip(&mutated_fields).enumerate() {
                if index == target {
                    assert_eq!(*after, sentinel);
                } else {
                    assert_eq!(after, before);
                }
            }
        }
    }

    #[test]
    fn obj_and_head_ref_visitors_stay_aligned() {
        let trace = Trace::generate(43, 200);

        let obj_refs = collect_obj_refs(&trace);
        assert_eq!(obj_refs.len(), count_obj_refs(&trace));
        for target in 0..obj_refs.len() {
            let mut mutated = trace.clone();
            let mut seen = 0;
            let sentinel = VmObjRef::Invalid { slot: 0xab };
            assert!(set_nth_obj_ref(
                &mut mutated,
                target,
                sentinel.clone(),
                &mut seen
            ));
            assert_eq!(collect_obj_refs(&mutated)[target], sentinel);
        }

        let head_refs = collect_head_refs(&trace);
        assert_eq!(head_refs.len(), count_head_refs(&trace));
        for target in 0..head_refs.len() {
            let mut mutated = trace.clone();
            let mut seen = 0;
            let sentinel = VmHeadRef::Slot { slot: 0xcd };
            assert!(set_nth_head_ref(
                &mut mutated,
                target,
                sentinel.clone(),
                &mut seen
            ));
            assert_eq!(collect_head_refs(&mutated)[target], sentinel);
        }
    }

    #[test]
    fn generate_is_deterministic() {
        let a = Trace::generate(7, 50);
        let b = Trace::generate(7, 50);
        assert_eq!(
            serde_json::to_string(&a).unwrap(),
            serde_json::to_string(&b).unwrap()
        );
    }
}
