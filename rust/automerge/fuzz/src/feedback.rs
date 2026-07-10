use std::collections::{BTreeMap, HashSet};

use crate::coverage::{CmpKind, CmpObservation};
use crate::runner::{BehaviorStats, RunReport};
use crate::trace::{
    Trace, VmApplyOrder, VmInstr, VmObjRef, VmOp, VmPersistMode, VmSyncFault, VmSyncOp, VmValue,
};

const MAX_BEHAVIOR_BUCKETS: usize = 65536;
const MAX_STRUCTURAL_BUCKETS: usize = 512;
const MAX_COVERAGE_BUCKETS: usize = 262144;
const MAX_COMPARISON_BUCKETS: usize = 32768;
const MAX_COMPARISON_U8_VALUES: usize = 1024;
const RARE_COUNTER_RETAIN_THRESHOLD: usize = 64;

#[derive(Default)]
pub struct FeedbackState {
    features: HashSet<&'static str>,
    feature_pairs: HashSet<(&'static str, &'static str)>,
    sometimes_labels: HashSet<&'static str>,
    sometimes_count_buckets: HashSet<(&'static str, u8)>,
    sometimes_counts: BTreeMap<&'static str, u64>,
    coverage_buckets: HashSet<(u64, u8)>,
    coverage_counter_frequency: BTreeMap<u64, u32>,
    comparison_buckets: HashSet<ComparisonKey>,
    comparison_u8_values: Vec<u8>,
    comparison_u8_value_set: HashSet<u8>,
    diff_checks: u64,
    diff_unsupported: u64,
    patches_checked: u64,
    merge_checks: u64,
    persistence_transfers: u64,
    bundle_transfers: u64,
    isolation_transitions: u64,
    historical_transactions: u64,
    behaviors: HashSet<BehaviorKey>,
    structures: HashSet<StructuralKey>,
}

/// Log-bucketed [`BehaviorStats`]. Two runs that land in the same key took
/// the documents to roughly the same place, whatever their traces look like.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct BehaviorKey {
    docs: u8,
    max_heads: u8,
    total_changes: u8,
    total_objects: u8,
    max_depth: u8,
    total_text_len: u8,
    total_seq_len: u8,
    conflicted_props: u8,
    list_marks: u8,
    text_marks: u8,
    max_saved_bytes: u8,
    diff_checks: u8,
    diff_unsupported: u8,
    total_patches: u8,
    merge_checks: u8,
    persistence_transfers: u8,
    bundle_transfers: u8,
    isolation_transitions: u8,
    historical_transactions: u8,
}

fn behavior_key(stats: &BehaviorStats) -> BehaviorKey {
    BehaviorKey {
        docs: bucket(stats.docs),
        // Head count and depth are small and change one at a time; keep them
        // exact so e.g. two vs. three concurrent heads count as different
        // behavior.
        max_heads: stats.max_heads.min(15) as u8,
        total_changes: bucket(stats.total_changes),
        total_objects: bucket(stats.total_objects),
        max_depth: stats.max_depth.min(15) as u8,
        total_text_len: bucket(stats.total_text_len),
        total_seq_len: bucket(stats.total_seq_len),
        conflicted_props: bucket(stats.conflicted_props),
        list_marks: bucket(stats.list_marks),
        text_marks: bucket(stats.text_marks),
        max_saved_bytes: bucket(stats.max_saved_bytes),
        diff_checks: bucket(stats.diff_checks),
        diff_unsupported: bucket(stats.diff_unsupported),
        total_patches: bucket(stats.total_patches),
        merge_checks: bucket(stats.merge_checks),
        persistence_transfers: bucket(stats.persistence_transfers),
        bundle_transfers: bucket(stats.bundle_transfers),
        isolation_transitions: bucket(stats.isolation_transitions),
        historical_transactions: bucket(stats.historical_transactions),
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ComparisonKey {
    kind: CmpKind,
    width: u8,
    relation: u8,
    distance: u8,
    left: u8,
    right: u8,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct StructuralKey {
    instructions: u8,
    ops: u8,
    actors: u8,
    change_instrs: u8,
    transact_instrs: u8,
    transact_at_instrs: u8,
    persist_instrs: u8,
    isolate_instrs: u8,
    max_ops_in_instr: u8,
    max_obj_depth: u8,
    save_load_instrs: u8,
    fork_instrs: u8,
    merge_instrs: u8,
    apply_changes_instrs: u8,
    sync_instrs: u8,
    sync_session_instrs: u8,
    observe_instrs: u8,
    save_heads_instrs: u8,
    diff_range_instrs: u8,
    update_diff_cursor_instrs: u8,
    reset_diff_cursor_instrs: u8,
    diff_incremental_instrs: u8,
    puts: u8,
    deletes: u8,
    list_inserts: u8,
    list_splices: u8,
    text_splices: u8,
    text_updates: u8,
    marks: u8,
    unmarks: u8,
    map_creates: u8,
    list_creates: u8,
    text_creates: u8,
    update_objects: u8,
    batch_creates: u8,
}

impl FeedbackState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn consider(&mut self, trace: &Trace, report: &RunReport) -> Option<String> {
        self.diff_checks = self
            .diff_checks
            .saturating_add(report.behavior.diff_checks as u64);
        self.diff_unsupported = self
            .diff_unsupported
            .saturating_add(report.behavior.diff_unsupported as u64);
        self.patches_checked = self
            .patches_checked
            .saturating_add(report.behavior.total_patches as u64);
        self.merge_checks = self
            .merge_checks
            .saturating_add(report.behavior.merge_checks as u64);
        self.persistence_transfers = self
            .persistence_transfers
            .saturating_add(report.behavior.persistence_transfers as u64);
        self.bundle_transfers = self
            .bundle_transfers
            .saturating_add(report.behavior.bundle_transfers as u64);
        self.isolation_transitions = self
            .isolation_transitions
            .saturating_add(report.behavior.isolation_transitions as u64);
        self.historical_transactions = self
            .historical_transactions
            .saturating_add(report.behavior.historical_transactions as u64);
        for hit in &report.sometimes_hits {
            *self.sometimes_counts.entry(hit.name).or_default() += hit.count;
        }

        let coverage_reason = self.consider_coverage(report);
        let comparison_reason = self.consider_comparisons(report);

        for hit in &report.sometimes_hits {
            if self.sometimes_labels.insert(hit.name) {
                let bucket = count_bucket(hit.count);
                self.sometimes_count_buckets.insert((hit.name, bucket));
                return Some(format!("new sometimes {}", hit.name));
            }
        }

        for hit in &report.sometimes_hits {
            let bucket = count_bucket(hit.count);
            if self.sometimes_count_buckets.insert((hit.name, bucket)) {
                return Some(format!(
                    "new sometimes count bucket {}={}",
                    hit.name, hit.count
                ));
            }
        }

        let behavior = behavior_key(&report.behavior);
        if self.behaviors.len() < MAX_BEHAVIOR_BUCKETS && !self.behaviors.contains(&behavior) {
            let reason = format!(
                "new behavior bucket docs={} heads={} changes={} objects={} depth={} text={} seq={} conflicts={} marks={} saved={} diffs={} unsupported={} patches={} merge_checks={} persist={} bundles={} isolation={} historical_tx={}",
                behavior.docs,
                behavior.max_heads,
                behavior.total_changes,
                behavior.total_objects,
                behavior.max_depth,
                behavior.total_text_len,
                behavior.total_seq_len,
                behavior.conflicted_props,
                behavior.list_marks.max(behavior.text_marks),
                behavior.max_saved_bytes,
                behavior.diff_checks,
                behavior.diff_unsupported,
                behavior.total_patches,
                behavior.merge_checks,
                behavior.persistence_transfers,
                behavior.bundle_transfers,
                behavior.isolation_transitions,
                behavior.historical_transactions,
            );
            self.behaviors.insert(behavior);
            return Some(reason);
        }

        let mut trace_features = features(trace);
        trace_features.sort_unstable();
        trace_features.dedup();
        let mut first_new_feature = None;
        for feature in &trace_features {
            if self.features.insert(*feature) && first_new_feature.is_none() {
                first_new_feature = Some(*feature);
            }
        }

        // Individual feature coverage saturates quickly. Retaining new pairs
        // keeps histories that combine mechanisms (for example read-only sync
        // + state persistence, or text marks + concurrent deletes), which is
        // where CRDT bugs disproportionately live.
        let mut new_pairs = 0usize;
        for (index, left) in trace_features.iter().enumerate() {
            for right in trace_features.iter().skip(index + 1) {
                if self.feature_pairs.insert((*left, *right)) {
                    new_pairs += 1;
                }
            }
        }
        if let Some(feature) = first_new_feature {
            return Some(format!("new feature {feature}"));
        }
        if new_pairs != 0 {
            return Some(format!("new feature pairs {new_pairs}"));
        }

        let key = structural_key(trace);
        if self.structures.len() < MAX_STRUCTURAL_BUCKETS && self.structures.insert(key) {
            return Some("new structural bucket".to_string());
        }

        comparison_reason.or(coverage_reason)
    }

    fn consider_comparisons(&mut self, report: &RunReport) -> Option<String> {
        let mut new_buckets = 0usize;
        for observation in &report.cmp_observations {
            for value in comparison_u8_values(observation) {
                if self.comparison_u8_values.len() < MAX_COMPARISON_U8_VALUES
                    && self.comparison_u8_value_set.insert(value)
                {
                    self.comparison_u8_values.push(value);
                }
            }

            if self.comparison_buckets.len() < MAX_COMPARISON_BUCKETS
                && self.comparison_buckets.insert(comparison_key(observation))
            {
                new_buckets += 1;
            }
        }

        if new_buckets >= comparison_retain_threshold(self.comparison_buckets.len()) {
            Some(format!("new comparison buckets {new_buckets}"))
        } else {
            None
        }
    }

    fn consider_coverage(&mut self, report: &RunReport) -> Option<String> {
        let mut new_buckets = 0usize;
        let mut rare_score = 0usize;
        let mut unique_counters = HashSet::new();

        for hit in &report.coverage_hits {
            if self.coverage_buckets.len() < MAX_COVERAGE_BUCKETS
                && self.coverage_buckets.insert((hit.id, hit.bucket))
            {
                new_buckets += 1;
            }
            unique_counters.insert(hit.id);
        }

        for counter in unique_counters {
            let frequency = self.coverage_counter_frequency.entry(counter).or_default();
            if *frequency < 4 {
                rare_score += 1;
            }
            *frequency = frequency.saturating_add(1);
        }

        let threshold = coverage_bucket_retain_threshold(self.coverage_buckets.len());
        if new_buckets >= threshold {
            Some(format!("new coverage buckets {new_buckets}"))
        } else if rare_score >= RARE_COUNTER_RETAIN_THRESHOLD {
            Some(format!("rare coverage counters {rare_score}"))
        } else {
            None
        }
    }

    pub fn power_score(&self, report: &RunReport) -> u32 {
        let mut unique_counters = HashSet::new();
        for hit in &report.coverage_hits {
            unique_counters.insert(hit.id);
        }

        let mut score = 0u32;
        for counter in unique_counters {
            let frequency = self
                .coverage_counter_frequency
                .get(&counter)
                .copied()
                .unwrap_or(0);
            score = score.saturating_add(match frequency {
                0 | 1 => 32,
                2 => 24,
                3..=4 => 16,
                5..=8 => 8,
                9..=16 => 4,
                17..=32 => 2,
                _ => 1,
            });
        }

        let bucket_diversity = report
            .coverage_hits
            .iter()
            .map(|hit| (hit.id, hit.bucket))
            .collect::<HashSet<_>>()
            .len() as u32;
        score = score.saturating_add(bucket_diversity.min(64));
        if !report.sometimes_hits.is_empty() {
            score = score.saturating_add(64);
        }

        (1 + score / 64).clamp(1, 64)
    }

    pub fn feature_count(&self) -> usize {
        self.features.len()
    }

    pub fn feature_pair_count(&self) -> usize {
        self.feature_pairs.len()
    }

    pub fn behavior_bucket_count(&self) -> usize {
        self.behaviors.len()
    }

    pub fn diff_check_count(&self) -> u64 {
        self.diff_checks
    }

    pub fn unsupported_diff_count(&self) -> u64 {
        self.diff_unsupported
    }

    pub fn patch_count(&self) -> u64 {
        self.patches_checked
    }

    pub fn merge_check_count(&self) -> u64 {
        self.merge_checks
    }

    pub fn persistence_transfer_count(&self) -> u64 {
        self.persistence_transfers
    }

    pub fn bundle_transfer_count(&self) -> u64 {
        self.bundle_transfers
    }

    pub fn isolation_transition_count(&self) -> u64 {
        self.isolation_transitions
    }

    pub fn historical_transaction_count(&self) -> u64 {
        self.historical_transactions
    }

    pub fn structural_bucket_count(&self) -> usize {
        self.structures.len()
    }

    pub fn coverage_bucket_count(&self) -> usize {
        self.coverage_buckets.len()
    }

    pub fn comparison_bucket_count(&self) -> usize {
        self.comparison_buckets.len()
    }

    pub fn comparison_value_count(&self) -> usize {
        self.comparison_u8_values.len()
    }

    pub fn comparison_u8_values(&self) -> &[u8] {
        &self.comparison_u8_values
    }

    pub fn seed_comparison_u8_values(&mut self, values: impl IntoIterator<Item = u8>) {
        for value in values {
            if self.comparison_u8_values.len() < MAX_COMPARISON_U8_VALUES
                && self.comparison_u8_value_set.insert(value)
            {
                self.comparison_u8_values.push(value);
            }
        }
    }

    pub fn sometimes_label_count(&self) -> usize {
        self.sometimes_labels.len()
    }

    pub fn sometimes_count_bucket_count(&self) -> usize {
        self.sometimes_count_buckets.len()
    }

    pub fn sometimes_counts(&self) -> &BTreeMap<&'static str, u64> {
        &self.sometimes_counts
    }
}

fn coverage_bucket_retain_threshold(total_buckets: usize) -> usize {
    match total_buckets {
        0..=9_999 => 16,
        10_000..=49_999 => 32,
        50_000..=99_999 => 64,
        _ => 128,
    }
}

fn comparison_retain_threshold(total_buckets: usize) -> usize {
    match total_buckets {
        0..=255 => 8,
        256..=1_023 => 16,
        1_024..=8_191 => 32,
        _ => 64,
    }
}

fn comparison_key(observation: &CmpObservation) -> ComparisonKey {
    let distance = observation.left.abs_diff(observation.right);
    ComparisonKey {
        kind: observation.kind,
        width: observation.width.min(8),
        relation: comparison_relation(observation.left, observation.right),
        distance: log_bucket(distance),
        left: log_bucket(observation.left),
        right: log_bucket(observation.right),
    }
}

fn comparison_relation(left: u64, right: u64) -> u8 {
    match left.cmp(&right) {
        std::cmp::Ordering::Less => 0,
        std::cmp::Ordering::Equal => 1,
        std::cmp::Ordering::Greater => 2,
    }
}

fn comparison_u8_values(observation: &CmpObservation) -> impl Iterator<Item = u8> {
    [observation.left, observation.right]
        .into_iter()
        .flat_map(|value| {
            let low = (value & 0xff) as u8;
            let high = ((value >> 8) & 0xff) as u8;
            let direct = if value <= u64::from(u8::MAX) {
                Some(value as u8)
            } else {
                None
            };
            [direct, Some(low), Some(high)]
                .into_iter()
                .flatten()
                .filter(|value| *value <= 64 || *value == u8::MAX)
        })
}

fn log_bucket(value: u64) -> u8 {
    match value {
        0 => 0,
        1 => 1,
        2 => 2,
        3..=4 => 3,
        5..=8 => 4,
        9..=16 => 5,
        17..=32 => 6,
        33..=64 => 7,
        65..=128 => 8,
        129..=256 => 9,
        257..=1024 => 10,
        1025..=4096 => 11,
        _ => 12,
    }
}

fn features(trace: &Trace) -> Vec<&'static str> {
    let mut features = Vec::new();
    let mut actors = HashSet::new();

    for instr in &trace.steps {
        match instr {
            VmInstr::Fork { .. } => features.push("fork_doc"),
            VmInstr::ForkAt { .. } => features.push("fork_at"),
            VmInstr::ApplyChanges { order, .. } => {
                features.push("apply_changes");
                features.push(match order {
                    VmApplyOrder::InOrder => "apply_changes_in_order",
                    VmApplyOrder::Reversed => "apply_changes_reversed",
                    VmApplyOrder::Shuffled { .. } => "apply_changes_shuffled",
                    VmApplyOrder::Duplicated => "apply_changes_duplicated",
                    VmApplyOrder::DropHalf => "apply_changes_drop_half",
                });
            }
            VmInstr::Merge { .. } => features.push("merge"),
            VmInstr::Persist { mode, .. } => {
                features.push("persist");
                features.push(match mode {
                    VmPersistMode::Incremental => "persist_incremental",
                    VmPersistMode::SaveAfter { .. } => "persist_save_after",
                    VmPersistMode::Bundle { .. } => "persist_bundle",
                });
            }
            VmInstr::Isolate { .. } => features.push("isolate"),
            VmInstr::Integrate { .. } => features.push("integrate"),
            VmInstr::SaveLoad { .. } => features.push("save_load"),
            VmInstr::Sync { .. } => features.push("sync"),
            VmInstr::SyncSession { op, .. } => {
                features.push("sync_session");
                features.push(match op {
                    VmSyncOp::Start { .. } => "sync_session_start",
                    VmSyncOp::Generate { .. } => "sync_session_generate",
                    VmSyncOp::Deliver { fault, .. } => match fault {
                        VmSyncFault::None => "sync_session_deliver",
                        VmSyncFault::Drop => "sync_session_drop",
                        VmSyncFault::Duplicate => "sync_session_duplicate",
                        VmSyncFault::Reorder => "sync_session_reorder",
                    },
                    VmSyncOp::SaveStates => "sync_session_save_states",
                    VmSyncOp::SetReadOnly {
                        read_only: true, ..
                    } => "sync_session_read_only",
                    VmSyncOp::SetReadOnly {
                        read_only: false, ..
                    } => "sync_session_read_write",
                    VmSyncOp::Finish { .. } => "sync_session_finish",
                });
            }
            VmInstr::Observe { .. } => features.push("observe"),
            VmInstr::SaveHeads { .. } => features.push("save_heads"),
            VmInstr::DiffRange { .. } => features.push("diff_range"),
            VmInstr::UpdateDiffCursor { .. } => features.push("update_diff_cursor"),
            VmInstr::ResetDiffCursor { .. } => features.push("reset_diff_cursor"),
            VmInstr::DiffIncremental { .. } => features.push("diff_incremental"),
            VmInstr::Change { actor, ops, .. }
            | VmInstr::Transact { actor, ops, .. }
            | VmInstr::TransactAt { actor, ops, .. } => {
                match instr {
                    VmInstr::Transact { commit, .. } => features.push(if *commit {
                        "transact_commit"
                    } else {
                        "transact_rollback"
                    }),
                    VmInstr::TransactAt { commit, .. } => {
                        features.push("transact_at");
                        features.push(if *commit {
                            "owned_transact_commit"
                        } else {
                            "owned_transact_rollback"
                        });
                    }
                    _ => {}
                }
                actors.insert(*actor);
                if ops.is_empty() {
                    features.push("empty_change");
                }
                if ops.len() > 1 {
                    features.push("multi_op_change");
                }
                for op in ops {
                    if vm_op_obj_depth(op) > 0 {
                        features.push("non_root_object_ref");
                    }
                    match op {
                        VmOp::Put { value, .. }
                        | VmOp::Insert { value, .. }
                        | VmOp::PutSeq { value, .. }
                        | VmOp::Mark { value, .. } => features.push(value_feature(value)),
                        VmOp::SpliceList { values, .. } => {
                            features.extend(values.iter().map(value_feature));
                        }
                        _ => {}
                    }
                    match op {
                        VmOp::Put { .. } => features.push("put"),
                        VmOp::PutSeq { .. } => features.push("put_seq"),
                        VmOp::MakeMap { .. } => features.push("make_map"),
                        VmOp::MakeList { .. } => features.push("make_list"),
                        VmOp::MakeText { .. } => features.push("make_text"),
                        VmOp::Insert { .. } => features.push("list_insert"),
                        VmOp::SpliceList { delete, values, .. }
                            if *delete > 0 && !values.is_empty() =>
                        {
                            features.push("list_replace")
                        }
                        VmOp::SpliceList { delete, .. } if *delete > 0 => {
                            features.push("list_delete")
                        }
                        VmOp::SpliceList { .. } => features.push("list_splice"),
                        VmOp::SpliceText { delete, .. } if *delete > 0 => {
                            features.push("text_delete")
                        }
                        VmOp::SpliceText { .. } => features.push("text_insert"),
                        VmOp::UpdateText { .. } => features.push("text_update"),
                        VmOp::EditText { .. } => features.push("text_edit"),
                        VmOp::UpdateSpans { .. } => features.push("update_spans"),
                        VmOp::Increment { .. } => features.push("increment"),
                        VmOp::Mark { .. } => features.push("mark"),
                        VmOp::Unmark { .. } => features.push("unmark"),
                        VmOp::Delete { .. } => features.push("delete"),
                        VmOp::DeleteSeq { .. } => features.push("delete_seq"),
                        VmOp::UpdateObject { .. } => features.push("update_object"),
                        VmOp::BatchCreate { .. } => features.push("batch_create"),
                    }
                }
            }
        }
    }

    if actors.len() > 1 {
        features.push("multiple_actors");
    }
    if trace.actors.iter().any(|actor| actor.bytes.len() > 16) {
        features.push("long_actor_id");
    }
    if trace.actors.iter().any(|actor| actor.bytes.len() > 1) {
        features.push("multibyte_actor_id");
    }
    if trace.actors.iter().enumerate().any(|(index, left)| {
        trace.actors.iter().skip(index + 1).any(|right| {
            left.bytes.starts_with(&right.bytes) || right.bytes.starts_with(&left.bytes)
        })
    }) {
        features.push("actor_id_prefix_pair");
    }

    if let Some(encoding) = trace.text_encoding {
        features.push(match encoding {
            crate::trace::VmTextEncoding::CodePoint => "encoding_code_point",
            crate::trace::VmTextEncoding::Utf8 => "encoding_utf8",
            crate::trace::VmTextEncoding::Utf16 => "encoding_utf16",
            crate::trace::VmTextEncoding::Grapheme => "encoding_grapheme",
        });
    }

    features
}

fn value_feature(value: &VmValue) -> &'static str {
    match value {
        VmValue::Null => "scalar_null",
        VmValue::Bool { .. } => "scalar_bool",
        VmValue::Int { .. } => "scalar_int",
        VmValue::Uint { .. } => "scalar_uint",
        VmValue::Str { .. } => "scalar_string",
        VmValue::Counter { .. } => "scalar_counter",
        VmValue::Timestamp { .. } => "scalar_timestamp",
        VmValue::F64 { .. } => "scalar_f64",
        VmValue::Bytes { .. } => "scalar_bytes",
    }
}

fn structural_key(trace: &Trace) -> StructuralKey {
    let mut stats = StructuralStats::default();

    for instr in &trace.steps {
        match instr {
            VmInstr::Fork { .. } | VmInstr::ForkAt { .. } => stats.fork_instrs += 1,
            VmInstr::ApplyChanges { .. } => stats.apply_changes_instrs += 1,
            VmInstr::Merge { .. } => stats.merge_instrs += 1,
            VmInstr::SaveLoad { .. } => stats.save_load_instrs += 1,
            VmInstr::Sync { .. } => stats.sync_instrs += 1,
            VmInstr::SyncSession { .. } => stats.sync_session_instrs += 1,
            VmInstr::Observe { .. } => stats.observe_instrs += 1,
            VmInstr::SaveHeads { .. } => stats.save_heads_instrs += 1,
            VmInstr::DiffRange { .. } => stats.diff_range_instrs += 1,
            VmInstr::UpdateDiffCursor { .. } => stats.update_diff_cursor_instrs += 1,
            VmInstr::ResetDiffCursor { .. } => stats.reset_diff_cursor_instrs += 1,
            VmInstr::DiffIncremental { .. } => stats.diff_incremental_instrs += 1,
            VmInstr::Persist { .. } => stats.persist_instrs += 1,
            VmInstr::Isolate { .. } | VmInstr::Integrate { .. } => stats.isolate_instrs += 1,
            VmInstr::Change { ops, .. }
            | VmInstr::Transact { ops, .. }
            | VmInstr::TransactAt { ops, .. } => {
                match instr {
                    VmInstr::Transact { .. } => stats.transact_instrs += 1,
                    VmInstr::TransactAt { .. } => stats.transact_at_instrs += 1,
                    _ => stats.change_instrs += 1,
                }
                stats.max_ops_in_instr = stats.max_ops_in_instr.max(ops.len());
                for op in ops {
                    stats.ops += 1;
                    stats.max_obj_depth = stats.max_obj_depth.max(vm_op_obj_depth(op));
                    match op {
                        VmOp::Put { .. } | VmOp::PutSeq { .. } => stats.puts += 1,
                        VmOp::MakeMap { .. } => stats.map_creates += 1,
                        VmOp::MakeList { .. } => stats.list_creates += 1,
                        VmOp::MakeText { .. } => stats.text_creates += 1,
                        VmOp::Insert { .. } => stats.list_inserts += 1,
                        VmOp::SpliceList { .. } => stats.list_splices += 1,
                        VmOp::SpliceText { .. } => stats.text_splices += 1,
                        VmOp::UpdateText { .. }
                        | VmOp::EditText { .. }
                        | VmOp::UpdateSpans { .. } => stats.text_updates += 1,
                        VmOp::Increment { .. } => stats.puts += 1,
                        VmOp::Mark { .. } => stats.marks += 1,
                        VmOp::Unmark { .. } => stats.unmarks += 1,
                        VmOp::Delete { .. } | VmOp::DeleteSeq { .. } => stats.deletes += 1,
                        VmOp::UpdateObject { .. } => stats.update_objects += 1,
                        VmOp::BatchCreate { .. } => stats.batch_creates += 1,
                    }
                }
            }
        }
    }

    StructuralKey {
        instructions: bucket(trace.steps.len()),
        ops: bucket(stats.ops),
        actors: bucket(trace.actors.len()),
        change_instrs: bucket(stats.change_instrs),
        transact_instrs: bucket(stats.transact_instrs),
        transact_at_instrs: bucket(stats.transact_at_instrs),
        persist_instrs: bucket(stats.persist_instrs),
        isolate_instrs: bucket(stats.isolate_instrs),
        max_ops_in_instr: bucket(stats.max_ops_in_instr),
        max_obj_depth: bucket(stats.max_obj_depth),
        save_load_instrs: bucket(stats.save_load_instrs),
        fork_instrs: bucket(stats.fork_instrs),
        apply_changes_instrs: bucket(stats.apply_changes_instrs),
        merge_instrs: bucket(stats.merge_instrs),
        sync_instrs: bucket(stats.sync_instrs),
        sync_session_instrs: bucket(stats.sync_session_instrs),
        observe_instrs: bucket(stats.observe_instrs),
        save_heads_instrs: bucket(stats.save_heads_instrs),
        diff_range_instrs: bucket(stats.diff_range_instrs),
        update_diff_cursor_instrs: bucket(stats.update_diff_cursor_instrs),
        reset_diff_cursor_instrs: bucket(stats.reset_diff_cursor_instrs),
        diff_incremental_instrs: bucket(stats.diff_incremental_instrs),
        puts: bucket(stats.puts),
        deletes: bucket(stats.deletes),
        list_inserts: bucket(stats.list_inserts),
        list_splices: bucket(stats.list_splices),
        text_splices: bucket(stats.text_splices),
        text_updates: bucket(stats.text_updates),
        marks: bucket(stats.marks),
        unmarks: bucket(stats.unmarks),
        map_creates: bucket(stats.map_creates),
        list_creates: bucket(stats.list_creates),
        text_creates: bucket(stats.text_creates),
        update_objects: bucket(stats.update_objects),
        batch_creates: bucket(stats.batch_creates),
    }
}

#[derive(Default)]
struct StructuralStats {
    ops: usize,
    change_instrs: usize,
    transact_instrs: usize,
    transact_at_instrs: usize,
    persist_instrs: usize,
    isolate_instrs: usize,
    max_ops_in_instr: usize,
    max_obj_depth: usize,
    save_load_instrs: usize,
    fork_instrs: usize,
    merge_instrs: usize,
    apply_changes_instrs: usize,
    sync_instrs: usize,
    sync_session_instrs: usize,
    observe_instrs: usize,
    save_heads_instrs: usize,
    diff_range_instrs: usize,
    update_diff_cursor_instrs: usize,
    reset_diff_cursor_instrs: usize,
    diff_incremental_instrs: usize,
    puts: usize,
    deletes: usize,
    list_inserts: usize,
    list_splices: usize,
    text_splices: usize,
    text_updates: usize,
    marks: usize,
    unmarks: usize,
    map_creates: usize,
    list_creates: usize,
    text_creates: usize,
    update_objects: usize,
    batch_creates: usize,
}

fn vm_op_obj_depth(op: &VmOp) -> usize {
    let obj = match op {
        VmOp::Put { obj, .. }
        | VmOp::MakeMap { obj, .. }
        | VmOp::MakeList { obj, .. }
        | VmOp::MakeText { obj, .. }
        | VmOp::Insert { obj, .. }
        | VmOp::PutSeq { obj, .. }
        | VmOp::SpliceList { obj, .. }
        | VmOp::SpliceText { obj, .. }
        | VmOp::UpdateText { obj, .. }
        | VmOp::EditText { obj, .. }
        | VmOp::UpdateSpans { obj, .. }
        | VmOp::Increment { obj, .. }
        | VmOp::Mark { obj, .. }
        | VmOp::Unmark { obj, .. }
        | VmOp::Delete { obj, .. }
        | VmOp::DeleteSeq { obj, .. }
        | VmOp::UpdateObject { obj, .. }
        | VmOp::BatchCreate { obj, .. } => obj,
    };

    match obj {
        VmObjRef::Root => 0,
        VmObjRef::Slot { .. } => 1,
        VmObjRef::Recent { .. } => 2,
        VmObjRef::Invalid { .. } => 3,
    }
}

fn bucket(n: usize) -> u8 {
    if n == 0 {
        0
    } else {
        usize::BITS as u8 - n.leading_zeros() as u8
    }
}

fn count_bucket(n: u64) -> u8 {
    if n == 0 {
        0
    } else {
        // Cap the bucket so repeated long traces do not produce unbounded
        // novelty for the same semantic label.
        (u64::BITS as u8 - n.leading_zeros() as u8).min(10)
    }
}
