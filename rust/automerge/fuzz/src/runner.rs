use automerge::hydrate;
use automerge::marks::{ExpandMark, Mark, MarkSet, UpdateSpansConfig};
use automerge::sync::{self, SyncDoc};
use automerge::transaction::Transactable;
use automerge::Span;
use std::collections::{HashMap, VecDeque};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use automerge::{
    ActorId, AutoCommit, Bundle, ChangeHash, Cursor, LoadOptions, MoveCursor, ObjId, ObjType,
    Patch, PatchAction, PatchLog, ReadDoc, ScalarValue, TextEncoding, ROOT,
};

use crate::trace::{
    MarkExpand, Trace, VmApplyOrder, VmHeadRef, VmHydrated, VmInstr, VmObjRef, VmObserveMode, VmOp,
    VmPersistMode, VmSyncFault, VmSyncOp, VmTextEncoding, VmValue,
};

pub struct Runner {
    runs: u64,
    cmp_sample_rate: u64,
    prefix_cache: HashMap<PrefixCacheKey, CachedPrefix>,
    prefix_cache_order: VecDeque<PrefixCacheKey>,
    checkpoint_cache: HashMap<PrefixCacheKey, CachedPrefix>,
    checkpoint_cache_order: VecDeque<PrefixCacheKey>,
    prefix_cache_interval: usize,
    prefix_cache_max_entries: usize,
    checkpoint_cache_max_entries: usize,
    prefix_cache_hits: u64,
    checkpoint_cache_hits: u64,
    prefix_cache_steps_skipped: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct PrefixCacheKey {
    steps: usize,
    hash: u64,
}

#[derive(Clone)]
struct CachedPrefix {
    state: RunState,
    ops: usize,
}

#[derive(Clone, Copy)]
enum CacheHitSource {
    Interval,
    Checkpoint,
}

#[derive(Debug)]
pub enum RunError {
    MissingDoc { doc: usize },
    MissingObject { obj: String },
    MissingHeads { doc: usize, slot: u8 },
    Automerge(String),
    Load(String),
    Invariant(String),
    Timeout { step: usize, elapsed_ms: u128 },
    Panic(String),
}

impl Default for Runner {
    fn default() -> Self {
        Self::new()
    }
}

impl Runner {
    pub fn new() -> Self {
        Self {
            runs: 0,
            cmp_sample_rate: cmp_sample_rate(),
            prefix_cache: HashMap::new(),
            prefix_cache_order: VecDeque::new(),
            checkpoint_cache: HashMap::new(),
            checkpoint_cache_order: VecDeque::new(),
            prefix_cache_interval: prefix_cache_interval(),
            prefix_cache_max_entries: prefix_cache_max_entries(),
            checkpoint_cache_max_entries: checkpoint_cache_max_entries(),
            prefix_cache_hits: 0,
            checkpoint_cache_hits: 0,
            prefix_cache_steps_skipped: 0,
        }
    }

    pub fn run_catching(&mut self, trace: &Trace) -> Result<RunReport, RunError> {
        automerge::sometimes::reset();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.run(trace)));
        match result {
            Ok(Ok(mut report)) => {
                report.sometimes_hits = automerge::sometimes::take_hits();
                Ok(report)
            }
            Ok(Err(err)) => {
                let _ = crate::coverage::take_cmp_observations();
                let _ = automerge::sometimes::take_hits();
                Err(err)
            }
            Err(payload) => {
                let _ = crate::coverage::take_cmp_observations();
                let _ = automerge::sometimes::take_hits();
                Err(RunError::Panic(panic_message(payload)))
            }
        }
    }

    pub fn run(&mut self, trace: &Trace) -> Result<RunReport, RunError> {
        crate::coverage::reset_edge_counters();
        let collect_cmps = self.should_collect_cmps();
        crate::coverage::begin_cmp_observations(collect_cmps);
        let (mut state, ops) = self.execute_trace_steps(trace)?;

        let mut behavior = BehaviorStats {
            docs: state.docs.len(),
            diff_checks: state.diff_checks,
            diff_unsupported: state.diff_unsupported,
            total_patches: state.total_patches,
            merge_checks: state.merge_checks,
            persistence_transfers: state.persistence_transfers,
            bundle_transfers: state.bundle_transfers,
            isolation_transitions: state.isolation_transitions,
            historical_transactions: state.historical_transactions,
            ..BehaviorStats::default()
        };
        let text_encoding = state.text_encoding;
        for doc in 0..state.docs.len() {
            let outcome = state.save_load(doc)?;
            behavior.max_saved_bytes = behavior.max_saved_bytes.max(outcome.saved_bytes);
            walk_hydrated(&outcome.hydrated, 1, &mut behavior);
            let doc_state = &mut state.docs[doc];
            behavior.max_heads = behavior.max_heads.max(doc_state.doc.get_heads().len());
            behavior.total_changes += doc_state.doc.get_changes_meta(&[]).len();
            behavior.text_marks += doc_state.count_text_marks();
            doc_state.check_text_invariants(text_encoding)?;
        }

        let docs = state.docs.len();
        let coverage_hits = crate::coverage::counter_hit_buckets();
        let cmp_observations = crate::coverage::take_cmp_observations();
        Ok(RunReport {
            steps: trace.steps.len(),
            ops,
            docs,
            behavior,
            sometimes_hits: Vec::new(),
            coverage_hits,
            cmp_observations,
        })
    }
}

/// Execution-derived statistics describing where a trace actually took the
/// documents, as opposed to what the trace text says. Collected once per run
/// from the final state that the save/load invariant already hydrates.
#[derive(Clone, Copy, Debug, Default)]
pub struct BehaviorStats {
    pub docs: usize,
    pub max_heads: usize,
    pub total_changes: usize,
    pub total_objects: usize,
    pub max_depth: usize,
    pub total_text_len: usize,
    pub total_seq_len: usize,
    pub conflicted_props: usize,
    pub list_marks: usize,
    pub text_marks: usize,
    pub max_saved_bytes: usize,
    /// Number of diffs checked by applying their patches to hydrated state.
    pub diff_checks: usize,
    /// Diffs containing patch actions hydrate cannot currently apply (marks or
    /// conflict-only actions), and therefore excluded from the oracle.
    pub diff_unsupported: usize,
    pub total_patches: usize,
    /// Number of explicit merges checked in both directions for convergence.
    pub merge_checks: usize,
    pub persistence_transfers: usize,
    pub bundle_transfers: usize,
    pub isolation_transitions: usize,
    pub historical_transactions: usize,
}

struct PatchCheck {
    patches: usize,
    checked: bool,
}

/// Use hydrated values as a model for root diffs: applying a supported patch
/// stream to the state at `before` must exactly produce the state at `after`.
/// Hydrate deliberately does not yet implement standalone mark/conflict patch
/// actions, so those streams are measured but excluded rather than producing
/// harness false positives.
fn verify_diff_patches(
    mut before: hydrate::Value,
    after: hydrate::Value,
    patches: Vec<Patch>,
    text_encoding: TextEncoding,
) -> Result<PatchCheck, RunError> {
    let patch_count = patches.len();
    let supported = !patches.iter().any(|patch| {
        matches!(
            &patch.action,
            PatchAction::Mark { .. } | PatchAction::Conflict { .. }
        )
    });
    if !supported {
        return Ok(PatchCheck {
            patches: patch_count,
            checked: false,
        });
    }
    before
        .apply_patches(text_encoding, patches)
        .map_err(|err| RunError::Invariant(format!("diff patches failed to apply: {err}")))?;
    if before != after {
        return Err(RunError::Invariant(
            "applying diff patches did not produce the target hydrated state".to_string(),
        ));
    }
    Ok(PatchCheck {
        patches: patch_count,
        checked: true,
    })
}

fn walk_hydrated(value: &hydrate::Value, depth: usize, stats: &mut BehaviorStats) {
    match value {
        hydrate::Value::Scalar(_) => {}
        hydrate::Value::Map(map) => {
            stats.total_objects += 1;
            stats.max_depth = stats.max_depth.max(depth);
            for (_, entry) in map.iter() {
                if entry.conflict {
                    stats.conflicted_props += 1;
                }
                walk_hydrated(&entry.value, depth + 1, stats);
            }
        }
        hydrate::Value::List(list) => {
            stats.total_objects += 1;
            stats.max_depth = stats.max_depth.max(depth);
            stats.total_seq_len += list.len();
            for entry in list.iter() {
                if entry.conflict {
                    stats.conflicted_props += 1;
                }
                stats.list_marks += entry.marks.len();
                walk_hydrated(&entry.value, depth + 1, stats);
            }
        }
        hydrate::Value::Text(text) => {
            stats.total_objects += 1;
            stats.max_depth = stats.max_depth.max(depth);
            stats.total_text_len += text.len();
        }
    }
}

impl Runner {
    fn should_collect_cmps(&mut self) -> bool {
        self.runs = self.runs.saturating_add(1);
        self.cmp_sample_rate != 0 && self.runs % self.cmp_sample_rate == 0
    }

    pub fn state_cache_status(&self) -> Option<String> {
        if self.prefix_cache_interval == 0
            && self.checkpoint_cache.is_empty()
            && self.prefix_cache.is_empty()
        {
            return None;
        }
        Some(format!(
            "entries={} checkpoint_entries={} hits={} checkpoint_hits={} steps_skipped={}",
            self.prefix_cache.len(),
            self.checkpoint_cache.len(),
            self.prefix_cache_hits,
            self.checkpoint_cache_hits,
            self.prefix_cache_steps_skipped
        ))
    }

    pub fn cache_checkpoint_prefix(&mut self, trace: &Trace) -> Result<(), RunError> {
        if trace.steps.is_empty() || self.checkpoint_cache_max_entries == 0 {
            return Ok(());
        }

        automerge::sometimes::reset();
        crate::coverage::reset_edge_counters();
        crate::coverage::begin_cmp_observations(false);
        let (state, ops) = self.execute_trace_steps_from_scratch(trace)?;
        let _ = automerge::sometimes::take_hits();
        let _ = crate::coverage::counter_hit_buckets();
        let _ = crate::coverage::take_cmp_observations();

        let key = trace_prefix_key(trace);
        if self.checkpoint_cache.contains_key(&key) {
            return Ok(());
        }
        while self.checkpoint_cache.len() >= self.checkpoint_cache_max_entries {
            let Some(oldest) = self.checkpoint_cache_order.pop_front() else {
                break;
            };
            self.checkpoint_cache.remove(&oldest);
        }
        self.checkpoint_cache_order.push_back(key);
        self.checkpoint_cache
            .insert(key, CachedPrefix { state, ops });
        Ok(())
    }

    fn execute_trace_steps(&mut self, trace: &Trace) -> Result<(RunState, usize), RunError> {
        let started = Instant::now();
        let timeout = trace_timeout();
        let (mut state, mut ops, start_step, mut prefix_hash, source) = self.cached_start(trace);

        for (step_index, step) in trace.steps.iter().enumerate().skip(start_step) {
            if started.elapsed() > timeout {
                return Err(RunError::Timeout {
                    step: step_index,
                    elapsed_ms: started.elapsed().as_millis(),
                });
            }
            ops += state
                .run_vm(std::slice::from_ref(step))
                .map_err(|err| err.at(step_index))?;
            update_prefix_hash_for_instr(&mut prefix_hash, step);
            self.maybe_cache_prefix(step_index + 1, prefix_hash, &state, ops);
        }

        if let Some(source) = source {
            match source {
                CacheHitSource::Interval => {
                    self.prefix_cache_hits = self.prefix_cache_hits.saturating_add(1)
                }
                CacheHitSource::Checkpoint => {
                    self.checkpoint_cache_hits = self.checkpoint_cache_hits.saturating_add(1)
                }
            }
            self.prefix_cache_steps_skipped = self
                .prefix_cache_steps_skipped
                .saturating_add(start_step as u64);
        }

        Ok((state, ops))
    }

    fn execute_trace_steps_from_scratch(
        &mut self,
        trace: &Trace,
    ) -> Result<(RunState, usize), RunError> {
        let started = Instant::now();
        let timeout = trace_timeout();
        let mut state = RunState::new(trace);
        let mut ops = 0usize;
        let mut prefix_hash = initial_prefix_hash(trace);
        for (step_index, step) in trace.steps.iter().enumerate() {
            if started.elapsed() > timeout {
                return Err(RunError::Timeout {
                    step: step_index,
                    elapsed_ms: started.elapsed().as_millis(),
                });
            }
            ops += state
                .run_vm(std::slice::from_ref(step))
                .map_err(|err| err.at(step_index))?;
            update_prefix_hash_for_instr(&mut prefix_hash, step);
            self.maybe_cache_prefix(step_index + 1, prefix_hash, &state, ops);
        }
        Ok((state, ops))
    }

    fn cached_start(
        &mut self,
        trace: &Trace,
    ) -> (RunState, usize, usize, u64, Option<CacheHitSource>) {
        if self.prefix_cache_interval == 0
            && self.prefix_cache.is_empty()
            && self.checkpoint_cache.is_empty()
        {
            return (RunState::new(trace), 0, 0, initial_prefix_hash(trace), None);
        }

        let mut hash = initial_prefix_hash(trace);
        let mut best = None;
        for (index, step) in trace.steps.iter().enumerate() {
            update_prefix_hash_for_instr(&mut hash, step);
            let completed = index + 1;
            let key = PrefixCacheKey {
                steps: completed,
                hash,
            };
            if let Some(cached) = self.checkpoint_cache.get(&key) {
                best = Some((
                    cached.state.clone(),
                    cached.ops,
                    completed,
                    hash,
                    CacheHitSource::Checkpoint,
                ));
            }
            if self.prefix_cache_interval != 0 && completed % self.prefix_cache_interval == 0 {
                if let Some(cached) = self.prefix_cache.get(&key) {
                    best = Some((
                        cached.state.clone(),
                        cached.ops,
                        completed,
                        hash,
                        CacheHitSource::Interval,
                    ));
                }
            }
        }

        if let Some((state, ops, completed, hash, source)) = best {
            (state, ops, completed, hash, Some(source))
        } else {
            (RunState::new(trace), 0, 0, initial_prefix_hash(trace), None)
        }
    }

    fn maybe_cache_prefix(&mut self, steps: usize, hash: u64, state: &RunState, ops: usize) {
        if self.prefix_cache_interval == 0
            || self.prefix_cache_max_entries == 0
            || steps % self.prefix_cache_interval != 0
        {
            return;
        }
        let key = PrefixCacheKey { steps, hash };
        if let Some(cached) = self.prefix_cache.get_mut(&key) {
            cached.state = state.clone();
            cached.ops = ops;
            return;
        }
        while self.prefix_cache.len() >= self.prefix_cache_max_entries {
            let Some(oldest) = self.prefix_cache_order.pop_front() else {
                break;
            };
            self.prefix_cache.remove(&oldest);
        }
        self.prefix_cache_order.push_back(key);
        self.prefix_cache.insert(
            key,
            CachedPrefix {
                state: state.clone(),
                ops,
            },
        );
    }
}

fn cmp_sample_rate() -> u64 {
    static CMP_SAMPLE_RATE: OnceLock<u64> = OnceLock::new();
    *CMP_SAMPLE_RATE.get_or_init(|| {
        std::env::var("TRACE_FUZZ_CMP_SAMPLE_RATE")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(1)
    })
}

fn prefix_cache_interval() -> usize {
    static PREFIX_CACHE_INTERVAL: OnceLock<usize> = OnceLock::new();
    *PREFIX_CACHE_INTERVAL.get_or_init(|| {
        std::env::var("TRACE_FUZZ_STATE_CACHE_INTERVAL")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0)
    })
}

fn prefix_cache_max_entries() -> usize {
    static PREFIX_CACHE_MAX_ENTRIES: OnceLock<usize> = OnceLock::new();
    *PREFIX_CACHE_MAX_ENTRIES.get_or_init(|| {
        std::env::var("TRACE_FUZZ_STATE_CACHE_MAX_ENTRIES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(2048)
    })
}

fn checkpoint_cache_max_entries() -> usize {
    static CHECKPOINT_CACHE_MAX_ENTRIES: OnceLock<usize> = OnceLock::new();
    *CHECKPOINT_CACHE_MAX_ENTRIES.get_or_init(|| {
        std::env::var("TRACE_FUZZ_CHECKPOINT_CACHE_MAX_ENTRIES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(512)
    })
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn initial_prefix_hash(trace: &Trace) -> u64 {
    let mut hash = FNV_OFFSET;
    combine_prefix_hash(&mut hash, u64::from(trace.version));
    combine_prefix_hash(&mut hash, structural_hash(&trace.actors));
    combine_prefix_hash(&mut hash, structural_hash(&trace.text_encoding));
    hash
}

fn trace_prefix_key(trace: &Trace) -> PrefixCacheKey {
    let mut hash = initial_prefix_hash(trace);
    for instr in &trace.steps {
        update_prefix_hash_for_instr(&mut hash, instr);
    }
    PrefixCacheKey {
        steps: trace.steps.len(),
        hash,
    }
}

fn update_prefix_hash_for_instr(hash: &mut u64, instr: &VmInstr) {
    combine_prefix_hash(hash, structural_hash(instr));
}

// DefaultHasher::new() uses fixed keys, so these hashes are stable for the
// lifetime of the process, which is all the in-memory prefix caches need.
fn structural_hash(value: &impl std::hash::Hash) -> u64 {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn combine_prefix_hash(hash: &mut u64, value: u64) {
    for byte in value.to_le_bytes() {
        *hash ^= u64::from(byte);
        *hash = hash.wrapping_mul(FNV_PRIME);
    }
}

fn trace_timeout() -> Duration {
    static TRACE_TIMEOUT: OnceLock<Duration> = OnceLock::new();
    *TRACE_TIMEOUT.get_or_init(|| {
        std::env::var("TRACE_FUZZ_TRACE_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_secs(2))
    })
}

#[derive(Clone, Debug)]
pub struct RunReport {
    pub steps: usize,
    pub ops: usize,
    pub docs: usize,
    pub behavior: BehaviorStats,
    pub sometimes_hits: Vec<automerge::sometimes::SometimesHit>,
    pub coverage_hits: Vec<crate::coverage::CoverageCounterHit>,
    pub cmp_observations: Vec<crate::coverage::CmpObservation>,
}

struct SaveLoadOutcome {
    saved_bytes: usize,
    hydrated: hydrate::Value,
}

#[derive(Clone)]
struct RunState {
    actors: Vec<ActorId>,
    docs: Vec<DocState>,
    sessions: Vec<Option<SyncSession>>,
    text_encoding: TextEncoding,
    diff_checks: usize,
    diff_unsupported: usize,
    total_patches: usize,
    merge_checks: usize,
    persistence_transfers: usize,
    bundle_transfers: usize,
    isolation_transitions: usize,
    historical_transactions: usize,
}

#[derive(Clone)]
struct DocState {
    doc: AutoCommit,
    objects: Vec<ObjId>,
    head_slots: Vec<Option<Vec<ChangeHash>>>,
    /// Bumped whenever `Fork` replaces the document at this index with an
    /// unrelated history, which invalidates sync sessions targeting it.
    generation: u64,
}

const MAX_SYNC_SESSIONS: usize = 8;

/// A persistent sync exchange between two documents. Messages sit in the
/// in-flight queues in encoded form, so every delivery exercises the message
/// codec, and delivery faults (drop/duplicate/reorder) can be injected.
#[derive(Clone)]
struct SyncSession {
    left: usize,
    right: usize,
    left_generation: u64,
    right_generation: u64,
    left_state: sync::State,
    right_state: sync::State,
    to_left: VecDeque<Vec<u8>>,
    to_right: VecDeque<Vec<u8>>,
}

impl SyncSession {
    /// Doc index, sync state, and inbound queue for one side.
    fn side_mut(&mut self, left: bool) -> (usize, &mut sync::State, &mut VecDeque<Vec<u8>>) {
        if left {
            (self.left, &mut self.left_state, &mut self.to_left)
        } else {
            (self.right, &mut self.right_state, &mut self.to_right)
        }
    }
}

fn vm_text_encoding(encoding: Option<VmTextEncoding>) -> TextEncoding {
    match encoding {
        None | Some(VmTextEncoding::CodePoint) => TextEncoding::UnicodeCodePoint,
        Some(VmTextEncoding::Utf8) => TextEncoding::Utf8CodeUnit,
        Some(VmTextEncoding::Utf16) => TextEncoding::Utf16CodeUnit,
        Some(VmTextEncoding::Grapheme) => TextEncoding::GraphemeCluster,
    }
}

impl RunState {
    fn new(trace: &Trace) -> Self {
        let actors = if trace.actors.is_empty() {
            vec![ActorId::from(vec![0])]
        } else {
            trace
                .actors
                .iter()
                .map(|actor| ActorId::from(actor.bytes.clone()))
                .collect()
        };
        let text_encoding = vm_text_encoding(trace.text_encoding);
        let mut doc = AutoCommit::new_with_encoding(text_encoding);
        doc.set_actor(actors[0].clone());
        Self {
            actors,
            docs: vec![DocState {
                doc,
                objects: Vec::new(),
                head_slots: Vec::new(),
                generation: 0,
            }],
            sessions: vec![None; MAX_SYNC_SESSIONS],
            text_encoding,
            diff_checks: 0,
            diff_unsupported: 0,
            total_patches: 0,
            merge_checks: 0,
            persistence_transfers: 0,
            bundle_transfers: 0,
            isolation_transitions: 0,
            historical_transactions: 0,
        }
    }

    fn fork_doc(&mut self, from: usize, to: usize) -> Result<(), RunError> {
        let (doc, objects, head_slots) = {
            let from_doc = self.doc_mut(from)?;
            (
                from_doc.doc.fork(),
                from_doc.objects.clone(),
                from_doc.head_slots.clone(),
            )
        };
        self.install_doc(to, doc, objects, head_slots);
        Ok(())
    }

    fn fork_doc_at(&mut self, from: usize, to: usize, head: &VmHeadRef) -> Result<(), RunError> {
        if from == to {
            return Ok(());
        }
        let (doc, objects, head_slots) = {
            let from_doc = self.doc_mut(from)?;
            let heads = from_doc.resolve_heads(from, head)?;
            let doc = from_doc
                .doc
                .fork_at(&heads)
                .map_err(|err| RunError::Automerge(err.to_string()))?;
            (doc, from_doc.objects.clone(), from_doc.head_slots.clone())
        };
        self.install_doc(to, doc, objects, head_slots);
        Ok(())
    }

    fn install_doc(
        &mut self,
        to: usize,
        mut doc: AutoCommit,
        objects: Vec<ObjId>,
        head_slots: Vec<Option<Vec<ChangeHash>>>,
    ) {
        let actor = self.actors[to % self.actors.len()].clone();
        doc.set_actor(actor);
        if to >= self.docs.len() {
            let text_encoding = self.text_encoding;
            self.docs.resize_with(to + 1, || DocState {
                doc: AutoCommit::new_with_encoding(text_encoding),
                objects: Vec::new(),
                head_slots: Vec::new(),
                generation: 0,
            });
        }
        let generation = self.docs[to].generation + 1;
        self.docs[to] = DocState {
            doc,
            objects,
            head_slots,
            generation,
        };
    }

    /// Transfer the changes `into` is missing from `from` through
    /// `apply_changes`, delivered in an adversarial order. For complete
    /// delivery orders, `into` must end up containing all of `from`'s heads
    /// once the causal queue drains.
    fn apply_changes_transfer(
        &mut self,
        from: usize,
        into: usize,
        order: &VmApplyOrder,
    ) -> Result<(), RunError> {
        if from == into {
            return Ok(());
        }
        if from >= self.docs.len() {
            return Err(RunError::MissingDoc { doc: from });
        }
        if into >= self.docs.len() {
            return Err(RunError::MissingDoc { doc: into });
        }

        let into_heads = self.docs[into].doc.get_heads();
        let from_doc = &mut self.docs[from];
        let from_heads = from_doc.doc.get_heads();
        let mut changes = from_doc.doc.get_changes(&into_heads);
        match order {
            VmApplyOrder::InOrder => {}
            VmApplyOrder::Reversed => changes.reverse(),
            VmApplyOrder::Shuffled { seed } => {
                let mut state = u64::from(*seed) | 0x9e37_79b9_0000_0001;
                for index in (1..changes.len()).rev() {
                    // xorshift64: cheap deterministic shuffle without pulling
                    // a RNG dependency into the runner.
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    changes.swap(index, (state as usize) % (index + 1));
                }
            }
            VmApplyOrder::Duplicated => {
                let duplicates = changes.clone();
                changes.extend(duplicates);
            }
            VmApplyOrder::DropHalf => {
                let mut keep = false;
                changes.retain(|_| {
                    keep = !keep;
                    keep
                });
            }
        }

        let complete = !matches!(order, VmApplyOrder::DropHalf);
        let into_doc = &mut self.docs[into];
        into_doc
            .doc
            .apply_changes(changes)
            .map_err(|err| RunError::Automerge(err.to_string()))?;
        if complete {
            for head in from_heads {
                if into_doc.doc.get_change_by_hash(&head).is_none() {
                    return Err(RunError::Invariant(
                        "apply_changes lost a change despite complete delivery".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, into: usize, from: usize) -> Result<(), RunError> {
        if into == from {
            return Ok(());
        }
        if into >= self.docs.len() {
            return Err(RunError::MissingDoc { doc: into });
        }
        if from >= self.docs.len() {
            return Err(RunError::MissingDoc { doc: from });
        }

        // Check the CRDT law, not just the API call: when both directions
        // accept a history, merging the same pair in opposite directions must
        // converge to the same heads and value. A rejected direction rejects
        // this VM instruction because mutation can construct actor-sequence
        // collisions outside Automerge's collaboration model.
        let mut into_first = self.docs[into].doc.clone();
        let mut from_peer = self.docs[from].doc.clone();
        let mut from_first = self.docs[from].doc.clone();
        let mut into_peer = self.docs[into].doc.clone();
        let forward = into_first.merge(&mut from_peer);
        let reverse = from_first.merge(&mut into_peer);
        match (forward, reverse) {
            (Ok(_), Ok(_)) => {
                let into_plain = into_first.document().clone();
                let from_plain = from_first.document().clone();
                if into_plain.get_heads() != from_plain.get_heads()
                    || into_plain.hydrate(None) != from_plain.hydrate(None)
                {
                    return Err(RunError::Invariant(
                        "merge directions did not converge".to_string(),
                    ));
                }

                // Independently exercise the explicit patch-logging merge API
                // and check that its patches materialize the merged state.
                let mut logged_into = self.docs[into].doc.document().clone();
                let mut logged_from = self.docs[from].doc.document().clone();
                let before = logged_into.hydrate(None);
                let mut patch_log = PatchLog::active();
                logged_into
                    .merge_and_log_patches(&mut logged_from, &mut patch_log)
                    .map_err(|err| RunError::Automerge(err.to_string()))?;
                let after = logged_into.hydrate(None);
                let patches = logged_into.make_patches(&mut patch_log);
                let outcome = verify_diff_patches(before, after, patches, self.text_encoding)?;
                self.record_patch_check(outcome);
                self.merge_checks = self.merge_checks.saturating_add(1);
            }
            // Reusing one actor on divergent branches is outside Automerge's
            // collaboration model and can make either merge reject a duplicate
            // sequence number. Reject the VM instruction without changing the
            // live docs; the convergence oracle only applies when both merge
            // directions accept the history.
            (Err(forward), Err(_)) | (Err(forward), Ok(_)) => {
                return Err(RunError::Automerge(format!(
                    "merge oracle rejected generated history: {forward}"
                )));
            }
            (Ok(_), Err(reverse)) => {
                return Err(RunError::Automerge(format!(
                    "reverse merge oracle rejected generated history: {reverse}"
                )));
            }
        }

        let (low, high) = self.docs.split_at_mut(into.max(from));
        let (into_doc, from_doc) = if into < from {
            (&mut low[into], &mut high[0])
        } else {
            (&mut high[0], &mut low[from])
        };
        into_doc
            .doc
            .merge(&mut from_doc.doc)
            .map_err(|err| RunError::Automerge(err.to_string()))?;
        Ok(())
    }

    fn observe(
        &mut self,
        doc: usize,
        object: &VmObjRef,
        mode: VmObserveMode,
        head: &VmHeadRef,
        budget: u8,
    ) -> Result<(), RunError> {
        self.doc_mut(doc)?.observe(doc, object, mode, head, budget)
    }

    fn save_heads(&mut self, doc: usize, slot: u8) -> Result<(), RunError> {
        let doc_state = self.doc_mut(doc)?;
        let slot = usize::from(slot);
        if slot >= doc_state.head_slots.len() {
            doc_state.head_slots.resize_with(slot + 1, || None);
        }
        doc_state.head_slots[slot] = Some(doc_state.doc.get_heads());
        Ok(())
    }

    fn diff_range(
        &mut self,
        doc: usize,
        before: &VmHeadRef,
        after: &VmHeadRef,
    ) -> Result<(), RunError> {
        let text_encoding = self.text_encoding;
        let outcome = {
            let doc_state = self.doc_mut(doc)?;
            let before = doc_state.resolve_heads(doc, before)?;
            let after = doc_state.resolve_heads(doc, after)?;
            let before_value = doc_state
                .doc
                .hydrate(&ROOT, Some(&before))
                .map_err(|err| RunError::Automerge(err.to_string()))?;
            let after_value = doc_state
                .doc
                .hydrate(&ROOT, Some(&after))
                .map_err(|err| RunError::Automerge(err.to_string()))?;
            let patches = doc_state.doc.diff(&before, &after);
            verify_diff_patches(before_value, after_value, patches, text_encoding)?
        };
        self.record_patch_check(outcome);
        Ok(())
    }

    fn update_diff_cursor(&mut self, doc: usize) -> Result<(), RunError> {
        self.doc_mut(doc)?.doc.update_diff_cursor();
        Ok(())
    }

    fn reset_diff_cursor(&mut self, doc: usize) -> Result<(), RunError> {
        self.doc_mut(doc)?.doc.reset_diff_cursor();
        Ok(())
    }

    fn diff_incremental(&mut self, doc: usize) -> Result<(), RunError> {
        let text_encoding = self.text_encoding;
        let outcome = {
            let doc_state = self.doc_mut(doc)?;
            let before = doc_state.doc.diff_cursor();
            let after = doc_state.doc.get_heads();
            let before_value = doc_state
                .doc
                .hydrate(&ROOT, Some(&before))
                .map_err(|err| RunError::Automerge(err.to_string()))?;
            let after_value = doc_state
                .doc
                .hydrate(&ROOT, Some(&after))
                .map_err(|err| RunError::Automerge(err.to_string()))?;
            let patches = doc_state.doc.diff_incremental();
            verify_diff_patches(before_value, after_value, patches, text_encoding)?
        };
        self.record_patch_check(outcome);
        Ok(())
    }

    fn record_patch_check(&mut self, outcome: PatchCheck) {
        self.total_patches = self.total_patches.saturating_add(outcome.patches);
        if outcome.checked {
            self.diff_checks = self.diff_checks.saturating_add(1);
        } else {
            self.diff_unsupported = self.diff_unsupported.saturating_add(1);
        }
    }

    fn sync(&mut self, left: usize, right: usize, rounds: u8) -> Result<(), RunError> {
        if left == right {
            return Ok(());
        }

        let mut left_state = sync::State::new();
        let mut right_state = sync::State::new();

        let mut quiesced = false;
        for _ in 0..usize::from(rounds) {
            let (left_done, right_done) =
                self.sync_round(left, right, &mut left_state, &mut right_state)?;
            if left_done && right_done {
                quiesced = true;
                break;
            }
        }

        // `rounds` is a trace-controlled sync schedule, not a convergence
        // deadline. A non-quiesced schedule is simply an incomplete exchange.
        // If the schedule does reach quiescence, then the protocol claims there
        // are no more messages to send, so both documents must have identical
        // heads.
        if quiesced {
            let left_heads = self.doc_mut(left)?.doc.document().get_heads();
            let right_heads = self.doc_mut(right)?.doc.document().get_heads();
            if left_heads != right_heads {
                return Err(RunError::Invariant(
                    "sync quiesced with mismatched document heads".to_string(),
                ));
            }
        }

        Ok(())
    }

    fn sync_round(
        &mut self,
        left: usize,
        right: usize,
        left_state: &mut sync::State,
        right_state: &mut sync::State,
    ) -> Result<(bool, bool), RunError> {
        let left_to_right = self
            .doc_mut(left)?
            .doc
            .sync()
            .generate_sync_message(left_state);
        let left_done = left_to_right.is_none();
        if let Some(message) = left_to_right {
            self.doc_mut(right)?
                .doc
                .sync()
                .receive_sync_message(right_state, message)
                .map_err(|err| RunError::Automerge(err.to_string()))?;
        }

        let right_to_left = self
            .doc_mut(right)?
            .doc
            .sync()
            .generate_sync_message(right_state);
        let right_done = right_to_left.is_none();
        if let Some(message) = right_to_left {
            self.doc_mut(left)?
                .doc
                .sync()
                .receive_sync_message(left_state, message)
                .map_err(|err| RunError::Automerge(err.to_string()))?;
        }

        Ok((left_done, right_done))
    }

    fn sync_session(&mut self, session: u8, op: &VmSyncOp) -> Result<(), RunError> {
        let slot = usize::from(session) % MAX_SYNC_SESSIONS;
        match op {
            VmSyncOp::Start { left, right } => {
                let left = usize::from(*left);
                let right = usize::from(*right);
                if left == right {
                    return Ok(());
                }
                let left_generation = self.doc_mut(left)?.generation;
                let right_generation = self.doc_mut(right)?.generation;
                self.sessions[slot] = Some(SyncSession {
                    left,
                    right,
                    left_generation,
                    right_generation,
                    left_state: sync::State::new(),
                    right_state: sync::State::new(),
                    to_left: VecDeque::new(),
                    to_right: VecDeque::new(),
                });
            }
            VmSyncOp::Generate { from_left } => {
                let Some(session) = self.sessions[slot].as_mut() else {
                    return Ok(());
                };
                let (doc, state, _) = session.side_mut(*from_left);
                let Some(doc_state) = self.docs.get_mut(doc) else {
                    return Ok(());
                };
                if let Some(message) = doc_state.doc.sync().generate_sync_message(state) {
                    let outbound = if *from_left {
                        &mut session.to_right
                    } else {
                        &mut session.to_left
                    };
                    outbound.push_back(message.encode());
                }
            }
            VmSyncOp::Deliver { to_left, fault } => {
                let Some(session) = self.sessions[slot].as_mut() else {
                    return Ok(());
                };
                let (doc, state, inbound) = session.side_mut(*to_left);
                let bytes = match fault {
                    VmSyncFault::Reorder => inbound.pop_back(),
                    _ => inbound.pop_front(),
                };
                let Some(bytes) = bytes else {
                    return Ok(());
                };
                if matches!(fault, VmSyncFault::Drop) {
                    return Ok(());
                }
                let Some(doc_state) = self.docs.get_mut(doc) else {
                    return Ok(());
                };
                let deliveries = if matches!(fault, VmSyncFault::Duplicate) {
                    2
                } else {
                    1
                };
                for _ in 0..deliveries {
                    let message = sync::Message::decode(&bytes).map_err(|err| {
                        RunError::Invariant(format!(
                            "sync message failed to decode its own encoding: {err}"
                        ))
                    })?;
                    doc_state
                        .doc
                        .sync()
                        .receive_sync_message(state, message)
                        .map_err(|err| RunError::Automerge(err.to_string()))?;
                }
            }
            VmSyncOp::SaveStates => {
                let Some(session) = self.sessions[slot].as_mut() else {
                    return Ok(());
                };
                for state in [&mut session.left_state, &mut session.right_state] {
                    let decoded = sync::State::decode(&state.encode()).map_err(|err| {
                        RunError::Invariant(format!(
                            "sync state failed to decode its own encoding: {err}"
                        ))
                    })?;
                    // Decoding deliberately resets the in-memory-only fields
                    // (`their_have` and friends), exactly as a process restart
                    // would, so no equality check here; the protocol has to
                    // recover from the reset, which `Finish` verifies.
                    *state = decoded;
                }
            }
            VmSyncOp::SetReadOnly { left, read_only } => {
                let Some(session) = self.sessions[slot].as_mut() else {
                    return Ok(());
                };
                let (_, state, _) = session.side_mut(*left);
                state.set_read_only(*read_only);
            }
            VmSyncOp::Finish { rounds } => return self.sync_session_finish(slot, *rounds),
        }
        Ok(())
    }

    fn sync_session_finish(&mut self, slot: usize, rounds: u8) -> Result<(), RunError> {
        let Some(mut session) = self.sessions[slot].take() else {
            return Ok(());
        };
        if session.left >= self.docs.len() || session.right >= self.docs.len() {
            return Ok(());
        }

        // Flush anything still in flight, then run the protocol reliably.
        for to_left in [true, false] {
            loop {
                let (doc, state, inbound) = session.side_mut(to_left);
                let Some(bytes) = inbound.pop_front() else {
                    break;
                };
                let Ok(message) = sync::Message::decode(&bytes) else {
                    break;
                };
                let doc_state = &mut self.docs[doc];
                doc_state
                    .doc
                    .sync()
                    .receive_sync_message(state, message)
                    .map_err(|err| RunError::Automerge(err.to_string()))?;
            }
        }

        let mut quiesced = false;
        for _ in 0..usize::from(rounds) {
            let mut sent = false;
            for from_left in [true, false] {
                let (from_doc, from_state, _) = session.side_mut(from_left);
                let message = self.docs[from_doc]
                    .doc
                    .sync()
                    .generate_sync_message(from_state);
                if let Some(message) = message {
                    sent = true;
                    let (to_doc, to_state, _) = session.side_mut(!from_left);
                    self.docs[to_doc]
                        .doc
                        .sync()
                        .receive_sync_message(to_state, message)
                        .map_err(|err| RunError::Automerge(err.to_string()))?;
                }
            }
            if !sent {
                quiesced = true;
                break;
            }
        }

        // The convergence invariant only applies if neither document was
        // replaced by a Fork since the session started; reusing a sync state
        // against an unrelated history is API misuse the protocol does not
        // promise to converge from.
        let clean = self.docs[session.left].generation == session.left_generation
            && self.docs[session.right].generation == session.right_generation;
        if quiesced && clean {
            let left_heads = self.docs[session.left].doc.document().get_heads();
            let right_heads = self.docs[session.right].doc.document().get_heads();
            match (session.left_state.read_only, session.right_state.read_only) {
                (false, false) if left_heads != right_heads => {
                    return Err(RunError::Invariant(
                        "sync session quiesced without converging".to_string(),
                    ));
                }
                // A read-only side publishes but does not receive. At
                // quiescence the writable peer must therefore contain every
                // change advertised by the read-only peer.
                (true, false) => self.check_heads_present(session.right, &left_heads)?,
                (false, true) => self.check_heads_present(session.left, &right_heads)?,
                (true, true) | (false, false) => {}
            }
        }
        Ok(())
    }

    fn check_heads_present(&mut self, doc: usize, heads: &[ChangeHash]) -> Result<(), RunError> {
        let doc_state = self.doc_mut(doc)?;
        if heads
            .iter()
            .any(|head| doc_state.doc.get_change_by_hash(head).is_none())
        {
            return Err(RunError::Invariant(
                "directional sync lost a read-only peer change".to_string(),
            ));
        }
        Ok(())
    }

    fn persist(&mut self, from: usize, into: usize, mode: &VmPersistMode) -> Result<(), RunError> {
        if from >= self.docs.len() {
            return Err(RunError::MissingDoc { doc: from });
        }
        if into >= self.docs.len() {
            return Err(RunError::MissingDoc { doc: into });
        }
        let mut prerequisite_heads = None;
        let bytes = match mode {
            VmPersistMode::Incremental => self.docs[from].doc.save_incremental(),
            VmPersistMode::SaveAfter { since } => {
                let heads = self.docs[from].resolve_heads(from, since)?;
                prerequisite_heads = Some(heads.clone());
                self.docs[from].doc.save_after(&heads)
            }
            VmPersistMode::Bundle { since } => {
                let heads = self.docs[from].resolve_heads(from, since)?;
                prerequisite_heads = Some(heads.clone());
                let hashes = self.docs[from]
                    .doc
                    .get_changes(&heads)
                    .into_iter()
                    .map(|change| change.hash())
                    .collect::<Vec<_>>();
                let bundle = self.docs[from]
                    .doc
                    .bundle(hashes)
                    .map_err(|err| RunError::Automerge(err.to_string()))?;
                // Exercise the structured bundle views as well as the encoded
                // load path, and require the bundle to decode its own bytes.
                let _ = bundle.actors();
                let _ = bundle.authors();
                let _ = bundle.deps();
                let _ = bundle.iter_changes().count();
                let bytes = bundle.bytes().to_vec();
                let decoded = Bundle::try_from(bytes.as_slice()).map_err(|err| {
                    RunError::Invariant(format!("bundle failed to decode: {err}"))
                })?;
                let _ = decoded.to_changes().map_err(|err| {
                    RunError::Invariant(format!("bundle failed to reconstruct changes: {err}"))
                })?;
                bytes
            }
        };
        let can_complete = prerequisite_heads.as_ref().is_some_and(|heads| {
            heads
                .iter()
                .all(|head| self.docs[into].doc.get_change_by_hash(head).is_some())
        });
        let source_heads = self.docs[from].doc.document().get_heads();
        self.load_incremental_with_oracle(into, &bytes)?;
        self.persistence_transfers = self.persistence_transfers.saturating_add(1);
        if matches!(mode, VmPersistMode::Bundle { .. }) {
            self.bundle_transfers = self.bundle_transfers.saturating_add(1);
        }
        if can_complete
            && source_heads
                .iter()
                .any(|head| self.docs[into].doc.get_change_by_hash(head).is_none())
        {
            return Err(RunError::Invariant(
                "persistence transfer lost a source change".to_string(),
            ));
        }
        Ok(())
    }

    fn load_incremental_with_oracle(&mut self, into: usize, bytes: &[u8]) -> Result<(), RunError> {
        let text_encoding = self.text_encoding;
        let before = self.docs[into]
            .doc
            .hydrate(&ROOT, None)
            .map_err(|err| RunError::Automerge(err.to_string()))?;
        let mut plain = self.docs[into].doc.document().clone();
        let mut patch_log = PatchLog::active();
        plain
            .load_incremental_log_patches(bytes, &mut patch_log)
            .map_err(|err| RunError::Automerge(err.to_string()))?;
        let expected = plain.hydrate(None);
        let expected_heads = plain.get_heads();
        let patches = plain.make_patches(&mut patch_log);
        let outcome = verify_diff_patches(before, expected.clone(), patches, text_encoding)?;
        self.record_patch_check(outcome);

        self.docs[into]
            .doc
            .load_incremental(bytes)
            .map_err(|err| RunError::Automerge(err.to_string()))?;
        let actual = self.docs[into]
            .doc
            .hydrate(&ROOT, None)
            .map_err(|err| RunError::Automerge(err.to_string()))?;
        if actual != expected || self.docs[into].doc.document().get_heads() != expected_heads {
            return Err(RunError::Invariant(
                "AutoCommit and Automerge incremental loads disagreed".to_string(),
            ));
        }
        Ok(())
    }

    fn isolate(&mut self, doc: usize, head: &VmHeadRef) -> Result<(), RunError> {
        let doc_state = self.doc_mut(doc)?;
        let heads = doc_state.resolve_heads(doc, head)?;
        doc_state.doc.isolate(&heads);
        if doc_state.doc.get_heads() != heads {
            return Err(RunError::Invariant(
                "isolated AutoCommit did not expose requested heads".to_string(),
            ));
        }
        // Exercise reads through the isolated scope. `hydrate` currently takes
        // its explicit heads argument directly rather than consulting the
        // AutoCommit isolation scope, so it is checked explicitly at `heads`
        // instead of being used as an isolation oracle here.
        let _ = doc_state.doc.keys(&ROOT).count();
        let _ = doc_state.doc.hydrate(&ROOT, Some(&heads));
        self.isolation_transitions = self.isolation_transitions.saturating_add(1);
        Ok(())
    }

    fn integrate(&mut self, doc: usize) -> Result<(), RunError> {
        let doc_state = self.doc_mut(doc)?;
        let expected = doc_state.doc.document().hydrate(None);
        let expected_heads = doc_state.doc.document().get_heads();
        doc_state.doc.integrate();
        let actual = doc_state
            .doc
            .hydrate(&ROOT, None)
            .map_err(|err| RunError::Automerge(err.to_string()))?;
        if doc_state.doc.get_heads() != expected_heads || actual != expected {
            return Err(RunError::Invariant(
                "integrating AutoCommit did not restore current state".to_string(),
            ));
        }
        self.isolation_transitions = self.isolation_transitions.saturating_add(1);
        Ok(())
    }

    fn save_load(&mut self, doc: usize) -> Result<SaveLoadOutcome, RunError> {
        let text_encoding = self.text_encoding;
        let doc_state = self.doc_mut(doc)?;
        let before = doc_state
            .doc
            .hydrate(&ROOT, None)
            .map_err(|err| RunError::Automerge(err.to_string()))?;
        let heads_before = doc_state.doc.document().get_heads();
        let nocompressed = doc_state.doc.save_nocompress();
        let nocompressed_loaded = AutoCommit::load_with_options(
            &nocompressed,
            LoadOptions::new().text_encoding(text_encoding),
        )
        .map_err(|err| RunError::Load(err.to_string()))?;
        let nocompressed_after = nocompressed_loaded
            .hydrate(&ROOT, None)
            .map_err(|err| RunError::Automerge(err.to_string()))?;
        if nocompressed_after != before {
            return Err(RunError::Invariant(
                "save_nocompress/load changed hydrated document".to_string(),
            ));
        }
        let bytes = doc_state
            .doc
            .save_and_verify()
            .map_err(|err| RunError::Invariant(format!("save_and_verify failed: {err}")))?;
        let mut loaded =
            AutoCommit::load_with_options(&bytes, LoadOptions::new().text_encoding(text_encoding))
                .map_err(|err| RunError::Load(err.to_string()))?;
        let after = loaded
            .hydrate(&ROOT, None)
            .map_err(|err| RunError::Automerge(err.to_string()))?;
        if before != after {
            return Err(RunError::Invariant(
                "save/load changed hydrated document".to_string(),
            ));
        }
        if heads_before != loaded.get_heads() {
            return Err(RunError::Invariant(
                "save/load changed document heads".to_string(),
            ));
        }
        doc_state.doc = loaded;
        Ok(SaveLoadOutcome {
            saved_bytes: bytes.len(),
            hydrated: after,
        })
    }

    fn run_vm(&mut self, instructions: &[VmInstr]) -> Result<usize, RunError> {
        let mut ops = 0;
        for instr in instructions {
            let result = match instr {
                VmInstr::Fork { from, to } => self.fork_doc(usize::from(*from), usize::from(*to)),
                VmInstr::Merge { into, from } => self.merge(usize::from(*into), usize::from(*from)),
                VmInstr::ForkAt { from, to, head } => {
                    self.fork_doc_at(usize::from(*from), usize::from(*to), head)
                }
                VmInstr::ApplyChanges { from, into, order } => {
                    self.apply_changes_transfer(usize::from(*from), usize::from(*into), order)
                }
                VmInstr::Change {
                    doc,
                    actor,
                    ops: vm_ops,
                } => {
                    let result =
                        self.apply_vm_change(usize::from(*doc), usize::from(*actor), vm_ops);
                    if result.is_ok() {
                        ops += vm_ops.len();
                    }
                    result
                }
                VmInstr::Transact {
                    doc,
                    actor,
                    ops: vm_ops,
                    commit,
                } => {
                    let result =
                        self.transact(usize::from(*doc), usize::from(*actor), vm_ops, *commit);
                    if result.is_ok() && *commit {
                        ops += vm_ops.len();
                    }
                    result
                }
                VmInstr::TransactAt {
                    doc,
                    actor,
                    head,
                    ops: vm_ops,
                    commit,
                } => {
                    let result = self.transact_at_owned(
                        usize::from(*doc),
                        usize::from(*actor),
                        head,
                        vm_ops,
                        *commit,
                    );
                    if result.is_ok() && *commit {
                        ops += vm_ops.len();
                    }
                    result
                }
                VmInstr::Persist { from, into, mode } => {
                    self.persist(usize::from(*from), usize::from(*into), mode)
                }
                VmInstr::Isolate { doc, head } => self.isolate(usize::from(*doc), head),
                VmInstr::Integrate { doc } => self.integrate(usize::from(*doc)),
                VmInstr::SaveLoad { doc } => self.save_load(usize::from(*doc)).map(drop),
                VmInstr::Observe {
                    doc,
                    object,
                    mode,
                    head,
                    budget,
                } => self.observe(usize::from(*doc), object, *mode, head, *budget),
                VmInstr::SaveHeads { doc, slot } => self.save_heads(usize::from(*doc), *slot),
                VmInstr::DiffRange { doc, before, after } => {
                    self.diff_range(usize::from(*doc), before, after)
                }
                VmInstr::UpdateDiffCursor { doc } => self.update_diff_cursor(usize::from(*doc)),
                VmInstr::ResetDiffCursor { doc } => self.reset_diff_cursor(usize::from(*doc)),
                VmInstr::DiffIncremental { doc } => self.diff_incremental(usize::from(*doc)),
                VmInstr::Sync {
                    left,
                    right,
                    rounds,
                } => self.sync(usize::from(*left), usize::from(*right), *rounds),
                VmInstr::SyncSession { session, op } => self.sync_session(*session, op),
            };
            if let Err(err) = result {
                if err.is_invariant_or_panic() {
                    return Err(err);
                }
                // VM programs are a mutation substrate rather than a strict
                // user-authored trace format. Invalid references and invalid
                // operation/object combinations are treated as no-ops so that
                // later instructions can still execute and mutate around the
                // current live state.
            }
        }
        Ok(ops)
    }

    fn apply_vm_change(
        &mut self,
        doc: usize,
        actor: usize,
        vm_ops: &[VmOp],
    ) -> Result<(), RunError> {
        let actor_id = self.actors[actor % self.actors.len()].clone();
        self.doc_mut(doc)?.doc.set_actor(actor_id);
        for op in vm_ops {
            if let Err(err) = self.doc_mut(doc)?.apply_vm_op(op) {
                if err.is_invariant_or_panic() {
                    return Err(err);
                }
                // Once an operation in an AutoCommit transaction returns an
                // error, do not keep issuing more operations into the same
                // transaction. Some APIs may have performed partial setup
                // before returning the error; ending the VM change here keeps
                // invalid VM operands as no-ops without compounding them into
                // artificial transaction states.
                break;
            }
        }
        let _ = self.doc_mut(doc)?.doc.commit();
        Ok(())
    }

    /// Run `vm_ops` inside an explicit `Automerge::transaction()` on a copy of
    /// the document. Committed transactions are integrated into the live doc
    /// via `apply_changes`, exercising the change-application queue; rolled
    /// back transactions must leave the copy exactly as it was.
    fn transact(
        &mut self,
        doc: usize,
        actor: usize,
        vm_ops: &[VmOp],
        commit: bool,
    ) -> Result<(), RunError> {
        let actor_id = self.actors[actor % self.actors.len()].clone();
        let (heads_before, mut plain, mut objects) = {
            let doc_state = self.doc_mut(doc)?;
            let plain = doc_state.doc.document().clone();
            (plain.get_heads(), plain, doc_state.objects.clone())
        };
        plain.set_actor(actor_id);
        let before = plain.hydrate(None);

        let mut tx = plain
            .transaction_log_patches(PatchLog::active())
            .map_err(|err| RunError::Automerge(err.to_string()))?;
        for op in vm_ops {
            if let Err(err) = apply_vm_op_tx(&mut tx, &mut objects, op) {
                if err.is_invariant_or_panic() {
                    return Err(err);
                }
                // Same policy as apply_vm_change: stop issuing ops into a
                // transaction after the first error.
                break;
            }
        }

        if commit {
            let (_, mut patch_log) = tx.commit();
            let after = plain.hydrate(None);
            let patches = plain.make_patches(&mut patch_log);
            let outcome = verify_diff_patches(before, after, patches, self.text_encoding)?;
            self.record_patch_check(outcome);
            let changes = plain.get_changes(&heads_before);
            let doc_state = self.doc_mut(doc)?;
            doc_state
                .doc
                .apply_changes(changes)
                .map_err(|err| RunError::Automerge(err.to_string()))?;
            doc_state.objects = objects;
        } else {
            tx.rollback();
            if plain.get_heads() != heads_before {
                return Err(RunError::Invariant(
                    "transaction rollback changed document heads".to_string(),
                ));
            }
            let after = plain.hydrate(None);
            if before != after {
                return Err(RunError::Invariant(
                    "transaction rollback changed hydrated document".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn transact_at_owned(
        &mut self,
        doc: usize,
        actor: usize,
        head: &VmHeadRef,
        vm_ops: &[VmOp],
        commit: bool,
    ) -> Result<(), RunError> {
        let actor_id = self.actors[actor % self.actors.len()].clone();
        let (live_heads, historical_heads, mut plain, mut objects) = {
            let doc_state = self.doc_mut(doc)?;
            let historical_heads = doc_state.resolve_heads(doc, head)?;
            let plain = doc_state.doc.document().clone();
            (
                plain.get_heads(),
                historical_heads,
                plain,
                doc_state.objects.clone(),
            )
        };
        plain.set_actor(actor_id);
        let historical = plain.hydrate(Some(&historical_heads));
        let full_before = (!commit).then(|| plain.hydrate(None));
        let mut tx = plain
            .into_transaction(Some(PatchLog::active()), Some(&historical_heads))
            .map_err(|err| RunError::Automerge(err.to_string()))?;
        for op in vm_ops {
            if let Err(err) = apply_vm_op_tx(&mut tx, &mut objects, op) {
                if err.is_invariant_or_panic() {
                    return Err(err);
                }
                break;
            }
        }

        if commit {
            let (plain, hash, mut patch_log) = tx.commit();
            let branch_heads = hash.map_or_else(|| historical_heads.clone(), |hash| vec![hash]);
            let branch_after = plain.hydrate(Some(&branch_heads));
            let patches = plain.make_patches(&mut patch_log);
            let outcome =
                verify_diff_patches(historical, branch_after, patches, self.text_encoding)?;
            self.record_patch_check(outcome);
            let changes = plain.get_changes(&live_heads);
            let doc_state = self.doc_mut(doc)?;
            doc_state
                .doc
                .apply_changes(changes)
                .map_err(|err| RunError::Automerge(err.to_string()))?;
            doc_state.objects = objects;
        } else {
            let (plain, _) = tx.rollback();
            if plain.get_heads() != live_heads || full_before.as_ref() != Some(&plain.hydrate(None))
            {
                return Err(RunError::Invariant(
                    "owned historical transaction rollback changed document".to_string(),
                ));
            }
        }
        self.historical_transactions = self.historical_transactions.saturating_add(1);
        Ok(())
    }

    fn doc_mut(&mut self, doc: usize) -> Result<&mut DocState, RunError> {
        self.docs.get_mut(doc).ok_or(RunError::MissingDoc { doc })
    }
}

impl DocState {
    /// Cross-check text objects this doc created against fresh recomputations.
    ///
    /// Two oracles, both aimed at multi-code-point / grapheme accounting:
    /// * the `Span::Text` runs from `spans()`, concatenated, must equal
    ///   `text()` (sound under every encoding). Block/object markers embedded
    ///   in the sequence appear in `spans()` but not in `text()`, so only the
    ///   text runs are compared.
    /// * `length()` (the internal width index) must equal the width of
    ///   `text()` recomputed from scratch. This is only checked when the text
    ///   has no embedded blocks (whose sequence width need not match any
    ///   code-unit count) and is not grapheme-encoded: the grapheme encoding
    ///   stores each spliced value's clusters separately, so re-segmenting the
    ///   rendered string can legitimately merge clusters across splice
    ///   boundaries and change the count.
    fn check_text_invariants(&mut self, encoding: TextEncoding) -> Result<(), RunError> {
        let objects: Vec<ObjId> = self.objects.iter().rev().take(16).cloned().collect();
        for obj in objects {
            if self.doc.object_type(&obj) != Ok(ObjType::Text) {
                continue;
            }
            let Ok(text) = self.doc.text(&obj) else {
                continue;
            };
            // Embedded objects and block markers appear in spans() (as
            // `Span::Block`, or as the U+FFFC object-replacement placeholder
            // inside a text run) but are omitted from text(). Ignore them when
            // reconciling the two, and skip the width check when present since
            // their sequence width need not match any code-unit count.
            let mut has_embedded = false;
            if let Ok(spans) = self.doc.spans(&obj) {
                let mut text_runs = String::new();
                for span in spans {
                    match span {
                        Span::Text { text, .. } => text_runs.push_str(&text),
                        Span::Block(_) => has_embedded = true,
                    }
                }
                if text_runs.contains(OBJECT_REPLACEMENT) {
                    has_embedded = true;
                }
                let text_runs = strip_object_placeholders(&text_runs);
                let text_visible = strip_object_placeholders(&text);
                if text_runs != text_visible {
                    return Err(RunError::Invariant(
                        "spans text runs do not reconstruct text()".to_string(),
                    ));
                }
            }
            if !has_embedded && encoding != TextEncoding::GraphemeCluster {
                let expected = match encoding {
                    TextEncoding::UnicodeCodePoint => text.chars().count(),
                    TextEncoding::Utf8CodeUnit => text.len(),
                    TextEncoding::Utf16CodeUnit => text.chars().map(char::len_utf16).sum(),
                    TextEncoding::GraphemeCluster => unreachable!(),
                };
                let actual = self.doc.length(&obj);
                if actual != expected {
                    return Err(RunError::Invariant(format!(
                        "text length {actual} != recomputed width {expected} for {encoding:?}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Best-effort count of mark spans on text objects this doc created
    /// itself. Objects merged in from other docs are not tracked here, so this
    /// undercounts; it only needs to be deterministic for bucketing.
    fn count_text_marks(&mut self) -> usize {
        let objects = self
            .objects
            .iter()
            .rev()
            .take(8)
            .cloned()
            .collect::<Vec<_>>();
        let mut total = 0;
        for obj in objects {
            if self.doc.object_type(&obj) == Ok(ObjType::Text) {
                if let Ok(marks) = self.doc.marks(&obj) {
                    total += marks.len();
                }
            }
        }
        total
    }

    fn observe(
        &mut self,
        doc: usize,
        object: &VmObjRef,
        mode: VmObserveMode,
        head: &VmHeadRef,
        budget: u8,
    ) -> Result<(), RunError> {
        let obj = self.resolve_vm_obj(object)?;
        let obj_type = self
            .doc
            .object_type(&obj)
            .map_err(|err| RunError::Automerge(err.to_string()))?;
        let budget = usize::from(budget.clamp(1, 32));

        match mode {
            VmObserveMode::Shallow => self.observe_shallow(&obj, budget),
            VmObserveMode::Hydrate => self.observe_hydrate(doc, &obj, head)?,
            VmObserveMode::Ranges => self.observe_ranges(doc, &obj, head, budget)?,
            VmObserveMode::Historical => {
                self.observe_historical(doc, &obj, obj_type, head, budget)?
            }
            VmObserveMode::MapGets => self.observe_map_gets(doc, &obj, obj_type, head, budget)?,
            VmObserveMode::Text => self.observe_text(doc, &obj, obj_type, head, budget)?,
            VmObserveMode::Cursors => self.observe_cursors(doc, &obj, obj_type, head)?,
            VmObserveMode::Marks => self.observe_marks(doc, &obj, obj_type, head)?,
        }
        Ok(())
    }

    fn observe_shallow(&mut self, obj: &ObjId, budget: usize) {
        let _ = self.doc.parents(obj);
        let _ = self.doc.length(obj);
        let _ = self.doc.keys(obj).take(budget).count();
        let _ = self.doc.values(obj).take(budget).count();
    }

    fn observe_hydrate(
        &mut self,
        doc: usize,
        obj: &ObjId,
        head: &VmHeadRef,
    ) -> Result<(), RunError> {
        if matches!(head, VmHeadRef::Current) {
            let _ = self.doc.hydrate(obj, None);
        } else {
            let heads = self.resolve_heads(doc, head)?;
            let _ = self.doc.hydrate(obj, Some(&heads));
        }
        Ok(())
    }

    fn observe_ranges(
        &mut self,
        doc: usize,
        obj: &ObjId,
        head: &VmHeadRef,
        budget: usize,
    ) -> Result<(), RunError> {
        if matches!(head, VmHeadRef::Current) {
            let _ = self.doc.keys(obj).take(budget).count();
            let _ = self.doc.values(obj).take(budget).count();
            let _ = self.doc.map_range(obj, ..).take(budget).count();
            let _ = self.doc.list_range(obj, ..).take(budget).count();
        } else {
            let heads = self.resolve_heads(doc, head)?;
            let _ = self.doc.keys_at(obj, &heads).take(budget).count();
            let _ = self.doc.values_at(obj, &heads).take(budget).count();
            let _ = self.doc.map_range_at(obj, .., &heads).take(budget).count();
            let _ = self.doc.list_range_at(obj, .., &heads).take(budget).count();
        }
        Ok(())
    }

    fn observe_historical(
        &mut self,
        doc: usize,
        obj: &ObjId,
        obj_type: ObjType,
        head: &VmHeadRef,
        budget: usize,
    ) -> Result<(), RunError> {
        let heads = self.resolve_heads(doc, head)?;
        let _ = self.doc.hydrate(obj, Some(&heads));
        let _ = self.doc.parents_at(obj, &heads);
        let _ = self.doc.keys_at(obj, &heads).take(budget).count();
        let _ = self.doc.values_at(obj, &heads).take(budget).count();
        let _ = self.doc.map_range_at(obj, .., &heads).take(budget).count();
        let _ = self.doc.list_range_at(obj, .., &heads).take(budget).count();
        match obj_type {
            ObjType::Map | ObjType::Table => {
                for key in self.doc.keys(obj).take(budget).collect::<Vec<_>>() {
                    let _ = self.doc.get_at(obj, key.as_str(), &heads);
                    let _ = self.doc.get_all_at(obj, key.as_str(), &heads);
                }
            }
            ObjType::Text => {
                let _ = self.doc.text_at(obj, &heads);
                let _ = self
                    .doc
                    .spans_at(obj, &heads)
                    .map(|spans| spans.take(budget).count());
                let _ = self.doc.marks_at(obj, &heads);
            }
            _ => {}
        }
        Ok(())
    }

    fn observe_map_gets(
        &mut self,
        doc: usize,
        obj: &ObjId,
        obj_type: ObjType,
        head: &VmHeadRef,
        budget: usize,
    ) -> Result<(), RunError> {
        if !matches!(obj_type, ObjType::Map | ObjType::Table) {
            return Ok(());
        }
        let keys = self.doc.keys(obj).take(budget).collect::<Vec<_>>();
        if matches!(head, VmHeadRef::Current) {
            for key in keys {
                let _ = self.doc.get(obj, key.as_str());
                let _ = self.doc.get_all(obj, key.as_str());
            }
        } else {
            let heads = self.resolve_heads(doc, head)?;
            for key in keys {
                let _ = self.doc.get_at(obj, key.as_str(), &heads);
                let _ = self.doc.get_all_at(obj, key.as_str(), &heads);
            }
        }
        Ok(())
    }

    fn observe_text(
        &mut self,
        doc: usize,
        obj: &ObjId,
        obj_type: ObjType,
        head: &VmHeadRef,
        budget: usize,
    ) -> Result<(), RunError> {
        if obj_type != ObjType::Text {
            return Ok(());
        }
        if matches!(head, VmHeadRef::Current) {
            let _ = self.doc.text(obj);
            let _ = self.doc.spans(obj).map(|spans| spans.take(budget).count());
        } else {
            let heads = self.resolve_heads(doc, head)?;
            let _ = self.doc.text_at(obj, &heads);
            let _ = self
                .doc
                .spans_at(obj, &heads)
                .map(|spans| spans.take(budget).count());
        }
        Ok(())
    }

    fn observe_cursors(
        &mut self,
        doc: usize,
        obj: &ObjId,
        obj_type: ObjType,
        head: &VmHeadRef,
    ) -> Result<(), RunError> {
        if !matches!(obj_type, ObjType::List | ObjType::Text) {
            return Ok(());
        }
        let heads = if matches!(head, VmHeadRef::Current) {
            None
        } else {
            Some(self.resolve_heads(doc, head)?)
        };
        let at = heads.as_deref();
        let len = self.doc.length(obj);
        if len > 0 {
            let pos = len / 2;
            if let Ok(cursor) = self.doc.get_cursor(obj, pos, at) {
                let position = self.doc.get_cursor_position(obj, &cursor, at).ok();
                // Round-trip the cursor through both serialized forms; a
                // decoded cursor must resolve to the same position.
                let bytes = cursor.to_bytes();
                let decoded = Cursor::try_from(&bytes[..]).map_err(|err| {
                    RunError::Invariant(format!("cursor failed to decode its own bytes: {err}"))
                })?;
                let text = cursor.to_string();
                let parsed = Cursor::try_from(text.as_str()).map_err(|err| {
                    RunError::Invariant(format!(
                        "cursor failed to parse its own string form: {err}"
                    ))
                })?;
                for copy in [decoded, parsed] {
                    let round_tripped = self.doc.get_cursor_position(obj, &copy, at).ok();
                    if round_tripped != position {
                        return Err(RunError::Invariant(
                            "cursor round trip changed its position".to_string(),
                        ));
                    }
                }
            }
            let move_cursor = if obj_type == ObjType::Text {
                MoveCursor::After
            } else {
                MoveCursor::Before
            };
            if let Ok(cursor) = self.doc.get_cursor_moving(obj, pos, at, move_cursor) {
                let _ = self.doc.get_cursor_position(obj, &cursor, at);
            }
        }
        Ok(())
    }

    fn observe_marks(
        &mut self,
        doc: usize,
        obj: &ObjId,
        obj_type: ObjType,
        head: &VmHeadRef,
    ) -> Result<(), RunError> {
        if obj_type != ObjType::Text {
            return Ok(());
        }
        if matches!(head, VmHeadRef::Current) {
            let _ = self.doc.marks(obj);
            let len = self.doc.length(obj);
            if len > 0 {
                let _ = self.doc.get_marks(obj, len / 2, None);
            }
        } else {
            let heads = self.resolve_heads(doc, head)?;
            let _ = self.doc.marks_at(obj, &heads);
            let len = self.doc.length(obj);
            if len > 0 {
                let _ = self.doc.get_marks(obj, len / 2, Some(&heads));
            }
        }
        Ok(())
    }

    fn resolve_heads(
        &mut self,
        doc: usize,
        head_ref: &VmHeadRef,
    ) -> Result<Vec<ChangeHash>, RunError> {
        match head_ref {
            VmHeadRef::Empty => Ok(Vec::new()),
            VmHeadRef::Current => Ok(self.doc.get_heads()),
            VmHeadRef::Slot { slot } => self
                .head_slots
                .get(usize::from(*slot))
                .and_then(|heads| heads.clone())
                .ok_or(RunError::MissingHeads { doc, slot: *slot }),
        }
    }

    fn apply_vm_op(&mut self, op: &VmOp) -> Result<(), RunError> {
        apply_vm_op_tx(&mut self.doc, &mut self.objects, op)
    }

    fn resolve_vm_obj(&self, obj: &VmObjRef) -> Result<ObjId, RunError> {
        resolve_vm_obj(&self.objects, obj)
    }
}

/// Apply one VM operation through any transaction-capable document view. This
/// is shared between the [`AutoCommit`] fast path and explicit
/// [`automerge::transaction::Transaction`]s run by `Transact` instructions.
fn apply_vm_op_tx<T: ReadDoc + Transactable>(
    doc: &mut T,
    objects: &mut Vec<ObjId>,
    op: &VmOp,
) -> Result<(), RunError> {
    match op {
        VmOp::Put { obj, key, value } => {
            let obj = resolve_vm_obj(objects, obj)?;
            doc.put(&obj, vm_key(*key), value.to_scalar_value())
                .map_err(|err| RunError::Automerge(err.to_string()))?;
        }
        VmOp::MakeMap { obj, key } => vm_make_object(doc, objects, obj, *key, ObjType::Map)?,
        VmOp::MakeList { obj, key } => vm_make_object(doc, objects, obj, *key, ObjType::List)?,
        VmOp::MakeText { obj, key } => vm_make_object(doc, objects, obj, *key, ObjType::Text)?,
        VmOp::Insert { obj, index, value } => {
            let obj = resolve_vm_obj(objects, obj)?;
            let len = doc.length(&obj);
            doc.insert(
                &obj,
                usize::from(*index) % (len + 1),
                value.to_scalar_value(),
            )
            .map_err(|err| RunError::Automerge(err.to_string()))?;
        }
        VmOp::PutSeq { obj, index, value } => {
            let obj = resolve_vm_obj(objects, obj)?;
            let len = doc.length(&obj);
            if len == 0 {
                doc.insert(&obj, 0, value.to_scalar_value())
                    .map_err(|err| RunError::Automerge(err.to_string()))?;
            } else {
                doc.put(&obj, usize::from(*index) % len, value.to_scalar_value())
                    .map_err(|err| RunError::Automerge(err.to_string()))?;
            }
        }
        VmOp::SpliceList {
            obj,
            index,
            delete,
            values,
        } => {
            let obj = resolve_vm_obj(objects, obj)?;
            let len = doc.length(&obj);
            let index = usize::from(*index) % (len + 1);
            let delete = usize::from(*delete).min(len.saturating_sub(index)) as isize;
            let values = values
                .iter()
                .map(VmValue::to_scalar_value)
                .collect::<Vec<_>>();
            doc.splice(&obj, index, delete, values)
                .map_err(|err| RunError::Automerge(err.to_string()))?;
        }
        VmOp::SpliceText {
            obj,
            index,
            delete,
            value,
        } => {
            let obj = resolve_vm_obj(objects, obj)?;
            let len = doc.length(&obj);
            let index = usize::from(*index) % (len + 1);
            let delete = usize::from(*delete).min(len.saturating_sub(index)) as isize;
            let value = vm_splice_text(*value);
            doc.splice_text(&obj, index, delete, &value)
                .map_err(|err| RunError::Automerge(err.to_string()))?;
        }
        VmOp::UpdateText { obj, value } => {
            let obj = resolve_vm_obj(objects, obj)?;
            let value = vm_text(*value);
            doc.update_text(&obj, value)
                .map_err(|err| RunError::Automerge(err.to_string()))?;
        }
        VmOp::EditText { obj, seed } => {
            let obj = resolve_vm_obj(objects, obj)?;
            let current = doc
                .text(&obj)
                .map_err(|err| RunError::Automerge(err.to_string()))?;
            let edited = vm_edit_text(&current, *seed);
            doc.update_text(&obj, edited)
                .map_err(|err| RunError::Automerge(err.to_string()))?;
        }
        VmOp::UpdateSpans { obj, seed } => {
            let obj = resolve_vm_obj(objects, obj)?;
            let spans = vm_spans(*seed);
            doc.update_spans(&obj, UpdateSpansConfig::default(), spans)
                .map_err(|err| RunError::Automerge(err.to_string()))?;
        }
        VmOp::Increment { obj, key, value } => {
            let obj = resolve_vm_obj(objects, obj)?;
            doc.increment(&obj, vm_key(*key), i64::from(*value))
                .map_err(|err| RunError::Automerge(err.to_string()))?;
        }
        VmOp::Mark {
            obj,
            start,
            end,
            name,
            value,
            expand,
        } => {
            let obj = resolve_vm_obj(objects, obj)?;
            let len = doc.length(&obj);
            let start = usize::from(*start).min(len);
            let end = usize::from(*end).min(len).max(start);
            let mark = Mark::new(vm_mark_name(*name), value.to_scalar_value(), start, end);
            doc.mark(&obj, mark, (*expand).into())
                .map_err(|err| RunError::Automerge(err.to_string()))?;
        }
        VmOp::Unmark {
            obj,
            start,
            end,
            name,
            expand,
        } => {
            let obj = resolve_vm_obj(objects, obj)?;
            let len = doc.length(&obj);
            let start = usize::from(*start).min(len);
            let end = usize::from(*end).min(len).max(start);
            let name = vm_mark_name(*name);
            doc.unmark(&obj, &name, start, end, (*expand).into())
                .map_err(|err| RunError::Automerge(err.to_string()))?;
        }
        VmOp::Delete { obj, key } => {
            let obj = resolve_vm_obj(objects, obj)?;
            doc.delete(&obj, vm_key(*key))
                .map_err(|err| RunError::Automerge(err.to_string()))?;
        }
        VmOp::DeleteSeq { obj, index } => {
            let obj = resolve_vm_obj(objects, obj)?;
            let len = doc.length(&obj);
            if len > 0 {
                doc.delete(&obj, usize::from(*index) % len)
                    .map_err(|err| RunError::Automerge(err.to_string()))?;
            }
        }
        VmOp::UpdateObject { obj, value } => {
            let obj = resolve_vm_obj(objects, obj)?;
            doc.update_object(&obj, &value.to_hydrate_value())
                .map_err(|err| RunError::Automerge(err.to_string()))?;
        }
        VmOp::BatchCreate { obj, key, value } => {
            let obj = resolve_vm_obj(objects, obj)?;
            let new_obj = doc
                .batch_create_object(&obj, vm_key(*key), &value.to_hydrate_value(), false)
                .map_err(|err| RunError::Automerge(err.to_string()))?;
            objects.push(new_obj);
        }
    }
    Ok(())
}

fn vm_make_object<T: ReadDoc + Transactable>(
    doc: &mut T,
    objects: &mut Vec<ObjId>,
    obj: &VmObjRef,
    key: u8,
    kind: ObjType,
) -> Result<(), RunError> {
    let obj = resolve_vm_obj(objects, obj)?;
    let new_obj = doc
        .put_object(&obj, vm_key(key), kind)
        .map_err(|err| RunError::Automerge(err.to_string()))?;
    objects.push(new_obj);
    Ok(())
}

fn resolve_vm_obj(objects: &[ObjId], obj: &VmObjRef) -> Result<ObjId, RunError> {
    match obj {
        VmObjRef::Root => Ok(ROOT),
        VmObjRef::Slot { slot } => {
            if objects.is_empty() {
                Ok(ROOT)
            } else {
                Ok(objects[usize::from(*slot) % objects.len()].clone())
            }
        }
        VmObjRef::Recent { back } => {
            if objects.is_empty() {
                Ok(ROOT)
            } else {
                let back = usize::from(*back).min(objects.len() - 1);
                Ok(objects[objects.len() - 1 - back].clone())
            }
        }
        VmObjRef::Invalid { slot } => objects
            .get(usize::from(*slot).saturating_add(objects.len()))
            .cloned()
            .ok_or_else(|| RunError::MissingObject {
                obj: format!("invalid vm object slot {slot}"),
            }),
    }
}

impl From<MarkExpand> for ExpandMark {
    fn from(expand: MarkExpand) -> Self {
        match expand {
            MarkExpand::Before => Self::Before,
            MarkExpand::After => Self::After,
            MarkExpand::Both => Self::Both,
            MarkExpand::None => Self::None,
        }
    }
}

impl VmValue {
    fn to_scalar_value(&self) -> ScalarValue {
        match self {
            Self::Null => ScalarValue::Null,
            Self::Bool { slot } => ScalarValue::Boolean(slot % 2 == 0),
            Self::Int { slot } => ScalarValue::Int(i64::from(*slot) - 128),
            Self::Uint { slot } => ScalarValue::Uint(u64::from(*slot)),
            Self::Str { slot } => ScalarValue::Str(vm_string(*slot).into()),
            Self::Counter { slot } => ScalarValue::counter(i64::from(*slot) - 128),
            Self::Timestamp { slot } => ScalarValue::Timestamp(vm_signed(*slot)),
            Self::F64 { slot } => ScalarValue::F64(vm_f64(*slot)),
            Self::Bytes { slot } => ScalarValue::Bytes(vm_bytes(*slot)),
        }
    }
}

impl VmHydrated {
    fn to_hydrate_value(&self) -> hydrate::Value {
        match self {
            Self::Scalar { value } => hydrate::Value::Scalar(value.to_scalar_value()),
            Self::Map { seed, depth } => vm_hydrated_map(*seed, *depth),
            Self::List { seed, depth } => vm_hydrated_list(*seed, *depth),
            Self::Text { slot } => {
                hydrate::Value::text(automerge::TextEncoding::UnicodeCodePoint, &vm_text(*slot))
            }
        }
    }
}

fn vm_key(slot: u8) -> String {
    format!("k{}", slot % 32)
}

fn vm_mark_name(slot: u8) -> String {
    const NAMES: &[&str] = &["bold", "italic", "link", "comment", "color"];
    NAMES[usize::from(slot) % NAMES.len()].to_string()
}

fn vm_signed(slot: u8) -> i64 {
    const VALUES: &[i64] = &[
        0,
        1,
        -1,
        i64::MIN,
        i64::MAX,
        1_000,
        -1_000,
        1_700_000_000_000,
    ];
    VALUES[usize::from(slot) % VALUES.len()]
}

fn vm_f64(slot: u8) -> f64 {
    const VALUES: &[f64] = &[
        0.0,
        -0.0,
        1.0,
        -1.0,
        f64::MIN,
        f64::MAX,
        f64::MIN_POSITIVE,
        f64::EPSILON,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ];
    VALUES[usize::from(slot) % VALUES.len()]
}

fn vm_bytes(slot: u8) -> Vec<u8> {
    const VALUES: &[&[u8]] = &[
        b"",
        b"a",
        b"hello\0world",
        &[0xff],
        &[0, 1, 2, 3, 127, 128, 254, 255],
        &[0; 32],
        &[0xff; 32],
    ];
    VALUES[usize::from(slot) % VALUES.len()].to_vec()
}

fn vm_string(slot: u8) -> String {
    const STRINGS: &[&str] = &[
        "",
        "a",
        "hello",
        "hello world",
        "\u{1F98A}\u{1F43B}",
        "multi\nline",
        "abcdefghijklmnopqrstuvwxyz",
        "e\u{0301}e\u{0301}", // decomposed accents
        "\u{1F468}\u{200D}\u{1F469}\u{200D}\u{1F467}\u{200D}\u{1F466}", // ZWJ family
        "a\u{0301}\u{0302}\u{0303}", // stacked combining marks
        "\u{1F469}\u{1F3FF}\u{200D}\u{1F692}", // skin tone + ZWJ profession
        "\u{1F1FA}\u{1F1F8}\u{1F1EB}\u{1F1F7}", // two flags (4 regional indicators)
        "\u{1F1E6}",          // dangling regional indicator
        "a\u{0301}\r\n\u{FE0F}", // mixed hostile boundaries
    ];
    STRINGS[usize::from(slot) % STRINGS.len()].to_string()
}

fn vm_text(slot: u8) -> String {
    if slot % 8 == 0 {
        "the quick brown fox jumps over the lazy dog".to_string()
    } else {
        vm_string(slot)
    }
}

/// The Unicode object-replacement character, used by Automerge's `spans()` and
/// text APIs to stand in for embedded objects / block markers.
const OBJECT_REPLACEMENT: char = '\u{fffc}';

fn strip_object_placeholders(text: &str) -> String {
    text.chars().filter(|c| *c != OBJECT_REPLACEMENT).collect()
}

/// Composable fragments that break or fuse grapheme clusters when placed next
/// to existing text: lone combining marks, ZWJ, variation selectors, skin-tone
/// modifiers, regional indicators (which pair into flags), tag characters, and
/// astral bases. Inserting these at code-point boundaries — including *inside*
/// an existing grapheme — is what stresses the width index and the grapheme
/// re-segmentation in text_diff / text_value.
const HOSTILE_FRAGMENTS: &[&str] = &[
    "x",
    " ",
    "\u{1F98A}",                                   // astral base (fox)
    "\u{0301}",                                    // lone combining acute
    "\u{0301}\u{0302}\u{0303}",                    // stacked combining marks
    "\u{200D}",                                    // lone zero-width joiner
    "\u{FE0F}",                                    // emoji variation selector
    "\u{FE0E}",                                    // text variation selector
    "\u{1F3FB}",                                   // lone skin-tone modifier
    "\u{1F1E6}",                                   // single regional indicator (dangling)
    "\u{1F1E6}\u{1F1FA}",                          // regional indicator pair (flag)
    "\u{1F468}\u{200D}\u{1F469}\u{200D}\u{1F467}", // ZWJ family cluster
    "\u{4E2D}",                                    // BMP CJK
    "e\u{0301}",                                   // base + combining
    "\r\n",                                        // CRLF (one grapheme)
    "\u{E0067}\u{E0062}\u{E0073}",                 // tag characters (subdivision flag)
];

/// A small edit of `current`, biased toward boundary-hostile insertions so the
/// Myers text diff, the grapheme re-segmentation, and the width indexes see
/// near-identical strings that fuse or split clusters across the edit.
fn vm_edit_text(current: &str, seed: u8) -> String {
    let chars: Vec<char> = current.chars().collect();
    let insert = HOSTILE_FRAGMENTS[usize::from(seed) % HOSTILE_FRAGMENTS.len()];
    let pos = if chars.is_empty() {
        0
    } else {
        usize::from(seed) % (chars.len() + 1)
    };
    let rebuild = |range: std::ops::Range<usize>, middle: &str| -> String {
        let mut out: String = chars[..range.start].iter().collect();
        out.push_str(middle);
        out.extend(&chars[range.end..]);
        out
    };
    let target = pos.min(chars.len().saturating_sub(1));
    match seed % 8 {
        // Insert at a code-point boundary, which may land inside a grapheme.
        0 => rebuild(pos..pos, insert),
        // Delete a single code point, which may split a grapheme.
        1 if !chars.is_empty() => rebuild(target..target + 1, ""),
        // Replace a single code point.
        2 if !chars.is_empty() => rebuild(target..target + 1, insert),
        // Duplicate the first half: a large but highly similar edit.
        3 => {
            let half: String = chars[..chars.len() / 2].iter().collect();
            format!("{half}{current}")
        }
        // Prepend a fragment that fuses with the first existing cluster.
        4 => format!("{insert}{current}"),
        // Append a fragment that fuses with the last existing cluster.
        5 => format!("{current}{insert}"),
        // Swap two adjacent code points, which can break or join clusters.
        6 if chars.len() >= 2 => {
            let mut swapped = chars.clone();
            let index = target.min(swapped.len() - 2);
            swapped.swap(index, index + 1);
            swapped.into_iter().collect()
        }
        _ => format!("{current}{current}"),
    }
}

/// A deterministic sequence of rich-text spans: several text runs carrying
/// different mark sets, occasionally interrupted by a block marker. Drives
/// `update_spans` and the block/marks diff path in text_diff.rs.
fn vm_spans(seed: u8) -> Vec<Span> {
    const MARK_NAMES: &[&str] = &["bold", "italic", "link", "comment"];
    let run_count = 1 + usize::from(seed % 4);
    let mut spans = Vec::new();
    for run in 0..run_count {
        let mix = seed.wrapping_add(run as u8);
        if mix % 5 == 4 {
            let mut block = std::collections::HashMap::new();
            block.insert("type", hydrate::Value::scalar(vm_string(mix)));
            spans.push(Span::Block(block.into()));
            continue;
        }
        let marks: Option<std::sync::Arc<MarkSet>> = if mix % 3 == 0 {
            None
        } else {
            let name = MARK_NAMES[usize::from(mix) % MARK_NAMES.len()].to_string();
            let value = ScalarValue::Boolean(mix % 2 == 0);
            Some(std::sync::Arc::new(
                std::iter::once((name, value)).collect::<MarkSet>(),
            ))
        };
        spans.push(Span::Text {
            text: vm_splice_text(mix),
            marks,
        });
    }
    spans
}

fn vm_splice_text(slot: u8) -> String {
    match slot % 8 {
        // Exercise Automerge's optimized text-run paths: these increase the
        // number of text ops/history without creating deeply nested objects.
        0 => "a".repeat(32),
        1 => "the quick brown fox jumps over the lazy dog ".repeat(2),
        2 => "abcdefghijklmnopqrstuvwxyz".repeat(4),
        3 => "🦊🐻".repeat(16),
        _ => vm_string(slot),
    }
}

fn vm_hydrated_map(seed: u8, depth: u8) -> hydrate::Value {
    let mut entries = std::collections::HashMap::new();
    entries.insert(format!("m{}", seed % 8), vm_hydrated_value(seed, depth));
    entries.insert(
        format!("n{}", seed % 8),
        vm_hydrated_value(seed.wrapping_add(1), depth.saturating_sub(1)),
    );
    hydrate::Value::Map(entries.into())
}

fn vm_hydrated_list(seed: u8, depth: u8) -> hydrate::Value {
    hydrate::Value::from(vec![
        vm_hydrated_value(seed, depth),
        vm_hydrated_value(seed.wrapping_add(1), depth.saturating_sub(1)),
    ])
}

fn vm_hydrated_value(seed: u8, depth: u8) -> hydrate::Value {
    if depth == 0 {
        return hydrate::Value::Scalar(VmValue::Int { slot: seed }.to_scalar_value());
    }
    match seed % 4 {
        0 => vm_hydrated_map(seed, depth - 1),
        1 => vm_hydrated_list(seed, depth - 1),
        2 => hydrate::Value::text(automerge::TextEncoding::UnicodeCodePoint, &vm_text(seed)),
        _ => hydrate::Value::Scalar(VmValue::Str { slot: seed }.to_scalar_value()),
    }
}

impl RunError {
    fn is_invariant_or_panic(&self) -> bool {
        matches!(self, Self::Invariant(_) | Self::Panic(_))
    }

    fn at(self, step: usize) -> Self {
        match self {
            Self::Automerge(msg) => Self::Automerge(format!("at step {step}: {msg}")),
            Self::Load(msg) => Self::Load(format!("at step {step}: {msg}")),
            Self::Invariant(msg) => Self::Invariant(format!("at step {step}: {msg}")),
            Self::Timeout { elapsed_ms, .. } => Self::Timeout { step, elapsed_ms },
            other => other,
        }
    }
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingDoc { doc } => write!(f, "missing document {doc}"),
            Self::MissingObject { obj } => write!(f, "missing object {obj:?}"),
            Self::MissingHeads { doc, slot } => {
                write!(f, "missing saved heads for document {doc} slot {slot}")
            }
            Self::Automerge(msg) => write!(f, "automerge error: {msg}"),
            Self::Load(msg) => write!(f, "load error: {msg}"),
            Self::Invariant(msg) => write!(f, "invariant failed: {msg}"),
            Self::Timeout { step, elapsed_ms } => {
                write!(f, "trace timed out at step {step} after {elapsed_ms}ms")
            }
            Self::Panic(msg) => write!(f, "panic: {msg}"),
        }
    }
}

impl std::error::Error for RunError {}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(msg) = payload.downcast_ref::<&str>() {
        (*msg).to_string()
    } else if let Some(msg) = payload.downcast_ref::<String>() {
        msg.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace::{ActorSpec, Metadata};
    use crate::trace::{VmApplyOrder, VmHeadRef};
    use crate::trace::{VmSyncFault, VmSyncOp};

    fn transact_instr(key: u8, value: u8, commit: bool) -> VmInstr {
        VmInstr::Transact {
            doc: 0,
            actor: 0,
            ops: vec![VmOp::Put {
                obj: VmObjRef::Root,
                key,
                value: VmValue::Uint { slot: value },
            }],
            commit,
        }
    }

    #[test]
    fn hostile_fragment_edits_keep_text_accounting_consistent() {
        use crate::trace::VmTextEncoding;
        // Build up a text object with layered grapheme-fusing edits under every
        // encoding; check_text_invariants runs in the save/load pass and
        // asserts spans/text and width/length stay consistent throughout.
        for encoding in [
            None,
            Some(VmTextEncoding::CodePoint),
            Some(VmTextEncoding::Utf8),
            Some(VmTextEncoding::Utf16),
            Some(VmTextEncoding::Grapheme),
        ] {
            let mut steps = vec![VmInstr::Change {
                doc: 0,
                actor: 0,
                ops: vec![VmOp::MakeText {
                    obj: VmObjRef::Root,
                    key: 0,
                }],
            }];
            for seed in 0..40u8 {
                steps.push(VmInstr::Change {
                    doc: 0,
                    actor: 0,
                    ops: vec![VmOp::EditText {
                        obj: VmObjRef::Slot { slot: 0 },
                        seed,
                    }],
                });
            }
            steps.push(VmInstr::SaveLoad { doc: 0 });
            let trace = Trace {
                version: 1,
                metadata: Metadata::default(),
                actors: vec![ActorSpec::new(0)],
                text_encoding: encoding,
                steps,
            };
            Runner::new()
                .run_catching(&trace)
                .unwrap_or_else(|err| panic!("encoding {encoding:?} failed: {err}"));
        }
    }

    #[test]
    fn update_spans_rewrites_rich_text() {
        let mut steps = vec![VmInstr::Change {
            doc: 0,
            actor: 0,
            ops: vec![
                VmOp::MakeText {
                    obj: VmObjRef::Root,
                    key: 0,
                },
                VmOp::SpliceText {
                    obj: VmObjRef::Slot { slot: 0 },
                    index: 0,
                    delete: 0,
                    value: 3,
                },
            ],
        }];
        // Repeated update_spans with different seeds exercises the block/marks
        // diff path, including span replaces and mark changes over existing
        // content.
        for seed in 0..16u8 {
            steps.push(VmInstr::Change {
                doc: 0,
                actor: 0,
                ops: vec![VmOp::UpdateSpans {
                    obj: VmObjRef::Slot { slot: 0 },
                    seed,
                }],
            });
            steps.push(VmInstr::SaveLoad { doc: 0 });
        }
        let trace = Trace {
            version: 1,
            metadata: Metadata::default(),
            actors: vec![ActorSpec::new(0)],
            text_encoding: None,
            steps,
        };
        Runner::new()
            .run_catching(&trace)
            .expect("update_spans runs");
    }

    #[test]
    fn hostile_text_edits_run_under_every_encoding() {
        use crate::trace::VmTextEncoding;
        for encoding in [
            None,
            Some(VmTextEncoding::CodePoint),
            Some(VmTextEncoding::Utf8),
            Some(VmTextEncoding::Utf16),
            Some(VmTextEncoding::Grapheme),
        ] {
            let mut steps = vec![VmInstr::Change {
                doc: 0,
                actor: 0,
                ops: vec![VmOp::MakeText {
                    obj: VmObjRef::Root,
                    key: 0,
                }],
            }];
            // Cycle splices and near-miss edits through the hostile string
            // tables (ZWJ families, combining marks, astral plane), with a
            // save/load after each edit so the width indexes are rebuilt.
            for seed in 0..24u8 {
                steps.push(VmInstr::Change {
                    doc: 0,
                    actor: 0,
                    ops: vec![
                        VmOp::SpliceText {
                            obj: VmObjRef::Slot { slot: 0 },
                            index: seed,
                            delete: seed % 3,
                            value: seed,
                        },
                        VmOp::EditText {
                            obj: VmObjRef::Slot { slot: 0 },
                            seed,
                        },
                    ],
                });
                steps.push(VmInstr::SaveLoad { doc: 0 });
            }
            let trace = Trace {
                version: 1,
                metadata: Metadata::default(),
                actors: vec![ActorSpec::new(0)],
                text_encoding: encoding,
                steps,
            };
            Runner::new()
                .run_catching(&trace)
                .unwrap_or_else(|err| panic!("encoding {encoding:?} failed: {err}"));
        }
    }

    #[test]
    fn persistence_transfers_round_trip_incremental_save_after_and_bundle() {
        let put = |doc: u8, actor: u8, key: u8, value: u8| VmInstr::Change {
            doc,
            actor,
            ops: vec![VmOp::Put {
                obj: VmObjRef::Root,
                key,
                value: VmValue::Uint { slot: value },
            }],
        };
        let trace = Trace {
            version: 1,
            metadata: Metadata::default(),
            actors: vec![ActorSpec::new(0), ActorSpec::new(1)],
            text_encoding: None,
            steps: vec![
                put(0, 0, 0, 1),
                VmInstr::SaveHeads { doc: 0, slot: 0 },
                VmInstr::Fork { from: 0, to: 1 },
                VmInstr::Fork { from: 0, to: 2 },
                VmInstr::Fork { from: 0, to: 3 },
                put(0, 0, 1, 2),
                VmInstr::Persist {
                    from: 0,
                    into: 1,
                    mode: VmPersistMode::Incremental,
                },
                VmInstr::Persist {
                    from: 0,
                    into: 2,
                    mode: VmPersistMode::SaveAfter {
                        since: VmHeadRef::Slot { slot: 0 },
                    },
                },
                VmInstr::Persist {
                    from: 0,
                    into: 3,
                    mode: VmPersistMode::Bundle {
                        since: VmHeadRef::Slot { slot: 0 },
                    },
                },
            ],
        };
        let report = Runner::new()
            .run_catching(&trace)
            .expect("all persistence transfers run");
        assert_eq!(report.docs, 4);
        assert!(report.behavior.diff_checks >= 3);
    }

    #[test]
    fn isolation_and_owned_historical_transactions_preserve_views() {
        let trace = Trace {
            version: 1,
            metadata: Metadata::default(),
            actors: vec![ActorSpec::new(0), ActorSpec::new(1)],
            text_encoding: None,
            steps: vec![
                VmInstr::Change {
                    doc: 0,
                    actor: 0,
                    ops: vec![VmOp::Put {
                        obj: VmObjRef::Root,
                        key: 0,
                        value: VmValue::Uint { slot: 1 },
                    }],
                },
                VmInstr::SaveHeads { doc: 0, slot: 0 },
                VmInstr::Change {
                    doc: 0,
                    actor: 0,
                    ops: vec![VmOp::Put {
                        obj: VmObjRef::Root,
                        key: 1,
                        value: VmValue::Uint { slot: 2 },
                    }],
                },
                VmInstr::Isolate {
                    doc: 0,
                    head: VmHeadRef::Slot { slot: 0 },
                },
                VmInstr::Integrate { doc: 0 },
                VmInstr::TransactAt {
                    doc: 0,
                    actor: 1,
                    head: VmHeadRef::Slot { slot: 0 },
                    ops: vec![VmOp::Put {
                        obj: VmObjRef::Root,
                        key: 2,
                        value: VmValue::Bytes { slot: 4 },
                    }],
                    commit: true,
                },
                VmInstr::TransactAt {
                    doc: 0,
                    actor: 1,
                    head: VmHeadRef::Slot { slot: 0 },
                    ops: vec![VmOp::Delete {
                        obj: VmObjRef::Root,
                        key: 0,
                    }],
                    commit: false,
                },
            ],
        };
        let report = Runner::new()
            .run_catching(&trace)
            .expect("historical views and owned transactions run");
        assert!(report.behavior.diff_checks >= 1);
    }

    #[test]
    fn diff_patches_reconstruct_hydrated_target() {
        let trace = Trace {
            version: 1,
            metadata: Metadata::default(),
            actors: vec![ActorSpec::new(0)],
            text_encoding: None,
            steps: vec![
                VmInstr::Change {
                    doc: 0,
                    actor: 0,
                    ops: vec![
                        VmOp::Put {
                            obj: VmObjRef::Root,
                            key: 0,
                            value: VmValue::Uint { slot: 1 },
                        },
                        VmOp::MakeList {
                            obj: VmObjRef::Root,
                            key: 1,
                        },
                    ],
                },
                VmInstr::SaveHeads { doc: 0, slot: 0 },
                VmInstr::Change {
                    doc: 0,
                    actor: 0,
                    ops: vec![
                        VmOp::Put {
                            obj: VmObjRef::Root,
                            key: 0,
                            value: VmValue::Bytes { slot: 4 },
                        },
                        VmOp::SpliceList {
                            obj: VmObjRef::Slot { slot: 0 },
                            index: 0,
                            delete: 0,
                            values: vec![VmValue::Timestamp { slot: 3 }, VmValue::F64 { slot: 2 }],
                        },
                    ],
                },
                VmInstr::DiffRange {
                    doc: 0,
                    before: VmHeadRef::Slot { slot: 0 },
                    after: VmHeadRef::Current,
                },
            ],
        };
        let report = Runner::new().run_catching(&trace).expect("diff checks");
        assert_eq!(report.behavior.diff_checks, 1);
        assert!(report.behavior.total_patches >= 2);
    }

    #[test]
    fn every_scalar_kind_round_trips_through_storage() {
        let values = vec![
            VmValue::Null,
            VmValue::Bool { slot: 0 },
            VmValue::Int { slot: 0 },
            VmValue::Uint { slot: u8::MAX },
            VmValue::Str { slot: 8 },
            VmValue::Counter { slot: 0 },
            VmValue::Timestamp { slot: 4 },
            VmValue::F64 { slot: 8 },
            VmValue::Bytes { slot: 6 },
        ];
        let ops = values
            .into_iter()
            .enumerate()
            .map(|(key, value)| VmOp::Put {
                obj: VmObjRef::Root,
                key: key as u8,
                value,
            })
            .collect();
        let trace = Trace {
            version: 1,
            metadata: Metadata::default(),
            actors: vec![ActorSpec::new(0)],
            text_encoding: None,
            steps: vec![VmInstr::Change {
                doc: 0,
                actor: 0,
                ops,
            }],
        };
        Runner::new()
            .run_catching(&trace)
            .expect("all scalar kinds save/load");
    }

    #[test]
    fn read_only_sync_is_directional() {
        let put = |doc: u8, actor: u8, key: u8| VmInstr::Change {
            doc,
            actor,
            ops: vec![VmOp::Put {
                obj: VmObjRef::Root,
                key,
                value: VmValue::Uint { slot: key },
            }],
        };
        let session = |op| VmInstr::SyncSession { session: 0, op };
        let trace = Trace {
            version: 1,
            metadata: Metadata::default(),
            actors: vec![ActorSpec::new(0), ActorSpec::new(1)],
            text_encoding: None,
            steps: vec![
                put(0, 0, 0),
                VmInstr::Fork { from: 0, to: 1 },
                put(0, 0, 1),
                put(1, 1, 2),
                session(VmSyncOp::Start { left: 0, right: 1 }),
                session(VmSyncOp::SetReadOnly {
                    left: true,
                    read_only: true,
                }),
                session(VmSyncOp::Finish { rounds: 32 }),
            ],
        };
        Runner::new()
            .run_catching(&trace)
            .expect("writable peer receives read-only peer changes");
    }

    #[test]
    fn faulty_sync_session_converges_on_finish() {
        let put = |key: u8, value: u8| VmInstr::Change {
            doc: 0,
            actor: 0,
            ops: vec![VmOp::Put {
                obj: VmObjRef::Root,
                key,
                value: VmValue::Uint { slot: value },
            }],
        };
        let session = |op: VmSyncOp| VmInstr::SyncSession { session: 0, op };
        let trace = Trace {
            version: 1,
            metadata: Metadata::default(),
            actors: vec![ActorSpec::new(0), ActorSpec::new(1)],
            text_encoding: None,
            steps: vec![
                put(1, 5),
                VmInstr::Fork { from: 0, to: 1 },
                put(2, 9),
                session(VmSyncOp::Start { left: 0, right: 1 }),
                session(VmSyncOp::Generate { from_left: true }),
                session(VmSyncOp::Generate { from_left: true }),
                session(VmSyncOp::Deliver {
                    to_left: false,
                    fault: VmSyncFault::Drop,
                }),
                session(VmSyncOp::Generate { from_left: false }),
                session(VmSyncOp::Deliver {
                    to_left: true,
                    fault: VmSyncFault::Duplicate,
                }),
                session(VmSyncOp::SaveStates),
                // Edit mid-session, then finish reliably: the protocol must
                // recover from the drop, the duplicate, and the state
                // round-trip, and converge.
                put(3, 7),
                session(VmSyncOp::Finish { rounds: 32 }),
            ],
        };
        let report = Runner::new()
            .run_catching(&trace)
            .expect("session converges");
        assert_eq!(report.docs, 2);
    }

    #[test]
    fn fork_at_and_out_of_order_apply_changes_transfer_everything() {
        let put = |doc: u8, key: u8, value: u8| VmInstr::Change {
            doc,
            actor: doc,
            ops: vec![VmOp::Put {
                obj: VmObjRef::Root,
                key,
                value: VmValue::Uint { slot: value },
            }],
        };
        let trace = Trace {
            version: 1,
            metadata: Metadata::default(),
            actors: vec![ActorSpec::new(0), ActorSpec::new(1)],
            text_encoding: None,
            steps: vec![
                put(0, 1, 5),
                VmInstr::SaveHeads { doc: 0, slot: 0 },
                put(0, 2, 9),
                put(0, 3, 2),
                // Fork doc 1 from doc 0's historical state, then let it
                // diverge before transferring doc 0's changes children-first.
                VmInstr::ForkAt {
                    from: 0,
                    to: 1,
                    head: VmHeadRef::Slot { slot: 0 },
                },
                put(1, 4, 7),
                VmInstr::ApplyChanges {
                    from: 0,
                    into: 1,
                    order: VmApplyOrder::Reversed,
                },
                // Withholding changes must not trip the completeness check.
                VmInstr::ApplyChanges {
                    from: 1,
                    into: 0,
                    order: VmApplyOrder::DropHalf,
                },
                VmInstr::Merge { into: 0, from: 1 },
            ],
        };
        let report = Runner::new().run_catching(&trace).expect("trace runs");
        assert_eq!(report.docs, 2);
        assert_eq!(report.behavior.merge_checks, 1);
    }

    #[test]
    fn transact_commits_apply_and_rollbacks_leave_no_trace() {
        let trace = Trace {
            version: 1,
            metadata: Metadata::default(),
            actors: vec![ActorSpec::new(0)],
            text_encoding: None,
            steps: vec![
                transact_instr(1, 5, true),
                transact_instr(2, 9, false),
                transact_instr(3, 7, false),
            ],
        };
        let report = Runner::new().run_catching(&trace).expect("trace runs");
        // Only the committed transaction's ops count and only its change lands
        // in the document; the rollbacks must leave nothing behind.
        assert_eq!(report.ops, 1);
        assert_eq!(report.behavior.total_changes, 1);
    }
}
