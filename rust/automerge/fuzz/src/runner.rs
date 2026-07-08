use automerge::hydrate;
use automerge::marks::{ExpandMark, Mark};
use automerge::sync::{self, SyncDoc};
use automerge::transaction::Transactable;
use std::collections::{HashMap, VecDeque};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use automerge::{
    ActorId, AutoCommit, ChangeHash, MoveCursor, ObjId, ObjType, ReadDoc, ScalarValue, ROOT,
};

use crate::trace::{
    ActorSpec, MarkExpand, Trace, VmApplyOrder, VmHeadRef, VmHydrated, VmInstr, VmObjRef,
    VmObserveMode, VmOp, VmSyncFault, VmSyncOp, VmValue,
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
            ..BehaviorStats::default()
        };
        for doc in 0..state.docs.len() {
            let outcome = state.save_load(doc)?;
            behavior.max_saved_bytes = behavior.max_saved_bytes.max(outcome.saved_bytes);
            walk_hydrated(&outcome.hydrated, 1, &mut behavior);
            let doc_state = &mut state.docs[doc];
            behavior.max_heads = behavior.max_heads.max(doc_state.doc.get_heads().len());
            behavior.total_changes += doc_state.doc.get_changes_meta(&[]).len();
            behavior.text_marks += doc_state.count_text_marks();
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
        let mut state = RunState::new(&trace.actors);
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
            return (
                RunState::new(&trace.actors),
                0,
                0,
                initial_prefix_hash(trace),
                None,
            );
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
            (
                RunState::new(&trace.actors),
                0,
                0,
                initial_prefix_hash(trace),
                None,
            )
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

impl RunState {
    fn new(actors: &[ActorSpec]) -> Self {
        let actors = if actors.is_empty() {
            vec![ActorId::from(vec![0])]
        } else {
            actors
                .iter()
                .map(|actor| ActorId::from(actor.bytes.clone()))
                .collect()
        };
        let mut doc = AutoCommit::new();
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
            self.docs.resize_with(to + 1, || DocState {
                doc: AutoCommit::new(),
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
        let doc_state = self.doc_mut(doc)?;
        let before = doc_state.resolve_heads(doc, before)?;
        let after = doc_state.resolve_heads(doc, after)?;
        let _ = doc_state.doc.diff(&before, &after);
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
        let _ = self.doc_mut(doc)?.doc.diff_incremental();
        Ok(())
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
            let left_heads = self.doc_mut(left)?.doc.get_heads();
            let right_heads = self.doc_mut(right)?.doc.get_heads();
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
            let left_heads = self.docs[session.left].doc.get_heads();
            let right_heads = self.docs[session.right].doc.get_heads();
            if left_heads != right_heads {
                return Err(RunError::Invariant(
                    "sync session quiesced without converging".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn save_load(&mut self, doc: usize) -> Result<SaveLoadOutcome, RunError> {
        let doc_state = self.doc_mut(doc)?;
        let before = doc_state
            .doc
            .hydrate(&ROOT, None)
            .map_err(|err| RunError::Automerge(err.to_string()))?;
        let bytes = doc_state.doc.save();
        let loaded = AutoCommit::load(&bytes).map_err(|err| RunError::Load(err.to_string()))?;
        let after = loaded
            .hydrate(&ROOT, None)
            .map_err(|err| RunError::Automerge(err.to_string()))?;
        if before != after {
            return Err(RunError::Invariant(
                "save/load changed hydrated document".to_string(),
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
        let doc_state = self.doc_mut(doc)?;
        let heads_before = doc_state.doc.get_heads();
        let mut plain = doc_state.doc.document().clone();
        plain.set_actor(actor_id);
        let before = (!commit).then(|| plain.hydrate(None));

        let mut objects = doc_state.objects.clone();
        let mut tx = plain.transaction();
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
            tx.commit();
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
            if before.as_ref() != Some(&after) {
                return Err(RunError::Invariant(
                    "transaction rollback changed hydrated document".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn doc_mut(&mut self, doc: usize) -> Result<&mut DocState, RunError> {
        self.docs.get_mut(doc).ok_or(RunError::MissingDoc { doc })
    }
}

impl DocState {
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
                let _ = self.doc.get_cursor_position(obj, &cursor, at);
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

fn vm_string(slot: u8) -> String {
    const STRINGS: &[&str] = &[
        "",
        "a",
        "hello",
        "hello world",
        "🦊🐻",
        "multi\nline",
        "abcdefghijklmnopqrstuvwxyz",
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
    use crate::trace::Metadata;
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
            ],
        };
        let report = Runner::new().run_catching(&trace).expect("trace runs");
        assert_eq!(report.docs, 2);
    }

    #[test]
    fn transact_commits_apply_and_rollbacks_leave_no_trace() {
        let trace = Trace {
            version: 1,
            metadata: Metadata::default(),
            actors: vec![ActorSpec::new(0)],
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
