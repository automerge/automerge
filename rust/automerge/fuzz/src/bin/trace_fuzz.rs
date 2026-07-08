use std::collections::VecDeque;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::json;

use automerge_fuzz::coverage::CoverageReporter;
use automerge_fuzz::crash_view::show_crashes;
use automerge_fuzz::feedback::FeedbackState;
use automerge_fuzz::mutate::{
    mutate, mutation_batch, normalize_trace, prefix_extension_batch, MutationBatch,
    MAX_VM_INSTRUCTIONS,
};
use automerge_fuzz::trace::Trace;
use automerge_fuzz::trace_io::{load_trace, save_trace};
use automerge_fuzz::{RunError, Runner};
use clap::{Parser, Subcommand};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

#[derive(Debug, Parser)]
#[command(name = "trace_fuzz")]
#[command(about = "Stateful trace fuzzer for Automerge")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Generate {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value_t = 100)]
        steps: usize,
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
    Replay {
        trace: PathBuf,
    },
    Fuzz {
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value_t = 1000)]
        iterations: usize,
        #[arg(long, default_value_t = 20)]
        generated_steps: usize,
        #[arg(long, default_value_t = 8)]
        generated_traces: usize,
        #[arg(long, default_value = "corpus/trace")]
        corpus: PathBuf,
        #[arg(long, default_value_t = 1000)]
        report_every: usize,
        #[arg(long, default_value_t = 10)]
        report_secs: u64,
        #[arg(long, default_value_t = 1000)]
        max_corpus_load: usize,
        #[arg(long)]
        coverage_dir: Option<PathBuf>,
        #[arg(long, default_value_t = 30)]
        coverage_poll_secs: u64,
        #[arg(long)]
        coverage_retention: bool,
        #[arg(long, default_value_t = 128)]
        coverage_retention_window: usize,
        /// Append machine-readable run statistics to this JSONL file.
        /// Defaults to <corpus>/stats.jsonl.
        #[arg(long)]
        stats_file: Option<PathBuf>,
        /// Number of worker threads executing traces. With more than one job,
        /// results are processed in completion order, so runs are not
        /// reproducible run-to-run.
        #[arg(long, default_value_t = 1)]
        jobs: usize,
    },
    Crashes {
        /// Crash number, filename, or path. Omit to list crashes.
        crash: Option<String>,
        #[arg(long, default_value = "corpus/trace")]
        corpus: PathBuf,
        /// Print only the selected trace JSON.
        #[arg(long)]
        json: bool,
        /// Also print the full trace JSON for the selected crash.
        #[arg(long)]
        full_trace: bool,
        /// Replay the selected crash after printing its metadata.
        #[arg(long)]
        replay: bool,
    },
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Generate {
            seed,
            steps,
            output,
        } => {
            let trace = Trace::generate(seed, steps);
            if let Some(output) = output {
                save_trace(&output, &trace)?;
                println!("wrote {}", output.display());
            } else {
                println!("{}", serde_json::to_string_pretty(&trace)?);
            }
        }
        Command::Replay { trace } => {
            let trace_path = trace;
            let trace = load_trace(&trace_path)?;
            let report = Runner::new().run_catching(&trace)?;
            println!(
                "replayed {} successfully: steps={} ops={} docs={}",
                trace_path.display(),
                report.steps,
                report.ops,
                report.docs
            );
            if !report.sometimes_hits.is_empty() {
                println!("sometimes hits:");
                for hit in report.sometimes_hits {
                    println!("  {}={}", hit.name, hit.count);
                }
            }
        }
        Command::Fuzz {
            seed,
            iterations,
            generated_steps,
            generated_traces,
            corpus,
            report_every,
            report_secs,
            max_corpus_load,
            coverage_dir,
            coverage_poll_secs,
            coverage_retention,
            coverage_retention_window,
            stats_file,
            jobs,
        } => {
            fuzz(FuzzOptions {
                seed,
                iterations,
                generated_steps,
                generated_traces,
                corpus_dir: corpus,
                report_every,
                report_secs,
                max_corpus_load,
                coverage_dir,
                coverage_poll_secs,
                coverage_retention,
                coverage_retention_window,
                stats_file,
                jobs,
            })?;
        }
        Command::Crashes {
            crash,
            corpus,
            json,
            full_trace,
            replay,
        } => {
            show_crashes(&corpus, crash.as_deref(), json, full_trace, replay)?;
        }
    }
    Ok(())
}

struct FuzzOptions {
    seed: u64,
    iterations: usize,
    generated_steps: usize,
    generated_traces: usize,
    corpus_dir: PathBuf,
    report_every: usize,
    report_secs: u64,
    max_corpus_load: usize,
    coverage_dir: Option<PathBuf>,
    coverage_poll_secs: u64,
    coverage_retention: bool,
    coverage_retention_window: usize,
    stats_file: Option<PathBuf>,
    jobs: usize,
}

/// Counters accumulated over one fuzz run, shared between the human status
/// line and the machine-readable stats log.
#[derive(Default)]
struct Tally {
    valid: usize,
    interesting: usize,
    checkpoints: usize,
    coverage_saved: usize,
    crashes: usize,
    rejected: usize,
}

/// Appends one JSON object per event to a stats file so runs can be plotted
/// and compared offline. Logging failures disable the logger rather than
/// interrupting the fuzz run.
struct StatsLogger {
    file: Option<fs::File>,
}

impl StatsLogger {
    fn create(path: &Path) -> Self {
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        match fs::OpenOptions::new().create(true).append(true).open(path) {
            Ok(file) => Self { file: Some(file) },
            Err(err) => {
                eprintln!(
                    "stats: failed to open {}: {err}; stats logging disabled",
                    path.display()
                );
                Self { file: None }
            }
        }
    }

    fn log(&mut self, event: serde_json::Value) {
        let Some(file) = &mut self.file else {
            return;
        };
        if writeln!(file, "{event}").is_err() {
            eprintln!("stats: write failed; stats logging disabled");
            self.file = None;
        }
    }
}

struct PendingMutations {
    next_seq: u64,
    items: Vec<PendingTrace>,
    rejected_items: Vec<PendingTrace>,
}

struct PendingTrace {
    priority: u8,
    seq: u64,
    batch: MutationBatch,
}

impl PendingMutations {
    const MAX_PENDING: usize = 4096;

    fn new() -> Self {
        Self {
            next_seq: 0,
            items: Vec::new(),
            rejected_items: Vec::new(),
        }
    }

    fn push_batch(&mut self, priority: u8, batch: MutationBatch) {
        self.push_into(false, priority, batch);
    }

    fn push_rejected_batch(&mut self, priority: u8, batch: MutationBatch) {
        self.push_into(true, priority, batch);
    }

    fn push_into(&mut self, rejected: bool, priority: u8, batch: MutationBatch) {
        let items = if rejected {
            &mut self.rejected_items
        } else {
            &mut self.items
        };
        items.push(PendingTrace {
            priority,
            seq: self.next_seq,
            batch,
        });
        self.next_seq = self.next_seq.wrapping_add(1);
        self.truncate_low_priority();
    }

    fn pop(&mut self, rng: &mut StdRng) -> Option<Trace> {
        Self::pop_from(&mut self.items, rng)
            .or_else(|| Self::pop_from(&mut self.rejected_items, rng))
    }

    fn pop_from(items: &mut Vec<PendingTrace>, rng: &mut StdRng) -> Option<Trace> {
        let index = items
            .iter()
            .enumerate()
            .max_by_key(|(_, item)| (item.priority, item.seq))
            .map(|(index, _)| index)?;
        let trace = items[index].batch.next(rng)?;
        if items[index].batch.is_empty() {
            items.swap_remove(index);
        }
        Some(trace)
    }

    fn clear(&mut self) {
        self.items.clear();
        self.rejected_items.clear();
    }

    fn truncate_low_priority(&mut self) {
        truncate_queue(&mut self.items, Self::MAX_PENDING);
        truncate_queue(&mut self.rejected_items, Self::MAX_PENDING / 4);
    }
}

fn truncate_queue(items: &mut Vec<PendingTrace>, max_len: usize) {
    if items.len() <= max_len {
        return;
    }
    items.sort_by_key(|item| (item.priority, item.seq));
    let drop = items.len() - max_len;
    items.drain(0..drop);
}

fn is_coverage_reason(reason: &str) -> bool {
    reason.starts_with("new coverage buckets ") || reason.starts_with("rare coverage counters ")
}

fn reason_priority(reason: &str) -> u8 {
    if reason.starts_with("new sometimes ") && !reason.starts_with("new sometimes count bucket ") {
        5
    } else if reason.starts_with("new sometimes count bucket ") {
        4
    } else if reason.starts_with("new behavior bucket ") {
        3
    } else if is_coverage_reason(reason) {
        1
    } else {
        // Trace-syntax novelty: features, structural buckets, comparisons.
        // These describe the program text rather than where execution went, so
        // they rank below behavior buckets.
        2
    }
}

fn scheduled_effort(priority: u8) -> usize {
    match priority {
        5 => 48,
        4 => 24,
        3 => 16,
        2 => 8,
        _ => 1,
    }
}

fn effort_power(power: u32) -> usize {
    match power {
        0 | 1 => 1,
        2..=3 => 2,
        4..=7 => 3,
        8..=15 => 4,
        16..=31 => 6,
        _ => 8,
    }
}

fn fuzz(options: FuzzOptions) -> Result<(), Box<dyn std::error::Error>> {
    let FuzzOptions {
        seed,
        iterations,
        generated_steps,
        generated_traces,
        corpus_dir,
        report_every,
        report_secs,
        max_corpus_load,
        coverage_dir,
        coverage_poll_secs,
        coverage_retention,
        coverage_retention_window,
        stats_file,
        jobs,
    } = options;

    let jobs = effective_jobs(jobs);

    eprintln!(
        "initializing fuzz run: seed={seed} iterations={iterations} jobs={jobs} corpus_dir={}",
        corpus_dir.display()
    );

    let mut rng = StdRng::seed_from_u64(seed);
    let mut corpus = load_corpus(&corpus_dir, max_corpus_load)?;
    eprintln!(
        "loaded {} corpus traces (normalized to <= {MAX_VM_INSTRUCTIONS} steps)",
        corpus.len()
    );
    for generated in 0..generated_traces {
        corpus.push(Trace::generate(
            seed.wrapping_add(generated as u64),
            generated_steps,
        ));
    }
    if corpus.is_empty() {
        corpus.push(Trace::generate(seed, generated_steps));
    }
    let mut corpus_power = vec![1u32; corpus.len()];

    let interesting_dir = corpus_dir.join("interesting");
    let checkpoints_dir = corpus_dir.join("checkpoints");
    let crashes_dir = corpus_dir.join("crashes");
    let coverage_corpus_dir = corpus_dir.join("coverage");
    let comparison_values_path = corpus_dir.join("comparison-u8-values.txt");
    let mut coverage = coverage_dir
        .map(|dir| CoverageReporter::new(dir, Duration::from_secs(coverage_poll_secs)))
        .transpose()?;
    if coverage_retention && coverage.is_none() {
        eprintln!("coverage retention requested without --coverage-dir; no coverage traces will be retained");
    }

    let mut feedback = FeedbackState::new();
    let persisted_comparison_values = load_comparison_u8_values(&comparison_values_path)?;
    if !persisted_comparison_values.is_empty() {
        eprintln!(
            "loaded {} persisted comparison values",
            persisted_comparison_values.len()
        );
        feedback.seed_comparison_u8_values(persisted_comparison_values);
    }
    let mut runner = Runner::new();
    let tally = Tally {
        interesting: trace_file_count(&interesting_dir)?,
        coverage_saved: trace_file_count(&coverage_corpus_dir)?,
        checkpoints: trace_file_count(&checkpoints_dir)?,
        crashes: trace_file_count(&crashes_dir)?,
        ..Tally::default()
    };
    let mut pending_mutations = PendingMutations::new();
    let started = Instant::now();
    let report_interval = Duration::from_secs(report_secs);
    let mut last_report = started;

    let stats_path = stats_file.unwrap_or_else(|| corpus_dir.join("stats.jsonl"));
    let mut stats = StatsLogger::create(&stats_path);
    stats.log(json!({
        "event": "start",
        "time_unix": SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|time| time.as_secs())
            .unwrap_or(0),
        "seed": seed,
        "iterations": iterations,
        "jobs": jobs,
        "corpus_dir": corpus_dir.display().to_string(),
        "corpus_loaded": corpus.len(),
        "max_corpus_load": max_corpus_load,
    }));

    let pool = (jobs > 1).then(|| ExecutorPool::new(jobs));

    eprintln!("warming up feedback from {} corpus traces", corpus.len());
    let mut last_warmup_report = Instant::now();
    let mut warmup_progress = |completed: usize, total: usize| {
        if report_secs != 0 && last_warmup_report.elapsed() >= report_interval {
            eprintln!("warmup: replayed {completed}/{total} corpus traces");
            last_warmup_report = Instant::now();
        }
    };
    if let Some(pool) = &pool {
        let total = corpus.len();
        let mut submitted = 0usize;
        let mut completed = 0usize;
        while completed < total {
            while submitted < total && submitted - completed < jobs * 2 {
                pool.submit(Job {
                    tag: submitted,
                    trace: corpus[submitted].clone(),
                    parent_desc: String::new(),
                });
                submitted += 1;
            }
            let outcome = pool.recv()?;
            completed += 1;
            if let Ok(report) = outcome.result {
                warmup_consider(
                    &corpus[outcome.tag],
                    &report,
                    outcome.tag,
                    &mut rng,
                    &mut feedback,
                    &mut corpus_power,
                    &mut pending_mutations,
                );
            }
            warmup_progress(completed, total);
        }
    } else {
        for index in 0..corpus.len() {
            if let Ok(report) = runner.run_catching(&corpus[index]) {
                warmup_consider(
                    &corpus[index],
                    &report,
                    index,
                    &mut rng,
                    &mut feedback,
                    &mut corpus_power,
                    &mut pending_mutations,
                );
            }
            warmup_progress(index + 1, corpus.len());
        }
    }

    // The warmup snapshot is the baseline: novelty counted here came from the
    // existing corpus, so later report deltas are attributable to fuzzing.
    stats.log(report_event(
        "warmup",
        0,
        started.elapsed(),
        corpus.len(),
        &tally,
        &feedback,
    ));

    println!(
        "starting fuzz run: seed={seed} iterations={iterations} jobs={jobs} corpus={}",
        corpus.len()
    );
    flush_stdout();

    // Status exec/s and event timestamps measure the fuzz loop itself, not the
    // sequential-or-parallel warmup that precedes it.
    let loop_started = Instant::now();
    let mut state = FuzzLoop {
        rng,
        runner,
        corpus,
        corpus_power,
        feedback,
        pending_mutations,
        tally,
        stats,
        recent_valid: VecDeque::new(),
        boring: 0,
        reset_after: 1000,
        mutation_effort_multiplier: 1,
        started: loop_started,
        interesting_dir,
        checkpoints_dir,
        crashes_dir,
        coverage_corpus_dir,
        coverage_retention,
        coverage_retention_window,
    };

    if let Some(pool) = &pool {
        // Workers only execute candidates; generation, feedback, and corpus
        // management stay on this thread. Results are handled in completion
        // order, so a parallel run is not reproducible run-to-run, but every
        // saved trace remains individually replayable.
        let max_in_flight = jobs * 2;
        let mut submitted = 0usize;
        let mut completed = 0usize;
        while completed < iterations {
            while submitted < iterations && submitted - completed < max_in_flight {
                let (candidate, parent_desc) = state.next_candidate();
                pool.submit(Job {
                    tag: 0,
                    trace: candidate,
                    parent_desc,
                });
                submitted += 1;
            }
            let JobResult {
                trace,
                parent_desc,
                result,
                ..
            } = pool.recv()?;
            state.process(completed, trace, parent_desc, result)?;
            completed += 1;
            state.maybe_report(completed, report_every, report_secs, &mut last_report);
            state.poll_coverage(&mut coverage, completed)?;
        }
    } else {
        for iteration in 0..iterations {
            let (candidate, parent_desc) = state.next_candidate();
            let result = state.runner.run_catching(&candidate);
            state.process(iteration, candidate, parent_desc, result)?;
            let completed = iteration + 1;
            state.maybe_report(completed, report_every, report_secs, &mut last_report);
            state.poll_coverage(&mut coverage, completed)?;
        }
    }
    if let Some(pool) = pool {
        pool.shutdown();
    }

    let FuzzLoop {
        runner,
        corpus,
        feedback,
        tally,
        mut stats,
        ..
    } = state;

    print_fuzz_status(
        "done: iters",
        iterations,
        loop_started.elapsed(),
        corpus.len(),
        &tally,
        &feedback,
    );
    if let Some(status) = runner.state_cache_status() {
        println!("state cache: {status}");
    }

    if !feedback.sometimes_counts().is_empty() {
        println!("sometimes hit counts:");
        for (name, count) in feedback.sometimes_counts() {
            println!("  {name}={count}");
        }
    }

    let known_labels = automerge::sometimes::known_labels();
    let unhit = known_labels
        .iter()
        .copied()
        .filter(|label| !feedback.sometimes_counts().contains_key(label))
        .collect::<Vec<_>>();
    println!(
        "sometimes labels: known={} hit={} unhit={}",
        known_labels.len(),
        feedback.sometimes_counts().len(),
        unhit.len()
    );
    if !unhit.is_empty() {
        println!("sometimes labels not hit:");
        for label in &unhit {
            println!("  {label}");
        }
    }

    let mut done = report_event(
        "done",
        iterations,
        loop_started.elapsed(),
        corpus.len(),
        &tally,
        &feedback,
    );
    done["sometimes_known"] = json!(known_labels.len());
    done["sometimes_unhit"] = json!(unhit);
    stats.log(done);

    save_comparison_u8_values(&comparison_values_path, feedback.comparison_u8_values())?;

    if let Some(mut coverage_reporter) = coverage {
        match coverage_reporter.final_report() {
            Ok(Some(summary)) => println!("final coverage: {}", summary.status_line(None)),
            Ok(None) => eprintln!("coverage: no profile data collected"),
            Err(err) => eprintln!("coverage: failed to write final in-process report: {err}"),
        }
    }

    Ok(())
}

/// Coverage and sancov instrumentation record into process-global counters, so
/// per-trace attribution is only sound with a single executing thread.
fn effective_jobs(requested: usize) -> usize {
    let requested = requested.max(1);
    if cfg!(any(coverage, sancov)) && requested > 1 {
        eprintln!(
            "--jobs {requested} is unsupported with coverage/sancov instrumentation; using 1"
        );
        return 1;
    }
    requested
}

#[allow(clippy::too_many_arguments)]
fn warmup_consider(
    trace: &Trace,
    report: &automerge_fuzz::runner::RunReport,
    index: usize,
    rng: &mut StdRng,
    feedback: &mut FeedbackState,
    corpus_power: &mut [u32],
    pending_mutations: &mut PendingMutations,
) {
    let reason = feedback.consider(trace, report);
    let raw_power = feedback.power_score(report);
    corpus_power[index] = if reason.as_deref().is_some_and(is_coverage_reason) {
        raw_power.min(2)
    } else {
        raw_power
    };
    if let Some(reason) = reason {
        let priority = reason_priority(&reason);
        if !is_coverage_reason(&reason) {
            let effort = scheduled_effort(priority)
                .saturating_mul(effort_power(corpus_power[index]))
                .saturating_div(2)
                .max(1);
            let batch = mutation_batch(trace, rng, effort, feedback.comparison_u8_values());
            pending_mutations.push_batch(priority, batch);
        }
    }
}

/// All mutable state of the fuzzing loop. Candidate generation, feedback, and
/// corpus management run on the main thread whether execution is inline
/// (jobs=1) or delegated to an [`ExecutorPool`].
struct FuzzLoop {
    rng: StdRng,
    runner: Runner,
    corpus: Vec<Trace>,
    corpus_power: Vec<u32>,
    feedback: FeedbackState,
    pending_mutations: PendingMutations,
    tally: Tally,
    stats: StatsLogger,
    recent_valid: VecDeque<Trace>,
    boring: usize,
    reset_after: usize,
    mutation_effort_multiplier: usize,
    started: Instant,
    interesting_dir: PathBuf,
    checkpoints_dir: PathBuf,
    crashes_dir: PathBuf,
    coverage_corpus_dir: PathBuf,
    coverage_retention: bool,
    coverage_retention_window: usize,
}

impl FuzzLoop {
    fn next_candidate(&mut self) -> (Trace, String) {
        if let Some(candidate) = self.pending_mutations.pop(&mut self.rng) {
            (candidate, "scheduled recombination".to_string())
        } else {
            let base_index = select_base_index(&self.corpus, &self.corpus_power, &mut self.rng);
            let candidate = mutate(&self.corpus[base_index], &mut self.rng);
            (candidate, format!("corpus index {base_index}"))
        }
    }

    fn process(
        &mut self,
        iteration: usize,
        candidate: Trace,
        parent_desc: String,
        result: Result<automerge_fuzz::runner::RunReport, RunError>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match result {
            Ok(report) => {
                self.tally.valid += 1;
                if self.coverage_retention && self.coverage_retention_window > 0 {
                    self.recent_valid.push_back(candidate.clone());
                    while self.recent_valid.len() > self.coverage_retention_window {
                        self.recent_valid.pop_front();
                    }
                }
                if let Some(reason) = self.feedback.consider(&candidate, &report) {
                    let raw_candidate_power = self.feedback.power_score(&report);
                    let candidate_power = if is_coverage_reason(&reason) {
                        raw_candidate_power.min(2)
                    } else {
                        raw_candidate_power
                    };
                    let maybe_target = SometimesTarget::from_reason(&reason);
                    let mut saved = candidate.clone();
                    saved.metadata.parent = Some(parent_desc.clone());
                    saved.metadata.reason = Some(reason.clone());
                    let path = self
                        .interesting_dir
                        .join(format!("interesting-{:08}.amtrace", self.tally.interesting));
                    save_trace(&path, &saved)?;
                    let saved_for_batch = saved.clone();
                    self.corpus.push(saved);
                    self.corpus_power.push(candidate_power);
                    self.tally.interesting += 1;

                    self.boring = 0;
                    let priority = reason_priority(&reason);
                    self.stats.log(json!({
                        "event": "novelty",
                        "iteration": iteration,
                        "elapsed_secs": self.started.elapsed().as_secs_f64(),
                        "priority": priority,
                        "reason": reason,
                        "path": path.display().to_string(),
                    }));
                    if !is_coverage_reason(&reason) {
                        let batch = mutation_batch(
                            &saved_for_batch,
                            &mut self.rng,
                            scheduled_effort(priority)
                                * self.mutation_effort_multiplier
                                * effort_power(candidate_power),
                            self.feedback.comparison_u8_values(),
                        );
                        self.pending_mutations.push_batch(priority, batch);
                    }

                    if let Some(target) = maybe_target {
                        if let Some(mut checkpoint) =
                            find_sometimes_checkpoint(&candidate, &target, &mut self.runner)
                        {
                            checkpoint.metadata.parent = Some(path.display().to_string());
                            checkpoint.metadata.reason = Some(format!(
                                "checkpoint for {} at step {}",
                                target.description(),
                                checkpoint.steps.len()
                            ));
                            let checkpoint_path = self.checkpoints_dir.join(format!(
                                "sometimes-checkpoint-{:08}.amtrace",
                                self.tally.checkpoints
                            ));
                            save_trace(&checkpoint_path, &checkpoint)?;
                            if let Err(err) = self.runner.cache_checkpoint_prefix(&checkpoint) {
                                eprintln!("failed to cache checkpoint prefix: {err}");
                            }
                            self.stats.log(json!({
                                "event": "checkpoint",
                                "iteration": iteration,
                                "elapsed_secs": self.started.elapsed().as_secs_f64(),
                                "steps": checkpoint.steps.len(),
                                "target": target.description(),
                                "path": checkpoint_path.display().to_string(),
                            }));
                            let extension_batch = prefix_extension_batch(
                                &checkpoint,
                                &mut self.rng,
                                scheduled_effort(priority) * self.mutation_effort_multiplier,
                            );
                            self.pending_mutations
                                .push_batch(priority.max(3), extension_batch);
                            self.corpus.push(checkpoint);
                            self.corpus_power.push(candidate_power.max(2));
                            self.tally.checkpoints += 1;
                        }
                    }
                } else {
                    self.boring = self.boring.saturating_add(1);
                }
            }
            Err(err) if is_crash(&err) => {
                let mut saved = candidate.clone();
                saved.metadata.parent = Some(parent_desc.clone());
                saved.metadata.reason = Some(format!("failure: {err}"));
                let path = self
                    .crashes_dir
                    .join(format!("crash-{:08}.amtrace", self.tally.crashes));
                save_trace(&path, &saved)?;
                eprintln!("saved crash {}: {err}", path.display());
                self.stats.log(json!({
                    "event": "crash",
                    "iteration": iteration,
                    "elapsed_secs": self.started.elapsed().as_secs_f64(),
                    "error": err.to_string(),
                    "path": path.display().to_string(),
                }));
                self.tally.crashes += 1;
            }
            Err(_) => {
                self.tally.rejected += 1;
                self.boring = self.boring.saturating_add(1);
                if self.tally.rejected % 8 == 0 {
                    let batch = mutation_batch(
                        &candidate,
                        &mut self.rng,
                        self.mutation_effort_multiplier,
                        self.feedback.comparison_u8_values(),
                    );
                    self.pending_mutations.push_rejected_batch(1, batch);
                }
            }
        }

        if self.boring > self.reset_after {
            self.mutation_effort_multiplier =
                self.mutation_effort_multiplier.saturating_mul(2).min(16);
            self.reset_after = self.reset_after.saturating_mul(2);
            self.boring = 0;
            self.pending_mutations.clear();
            eprintln!(
                "mutation scheduler: no novelty recently; random effort multiplier now {}",
                self.mutation_effort_multiplier
            );
            let recent_start = self.corpus.len().saturating_sub(16);
            let recent = self.corpus[recent_start..].to_vec();
            for trace in &recent {
                let batch = mutation_batch(
                    trace,
                    &mut self.rng,
                    4 * self.mutation_effort_multiplier,
                    self.feedback.comparison_u8_values(),
                );
                self.pending_mutations.push_batch(2, batch);
            }
        }

        Ok(())
    }

    fn maybe_report(
        &mut self,
        completed: usize,
        report_every: usize,
        report_secs: u64,
        last_report: &mut Instant,
    ) {
        let now = Instant::now();
        let report_interval = Duration::from_secs(report_secs);
        let report_by_iteration = report_every != 0 && completed % report_every == 0;
        let report_by_time =
            report_secs != 0 && now.duration_since(*last_report) >= report_interval;
        if !(report_by_iteration || report_by_time) {
            return;
        }
        print_fuzz_status(
            "iters",
            completed,
            self.started.elapsed(),
            self.corpus.len(),
            &self.tally,
            &self.feedback,
        );
        self.stats.log(report_event(
            "report",
            completed,
            self.started.elapsed(),
            self.corpus.len(),
            &self.tally,
            &self.feedback,
        ));
        *last_report = now;
    }

    fn poll_coverage(
        &mut self,
        coverage: &mut Option<CoverageReporter>,
        completed: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let Some(coverage_reporter) = coverage.as_mut() else {
            return Ok(());
        };
        match coverage_reporter.maybe_poll() {
            Ok(Some(update)) => {
                println!("coverage: {}", update.status_line());
                self.stats.log(json!({
                    "event": "coverage",
                    "iteration": completed,
                    "elapsed_secs": self.started.elapsed().as_secs_f64(),
                    "lines_covered": update.summary.lines_covered,
                    "lines_total": update.summary.lines_total,
                    "regions_covered": update.summary.regions_covered,
                    "regions_total": update.summary.regions_total,
                    "functions_covered": update.summary.functions_covered,
                    "functions_total": update.summary.functions_total,
                }));
                if self.coverage_retention && update.increased() && !self.recent_valid.is_empty() {
                    let saved = save_coverage_window(
                        &self.coverage_corpus_dir,
                        &self.recent_valid,
                        self.tally.coverage_saved,
                        completed,
                    )?;
                    for trace in &self.recent_valid {
                        let batch = mutation_batch(
                            trace,
                            &mut self.rng,
                            4 * self.mutation_effort_multiplier,
                            self.feedback.comparison_u8_values(),
                        );
                        self.pending_mutations.push_batch(2, batch);
                    }
                    self.tally.coverage_saved += saved;
                    for trace in self.recent_valid.iter().cloned() {
                        self.corpus.push(trace);
                        self.corpus_power.push(2);
                    }
                    self.recent_valid.clear();
                }
            }
            Ok(None) => {}
            Err(err) => {
                eprintln!("coverage polling disabled: {err}");
                *coverage = None;
            }
        }
        Ok(())
    }
}

struct Job {
    tag: usize,
    trace: Trace,
    parent_desc: String,
}

struct JobResult {
    tag: usize,
    trace: Trace,
    parent_desc: String,
    result: Result<automerge_fuzz::runner::RunReport, RunError>,
}

/// A fixed pool of worker threads that execute traces. Each worker owns its
/// own [`Runner`]; `sometimes` hits are recorded thread-locally, so reports
/// stay attributed to the trace that produced them.
struct ExecutorPool {
    work_tx: Option<mpsc::Sender<Job>>,
    results_rx: mpsc::Receiver<JobResult>,
    workers: Vec<thread::JoinHandle<()>>,
}

impl ExecutorPool {
    fn new(jobs: usize) -> Self {
        let (work_tx, work_rx) = mpsc::channel::<Job>();
        let work_rx = Arc::new(Mutex::new(work_rx));
        let (results_tx, results_rx) = mpsc::channel::<JobResult>();
        let workers = (0..jobs)
            .map(|_| {
                let work_rx = Arc::clone(&work_rx);
                let results_tx = results_tx.clone();
                thread::spawn(move || {
                    let mut runner = Runner::new();
                    loop {
                        let job = {
                            let work_rx = work_rx.lock().unwrap_or_else(|err| err.into_inner());
                            work_rx.recv()
                        };
                        let Ok(job) = job else {
                            break;
                        };
                        let result = runner.run_catching(&job.trace);
                        let sent = results_tx.send(JobResult {
                            tag: job.tag,
                            trace: job.trace,
                            parent_desc: job.parent_desc,
                            result,
                        });
                        if sent.is_err() {
                            break;
                        }
                    }
                })
            })
            .collect();
        Self {
            work_tx: Some(work_tx),
            results_rx,
            workers,
        }
    }

    fn submit(&self, job: Job) {
        let _ = self
            .work_tx
            .as_ref()
            .expect("pool has not been shut down")
            .send(job);
    }

    fn recv(&self) -> Result<JobResult, mpsc::RecvError> {
        self.results_rx.recv()
    }

    fn shutdown(mut self) {
        self.work_tx.take();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

#[derive(Clone, Debug)]
enum SometimesTarget {
    Label { name: String },
}

impl SometimesTarget {
    fn from_reason(reason: &str) -> Option<Self> {
        // Checkpoints are for the first prefix that reaches a semantic label.
        // Count-bucket novelty is useful for corpus retention, but checkpointing
        // it causes many extra binary-search replays during startup.
        if reason.starts_with("new sometimes count bucket ") {
            return None;
        }

        reason
            .strip_prefix("new sometimes ")
            .map(|name| Self::Label {
                name: name.to_string(),
            })
    }

    fn matches(&self, report: &automerge_fuzz::runner::RunReport) -> bool {
        match self {
            Self::Label { name } => report.sometimes_hits.iter().any(|hit| hit.name == name),
        }
    }

    fn description(&self) -> String {
        match self {
            Self::Label { name } => format!("sometimes {name}"),
        }
    }
}

fn find_sometimes_checkpoint(
    trace: &Trace,
    target: &SometimesTarget,
    runner: &mut Runner,
) -> Option<Trace> {
    if trace.steps.is_empty() {
        return None;
    }

    let mut low = 1usize;
    let mut high = trace.steps.len();
    let mut best = None;

    while low <= high {
        let mid = low + (high - low) / 2;
        let mut prefix = trace.clone();
        prefix.steps.truncate(mid);
        let matched = runner
            .run_catching(&prefix)
            .map(|report| target.matches(&report))
            .unwrap_or(false);
        if matched {
            best = Some(prefix);
            if mid == 1 {
                break;
            }
            high = mid - 1;
        } else {
            low = mid + 1;
        }
    }

    best.filter(|prefix| prefix.steps.len() < trace.steps.len())
}

fn print_fuzz_status(
    prefix: &str,
    iterations: usize,
    elapsed: Duration,
    corpus_len: usize,
    tally: &Tally,
    feedback: &FeedbackState,
) {
    let elapsed = elapsed.as_secs_f64().max(0.001);
    println!(
        "{prefix}={iterations} exec/s={:.0} corpus={corpus_len} valid={} interesting={} checkpoints={} coverage_saved={} crashes={} rejected={} behavior={} features={} coverage_buckets={} cmp_buckets={} cmp_values={} sometimes={} sometimes_buckets={} buckets={}",
        iterations as f64 / elapsed,
        tally.valid,
        tally.interesting,
        tally.checkpoints,
        tally.coverage_saved,
        tally.crashes,
        tally.rejected,
        feedback.behavior_bucket_count(),
        feedback.feature_count(),
        feedback.coverage_bucket_count(),
        feedback.comparison_bucket_count(),
        feedback.comparison_value_count(),
        feedback.sometimes_label_count(),
        feedback.sometimes_count_bucket_count(),
        feedback.structural_bucket_count(),
    );
    flush_stdout();
}

/// One stats-log line with the same counters as the status line, so runs can
/// be plotted and A/B-compared offline.
fn report_event(
    event: &str,
    iterations: usize,
    elapsed: Duration,
    corpus_len: usize,
    tally: &Tally,
    feedback: &FeedbackState,
) -> serde_json::Value {
    let elapsed_secs = elapsed.as_secs_f64();
    json!({
        "event": event,
        "iterations": iterations,
        "elapsed_secs": elapsed_secs,
        "execs_per_sec": iterations as f64 / elapsed_secs.max(0.001),
        "corpus": corpus_len,
        "valid": tally.valid,
        "interesting": tally.interesting,
        "checkpoints": tally.checkpoints,
        "coverage_saved": tally.coverage_saved,
        "crashes": tally.crashes,
        "rejected": tally.rejected,
        "behavior_buckets": feedback.behavior_bucket_count(),
        "features": feedback.feature_count(),
        "coverage_buckets": feedback.coverage_bucket_count(),
        "cmp_buckets": feedback.comparison_bucket_count(),
        "cmp_values": feedback.comparison_value_count(),
        "sometimes_labels": feedback.sometimes_label_count(),
        "sometimes_count_buckets": feedback.sometimes_count_bucket_count(),
        "structural_buckets": feedback.structural_bucket_count(),
    })
}

fn flush_stdout() {
    let _ = io::stdout().flush();
}

fn select_base_index(corpus: &[Trace], powers: &[u32], rng: &mut StdRng) -> usize {
    if corpus.len() <= 1 {
        return 0;
    }

    match rng.random_range(0..100) {
        // AFL-like power scheduling: spend a large share of random mutations on
        // corpus entries that recently exercised rare LLVM counters or new
        // hit-count buckets.
        0..=39 => weighted_power_index(corpus, powers, rng),
        // Favor recently retained traces: these are often near a frontier.
        40..=64 => {
            let window = corpus.len().min(256);
            corpus.len() - 1 - rng.random_range(0..window)
        }
        // Favor smaller traces by tournament selection. This keeps execution
        // cheap and reduces the chance that novelty is just trace length.
        65..=89 => {
            let mut best = rng.random_range(0..corpus.len());
            for _ in 0..3 {
                let candidate = rng.random_range(0..corpus.len());
                if corpus[candidate].steps.len() < corpus[best].steps.len() {
                    best = candidate;
                }
            }
            best
        }
        _ => rng.random_range(0..corpus.len()),
    }
}

fn weighted_power_index(corpus: &[Trace], powers: &[u32], rng: &mut StdRng) -> usize {
    let total = (0..corpus.len()).fold(0u64, |total, index| {
        total.saturating_add(u64::from(*powers.get(index).unwrap_or(&1)).max(1))
    });
    if total == 0 {
        return rng.random_range(0..corpus.len());
    }

    let mut ticket = rng.random_range(0..total);
    for index in 0..corpus.len() {
        let weight = u64::from(*powers.get(index).unwrap_or(&1)).max(1);
        if ticket < weight {
            return index;
        }
        ticket -= weight;
    }
    corpus.len() - 1
}

fn save_coverage_window(
    dir: &Path,
    recent_valid: &VecDeque<Trace>,
    start_index: usize,
    iteration: usize,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut saved = 0;
    for (offset, trace) in recent_valid.iter().enumerate() {
        let mut trace = trace.clone();
        trace.metadata.reason = Some(format!("coverage increase near iteration {iteration}"));
        let path = dir.join(format!(
            "coverage-{:08}.amtrace",
            start_index.saturating_add(offset)
        ));
        save_trace(path, &trace)?;
        saved += 1;
    }
    Ok(saved)
}

fn is_crash(err: &RunError) -> bool {
    matches!(err, RunError::Panic(_) | RunError::Invariant(_))
}

fn load_comparison_u8_values(path: &Path) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let data = fs::read_to_string(path)?;
    let mut values = Vec::new();
    for line in data.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Ok(value) = line.parse::<u8>() {
            values.push(value);
        }
    }
    values.sort_unstable();
    values.dedup();
    Ok(values)
}

fn save_comparison_u8_values(path: &Path, values: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut values = values.to_vec();
    values.sort_unstable();
    values.dedup();
    let data = values
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(path, format!("{data}\n"))?;
    Ok(())
}

fn trace_file_count(dir: &Path) -> Result<usize, Box<dyn std::error::Error>> {
    if !dir.exists() {
        return Ok(0);
    }
    let mut count = 0usize;
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("amtrace") {
            count += 1;
        }
    }
    Ok(count)
}

fn load_corpus(
    corpus_dir: &Path,
    max_corpus_load: usize,
) -> Result<Vec<Trace>, Box<dyn std::error::Error>> {
    let mut traces = Vec::new();
    load_traces_from_dir(&corpus_dir.join("seeds"), &mut traces, usize::MAX)?;
    load_traces_from_dir(
        &corpus_dir.join("interesting"),
        &mut traces,
        max_corpus_load,
    )?;
    load_traces_from_dir(&corpus_dir.join("coverage"), &mut traces, max_corpus_load)?;
    load_traces_from_dir(
        &corpus_dir.join("checkpoints"),
        &mut traces,
        max_corpus_load,
    )?;
    Ok(traces)
}

fn load_traces_from_dir(
    dir: &Path,
    traces: &mut Vec<Trace>,
    limit: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if !dir.exists() {
        return Ok(());
    }

    eprintln!("loading traces from {}", dir.display());
    let before = traces.len();
    let mut paths = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("amtrace") {
            paths.push(path);
        }
    }
    // Sort so the corpus (and therefore a seeded fuzz run) does not depend on
    // filesystem iteration order.
    paths.sort();
    for path in paths.into_iter().take(limit) {
        let mut trace = load_trace(path)?;
        normalize_trace(&mut trace);
        traces.push(trace);
    }
    eprintln!(
        "loaded {} traces from {}",
        traces.len() - before,
        dir.display()
    );

    Ok(())
}
