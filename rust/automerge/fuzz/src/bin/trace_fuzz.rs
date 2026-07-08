use std::collections::VecDeque;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

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
    } else if reason.starts_with("new feature ") {
        3
    } else if is_coverage_reason(reason) {
        1
    } else {
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
    } = options;

    eprintln!(
        "initializing fuzz run: seed={seed} iterations={iterations} corpus_dir={}",
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
    let mut valid = 0usize;
    let mut interesting = trace_file_count(&interesting_dir)?;
    let mut coverage_saved = trace_file_count(&coverage_corpus_dir)?;
    let mut checkpoints = trace_file_count(&checkpoints_dir)?;
    let mut crashes = trace_file_count(&crashes_dir)?;
    let mut rejected = 0usize;
    let mut recent_valid = VecDeque::new();
    let mut pending_mutations = PendingMutations::new();
    let mut boring = 0usize;
    let mut reset_after = 1000usize;
    let mut mutation_effort_multiplier = 1usize;
    let started = Instant::now();
    let report_interval = Duration::from_secs(report_secs);
    let mut last_report = started;

    eprintln!("warming up feedback from {} corpus traces", corpus.len());
    let mut last_warmup_report = Instant::now();
    for (index, trace) in corpus.iter().enumerate() {
        if let Ok(report) = runner.run_catching(trace) {
            let reason = feedback.consider(trace, &report);
            let raw_power = feedback.power_score(&report);
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
                    let batch =
                        mutation_batch(trace, &mut rng, effort, feedback.comparison_u8_values());
                    pending_mutations.push_batch(priority, batch);
                }
            }
        }
        if report_secs != 0 && last_warmup_report.elapsed() >= report_interval {
            eprintln!(
                "warmup: replayed {}/{} corpus traces",
                index + 1,
                corpus.len()
            );
            last_warmup_report = Instant::now();
        }
    }

    println!(
        "starting fuzz run: seed={seed} iterations={iterations} corpus={}",
        corpus.len()
    );
    flush_stdout();

    for iteration in 0..iterations {
        let (candidate, parent_desc) = if let Some(candidate) = pending_mutations.pop(&mut rng) {
            (candidate, "scheduled recombination".to_string())
        } else {
            let base_index = select_base_index(&corpus, &corpus_power, &mut rng);
            let candidate = mutate(&corpus[base_index], &mut rng);
            let parent_desc = format!("corpus index {base_index}");
            (candidate, parent_desc)
        };

        match runner.run_catching(&candidate) {
            Ok(report) => {
                valid += 1;
                if coverage_retention && coverage_retention_window > 0 {
                    recent_valid.push_back(candidate.clone());
                    while recent_valid.len() > coverage_retention_window {
                        recent_valid.pop_front();
                    }
                }
                if let Some(reason) = feedback.consider(&candidate, &report) {
                    let raw_candidate_power = feedback.power_score(&report);
                    let candidate_power = if is_coverage_reason(&reason) {
                        raw_candidate_power.min(2)
                    } else {
                        raw_candidate_power
                    };
                    let maybe_target = SometimesTarget::from_reason(&reason);
                    let mut saved = candidate.clone();
                    saved.metadata.parent = Some(parent_desc.clone());
                    saved.metadata.reason = Some(reason.clone());
                    let path =
                        interesting_dir.join(format!("interesting-{interesting:08}.amtrace"));
                    save_trace(&path, &saved)?;
                    let saved_for_batch = saved.clone();
                    corpus.push(saved);
                    corpus_power.push(candidate_power);
                    interesting += 1;

                    boring = 0;
                    let priority = reason_priority(&reason);
                    if !is_coverage_reason(&reason) {
                        let batch = mutation_batch(
                            &saved_for_batch,
                            &mut rng,
                            scheduled_effort(priority)
                                * mutation_effort_multiplier
                                * effort_power(candidate_power),
                            feedback.comparison_u8_values(),
                        );
                        pending_mutations.push_batch(priority, batch);
                    }

                    if let Some(target) = maybe_target {
                        if let Some(mut checkpoint) =
                            find_sometimes_checkpoint(&candidate, &target, &mut runner)
                        {
                            checkpoint.metadata.parent = Some(path.display().to_string());
                            checkpoint.metadata.reason = Some(format!(
                                "checkpoint for {} at step {}",
                                target.description(),
                                checkpoint.steps.len()
                            ));
                            let checkpoint_path = checkpoints_dir
                                .join(format!("sometimes-checkpoint-{checkpoints:08}.amtrace"));
                            save_trace(&checkpoint_path, &checkpoint)?;
                            if let Err(err) = runner.cache_checkpoint_prefix(&checkpoint) {
                                eprintln!("failed to cache checkpoint prefix: {err}");
                            }
                            let extension_batch = prefix_extension_batch(
                                &checkpoint,
                                &mut rng,
                                scheduled_effort(priority) * mutation_effort_multiplier,
                            );
                            pending_mutations.push_batch(priority.max(3), extension_batch);
                            corpus.push(checkpoint);
                            corpus_power.push(candidate_power.max(2));
                            checkpoints += 1;
                        }
                    }
                } else {
                    boring = boring.saturating_add(1);
                }
            }
            Err(err) if is_crash(&err) => {
                let mut saved = candidate.clone();
                saved.metadata.parent = Some(parent_desc.clone());
                saved.metadata.reason = Some(format!("failure: {err}"));
                let path = crashes_dir.join(format!("crash-{crashes:08}.amtrace"));
                save_trace(&path, &saved)?;
                eprintln!("saved crash {}: {err}", path.display());
                crashes += 1;
            }
            Err(_) => {
                rejected += 1;
                boring = boring.saturating_add(1);
                if rejected % 8 == 0 {
                    let batch = mutation_batch(
                        &candidate,
                        &mut rng,
                        mutation_effort_multiplier,
                        feedback.comparison_u8_values(),
                    );
                    pending_mutations.push_rejected_batch(1, batch);
                }
            }
        }

        if boring > reset_after {
            mutation_effort_multiplier = mutation_effort_multiplier.saturating_mul(2).min(16);
            reset_after = reset_after.saturating_mul(2);
            boring = 0;
            pending_mutations.clear();
            eprintln!(
                "mutation scheduler: no novelty recently; random effort multiplier now {mutation_effort_multiplier}"
            );
            let recent_start = corpus.len().saturating_sub(16);
            let recent = corpus[recent_start..].to_vec();
            for trace in &recent {
                let batch = mutation_batch(
                    trace,
                    &mut rng,
                    4 * mutation_effort_multiplier,
                    feedback.comparison_u8_values(),
                );
                pending_mutations.push_batch(2, batch);
            }
        }

        let completed = iteration + 1;
        let now = Instant::now();
        let report_by_iteration = report_every != 0 && completed % report_every == 0;
        let report_by_time = report_secs != 0 && now.duration_since(last_report) >= report_interval;
        if report_by_iteration || report_by_time {
            print_fuzz_status(
                "iters",
                completed,
                started,
                corpus.len(),
                valid,
                interesting,
                checkpoints,
                coverage_saved,
                crashes,
                rejected,
                &feedback,
            );
            last_report = now;
        }

        let mut disable_coverage = false;
        if let Some(coverage_reporter) = &mut coverage {
            match coverage_reporter.maybe_poll() {
                Ok(Some(update)) => {
                    println!("coverage: {}", update.status_line());
                    if coverage_retention && update.increased() && !recent_valid.is_empty() {
                        let saved = save_coverage_window(
                            &coverage_corpus_dir,
                            &recent_valid,
                            coverage_saved,
                            completed,
                        )?;
                        for trace in &recent_valid {
                            let batch = mutation_batch(
                                trace,
                                &mut rng,
                                4 * mutation_effort_multiplier,
                                feedback.comparison_u8_values(),
                            );
                            pending_mutations.push_batch(2, batch);
                        }
                        coverage_saved += saved;
                        for trace in recent_valid.iter().cloned() {
                            corpus.push(trace);
                            corpus_power.push(2);
                        }
                        recent_valid.clear();
                    }
                }
                Ok(None) => {}
                Err(err) => {
                    eprintln!("coverage polling disabled: {err}");
                    disable_coverage = true;
                }
            }
        }
        if disable_coverage {
            coverage = None;
        }
    }

    print_fuzz_status(
        "done: iters",
        iterations,
        started,
        corpus.len(),
        valid,
        interesting,
        checkpoints,
        coverage_saved,
        crashes,
        rejected,
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
        for label in unhit {
            println!("  {label}");
        }
    }

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

#[allow(clippy::too_many_arguments)]
fn print_fuzz_status(
    prefix: &str,
    iterations: usize,
    started: Instant,
    corpus_len: usize,
    valid: usize,
    interesting: usize,
    checkpoints: usize,
    coverage_saved: usize,
    crashes: usize,
    rejected: usize,
    feedback: &FeedbackState,
) {
    let elapsed = started.elapsed().as_secs_f64().max(0.001);
    println!(
        "{prefix}={iterations} exec/s={:.0} corpus={corpus_len} valid={valid} interesting={interesting} checkpoints={checkpoints} coverage_saved={coverage_saved} crashes={crashes} rejected={rejected} features={} coverage_buckets={} cmp_buckets={} cmp_values={} sometimes={} sometimes_buckets={} buckets={}",
        iterations as f64 / elapsed,
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
