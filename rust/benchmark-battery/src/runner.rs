use crate::scenarios::{
    Benchmark, BenchmarkKind, MemoryBenchmark, SampledBenchmark, SeriesBenchmark,
};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub const SCHEMA_VERSION: u64 = 2;
pub const SUITE_NAME: &str = "benchmark-battery";

#[derive(Clone, Copy, Debug)]
pub struct RunOptions {
    pub sample_count: usize,
    pub warmup_count: usize,
    pub iters_per_sample: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BenchRun {
    pub schema: u64,
    pub suite: String,
    pub commit: Option<String>,
    pub timestamp_unix_seconds: u64,
    pub rustc: Option<String>,
    pub target: Option<String>,
    pub profile: String,
    pub sample_count: usize,
    pub results: Vec<BenchResult>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BenchResult {
    Sampled(SampledResult),
    Series(SeriesResult),
    Memory(MemoryResult),
    NotRun(NotRunResult),
}

impl BenchResult {
    pub fn name(&self) -> &str {
        match self {
            Self::Sampled(result) => &result.name,
            Self::Series(result) => &result.name,
            Self::Memory(result) => &result.name,
            Self::NotRun(result) => &result.name,
        }
    }

    pub fn unit(&self) -> &str {
        match self {
            Self::Sampled(result) => &result.unit,
            Self::Series(result) => &result.unit,
            Self::Memory(result) => &result.unit,
            Self::NotRun(result) => &result.unit,
        }
    }

    pub fn benchmark_kind(&self) -> BenchmarkKind {
        match self {
            Self::Sampled(_) => BenchmarkKind::Sampled,
            Self::Series(_) => BenchmarkKind::Series,
            Self::Memory(_) => BenchmarkKind::Memory,
            Self::NotRun(result) => result.benchmark_kind,
        }
    }

    pub fn was_run(&self) -> bool {
        !matches!(self, Self::NotRun(_))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NotRunResult {
    pub name: String,
    pub group: String,
    pub unit: String,
    pub benchmark_kind: BenchmarkKind,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SampledResult {
    pub name: String,
    pub group: String,
    pub unit: String,
    pub setup_duration_ns: u64,
    pub min: u64,
    pub median: u64,
    pub mean: u64,
    pub max: u64,
    pub samples: usize,
    pub iters_per_sample: u64,
    #[serde(default)]
    pub sample_values: Vec<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MemoryResult {
    pub name: String,
    pub group: String,
    pub unit: String,
    pub setup_duration_ns: u64,
    pub peak_bytes: u64,
    pub steady_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SeriesResult {
    pub name: String,
    pub group: String,
    pub unit: String,
    pub setup_duration_ns: u64,
    pub points: Vec<u64>,
    pub summary: SeriesSummary,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SeriesSummary {
    pub total: u64,
    pub mean: u64,
    pub median: u64,
    pub last_window_median: u64,
}

pub fn run(
    benchmarks: &[Benchmark],
    selected: &[&Benchmark],
    options: RunOptions,
) -> anyhow::Result<BenchRun> {
    anyhow::ensure!(
        options.sample_count > 0,
        "sample-count must be greater than zero"
    );
    anyhow::ensure!(
        options.iters_per_sample > 0,
        "iters-per-sample must be greater than zero"
    );
    ensure_unique_names(benchmarks.iter().map(Benchmark::name))?;
    let selected_names = selected
        .iter()
        .map(|benchmark| benchmark.name())
        .collect::<BTreeSet<_>>();

    let mut results = Vec::with_capacity(benchmarks.len());
    for benchmark in benchmarks {
        if !selected_names.contains(benchmark.name()) || !benchmark.is_available() {
            if selected_names.contains(benchmark.name()) && !benchmark.is_available() {
                eprintln!(
                    "not running {} (unavailable in this build)",
                    benchmark.name()
                );
            }
            results.push(not_run(benchmark));
            continue;
        }

        eprintln!("running {}", benchmark.name());
        results.push(match benchmark {
            Benchmark::Sampled(benchmark) => BenchResult::Sampled(run_sampled(benchmark, options)),
            Benchmark::Series(benchmark) => BenchResult::Series(run_series(benchmark)),
            Benchmark::Memory(benchmark) => BenchResult::Memory(run_memory(benchmark)),
        });
    }

    Ok(BenchRun {
        schema: SCHEMA_VERSION,
        suite: SUITE_NAME.to_string(),
        commit: git_commit(),
        timestamp_unix_seconds: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        rustc: command_output("rustc", &["--version"]),
        target: command_output("rustc", &["-vV"]).and_then(parse_host),
        profile: "release".to_string(),
        sample_count: options.sample_count,
        results,
    })
}

fn not_run(benchmark: &Benchmark) -> BenchResult {
    BenchResult::NotRun(NotRunResult {
        name: benchmark.name().to_string(),
        group: benchmark.group().to_string(),
        unit: benchmark.unit().to_string(),
        benchmark_kind: benchmark.kind(),
    })
}

fn run_sampled(benchmark: &SampledBenchmark, options: RunOptions) -> SampledResult {
    let setup_start = Instant::now();
    let mut operation = (benchmark.make_runner)();
    let setup_duration_ns = nanos_u64(setup_start.elapsed().as_nanos());

    for _ in 0..options.warmup_count {
        operation.run_sample(1);
    }

    let mut samples = Vec::with_capacity(options.sample_count);
    for _ in 0..options.sample_count {
        samples.push(operation.run_sample(options.iters_per_sample));
    }

    let stats = Stats::from_samples(&mut samples.clone());
    SampledResult {
        name: benchmark.name.to_string(),
        group: benchmark.group.to_string(),
        unit: "ns/iter".to_string(),
        setup_duration_ns,
        min: stats.min,
        median: stats.median,
        mean: stats.mean,
        max: stats.max,
        samples: options.sample_count,
        iters_per_sample: options.iters_per_sample,
        sample_values: samples,
    }
}

fn run_memory(benchmark: &MemoryBenchmark) -> MemoryResult {
    let setup_start = Instant::now();
    let mut measure = (benchmark
        .setup
        .as_ref()
        .expect("memory benchmark must be available before it is run"))();
    let setup_duration_ns = nanos_u64(setup_start.elapsed().as_nanos());
    let measurement = measure();
    MemoryResult {
        name: benchmark.name.to_string(),
        group: benchmark.group.to_string(),
        unit: "bytes".to_string(),
        setup_duration_ns,
        peak_bytes: measurement.peak_bytes,
        steady_bytes: measurement.steady_bytes,
    }
}

fn run_series(benchmark: &SeriesBenchmark) -> SeriesResult {
    let setup_start = Instant::now();
    let mut run_step = (benchmark.setup)();
    let setup_duration_ns = nanos_u64(setup_start.elapsed().as_nanos());
    let mut points = Vec::with_capacity(benchmark.steps);
    for step in 0..benchmark.steps {
        let start = Instant::now();
        run_step(step);
        points.push(nanos_u64(start.elapsed().as_nanos()));
    }
    let summary = SeriesSummary::from_points(&points);
    SeriesResult {
        name: benchmark.name.to_string(),
        group: benchmark.group.to_string(),
        unit: "ns/step".to_string(),
        setup_duration_ns,
        points,
        summary,
    }
}

#[derive(Debug)]
struct Stats {
    min: u64,
    median: u64,
    mean: u64,
    max: u64,
}

impl Stats {
    fn from_samples(samples: &mut [u64]) -> Self {
        samples.sort_unstable();
        let min = samples[0];
        let max = samples[samples.len() - 1];
        let median = median_sorted(samples);
        let mean = samples.iter().sum::<u64>() / samples.len() as u64;
        Self {
            min,
            median,
            mean,
            max,
        }
    }
}

impl SeriesSummary {
    fn from_points(points: &[u64]) -> Self {
        let total = points.iter().sum::<u64>();
        let mean = total / points.len() as u64;
        let mut sorted = points.to_vec();
        sorted.sort_unstable();
        let median = median_sorted(&sorted);
        let last_window_len = (points.len() / 10).max(1);
        let mut last_window = points[points.len() - last_window_len..].to_vec();
        last_window.sort_unstable();
        let last_window_median = median_sorted(&last_window);
        Self {
            total,
            mean,
            median,
            last_window_median,
        }
    }
}

fn median_sorted(samples: &[u64]) -> u64 {
    if samples.len().is_multiple_of(2) {
        let upper = samples.len() / 2;
        (samples[upper - 1] + samples[upper]) / 2
    } else {
        samples[samples.len() / 2]
    }
}

fn nanos_u64(nanos: u128) -> u64 {
    nanos.try_into().unwrap_or(u64::MAX)
}

pub fn append(mut existing: BenchRun, additional: BenchRun) -> anyhow::Result<BenchRun> {
    anyhow::ensure!(
        existing.schema == SCHEMA_VERSION,
        "cannot append to schema {}; expected {}",
        existing.schema,
        SCHEMA_VERSION
    );
    anyhow::ensure!(
        additional.schema == SCHEMA_VERSION,
        "cannot append schema {}; expected {}",
        additional.schema,
        SCHEMA_VERSION
    );
    anyhow::ensure!(
        existing.suite == additional.suite,
        "cannot append runs from different suites"
    );
    anyhow::ensure!(
        existing.commit == additional.commit,
        "cannot append runs from different commits"
    );
    anyhow::ensure!(
        existing.rustc == additional.rustc,
        "cannot append runs made with different Rust compilers"
    );
    anyhow::ensure!(
        existing.target == additional.target,
        "cannot append runs made for different targets"
    );
    anyhow::ensure!(
        existing.profile == additional.profile,
        "cannot append runs made with different profiles"
    );

    ensure_unique_names(existing.results.iter().map(BenchResult::name))?;
    ensure_unique_names(additional.results.iter().map(BenchResult::name))?;
    let mut existing_indices = existing
        .results
        .iter()
        .enumerate()
        .map(|(index, result)| (result.name().to_string(), index))
        .collect::<BTreeMap<_, _>>();

    for result in additional.results {
        if let Some(index) = existing_indices.get(result.name()).copied() {
            let previous = &existing.results[index];
            anyhow::ensure!(
                previous.benchmark_kind() == result.benchmark_kind(),
                "benchmark {} changed kind from {} to {}",
                result.name(),
                previous.benchmark_kind(),
                result.benchmark_kind()
            );
            anyhow::ensure!(
                previous.unit() == result.unit(),
                "benchmark {} changed unit from {} to {}",
                result.name(),
                previous.unit(),
                result.unit()
            );
            if result.was_run() {
                existing.results[index] = result;
            }
        } else {
            let index = existing.results.len();
            existing_indices.insert(result.name().to_string(), index);
            existing.results.push(result);
        }
    }

    Ok(existing)
}

fn ensure_unique_names<'a>(names: impl Iterator<Item = &'a str>) -> anyhow::Result<()> {
    let mut unique = BTreeSet::new();
    for name in names {
        anyhow::ensure!(unique.insert(name), "duplicate benchmark name {name}");
    }
    Ok(())
}

pub fn write_run(path: &Path, run: &BenchRun) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating parent directory {}", parent.display()))?;
    }
    let file = std::fs::File::create(path)
        .with_context(|| format!("creating result file {}", path.display()))?;
    serde_json::to_writer_pretty(file, run)
        .with_context(|| format!("writing result file {}", path.display()))?;
    Ok(())
}

pub fn read_run(path: &Path) -> anyhow::Result<BenchRun> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("opening result file {}", path.display()))?;
    serde_json::from_reader(file).with_context(|| format!("reading result file {}", path.display()))
}

fn git_commit() -> Option<String> {
    command_output("git", &["rev-parse", "HEAD"])
}

fn command_output(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn parse_host(rustc_verbose: String) -> Option<String> {
    rustc_verbose
        .lines()
        .find_map(|line| line.strip_prefix("host: ").map(str::to_string))
}

#[cfg(test)]
mod tests {
    use super::{
        append, run_memory, run_sampled, BenchResult, BenchRun, MemoryResult, NotRunResult,
        RunOptions, SeriesSummary, Stats, SCHEMA_VERSION, SUITE_NAME,
    };
    use crate::scenarios::{BenchmarkKind, MemoryBenchmark, MemoryMeasurement, SampledBenchmark};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static BATCHED_SETUPS: AtomicUsize = AtomicUsize::new(0);
    static BATCHED_CLONES: AtomicUsize = AtomicUsize::new(0);
    static BATCHED_RUNS: AtomicUsize = AtomicUsize::new(0);
    static BATCHED_DROPS: AtomicUsize = AtomicUsize::new(0);
    static MEMORY_SETUPS: AtomicUsize = AtomicUsize::new(0);
    static MEMORY_RUNS: AtomicUsize = AtomicUsize::new(0);

    struct TestInput;

    impl Clone for TestInput {
        fn clone(&self) -> Self {
            BATCHED_CLONES.fetch_add(1, Ordering::Relaxed);
            Self
        }
    }

    struct TestOutput;

    impl Drop for TestOutput {
        fn drop(&mut self) {
            BATCHED_DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn batched_setup() -> TestInput {
        BATCHED_SETUPS.fetch_add(1, Ordering::Relaxed);
        TestInput
    }

    fn batched_run(_: TestInput) -> TestOutput {
        BATCHED_RUNS.fetch_add(1, Ordering::Relaxed);
        TestOutput
    }

    fn memory_setup() -> Box<dyn FnMut() -> MemoryMeasurement> {
        MEMORY_SETUPS.fetch_add(1, Ordering::Relaxed);
        Box::new(|| {
            MEMORY_RUNS.fetch_add(1, Ordering::Relaxed);
            MemoryMeasurement {
                peak_bytes: 2_048,
                steady_bytes: 1_024,
            }
        })
    }

    #[test]
    fn stats_for_odd_samples() {
        let mut samples = [9, 1, 5];
        let stats = Stats::from_samples(&mut samples);
        assert_eq!(stats.min, 1);
        assert_eq!(stats.median, 5);
        assert_eq!(stats.mean, 5);
        assert_eq!(stats.max, 9);
    }

    #[test]
    fn stats_for_even_samples() {
        let mut samples = [10, 2, 4, 8];
        let stats = Stats::from_samples(&mut samples);
        assert_eq!(stats.min, 2);
        assert_eq!(stats.median, 6);
        assert_eq!(stats.mean, 6);
        assert_eq!(stats.max, 10);
    }

    #[test]
    fn batched_runner_clones_runs_and_drops_each_iteration() {
        BATCHED_SETUPS.store(0, Ordering::Relaxed);
        BATCHED_CLONES.store(0, Ordering::Relaxed);
        BATCHED_RUNS.store(0, Ordering::Relaxed);
        BATCHED_DROPS.store(0, Ordering::Relaxed);
        let result = run_sampled(
            &SampledBenchmark::batched("test", "test/batched", batched_setup, batched_run),
            RunOptions {
                sample_count: 2,
                warmup_count: 3,
                iters_per_sample: 4,
            },
        );
        assert_eq!(result.sample_values.len(), 2);
        assert_eq!(BATCHED_SETUPS.load(Ordering::Relaxed), 1);
        assert_eq!(BATCHED_CLONES.load(Ordering::Relaxed), 11);
        assert_eq!(BATCHED_RUNS.load(Ordering::Relaxed), 11);
        assert_eq!(BATCHED_DROPS.load(Ordering::Relaxed), 11);
    }

    #[test]
    fn memory_benchmark_runs_once() {
        MEMORY_SETUPS.store(0, Ordering::Relaxed);
        MEMORY_RUNS.store(0, Ordering::Relaxed);
        let result = run_memory(&MemoryBenchmark::new("memory", "memory/test", memory_setup));
        assert_eq!(result.peak_bytes, 2_048);
        assert_eq!(result.steady_bytes, 1_024);
        assert_eq!(MEMORY_SETUPS.load(Ordering::Relaxed), 1);
        assert_eq!(MEMORY_RUNS.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn append_preserves_measured_results_and_fills_not_run_results() {
        let existing = test_run(vec![memory_result("memory/a", 100), not_run("memory/b")]);
        let additional = test_run(vec![not_run("memory/a"), memory_result("memory/b", 200)]);

        let merged = append(existing, additional).unwrap();
        assert_eq!(merged.schema, SCHEMA_VERSION);
        assert_eq!(merged.results.len(), 2);
        let BenchResult::Memory(a) = &merged.results[0] else {
            panic!("existing measured result was replaced");
        };
        assert_eq!(a.peak_bytes, 100);
        let BenchResult::Memory(b) = &merged.results[1] else {
            panic!("not-run result was not filled");
        };
        assert_eq!(b.peak_bytes, 200);
    }

    fn test_run(results: Vec<BenchResult>) -> BenchRun {
        BenchRun {
            schema: SCHEMA_VERSION,
            suite: SUITE_NAME.to_string(),
            commit: Some("commit".to_string()),
            timestamp_unix_seconds: 0,
            rustc: Some("rustc".to_string()),
            target: Some("target".to_string()),
            profile: "release".to_string(),
            sample_count: 1,
            results,
        }
    }

    fn memory_result(name: &str, peak_bytes: u64) -> BenchResult {
        BenchResult::Memory(MemoryResult {
            name: name.to_string(),
            group: "memory".to_string(),
            unit: "bytes".to_string(),
            setup_duration_ns: 0,
            peak_bytes,
            steady_bytes: peak_bytes / 2,
        })
    }

    fn not_run(name: &str) -> BenchResult {
        BenchResult::NotRun(NotRunResult {
            name: name.to_string(),
            group: "memory".to_string(),
            unit: "bytes".to_string(),
            benchmark_kind: BenchmarkKind::Memory,
        })
    }

    #[test]
    fn series_summary_uses_last_ten_percent_window() {
        let points = (1..=20).collect::<Vec<_>>();
        let summary = SeriesSummary::from_points(&points);
        assert_eq!(summary.total, 210);
        assert_eq!(summary.mean, 10);
        assert_eq!(summary.median, 10);
        assert_eq!(summary.last_window_median, 19);
    }
}
