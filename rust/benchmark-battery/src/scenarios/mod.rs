pub mod apply;
pub mod audit_mode;
pub mod bestiary;
pub mod build;
pub mod diff;
pub mod edit_trace;
pub mod egwalker_paper;
pub mod length;
pub mod list;
pub mod load_save;
pub mod map;
pub mod marks;
pub mod memory;
pub mod range;
pub mod sync;
pub mod typing;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkKind {
    Sampled,
    Series,
    Memory,
}

impl std::fmt::Display for BenchmarkKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sampled => f.write_str("sampled"),
            Self::Series => f.write_str("series"),
            Self::Memory => f.write_str("memory"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Tier {
    Fast,
    Slow,
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum TierFilter {
    Fast,
    Slow,
    All,
}

impl std::fmt::Display for TierFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fast => f.write_str("fast"),
            Self::Slow => f.write_str("slow"),
            Self::All => f.write_str("all"),
        }
    }
}

pub struct Runner {
    run_sample: Box<dyn FnMut(u64) -> u64>,
}

impl Runner {
    fn batched<State, Run, Output>(state: State, mut run: Run) -> Self
    where
        State: Clone + 'static,
        Run: FnMut(State) -> Output + 'static,
        Output: 'static,
    {
        Self {
            run_sample: Box::new(move |iterations| {
                let mut inputs = (0..iterations).map(|_| state.clone()).collect::<Vec<_>>();
                let mut outputs = Vec::with_capacity(inputs.len());
                let start = std::time::Instant::now();
                for input in inputs.drain(..) {
                    outputs.push(run(input));
                }
                let elapsed = elapsed_per_iteration(start, iterations);
                std::hint::black_box(&outputs);
                drop(outputs);
                drop(inputs);
                elapsed
            }),
        }
    }

    pub fn run_sample(&mut self, iterations: u64) -> u64 {
        (self.run_sample)(iterations)
    }
}

pub struct SampledBenchmark {
    pub group: &'static str,
    pub name: &'static str,
    pub make_runner: Box<dyn Fn() -> Runner>,
}

impl SampledBenchmark {
    pub fn no_setup<Operation>(
        group: &'static str,
        name: &'static str,
        operation: Operation,
    ) -> Self
    where
        Operation: Fn() -> Box<dyn FnMut()> + 'static,
    {
        Self {
            group,
            name,
            make_runner: Box::new(move || {
                let mut operation = operation();
                Runner::batched((), move |()| operation())
            }),
        }
    }

    pub fn batched<Setup, Run, State, Output>(
        group: &'static str,
        name: &'static str,
        setup: Setup,
        run: Run,
    ) -> Self
    where
        Setup: Fn() -> State + 'static,
        Run: FnMut(State) -> Output + Clone + 'static,
        State: Clone + 'static,
        Output: 'static,
    {
        Self {
            group,
            name,
            make_runner: Box::new(move || Runner::batched(setup(), run.clone())),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryMeasurement {
    pub peak_bytes: u64,
    pub steady_bytes: u64,
}

type MemorySetup = Box<dyn Fn() -> Box<dyn FnMut() -> MemoryMeasurement>>;

pub struct MemoryBenchmark {
    pub group: &'static str,
    pub name: &'static str,
    pub setup: Option<MemorySetup>,
}

impl MemoryBenchmark {
    #[cfg(any(feature = "memory", test))]
    pub fn new<Setup>(group: &'static str, name: &'static str, setup: Setup) -> Self
    where
        Setup: Fn() -> Box<dyn FnMut() -> MemoryMeasurement> + 'static,
    {
        Self {
            group,
            name,
            setup: Some(Box::new(setup)),
        }
    }

    #[cfg(not(feature = "memory"))]
    pub fn unavailable(group: &'static str, name: &'static str) -> Self {
        Self {
            group,
            name,
            setup: None,
        }
    }
}

fn elapsed_per_iteration(start: std::time::Instant, iterations: u64) -> u64 {
    nanos_u64(start.elapsed().as_nanos() / u128::from(iterations))
}

fn nanos_u64(nanos: u128) -> u64 {
    nanos.try_into().unwrap_or(u64::MAX)
}

pub struct SeriesBenchmark {
    pub group: &'static str,
    pub name: &'static str,
    pub steps: usize,
    pub setup: fn() -> Box<dyn FnMut(usize)>,
}

pub enum Benchmark {
    Sampled(SampledBenchmark),
    Series(SeriesBenchmark),
    Memory(MemoryBenchmark),
}

impl Benchmark {
    pub fn group(&self) -> &'static str {
        match self {
            Self::Sampled(benchmark) => benchmark.group,
            Self::Series(benchmark) => benchmark.group,
            Self::Memory(benchmark) => benchmark.group,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Sampled(benchmark) => benchmark.name,
            Self::Series(benchmark) => benchmark.name,
            Self::Memory(benchmark) => benchmark.name,
        }
    }

    pub fn tier(&self) -> Tier {
        tier(self.name())
    }

    pub fn kind(&self) -> BenchmarkKind {
        match self {
            Self::Sampled(_) => BenchmarkKind::Sampled,
            Self::Series(_) => BenchmarkKind::Series,
            Self::Memory(_) => BenchmarkKind::Memory,
        }
    }

    pub fn unit(&self) -> &'static str {
        match self {
            Self::Sampled(_) => "ns/iter",
            Self::Series(_) => "ns/step",
            Self::Memory(_) => "bytes",
        }
    }

    pub fn is_available(&self) -> bool {
        match self {
            Self::Sampled(_) | Self::Series(_) => true,
            Self::Memory(benchmark) => benchmark.setup.is_some(),
        }
    }
}

impl From<SampledBenchmark> for Benchmark {
    fn from(benchmark: SampledBenchmark) -> Self {
        Self::Sampled(benchmark)
    }
}

impl From<SeriesBenchmark> for Benchmark {
    fn from(benchmark: SeriesBenchmark) -> Self {
        Self::Series(benchmark)
    }
}

impl From<MemoryBenchmark> for Benchmark {
    fn from(benchmark: MemoryBenchmark) -> Self {
        Self::Memory(benchmark)
    }
}

pub fn benchmarks() -> Vec<Benchmark> {
    let mut benchmarks = Vec::new();
    benchmarks.extend(apply::benchmarks().into_iter().map(Into::into));
    benchmarks.extend(audit_mode::benchmarks().into_iter().map(Into::into));
    benchmarks.extend(bestiary::benchmarks().into_iter().map(Into::into));
    benchmarks.extend(build::benchmarks().into_iter().map(Into::into));
    benchmarks.extend(diff::benchmarks().into_iter().map(Into::into));
    benchmarks.extend(edit_trace::benchmarks().into_iter().map(Into::into));
    benchmarks.extend(egwalker_paper::benchmarks().into_iter().map(Into::into));
    benchmarks.extend(length::benchmarks().into_iter().map(Into::into));
    benchmarks.extend(list::benchmarks().into_iter().map(Into::into));
    benchmarks.extend(load_save::benchmarks().into_iter().map(Into::into));
    benchmarks.extend(map::benchmarks());
    benchmarks.extend(marks::benchmarks().into_iter().map(Into::into));
    benchmarks.extend(memory::benchmarks().into_iter().map(Into::into));
    benchmarks.extend(range::benchmarks().into_iter().map(Into::into));
    benchmarks.extend(sync::benchmarks());
    benchmarks.extend(typing::benchmarks().into_iter().map(Into::into));
    benchmarks
}

pub fn filter<'a>(
    benchmarks: &'a [Benchmark],
    filter: Option<&str>,
    tier_filter: TierFilter,
) -> Vec<&'a Benchmark> {
    benchmarks
        .iter()
        .filter(|benchmark| tier_matches(benchmark.tier(), tier_filter))
        .filter(|benchmark| {
            filter.is_none_or(|filter| {
                benchmark.group().contains(filter) || benchmark.name().contains(filter)
            })
        })
        .collect()
}

fn tier_matches(tier: Tier, filter: TierFilter) -> bool {
    match filter {
        TierFilter::Fast => tier == Tier::Fast,
        TierFilter::Slow => tier == Tier::Slow,
        TierFilter::All => true,
    }
}

fn tier(name: &str) -> Tier {
    if is_fast(name) {
        Tier::Fast
    } else {
        Tier::Slow
    }
}

fn is_fast(name: &str) -> bool {
    name.contains("godot-modeled")
        || matches!(
            name,
            "build/build_big_paste"
                | "build/build_text_splice_100"
                | "egwalker_paper/get_text/A1"
                | "length/text_len_now"
                | "length/text_len_at"
                | "length/list_len_now"
                | "length/map_len_now"
                | "length/map_len_at"
                | "list/list_cursor_now"
                | "list/list_cursor_at"
                | "list/list_update_now"
                | "list/list_update_at"
                | "list/list_splice_index_now"
                | "list/list_splice_index_at"
                | "load_save/load_big_paste"
                | "load_save/save_big_paste"
                | "load_save/load_text_splice_100"
                | "load_save/save_text_splice_100"
                | "load_save/save_typing"
                | "map/increasing_put/1000"
                | "map/decreasing_put/1000"
                | "map/repeated_put/1000"
                | "map/repeated_increment/1000"
                | "map/deep_history/1000"
                | "marks/add_mark"
                | "marks/splice_without_marks"
                | "range/range/10000"
                | "range/range_at/10000"
                | "sync/full_many_tx"
                | "sync/full_one_tx"
                | "sync/full_one_tx/100"
                | "sync/full_one_tx/1000"
                | "sync/every_change/100"
                | "sync/big_chunky_sync_message"
                | "typing/single_char_100_bulk_incremental_load"
        )
}
