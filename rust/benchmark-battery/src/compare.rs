use crate::format::{format_bytes, format_duration};
use crate::graph::{self, SeriesLine};
use crate::runner::{
    BenchResult, BenchRun, MemoryResult, SampledResult, SeriesResult, SCHEMA_VERSION,
};
use crate::scenarios::BenchmarkKind;
use anyhow::Context;
use rand::rngs::StdRng;
use rand::SeedableRng;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

#[derive(Clone, Copy, Debug)]
pub struct CompareOptions {
    pub min_change_percent: f64,
    pub alpha: f64,
    pub bootstrap_resamples: usize,
    pub show_all: bool,
}

#[derive(Debug, Serialize)]
pub struct Comparison {
    pub before_commit: Option<String>,
    pub after_commit: Option<String>,
    pub min_change_percent: f64,
    pub alpha: f64,
    pub bootstrap_resamples: usize,
    pub entries: Vec<ComparisonEntry>,
    pub hidden_as_noise: usize,
}

#[derive(Debug, Serialize)]
pub struct ComparisonEntry {
    pub name: String,
    pub status: ComparisonStatus,
    #[serde(flatten)]
    pub data: ComparisonData,
}

#[derive(Debug, Serialize)]
#[serde(tag = "comparison_kind", rename_all = "snake_case")]
pub enum ComparisonData {
    Sampled(SampledComparison),
    Series(SeriesComparison),
    Memory(MemoryComparison),
    Added {
        result_kind: BenchmarkKind,
        median: Option<u64>,
    },
    Removed {
        result_kind: BenchmarkKind,
        median: Option<u64>,
    },
    NotRun {
        result_kind: BenchmarkKind,
        metric: Option<MemoryMetric>,
        before_run: bool,
        after_run: bool,
        before_value: Option<u64>,
        after_value: Option<u64>,
    },
}

#[derive(Debug, Serialize)]
pub struct SampledComparison {
    pub before_median: u64,
    pub after_median: u64,
    pub change_percent: f64,
    pub bootstrap_ci_percent: ConfidenceInterval,
    pub significant: bool,
}

#[derive(Debug, Serialize)]
pub struct MemoryComparison {
    pub metric: MemoryMetric,
    pub before_bytes: u64,
    pub after_bytes: u64,
    pub change_percent: f64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryMetric {
    Peak,
    SteadyState,
}

impl std::fmt::Display for MemoryMetric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Peak => f.write_str("peak memory"),
            Self::SteadyState => f.write_str("steady memory"),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct SeriesComparison {
    pub before_median: u64,
    pub after_median: u64,
    pub total_change_percent: f64,
    pub last_window_change_percent: Option<f64>,
    pub graph: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonStatus {
    Same,
    Faster,
    Slower,
    Added,
    Removed,
    NotRun,
    Noise,
}

pub fn compare(
    before: &BenchRun,
    after: &BenchRun,
    options: CompareOptions,
) -> anyhow::Result<Comparison> {
    anyhow::ensure!(
        before.schema == SCHEMA_VERSION,
        "unsupported before schema {}; expected {}",
        before.schema,
        SCHEMA_VERSION
    );
    anyhow::ensure!(
        after.schema == SCHEMA_VERSION,
        "unsupported after schema {}; expected {}",
        after.schema,
        SCHEMA_VERSION
    );
    anyhow::ensure!(
        options.alpha > 0.0 && options.alpha < 1.0,
        "alpha must be between 0 and 1"
    );
    anyhow::ensure!(
        options.bootstrap_resamples > 0,
        "bootstrap-resamples must be greater than zero"
    );

    let before_by_name =
        by_name(&before.results).context("before results contain duplicate names")?;
    let after_by_name = by_name(&after.results).context("after results contain duplicate names")?;
    let names = before_by_name
        .keys()
        .chain(after_by_name.keys())
        .copied()
        .collect::<BTreeSet<_>>();

    let mut entries = Vec::with_capacity(names.len());
    let mut hidden_as_noise = 0;
    for name in names {
        let compared = match (
            before_by_name.get(name).copied(),
            after_by_name.get(name).copied(),
        ) {
            (Some(before), Some(after)) => paired_entries(name, before, after, options)?,
            (Some(before), None) => vec![removed_entry(name, before)],
            (None, Some(after)) => vec![added_entry(name, after)],
            (None, None) => unreachable!(),
        };
        for entry in compared {
            if matches!(entry.status, ComparisonStatus::Noise) && !options.show_all {
                hidden_as_noise += 1;
            } else {
                entries.push(entry);
            }
        }
    }
    entries.sort_by(compare_entries);

    Ok(Comparison {
        before_commit: before.commit.clone(),
        after_commit: after.commit.clone(),
        min_change_percent: options.min_change_percent,
        alpha: options.alpha,
        bootstrap_resamples: options.bootstrap_resamples,
        entries,
        hidden_as_noise,
    })
}

pub fn write_series_graphs(
    before: &BenchRun,
    after: &BenchRun,
    report_dir: &Path,
) -> anyhow::Result<usize> {
    let before_by_name =
        by_name(&before.results).context("before results contain duplicate names")?;
    let after_by_name = by_name(&after.results).context("after results contain duplicate names")?;
    std::fs::create_dir_all(report_dir)
        .with_context(|| format!("creating report directory {}", report_dir.display()))?;

    let mut written = 0;
    for (name, before) in before_by_name {
        let Some(after) = after_by_name.get(name).copied() else {
            continue;
        };
        let (BenchResult::Series(before), BenchResult::Series(after)) = (before, after) else {
            continue;
        };
        let relative_path = graph::write_series_graph(
            report_dir,
            name,
            &[
                SeriesLine {
                    label: "before",
                    color: "#4e79a7",
                    points: &before.points,
                },
                SeriesLine {
                    label: "after",
                    color: "#e15759",
                    points: &after.points,
                },
            ],
        )?;
        let path = report_dir.join(relative_path);
        written += path.exists() as usize;
    }
    Ok(written)
}

fn added_entry(name: &str, result: &BenchResult) -> ComparisonEntry {
    ComparisonEntry {
        name: name.to_string(),
        status: ComparisonStatus::Added,
        data: ComparisonData::Added {
            result_kind: result.benchmark_kind(),
            median: result_median(result),
        },
    }
}

fn removed_entry(name: &str, result: &BenchResult) -> ComparisonEntry {
    ComparisonEntry {
        name: name.to_string(),
        status: ComparisonStatus::Removed,
        data: ComparisonData::Removed {
            result_kind: result.benchmark_kind(),
            median: result_median(result),
        },
    }
}

fn paired_entries(
    name: &str,
    before: &BenchResult,
    after: &BenchResult,
    options: CompareOptions,
) -> anyhow::Result<Vec<ComparisonEntry>> {
    anyhow::ensure!(
        before.unit() == after.unit(),
        "benchmark {name} has incompatible units: before={}, after={}",
        before.unit(),
        after.unit()
    );
    anyhow::ensure!(
        before.benchmark_kind() == after.benchmark_kind(),
        "benchmark {name} has incompatible result kinds"
    );

    if matches!(before, BenchResult::NotRun(_)) || matches!(after, BenchResult::NotRun(_)) {
        if matches!(
            (before, after),
            (BenchResult::NotRun(_), BenchResult::NotRun(_))
        ) {
            return Ok(Vec::new());
        }
        return Ok(not_run_entries(name, before, after));
    }

    match (before, after) {
        (BenchResult::Sampled(before), BenchResult::Sampled(after)) => {
            Ok(vec![sampled_entry(name, before, after, options)?])
        }
        (BenchResult::Series(before), BenchResult::Series(after)) => {
            Ok(vec![series_entry(name, before, after)])
        }
        (BenchResult::Memory(before), BenchResult::Memory(after)) => {
            Ok(memory_entries(name, before, after))
        }
        _ => anyhow::bail!("benchmark {name} has incompatible result kinds"),
    }
}

fn not_run_entries(name: &str, before: &BenchResult, after: &BenchResult) -> Vec<ComparisonEntry> {
    let metrics = match before.benchmark_kind() {
        BenchmarkKind::Memory => vec![Some(MemoryMetric::Peak), Some(MemoryMetric::SteadyState)],
        BenchmarkKind::Sampled | BenchmarkKind::Series => vec![None],
    };
    metrics
        .into_iter()
        .map(|metric| ComparisonEntry {
            name: name.to_string(),
            status: ComparisonStatus::NotRun,
            data: ComparisonData::NotRun {
                result_kind: before.benchmark_kind(),
                metric,
                before_run: before.was_run(),
                after_run: after.was_run(),
                before_value: result_value(before, metric),
                after_value: result_value(after, metric),
            },
        })
        .collect()
}

fn result_value(result: &BenchResult, metric: Option<MemoryMetric>) -> Option<u64> {
    match (result, metric) {
        (BenchResult::Sampled(result), None) => Some(result.median),
        (BenchResult::Series(result), None) => Some(result.summary.median),
        (BenchResult::Memory(result), Some(MemoryMetric::Peak)) => Some(result.peak_bytes),
        (BenchResult::Memory(result), Some(MemoryMetric::SteadyState)) => Some(result.steady_bytes),
        (BenchResult::NotRun(_), _) => None,
        _ => None,
    }
}

fn sampled_entry(
    name: &str,
    before: &SampledResult,
    after: &SampledResult,
    options: CompareOptions,
) -> anyhow::Result<ComparisonEntry> {
    let change_percent = percent_change(before.median, after.median).unwrap_or(0.0);
    let mut rng = StdRng::seed_from_u64(stable_seed(name));
    let ci = bootstrap_median_change_ci(
        &mut rng,
        &before.sample_values,
        &after.sample_values,
        options.alpha,
        options.bootstrap_resamples,
    )
    .with_context(|| format!("computing bootstrap CI for benchmark {name}"))?;
    let significant = ci.low > 0.0 || ci.high < 0.0;
    let large_enough = change_percent.abs() >= options.min_change_percent;
    let is_real_change = large_enough && significant;
    let status = if is_real_change {
        status_for_change(change_percent)
    } else if change_percent == 0.0 {
        ComparisonStatus::Same
    } else {
        ComparisonStatus::Noise
    };
    Ok(ComparisonEntry {
        name: name.to_string(),
        status,
        data: ComparisonData::Sampled(SampledComparison {
            before_median: before.median,
            after_median: after.median,
            change_percent,
            bootstrap_ci_percent: ci,
            significant,
        }),
    })
}

fn series_entry(name: &str, before: &SeriesResult, after: &SeriesResult) -> ComparisonEntry {
    let total_change = percent_change(before.summary.total, after.summary.total).unwrap_or(0.0);
    let last_window_change = percent_change(
        before.summary.last_window_median,
        after.summary.last_window_median,
    );
    ComparisonEntry {
        name: name.to_string(),
        status: status_for_change(total_change),
        data: ComparisonData::Series(SeriesComparison {
            before_median: before.summary.median,
            after_median: after.summary.median,
            total_change_percent: total_change,
            last_window_change_percent: last_window_change,
            graph: Some(graph::series_graph_png_path(name).display().to_string()),
        }),
    }
}

fn memory_entries(name: &str, before: &MemoryResult, after: &MemoryResult) -> Vec<ComparisonEntry> {
    [
        (MemoryMetric::Peak, before.peak_bytes, after.peak_bytes),
        (
            MemoryMetric::SteadyState,
            before.steady_bytes,
            after.steady_bytes,
        ),
    ]
    .into_iter()
    .map(|(metric, before_bytes, after_bytes)| {
        let change_percent = percent_change(before_bytes, after_bytes).unwrap_or(0.0);
        ComparisonEntry {
            name: name.to_string(),
            status: status_for_change(change_percent),
            data: ComparisonData::Memory(MemoryComparison {
                metric,
                before_bytes,
                after_bytes,
                change_percent,
            }),
        }
    })
    .collect()
}

fn status_for_change(change_percent: f64) -> ComparisonStatus {
    if change_percent < 0.0 {
        ComparisonStatus::Faster
    } else if change_percent > 0.0 {
        ComparisonStatus::Slower
    } else {
        ComparisonStatus::Same
    }
}

fn compare_entries(left: &ComparisonEntry, right: &ComparisonEntry) -> std::cmp::Ordering {
    right
        .effect_size()
        .total_cmp(&left.effect_size())
        .then_with(|| left.name.cmp(&right.name))
}

impl ComparisonEntry {
    fn effect_size(&self) -> f64 {
        match &self.data {
            ComparisonData::Sampled(data) => data.change_percent.abs(),
            ComparisonData::Series(data) => data.total_change_percent.abs(),
            ComparisonData::Memory(data) => data.change_percent.abs(),
            ComparisonData::Added { .. }
            | ComparisonData::Removed { .. }
            | ComparisonData::NotRun { .. } => 0.0,
        }
    }

    fn result_kind(&self) -> BenchmarkKind {
        match &self.data {
            ComparisonData::Sampled(_) => BenchmarkKind::Sampled,
            ComparisonData::Series(_) => BenchmarkKind::Series,
            ComparisonData::Memory(_) => BenchmarkKind::Memory,
            ComparisonData::Added { result_kind, .. }
            | ComparisonData::Removed { result_kind, .. }
            | ComparisonData::NotRun { result_kind, .. } => *result_kind,
        }
    }

    fn figure(&self) -> String {
        match &self.data {
            ComparisonData::Memory(data) => data.metric.to_string(),
            ComparisonData::NotRun {
                metric: Some(metric),
                ..
            } => metric.to_string(),
            _ => self.result_kind().to_string(),
        }
    }

    fn before_median(&self) -> Option<u64> {
        match &self.data {
            ComparisonData::Sampled(data) => Some(data.before_median),
            ComparisonData::Series(data) => Some(data.before_median),
            ComparisonData::Memory(data) => Some(data.before_bytes),
            ComparisonData::NotRun { before_value, .. } => *before_value,
            ComparisonData::Added { .. } => None,
            ComparisonData::Removed { median, .. } => *median,
        }
    }

    fn after_median(&self) -> Option<u64> {
        match &self.data {
            ComparisonData::Sampled(data) => Some(data.after_median),
            ComparisonData::Series(data) => Some(data.after_median),
            ComparisonData::Memory(data) => Some(data.after_bytes),
            ComparisonData::NotRun { after_value, .. } => *after_value,
            ComparisonData::Added { median, .. } => *median,
            ComparisonData::Removed { .. } => None,
        }
    }

    pub fn change_percent(&self) -> Option<f64> {
        match &self.data {
            ComparisonData::Sampled(data) => Some(data.change_percent),
            ComparisonData::Series(data) => Some(data.total_change_percent),
            ComparisonData::Memory(data) => Some(data.change_percent),
            ComparisonData::Added { .. }
            | ComparisonData::Removed { .. }
            | ComparisonData::NotRun { .. } => None,
        }
    }

    fn series_last_window_change_percent(&self) -> Option<f64> {
        match &self.data {
            ComparisonData::Series(data) => data.last_window_change_percent,
            _ => None,
        }
    }

    fn graph(&self) -> Option<&str> {
        match &self.data {
            ComparisonData::Series(data) => data.graph.as_deref(),
            _ => None,
        }
    }
}

fn by_name(results: &[BenchResult]) -> anyhow::Result<BTreeMap<&str, &BenchResult>> {
    let mut by_name = BTreeMap::new();
    for result in results {
        if by_name.insert(result.name(), result).is_some() {
            anyhow::bail!("duplicate benchmark name {}", result.name());
        }
    }
    Ok(by_name)
}

fn result_median(result: &BenchResult) -> Option<u64> {
    match result {
        BenchResult::Sampled(result) => Some(result.median),
        BenchResult::Series(result) => Some(result.summary.median),
        BenchResult::Memory(result) => Some(result.peak_bytes),
        BenchResult::NotRun(_) => None,
    }
}

fn percent_change(before: u64, after: u64) -> Option<f64> {
    if before == 0 {
        return None;
    }
    Some(((after as f64 - before as f64) / before as f64) * 100.0)
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct ConfidenceInterval {
    pub low: f64,
    pub high: f64,
}

fn bootstrap_median_change_ci<R: rand::RngExt>(
    rng: &mut R,
    before: &[u64],
    after: &[u64],
    alpha: f64,
    resamples: usize,
) -> anyhow::Result<ConfidenceInterval> {
    anyhow::ensure!(
        before.len() >= 2,
        "before result has {} samples; bootstrap CI requires at least 2",
        before.len()
    );
    anyhow::ensure!(
        after.len() >= 2,
        "after result has {} samples; bootstrap CI requires at least 2",
        after.len()
    );
    anyhow::ensure!(
        !before.iter().all(|value| *value == 0),
        "before samples are all zero"
    );
    let mut changes = Vec::with_capacity(resamples);
    let mut before_sample = vec![0_u64; before.len()];
    let mut after_sample = vec![0_u64; after.len()];
    for _ in 0..resamples {
        for value in &mut before_sample {
            *value = before[rng.random_range(0..before.len())];
        }
        for value in &mut after_sample {
            *value = after[rng.random_range(0..after.len())];
        }
        let before_median = median(&mut before_sample);
        if before_median == 0 {
            continue;
        }
        let after_median = median(&mut after_sample);
        changes.push(((after_median as f64 - before_median as f64) / before_median as f64) * 100.0);
    }
    anyhow::ensure!(
        !changes.is_empty(),
        "all bootstrap resamples had zero before median"
    );
    changes.sort_by(f64::total_cmp);
    Ok(ConfidenceInterval {
        low: percentile(&changes, alpha / 2.0),
        high: percentile(&changes, 1.0 - (alpha / 2.0)),
    })
}

fn stable_seed(name: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in name.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn median(values: &mut [u64]) -> u64 {
    values.sort_unstable();
    if values.len().is_multiple_of(2) {
        let upper = values.len() / 2;
        (values[upper - 1] + values[upper]) / 2
    } else {
        values[values.len() / 2]
    }
}

fn percentile(sorted_values: &[f64], percentile: f64) -> f64 {
    let idx = (percentile.clamp(0.0, 1.0) * (sorted_values.len() - 1) as f64).round() as usize;
    sorted_values[idx]
}

struct ReportSection<'a> {
    title: &'static str,
    entries: Vec<&'a ComparisonEntry>,
}

impl<'a> ReportSection<'a> {
    fn new(
        title: &'static str,
        entries: impl Iterator<Item = &'a ComparisonEntry>,
    ) -> Option<Self> {
        let entries = entries.collect::<Vec<_>>();
        (!entries.is_empty()).then_some(Self { title, entries })
    }
}

fn report_sections(comparison: &Comparison) -> Vec<ReportSection<'_>> {
    let mut sections = Vec::new();
    if let Some(section) = ReportSection::new(
        "Regressions",
        comparison
            .entries
            .iter()
            .filter(|e| matches!(e.status, ComparisonStatus::Slower)),
    ) {
        sections.push(section);
    }
    if let Some(section) = ReportSection::new(
        "Improvements",
        comparison
            .entries
            .iter()
            .filter(|e| matches!(e.status, ComparisonStatus::Faster)),
    ) {
        sections.push(section);
    }
    if let Some(section) = ReportSection::new(
        "Other",
        comparison.entries.iter().filter(|e| {
            !matches!(
                e.status,
                ComparisonStatus::Slower | ComparisonStatus::Faster
            )
        }),
    ) {
        sections.push(section);
    }
    sections
}

pub fn to_text(comparison: &Comparison) -> String {
    let mut out = String::new();
    for section in report_sections(comparison) {
        render_text_section(&mut out, &section);
    }
    if comparison.hidden_as_noise > 0 {
        out.push_str(&format!(
            "\n{} benchmark(s) hidden as noise; use --show-all to include them.\n",
            comparison.hidden_as_noise
        ));
    }
    out
}

fn render_text_section(out: &mut String, section: &ReportSection<'_>) {
    if !out.is_empty() {
        out.push('\n');
    }
    out.push_str(section.title);
    out.push('\n');
    out.push_str("benchmark                                     figure            before       after         change   last window       95% CI\n");
    out.push_str("-------------------------------------------------------------------------------------------------------------------------\n");
    for entry in &section.entries {
        out.push_str(&format!(
            "{:<42} {:>13} {:>10} {:>10} {:>14} {:>13} {:>16}\n",
            entry.name,
            entry.figure(),
            entry
                .before_median()
                .map(|value| format_result_value(entry, value))
                .unwrap_or_else(|| "-".to_string()),
            entry
                .after_median()
                .map(|value| format_result_value(entry, value))
                .unwrap_or_else(|| "-".to_string()),
            format_change(entry),
            format_last_window_change(entry),
            format_ci(entry)
        ));
    }
}

pub fn to_markdown(comparison: &Comparison) -> String {
    let confidence = (1.0 - comparison.alpha) * 100.0;
    let mut out = String::new();
    for section in report_sections(comparison) {
        render_markdown_section(&mut out, &section, confidence);
    }
    if comparison.hidden_as_noise > 0 {
        out.push_str(&format!(
            "\n_{} benchmark(s) hidden as noise; use `--show-all` to include them._\n",
            comparison.hidden_as_noise
        ));
    }
    out
}

fn render_markdown_section(out: &mut String, section: &ReportSection<'_>, confidence: f64) {
    if !out.is_empty() {
        out.push('\n');
    }
    out.push_str(&format!("## {}\n\n", section.title));
    out.push_str(&format!(
        "| benchmark | figure | before | after | change | last window | {:.0}% bootstrap CI | graph |\n",
        confidence
    ));
    out.push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |\n");
    for entry in &section.entries {
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} |\n",
            entry.name,
            entry.figure(),
            entry
                .before_median()
                .map(|value| format_result_value(entry, value))
                .unwrap_or_else(|| "-".to_string()),
            entry
                .after_median()
                .map(|value| format_result_value(entry, value))
                .unwrap_or_else(|| "-".to_string()),
            format_change(entry),
            format_last_window_change(entry),
            format_ci(entry),
            format_graph_link(entry)
        ));
    }
}

fn format_change(entry: &ComparisonEntry) -> String {
    match entry.change_percent() {
        Some(change) => match entry.status {
            ComparisonStatus::Noise => format!("{change:+.1}% (noise)"),
            _ => format!("{change:+.1}%"),
        },
        None => match entry.status {
            ComparisonStatus::Added => "added".to_string(),
            ComparisonStatus::Removed => "removed".to_string(),
            ComparisonStatus::NotRun => match &entry.data {
                ComparisonData::NotRun {
                    before_run: false,
                    after_run: true,
                    ..
                } => "not run before".to_string(),
                ComparisonData::NotRun {
                    before_run: true,
                    after_run: false,
                    ..
                } => "not run after".to_string(),
                _ => "not run".to_string(),
            },
            _ => "-".to_string(),
        },
    }
}

fn format_last_window_change(entry: &ComparisonEntry) -> String {
    entry
        .series_last_window_change_percent()
        .map(|change| format!("{change:+.1}%"))
        .unwrap_or_else(|| "-".to_string())
}

fn format_graph_link(entry: &ComparisonEntry) -> String {
    entry
        .graph()
        .map(|graph| format!("![series graph]({graph})"))
        .unwrap_or_else(|| "-".to_string())
}

fn format_ci(entry: &ComparisonEntry) -> String {
    match &entry.data {
        ComparisonData::Sampled(SampledComparison {
            bootstrap_ci_percent: ci,
            ..
        }) => format!("[{:+.1}%, {:+.1}%]", ci.low, ci.high),
        _ => "-".to_string(),
    }
}

fn format_result_value(entry: &ComparisonEntry, value: u64) -> String {
    match entry.result_kind() {
        BenchmarkKind::Memory => format_bytes(value),
        BenchmarkKind::Sampled | BenchmarkKind::Series => format_duration(value),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        bootstrap_median_change_ci, memory_entries, paired_entries, percent_change, to_markdown,
        CompareOptions, Comparison, ComparisonData, ComparisonStatus, MemoryMetric,
    };
    use crate::runner::{BenchResult, MemoryResult, NotRunResult};
    use crate::scenarios::BenchmarkKind;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn computes_percent_change() {
        assert_eq!(percent_change(100, 110), Some(10.0));
        assert_eq!(percent_change(100, 90), Some(-10.0));
        assert_eq!(percent_change(0, 90), None);
    }

    #[test]
    fn bootstrap_ci_detects_separated_distributions() {
        let ci = bootstrap_median_change_ci(
            &mut StdRng::seed_from_u64(1),
            &[100, 101, 102, 103, 104],
            &[150, 151, 152, 153, 154],
            0.05,
            1_000,
        )
        .unwrap();
        assert!(ci.low > 0.0, "ci={ci:?}");
    }

    #[test]
    fn compares_peak_and_steady_memory() {
        let before = MemoryResult {
            name: "memory/test".to_string(),
            group: "memory".to_string(),
            unit: "bytes".to_string(),
            setup_duration_ns: 0,
            peak_bytes: 1_024,
            steady_bytes: 1_000,
        };
        let after = MemoryResult {
            peak_bytes: 1_126,
            steady_bytes: 900,
            ..before.clone()
        };
        let entries = memory_entries("memory/test", &before, &after);
        assert_eq!(entries.len(), 2);

        let ComparisonData::Memory(peak) = &entries[0].data else {
            panic!("expected peak memory comparison");
        };
        assert_eq!(peak.metric, MemoryMetric::Peak);
        assert!(matches!(entries[0].status, ComparisonStatus::Slower));
        assert!((peak.change_percent - 9.9609375).abs() < f64::EPSILON);

        let ComparisonData::Memory(steady) = &entries[1].data else {
            panic!("expected steady memory comparison");
        };
        assert_eq!(steady.metric, MemoryMetric::SteadyState);
        assert!(matches!(entries[1].status, ComparisonStatus::Faster));
        assert_eq!(steady.change_percent, -10.0);

        let markdown = to_markdown(&Comparison {
            before_commit: None,
            after_commit: None,
            min_change_percent: 5.0,
            alpha: 0.05,
            bootstrap_resamples: 1_000,
            entries,
            hidden_as_noise: 0,
        });
        assert!(markdown.contains("1.00 KiB"));
        assert!(markdown.contains("peak memory"));
        assert!(markdown.contains("steady memory"));
        assert!(markdown.contains("-10.0%"));
    }

    #[test]
    fn reports_present_but_unmeasured_benchmark_as_not_run() {
        let before = BenchResult::NotRun(NotRunResult {
            name: "memory/test".to_string(),
            group: "memory".to_string(),
            unit: "bytes".to_string(),
            benchmark_kind: BenchmarkKind::Memory,
        });
        let after = BenchResult::Memory(MemoryResult {
            name: "memory/test".to_string(),
            group: "memory".to_string(),
            unit: "bytes".to_string(),
            setup_duration_ns: 0,
            peak_bytes: 1_024,
            steady_bytes: 512,
        });
        let entries = paired_entries(
            "memory/test",
            &before,
            &after,
            CompareOptions {
                min_change_percent: 5.0,
                alpha: 0.05,
                bootstrap_resamples: 10,
                show_all: false,
            },
        )
        .unwrap();

        assert_eq!(entries.len(), 2);
        assert!(entries
            .iter()
            .all(|entry| matches!(entry.status, ComparisonStatus::NotRun)));
        let ComparisonData::NotRun {
            metric,
            before_run,
            after_run,
            before_value,
            after_value,
            ..
        } = &entries[0].data
        else {
            panic!("expected not-run comparison");
        };
        assert_eq!(*metric, Some(MemoryMetric::Peak));
        assert!(!*before_run);
        assert!(*after_run);
        assert_eq!(*before_value, None);
        assert_eq!(*after_value, Some(1_024));
    }

    #[test]
    fn bootstrap_ci_overlaps_zero_for_similar_distributions() {
        let ci = bootstrap_median_change_ci(
            &mut StdRng::seed_from_u64(1),
            &[100, 101, 99, 100, 101],
            &[100, 101, 99, 100, 101],
            0.05,
            1_000,
        )
        .unwrap();
        assert!(ci.low <= 0.0 && ci.high >= 0.0, "ci={ci:?}");
    }
}
