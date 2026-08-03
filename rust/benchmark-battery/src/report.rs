use crate::format::{format_bytes, format_duration};
use crate::graph::{self, SeriesLine};
use crate::runner::{BenchResult, BenchRun};
use anyhow::Context;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum Format {
    Markdown,
    Html,
    Both,
}

impl std::fmt::Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Markdown => f.write_str("markdown"),
            Self::Html => f.write_str("html"),
            Self::Both => f.write_str("both"),
        }
    }
}

#[derive(Debug)]
pub struct Report {
    pub graph_count: usize,
    pub markdown_path: Option<PathBuf>,
    pub html_path: Option<PathBuf>,
}

pub fn write_run(report_dir: &Path, run: &BenchRun, format: Format) -> anyhow::Result<Report> {
    std::fs::create_dir_all(report_dir)
        .with_context(|| format!("creating report directory {}", report_dir.display()))?;

    let graph_count = write_series_graphs(report_dir, run)?;
    let markdown_path = write_markdown(report_dir, run, format)?;
    let html_path = write_html(report_dir, run, format)?;

    Ok(Report {
        graph_count,
        markdown_path,
        html_path,
    })
}

fn write_series_graphs(report_dir: &Path, run: &BenchRun) -> anyhow::Result<usize> {
    let mut graph_count = 0;
    for result in &run.results {
        let BenchResult::Series(result) = result else {
            continue;
        };
        graph::write_series_graph(
            report_dir,
            &result.name,
            &[SeriesLine {
                label: "run",
                color: "#4e79a7",
                points: &result.points,
            }],
        )?;
        graph_count += 1;
    }
    Ok(graph_count)
}

fn write_markdown(
    report_dir: &Path,
    run: &BenchRun,
    format: Format,
) -> anyhow::Result<Option<PathBuf>> {
    match format {
        Format::Markdown | Format::Both => {
            let path = report_dir.join("report.md");
            std::fs::write(&path, to_markdown(run))
                .with_context(|| format!("writing run report {}", path.display()))?;
            Ok(Some(path))
        }
        Format::Html => Ok(None),
    }
}

fn write_html(
    report_dir: &Path,
    run: &BenchRun,
    format: Format,
) -> anyhow::Result<Option<PathBuf>> {
    match format {
        Format::Html | Format::Both => {
            let path = report_dir.join("report.html");
            std::fs::write(&path, to_html(run))
                .with_context(|| format!("writing run report {}", path.display()))?;
            Ok(Some(path))
        }
        Format::Markdown => Ok(None),
    }
}

pub fn to_markdown(run: &BenchRun) -> String {
    let mut out = String::new();
    out.push_str("# Automerge battery run\n\n");
    write_sampled_markdown(&mut out, run);
    write_memory_markdown(&mut out, run);
    write_series_markdown(&mut out, run);
    out
}

pub fn to_html(run: &BenchRun) -> String {
    let mut out = String::new();
    out.push_str("<!doctype html>\n<html>\n<head>\n<meta charset=\"utf-8\">\n");
    out.push_str("<title>Automerge battery run</title>\n");
    out.push_str("<style>\n");
    out.push_str(
        "body{font-family:system-ui,sans-serif;margin:2rem;line-height:1.4}\
         table{border-collapse:collapse;margin:1rem 0 2rem}\
         th,td{border:1px solid #ddd;padding:.35rem .55rem;text-align:right}\
         th:first-child,td:first-child{text-align:left}\
         th{background:#f6f6f6}\
         h2{margin-top:2rem;border-bottom:1px solid #ddd;padding-bottom:.25rem}\
         h3{margin-top:2rem}\
         img{max-width:100%;height:auto;border:1px solid #ddd}\
         code{background:#f6f6f6;padding:.1rem .25rem;border-radius:.2rem}\n",
    );
    out.push_str("</style>\n</head>\n<body>\n");
    out.push_str("<h1>Automerge battery run</h1>\n");
    write_sampled_html(&mut out, run);
    write_memory_html(&mut out, run);
    write_series_html(&mut out, run);
    out.push_str("</body>\n</html>\n");
    out
}

fn write_sampled_markdown(out: &mut String, run: &BenchRun) {
    let sampled = sampled_results(run);
    if sampled.is_empty() {
        return;
    }

    out.push_str("## Sampled benchmarks\n\n");
    out.push_str("| benchmark | median | mean | min | max |\n");
    out.push_str("| --- | ---: | ---: | ---: | ---: |\n");
    for result in sampled {
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {} |\n",
            result.name,
            format_duration(result.median),
            format_duration(result.mean),
            format_duration(result.min),
            format_duration(result.max),
        ));
    }
    out.push('\n');
}

fn write_memory_markdown(out: &mut String, run: &BenchRun) {
    let memory = memory_results(run);
    if memory.is_empty() {
        return;
    }

    out.push_str("## Peak memory usage\n\n");
    out.push_str("| benchmark | peak heap |\n");
    out.push_str("| --- | ---: |\n");
    for result in &memory {
        out.push_str(&format!(
            "| `{}` | {} |\n",
            result.name,
            format_bytes(result.peak_bytes),
        ));
    }

    out.push_str("\n## Steady-state memory usage\n\n");
    out.push_str("| benchmark | steady heap |\n");
    out.push_str("| --- | ---: |\n");
    for result in memory {
        out.push_str(&format!(
            "| `{}` | {} |\n",
            result.name,
            format_bytes(result.steady_bytes),
        ));
    }
    out.push('\n');
}

fn write_series_markdown(out: &mut String, run: &BenchRun) {
    let series = series_results(run);
    if series.is_empty() {
        return;
    }

    out.push_str("## Series benchmarks\n\n");
    for result in series {
        out.push_str(&format!("### `{}`\n\n", result.name));
        out.push_str("| total | median | mean | last-window median |\n");
        out.push_str("| ---: | ---: | ---: | ---: |\n");
        out.push_str(&format!(
            "| {} | {} | {} | {} |\n\n",
            format_duration(result.summary.total),
            format_duration(result.summary.median),
            format_duration(result.summary.mean),
            format_duration(result.summary.last_window_median),
        ));
        out.push_str(&format!(
            "![{}]({})\n\n",
            result.name,
            graph_path(&result.name)
        ));
    }
}

fn write_sampled_html(out: &mut String, run: &BenchRun) {
    let sampled = sampled_results(run);
    if sampled.is_empty() {
        return;
    }

    out.push_str("<h2>Sampled benchmarks</h2>\n");
    out.push_str("<table><thead><tr><th>benchmark</th><th>median</th><th>mean</th><th>min</th><th>max</th></tr></thead><tbody>\n");
    for result in sampled {
        out.push_str(&format!(
            "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>\n",
            escape_html(&result.name),
            format_duration(result.median),
            format_duration(result.mean),
            format_duration(result.min),
            format_duration(result.max),
        ));
    }
    out.push_str("</tbody></table>\n");
}

fn write_memory_html(out: &mut String, run: &BenchRun) {
    let memory = memory_results(run);
    if memory.is_empty() {
        return;
    }

    out.push_str("<h2>Peak memory usage</h2>\n");
    out.push_str("<table><thead><tr><th>benchmark</th><th>peak heap</th></tr></thead><tbody>\n");
    for result in &memory {
        out.push_str(&format!(
            "<tr><td><code>{}</code></td><td>{}</td></tr>\n",
            escape_html(&result.name),
            format_bytes(result.peak_bytes),
        ));
    }
    out.push_str("</tbody></table>\n");

    out.push_str("<h2>Steady-state memory usage</h2>\n");
    out.push_str("<table><thead><tr><th>benchmark</th><th>steady heap</th></tr></thead><tbody>\n");
    for result in memory {
        out.push_str(&format!(
            "<tr><td><code>{}</code></td><td>{}</td></tr>\n",
            escape_html(&result.name),
            format_bytes(result.steady_bytes),
        ));
    }
    out.push_str("</tbody></table>\n");
}

fn write_series_html(out: &mut String, run: &BenchRun) {
    let series = series_results(run);
    if series.is_empty() {
        return;
    }

    out.push_str("<h2>Series benchmarks</h2>\n");
    for result in series {
        out.push_str(&format!(
            "<h3><code>{}</code></h3>\n",
            escape_html(&result.name)
        ));
        out.push_str("<table><thead><tr><th>total</th><th>median</th><th>mean</th><th>last-window median</th></tr></thead><tbody>\n");
        out.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>\n",
            format_duration(result.summary.total),
            format_duration(result.summary.median),
            format_duration(result.summary.mean),
            format_duration(result.summary.last_window_median),
        ));
        out.push_str("</tbody></table>\n");
        out.push_str(&format!(
            "<img src=\"{}\" alt=\"{}\">\n",
            graph_path(&result.name),
            escape_html(&result.name)
        ));
    }
}

fn sampled_results(run: &BenchRun) -> Vec<&crate::runner::SampledResult> {
    run.results
        .iter()
        .filter_map(|result| match result {
            BenchResult::Sampled(result) => Some(result),
            BenchResult::Series(_) | BenchResult::Memory(_) | BenchResult::NotRun(_) => None,
        })
        .collect()
}

fn memory_results(run: &BenchRun) -> Vec<&crate::runner::MemoryResult> {
    run.results
        .iter()
        .filter_map(|result| match result {
            BenchResult::Memory(result) => Some(result),
            BenchResult::Sampled(_) | BenchResult::Series(_) | BenchResult::NotRun(_) => None,
        })
        .collect()
}

fn series_results(run: &BenchRun) -> Vec<&crate::runner::SeriesResult> {
    run.results
        .iter()
        .filter_map(|result| match result {
            BenchResult::Sampled(_) | BenchResult::Memory(_) | BenchResult::NotRun(_) => None,
            BenchResult::Series(result) => Some(result),
        })
        .collect()
}

fn graph_path(name: &str) -> String {
    graph::series_graph_png_path(name).display().to_string()
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use super::to_markdown;
    use crate::runner::{BenchResult, BenchRun, MemoryResult, SCHEMA_VERSION, SUITE_NAME};

    #[test]
    fn memory_figures_have_separate_run_report_tables() {
        let report = to_markdown(&BenchRun {
            schema: SCHEMA_VERSION,
            suite: SUITE_NAME.to_string(),
            commit: None,
            timestamp_unix_seconds: 0,
            rustc: None,
            target: None,
            profile: "release".to_string(),
            sample_count: 1,
            results: vec![BenchResult::Memory(MemoryResult {
                name: "memory/test".to_string(),
                group: "memory".to_string(),
                unit: "bytes".to_string(),
                setup_duration_ns: 0,
                peak_bytes: 2_048,
                steady_bytes: 1_024,
            })],
        });

        assert!(report.contains("## Peak memory usage"));
        assert!(report.contains("## Steady-state memory usage"));
        assert!(report.contains("2.00 KiB"));
        assert!(report.contains("1.00 KiB"));
    }
}
