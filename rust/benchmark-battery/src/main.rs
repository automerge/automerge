use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

mod compare;
mod egwalker_data;
mod format;
mod graph;
#[cfg(feature = "memory")]
mod memory;
mod report;
mod runner;
mod scenarios;

#[derive(Debug, Parser)]
#[command(author, version, about = "Automerge Rust benchmark battery")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// List registered benchmarks.
    List {
        /// Only list benchmarks whose group or name contains this string.
        #[arg(long)]
        filter: Option<String>,
        /// Benchmark tier to list.
        #[arg(long, default_value_t = scenarios::TierFilter::All)]
        tier: scenarios::TierFilter,
    },
    /// Run benchmarks and write a stable JSON result file.
    Run {
        /// Output JSON path.
        #[arg(short, long)]
        output: PathBuf,
        /// Only run benchmarks whose group or name contains this string.
        #[arg(long)]
        filter: Option<String>,
        /// Benchmark tier to run. The default fast tier is suitable for frequent CI.
        #[arg(long, default_value_t = scenarios::TierFilter::Fast)]
        tier: scenarios::TierFilter,
        /// Number of measured samples per benchmark.
        #[arg(long, default_value_t = 30)]
        sample_count: usize,
        /// Number of warmup iterations per benchmark.
        #[arg(long, default_value_t = 3)]
        warmup_count: usize,
        /// Iterations inside each measured sample.
        #[arg(long, default_value_t = 1)]
        iters_per_sample: u64,
        /// Directory for run artifacts. Defaults to a fresh directory under the system temp directory.
        #[arg(long)]
        report_dir: Option<PathBuf>,
        /// Report output format.
        #[arg(long, default_value_t = report::Format::Html)]
        report_format: report::Format,
        /// Fail if the filter matches no benchmarks.
        #[arg(long, default_value_t = true)]
        fail_on_empty: bool,
        /// Add measured results to the existing output file, preserving earlier results.
        #[arg(long)]
        append: bool,
    },
    /// Generate Automerge documents from Eg-walker JSON traces.
    GenerateEgwalkerData {
        /// JSON trace files to convert. Each output is written alongside its trace.
        #[arg(required = true)]
        traces: Vec<PathBuf>,
    },
    /// Compare two JSON result files.
    Compare {
        /// Before result JSON.
        before: PathBuf,
        /// After result JSON.
        after: PathBuf,
        /// Output format.
        #[arg(long, default_value_t = CompareFormat::Text)]
        format: CompareFormat,
        /// Only report changed benchmarks whose median changed by at least this percentage.
        #[arg(long, default_value_t = 5.0)]
        min_change_percent: f64,
        /// Bootstrap confidence interval alpha. The default 0.05 means a 95% CI.
        #[arg(long, default_value_t = 0.05)]
        alpha: f64,
        /// Number of bootstrap resamples used by comparison.
        #[arg(long, default_value_t = 10_000)]
        bootstrap_resamples: usize,
        /// Show all benchmarks, including changes that are likely noise.
        #[arg(long)]
        show_all: bool,
        /// Directory for comparison artifacts such as series graphs.
        #[arg(long)]
        report_dir: Option<PathBuf>,
        /// Exit non-zero if any reported regression exceeds this percentage.
        #[arg(long)]
        fail_threshold: Option<f64>,
    },
}

#[derive(Clone, Debug, ValueEnum)]
enum CompareFormat {
    Text,
    Markdown,
    Json,
}

impl std::fmt::Display for CompareFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Text => f.write_str("text"),
            Self::Markdown => f.write_str("markdown"),
            Self::Json => f.write_str("json"),
        }
    }
}

fn file_url(path: &std::path::Path) -> String {
    let path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    format!("file://{}", path.display())
}

fn default_report_dir() -> PathBuf {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    std::env::temp_dir().join(format!("benchmark-battery-report-{millis}"))
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let benchmarks = scenarios::benchmarks();

    match cli.command {
        Command::List { filter, tier } => {
            for benchmark in scenarios::filter(&benchmarks, filter.as_deref(), tier) {
                println!("{}", benchmark.name());
            }
        }
        Command::Run {
            output,
            filter,
            tier,
            sample_count,
            warmup_count,
            iters_per_sample,
            report_dir,
            report_format,
            fail_on_empty,
            append,
        } => {
            let selected = scenarios::filter(&benchmarks, filter.as_deref(), tier);
            if selected.is_empty() && fail_on_empty {
                anyhow::bail!("no benchmarks matched filter {:?}", filter);
            }

            let existing = if append {
                Some(runner::read_run(&output)?)
            } else {
                None
            };
            let options = runner::RunOptions {
                sample_count,
                warmup_count,
                iters_per_sample,
            };
            let additional = runner::run(&benchmarks, &selected, options)?;
            let run = if let Some(existing) = existing {
                runner::append(existing, additional)?
            } else {
                additional
            };
            runner::write_run(&output, &run)?;
            let report_dir = report_dir.unwrap_or_else(default_report_dir);
            let report = report::write_run(&report_dir, &run, report_format)?;
            eprintln!(
                "wrote run report and {} series graph(s) to {}",
                report.graph_count,
                report_dir.display()
            );
            if let Some(html) = report.html_path {
                eprintln!("open report: {}", file_url(&html));
            } else if let Some(markdown) = report.markdown_path {
                eprintln!("wrote markdown report: {}", markdown.display());
            }
            eprintln!(
                "wrote {} measured benchmark result(s) to {}",
                run.results.iter().filter(|result| result.was_run()).count(),
                output.display()
            );
        }
        Command::GenerateEgwalkerData { traces } => {
            egwalker_data::generate(&traces)?;
        }
        Command::Compare {
            before,
            after,
            format,
            min_change_percent,
            alpha,
            bootstrap_resamples,
            show_all,
            report_dir,
            fail_threshold,
        } => {
            let before = runner::read_run(&before)?;
            let after = runner::read_run(&after)?;
            let options = compare::CompareOptions {
                min_change_percent,
                alpha,
                bootstrap_resamples,
                show_all,
            };
            let comparison = compare::compare(&before, &after, options)?;
            if let Some(report_dir) = &report_dir {
                let written = compare::write_series_graphs(&before, &after, report_dir)?;
                eprintln!(
                    "wrote {written} series graph(s) to {}",
                    report_dir.display()
                );
            }
            match format {
                CompareFormat::Text => print!("{}", compare::to_text(&comparison)),
                CompareFormat::Markdown => print!("{}", compare::to_markdown(&comparison)),
                CompareFormat::Json => println!("{}", serde_json::to_string_pretty(&comparison)?),
            }
            if let Some(threshold) = fail_threshold {
                if comparison.entries.iter().any(|entry| {
                    entry
                        .change_percent()
                        .is_some_and(|change| change > threshold)
                }) {
                    anyhow::bail!("one or more benchmarks regressed by more than {threshold:.1}%");
                }
            }
        }
    }

    Ok(())
}
