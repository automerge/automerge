use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::trace::{Trace, VmInstr, VmOp};
use crate::trace_io::load_trace;
use crate::Runner;

pub fn show_crashes(
    corpus_dir: &Path,
    crash: Option<&str>,
    json_only: bool,
    full_trace: bool,
    replay: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let crashes_dir = corpus_dir.join("crashes");
    let files = crash_files(&crashes_dir)?;

    let Some(selector) = crash else {
        if json_only || full_trace || replay {
            eprintln!("--json, --full-trace, and --replay apply to a selected crash only");
        }
        print_crash_list(&crashes_dir, &files)?;
        return Ok(());
    };

    let path = resolve_crash(&crashes_dir, &files, selector)?;
    let trace = load_trace(&path)?;
    if json_only {
        println!("{}", serde_json::to_string_pretty(&trace)?);
        return Ok(());
    }

    print_crash_detail(&path, &trace);

    if full_trace {
        println!("\ntrace:");
        println!("{}", serde_json::to_string_pretty(&trace)?);
    }

    if replay {
        println!("\nreplay:");
        match Runner::new().run_catching(&trace) {
            Ok(report) => println!(
                "  unexpectedly succeeded: steps={} ops={} docs={}",
                report.steps, report.ops, report.docs
            ),
            Err(err) => println!("  {err}"),
        }
    }

    Ok(())
}

fn print_crash_list(
    crashes_dir: &Path,
    files: &[PathBuf],
) -> Result<(), Box<dyn std::error::Error>> {
    println!("crashes in {}", crashes_dir.display());
    if files.is_empty() {
        println!("no crashes found");
        return Ok(());
    }

    println!(
        "{:<8} {:<12} {:>6} {:>5}  reason",
        "crash", "seed", "steps", "ops"
    );
    for path in files {
        let trace = load_trace(path)?;
        println!(
            "{:<8} {:<12} {:>6} {:>5}  {}",
            crash_label(path),
            trace
                .metadata
                .seed
                .map(|seed| seed.to_string())
                .unwrap_or_else(|| "-".to_string()),
            trace.steps.len(),
            trace_op_count(&trace),
            truncate(&reason_summary(&trace), 100),
        );
    }
    println!("\nshow one with: trace_fuzz crashes <crash>");
    println!("examples: trace_fuzz crashes 0, trace_fuzz crashes latest --full-trace");
    Ok(())
}

fn print_crash_detail(path: &Path, trace: &Trace) {
    println!("path: {}", path.display());
    println!("crash: {}", crash_label(path));
    println!(
        "seed: {}",
        trace
            .metadata
            .seed
            .map(|seed| seed.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    if let Some(parent) = &trace.metadata.parent {
        println!("parent: {parent}");
    }
    println!("actors: {}", trace.actors.len());
    println!("steps: {}", trace.steps.len());
    println!("ops: {}", trace_op_count(trace));

    println!("\nstep kinds:");
    for (kind, count) in step_counts(trace) {
        println!("  {kind:<18} {count}");
    }

    let ops = op_counts(trace);
    if !ops.is_empty() {
        println!("\nop kinds:");
        for (kind, count) in ops {
            println!("  {kind:<18} {count}");
        }
    }

    if let Some(reason) = &trace.metadata.reason {
        println!("\nreason:");
        for line in reason.lines() {
            println!("  {line}");
        }
    }

    println!(
        "\njson:       trace_fuzz crashes {} --json",
        crash_label(path)
    );
    println!(
        "full trace: trace_fuzz crashes {} --full-trace",
        crash_label(path)
    );
    println!(
        "replay:     trace_fuzz crashes {} --replay",
        crash_label(path)
    );
}

fn crash_files(crashes_dir: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    if !crashes_dir.exists() {
        return Ok(Vec::new());
    }

    let mut files = Vec::new();
    for entry in fs::read_dir(crashes_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("amtrace") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn resolve_crash(
    crashes_dir: &Path,
    files: &[PathBuf],
    selector: &str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if matches!(selector, "latest" | "last") {
        return files.last().cloned().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no crashes found in {}", crashes_dir.display()),
            )
            .into()
        });
    }

    let path = PathBuf::from(selector);
    if path.exists() {
        return Ok(path);
    }

    let path = crashes_dir.join(selector);
    if path.exists() {
        return Ok(path);
    }

    if !selector.ends_with(".amtrace") {
        let path = crashes_dir.join(format!("{selector}.amtrace"));
        if path.exists() {
            return Ok(path);
        }
    }

    if let Ok(number) = selector.parse::<usize>() {
        let path = crashes_dir.join(format!("crash-{number:08}.amtrace"));
        if path.exists() {
            return Ok(path);
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!(
            "could not find crash {selector:?} in {}",
            crashes_dir.display()
        ),
    )
    .into())
}

fn crash_label(path: &Path) -> String {
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("-");
    if let Some(number) = stem.strip_prefix("crash-") {
        if let Ok(number) = number.parse::<usize>() {
            return number.to_string();
        }
    }
    stem.to_string()
}

fn reason_summary(trace: &Trace) -> String {
    let reason = trace.metadata.reason.as_deref().unwrap_or("-");
    let reason = reason.strip_prefix("failure: ").unwrap_or(reason);
    reason.lines().next().unwrap_or("-").trim().to_string()
}

fn truncate(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let mut truncated = String::new();
    for _ in 0..max_chars {
        let Some(ch) = chars.next() else {
            return value.to_string();
        };
        truncated.push(ch);
    }
    if chars.next().is_some() {
        truncated.push('…');
    }
    truncated
}

fn trace_op_count(trace: &Trace) -> usize {
    trace
        .steps
        .iter()
        .map(|step| match step {
            VmInstr::Change { ops, .. } | VmInstr::Transact { ops, .. } => ops.len(),
            _ => 0,
        })
        .sum()
}

fn step_counts(trace: &Trace) -> BTreeMap<&'static str, usize> {
    let mut counts = BTreeMap::new();
    for step in &trace.steps {
        *counts.entry(step_kind(step)).or_default() += 1;
    }
    counts
}

fn op_counts(trace: &Trace) -> BTreeMap<&'static str, usize> {
    let mut counts = BTreeMap::new();
    for step in &trace.steps {
        if let VmInstr::Change { ops, .. } | VmInstr::Transact { ops, .. } = step {
            for op in ops {
                *counts.entry(op_kind(op)).or_default() += 1;
            }
        }
    }
    counts
}

fn step_kind(step: &VmInstr) -> &'static str {
    match step {
        VmInstr::Fork { .. } => "fork",
        VmInstr::ForkAt { .. } => "fork_at",
        VmInstr::ApplyChanges { .. } => "apply_changes",
        VmInstr::Change { .. } => "change",
        VmInstr::Transact { commit: true, .. } => "transact_commit",
        VmInstr::Transact { commit: false, .. } => "transact_rollback",
        VmInstr::Merge { .. } => "merge",
        VmInstr::SaveLoad { .. } => "save_load",
        VmInstr::Sync { .. } => "sync",
        VmInstr::SyncSession { .. } => "sync_session",
        VmInstr::Observe { .. } => "observe",
        VmInstr::SaveHeads { .. } => "save_heads",
        VmInstr::DiffRange { .. } => "diff_range",
        VmInstr::UpdateDiffCursor { .. } => "update_diff_cursor",
        VmInstr::ResetDiffCursor { .. } => "reset_diff_cursor",
        VmInstr::DiffIncremental { .. } => "diff_incremental",
    }
}

fn op_kind(op: &VmOp) -> &'static str {
    match op {
        VmOp::Put { .. } => "put",
        VmOp::PutSeq { .. } => "put_seq",
        VmOp::MakeMap { .. } => "make_map",
        VmOp::MakeList { .. } => "make_list",
        VmOp::MakeText { .. } => "make_text",
        VmOp::Insert { .. } => "insert",
        VmOp::SpliceList { .. } => "splice_list",
        VmOp::SpliceText { .. } => "splice_text",
        VmOp::UpdateText { .. } => "update_text",
        VmOp::Increment { .. } => "increment",
        VmOp::Mark { .. } => "mark",
        VmOp::Unmark { .. } => "unmark",
        VmOp::Delete { .. } => "delete",
        VmOp::DeleteSeq { .. } => "delete_seq",
        VmOp::UpdateObject { .. } => "update_object",
        VmOp::BatchCreate { .. } => "batch_create",
    }
}
