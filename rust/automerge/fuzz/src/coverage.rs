use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct CoverageSummary {
    pub lines_covered: u64,
    pub lines_total: u64,
    pub regions_covered: u64,
    pub regions_total: u64,
    pub functions_covered: u64,
    pub functions_total: u64,
    pub branches_covered: Option<u64>,
    pub branches_total: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct CoverageUpdate {
    pub summary: CoverageSummary,
    pub previous: Option<CoverageSummary>,
}

impl CoverageUpdate {
    pub fn increased(&self) -> bool {
        let Some(previous) = &self.previous else {
            return !self.summary.is_empty();
        };
        self.summary.lines_covered > previous.lines_covered
            || self.summary.regions_covered > previous.regions_covered
            || self.summary.functions_covered > previous.functions_covered
    }

    pub fn status_line(&self) -> String {
        self.summary.status_line(self.previous.as_ref())
    }
}

impl CoverageSummary {
    pub fn status_line(&self, previous: Option<&Self>) -> String {
        let line_delta = previous
            .map(|previous| self.lines_covered.saturating_sub(previous.lines_covered))
            .unwrap_or(0);
        let region_delta = previous
            .map(|previous| {
                self.regions_covered
                    .saturating_sub(previous.regions_covered)
            })
            .unwrap_or(0);

        format!(
            "lines={}/{} {:.2}% (+{}) regions={}/{} {:.2}% (+{}) funcs={}/{} {:.2}%",
            self.lines_covered,
            self.lines_total,
            percent(self.lines_covered, self.lines_total),
            line_delta,
            self.regions_covered,
            self.regions_total,
            percent(self.regions_covered, self.regions_total),
            region_delta,
            self.functions_covered,
            self.functions_total,
            percent(self.functions_covered, self.functions_total),
        )
    }
}

pub struct CoverageReporter {
    coverage_dir: PathBuf,
    profile_dir: PathBuf,
    binary: PathBuf,
    poll_interval: Duration,
    last_poll: Instant,
    last_summary: Option<CoverageSummary>,
    next_profile: u64,
}

impl CoverageReporter {
    pub fn new(coverage_dir: PathBuf, poll_interval: Duration) -> Result<Self, CoverageError> {
        fs::create_dir_all(&coverage_dir).map_err(|source| CoverageError::Io {
            action: "create coverage directory",
            path: coverage_dir.clone(),
            source,
        })?;
        let profile_dir = coverage_dir.join("profiles");
        fs::create_dir_all(&profile_dir).map_err(|source| CoverageError::Io {
            action: "create coverage profile directory",
            path: profile_dir.clone(),
            source,
        })?;
        remove_profraws(&profile_dir)?;
        let binary =
            std::env::current_exe().map_err(|source| CoverageError::CurrentExe { source })?;
        Ok(Self {
            coverage_dir,
            profile_dir,
            binary,
            poll_interval,
            last_poll: Instant::now(),
            last_summary: None,
            next_profile: 0,
        })
    }

    pub fn maybe_poll(&mut self) -> Result<Option<CoverageUpdate>, CoverageError> {
        if self.poll_interval.is_zero() || self.last_poll.elapsed() < self.poll_interval {
            return Ok(None);
        }
        self.last_poll = Instant::now();
        self.flush_profile()?;

        let Some(summary) = self.snapshot("current")? else {
            return Ok(None);
        };
        let previous = self.last_summary.replace(summary.clone());
        Ok(Some(CoverageUpdate { summary, previous }))
    }

    pub fn final_report(&mut self) -> Result<Option<CoverageSummary>, CoverageError> {
        self.flush_profile()?;
        let Some(summary) = self.snapshot("final")? else {
            return Ok(None);
        };

        let summary_path = self.coverage_dir.join("summary.json");
        let report_path = self.coverage_dir.join("report.txt");
        let profdata = self.coverage_dir.join("final.profdata");

        let export = self.llvm_cov_export(&profdata)?;
        fs::write(&summary_path, &export).map_err(|source| CoverageError::Io {
            action: "write coverage summary",
            path: summary_path,
            source,
        })?;

        let report = self.llvm_cov_report(&profdata)?;
        fs::write(&report_path, report).map_err(|source| CoverageError::Io {
            action: "write coverage report",
            path: report_path,
            source,
        })?;

        self.last_summary = Some(summary.clone());
        Ok(Some(summary))
    }

    fn snapshot(&self, name: &str) -> Result<Option<CoverageSummary>, CoverageError> {
        let profraws = collect_profraws(&self.profile_dir)?;
        if profraws.is_empty() {
            return Ok(None);
        }

        let profdata = self.coverage_dir.join(format!("{name}.profdata"));
        if !self.merge_profraws(&profraws, &profdata)? {
            return Ok(None);
        }
        let export = self.llvm_cov_export(&profdata)?;
        let summary = CoverageSummary::from_json(&export)?;
        if summary.is_empty() {
            Ok(None)
        } else {
            Ok(Some(summary))
        }
    }

    fn merge_profraws(&self, profraws: &[PathBuf], output: &Path) -> Result<bool, CoverageError> {
        let output_result = run(merge_command(profraws, output), "llvm-profdata merge")?;
        if output_result.status.success() {
            return Ok(true);
        }

        let initial_stderr = String::from_utf8_lossy(&output_result.stderr).into_owned();
        let (valid, invalid) = self.partition_valid_profraws(profraws)?;

        for profraw in &invalid {
            self.quarantine_profraw(profraw)?;
            eprintln!(
                "coverage: quarantined corrupt profile {}",
                profraw.display()
            );
        }

        if invalid.is_empty() {
            return Err(CoverageError::CommandFailed {
                name: "llvm-profdata merge",
                status: output_result.status.code(),
                stderr: initial_stderr,
            });
        }

        if valid.is_empty() {
            eprintln!("coverage: all current profile files were corrupt; skipping this poll");
            return Ok(false);
        }

        let retry = run(merge_command(&valid, output), "llvm-profdata merge")?;
        if retry.status.success() {
            Ok(true)
        } else {
            let retry_stderr = String::from_utf8_lossy(&retry.stderr).into_owned();
            Err(CoverageError::CommandFailed {
                name: "llvm-profdata merge",
                status: retry.status.code(),
                stderr: format!(
                    "initial merge failed:\n{initial_stderr}\nretry after quarantining corrupt profiles failed:\n{retry_stderr}"
                ),
            })
        }
    }

    fn partition_valid_profraws(
        &self,
        profraws: &[PathBuf],
    ) -> Result<(Vec<PathBuf>, Vec<PathBuf>), CoverageError> {
        let mut valid = Vec::new();
        let mut invalid = Vec::new();
        let check_output = self.coverage_dir.join(".profraw-check.profdata");

        for profraw in profraws {
            let result = run(
                merge_command(std::slice::from_ref(profraw), &check_output),
                "llvm-profdata merge",
            )?;
            if result.status.success() {
                valid.push(profraw.clone());
            } else if profraw.exists() {
                invalid.push(profraw.clone());
            }
        }

        let _ = fs::remove_file(&check_output);
        Ok((valid, invalid))
    }

    fn quarantine_profraw(&self, profraw: &Path) -> Result<(), CoverageError> {
        if !profraw.exists() {
            return Ok(());
        }
        let bad_dir = self.coverage_dir.join("bad-profraw");
        fs::create_dir_all(&bad_dir).map_err(|source| CoverageError::Io {
            action: "create bad profile directory",
            path: bad_dir.clone(),
            source,
        })?;
        let Some(file_name) = profraw.file_name() else {
            return Ok(());
        };
        let destination = bad_dir.join(file_name);
        let _ = fs::remove_file(&destination);
        match fs::rename(profraw, &destination) {
            Ok(()) => Ok(()),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(CoverageError::Io {
                action: "quarantine corrupt coverage profile",
                path: profraw.to_path_buf(),
                source,
            }),
        }
    }

    fn llvm_cov_export(&self, profdata: &Path) -> Result<String, CoverageError> {
        let mut command = Command::new("llvm-cov");
        command
            .arg("export")
            .arg(&self.binary)
            .arg(format!("--instr-profile={}", profdata.display()))
            .arg("--summary-only")
            .arg("--ignore-filename-regex=/.cargo/registry|/rustc/");
        command_output(command, "llvm-cov export")
    }

    fn llvm_cov_report(&self, profdata: &Path) -> Result<String, CoverageError> {
        let mut command = Command::new("llvm-cov");
        command
            .arg("report")
            .arg(&self.binary)
            .arg(format!("--instr-profile={}", profdata.display()))
            .arg("--ignore-filename-regex=/.cargo/registry|/rustc/");
        command_output(command, "llvm-cov report")
    }

    fn flush_profile(&mut self) -> Result<(), CoverageError> {
        let profile = self.profile_dir.join(format!(
            "trace-fuzz-{}-{:08}.profraw",
            std::process::id(),
            self.next_profile
        ));
        self.next_profile += 1;
        flush_coverage_profile(&profile)
    }
}

impl CoverageSummary {
    fn is_empty(&self) -> bool {
        self.lines_covered == 0 && self.regions_covered == 0 && self.functions_covered == 0
    }

    fn from_json(json: &str) -> Result<Self, CoverageError> {
        let value: serde_json::Value = serde_json::from_str(json).map_err(CoverageError::Json)?;
        let totals = value
            .get("data")
            .and_then(|data| data.get(0))
            .and_then(|data| data.get("totals"))
            .ok_or(CoverageError::MissingJsonField("data[0].totals"))?;

        let lines = totals
            .get("lines")
            .ok_or(CoverageError::MissingJsonField("totals.lines"))?;
        let regions = totals
            .get("regions")
            .ok_or(CoverageError::MissingJsonField("totals.regions"))?;
        let functions = totals
            .get("functions")
            .ok_or(CoverageError::MissingJsonField("totals.functions"))?;
        let branches = totals.get("branches");

        Ok(Self {
            lines_covered: covered(lines)?,
            lines_total: count(lines)?,
            regions_covered: covered(regions)?,
            regions_total: count(regions)?,
            functions_covered: covered(functions)?,
            functions_total: count(functions)?,
            branches_covered: branches.map(covered).transpose()?,
            branches_total: branches.map(count).transpose()?,
        })
    }
}

#[cfg(coverage)]
unsafe extern "C" {
    fn __llvm_profile_set_filename(filename: *const std::ffi::c_char);
    fn __llvm_profile_write_file() -> i32;
    fn __llvm_profile_reset_counters();
    fn __llvm_profile_begin_counters() -> *const u64;
    fn __llvm_profile_end_counters() -> *const u64;
}

#[derive(Clone, Copy, Debug)]
pub struct CoverageCounterHit {
    pub id: u64,
    pub bucket: u8,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CmpKind {
    Dynamic,
    Const,
    Switch,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CmpObservation {
    pub kind: CmpKind,
    pub width: u8,
    pub left: u64,
    pub right: u64,
}

#[cfg(sancov)]
static SANCOV_COUNTER_RANGES: std::sync::Mutex<Vec<(usize, usize)>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(sancov)]
const MAX_CMP_OBSERVATIONS: usize = 4096;

#[cfg(sancov)]
#[used]
static KEEP_SANCOV_8BIT_COUNTERS_INIT: extern "C" fn(*mut u8, *mut u8) =
    __sanitizer_cov_8bit_counters_init;

#[cfg(sancov)]
#[no_mangle]
pub extern "C" fn __sanitizer_cov_8bit_counters_init(start: *mut u8, stop: *mut u8) {
    if start.is_null() || stop.is_null() || start >= stop {
        return;
    }
    let range = (start as usize, stop as usize);
    let mut ranges = SANCOV_COUNTER_RANGES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !ranges.contains(&range) {
        ranges.push(range);
    }
}

#[cfg(sancov)]
unsafe extern "C" {
    fn trace_fuzz_cmp_begin(enabled: u8);
    fn trace_fuzz_cmp_take(out: *mut u64, max: usize) -> usize;
}

#[cfg(sancov)]
fn decode_cmp(encoded: u64) -> CmpObservation {
    let kind = match encoded >> 60 {
        1 => CmpKind::Const,
        2 => CmpKind::Switch,
        _ => CmpKind::Dynamic,
    };
    CmpObservation {
        kind,
        width: ((encoded >> 56) & 0x0f) as u8,
        left: (encoded >> 28) & 0x0fff_ffff,
        right: encoded & 0x0fff_ffff,
    }
}

#[cfg(sancov)]
pub fn reset_edge_counters() {
    let ranges = SANCOV_COUNTER_RANGES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for &(start, stop) in ranges.iter() {
        let len = stop.saturating_sub(start);
        if len != 0 {
            unsafe { std::ptr::write_bytes(start as *mut u8, 0, len) };
        }
    }
}

#[cfg(not(sancov))]
pub fn reset_edge_counters() {}

#[cfg(sancov)]
pub fn begin_cmp_observations(enabled: bool) {
    unsafe { trace_fuzz_cmp_begin(u8::from(enabled)) };
}

#[cfg(not(sancov))]
pub fn begin_cmp_observations(_enabled: bool) {}

#[cfg(sancov)]
pub fn take_cmp_observations() -> Vec<CmpObservation> {
    let mut encoded = vec![0u64; MAX_CMP_OBSERVATIONS];
    let count = unsafe { trace_fuzz_cmp_take(encoded.as_mut_ptr(), encoded.len()) };
    encoded.truncate(count.min(MAX_CMP_OBSERVATIONS));
    encoded.into_iter().map(decode_cmp).collect()
}

#[cfg(not(sancov))]
pub fn take_cmp_observations() -> Vec<CmpObservation> {
    Vec::new()
}

#[cfg(sancov)]
pub fn counter_hit_buckets() -> Vec<CoverageCounterHit> {
    let ranges = SANCOV_COUNTER_RANGES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut hits = Vec::new();
    let mut base = 0u64;
    for &(start, stop) in ranges.iter() {
        let len = stop.saturating_sub(start);
        if len == 0 {
            continue;
        }
        let counters = unsafe { std::slice::from_raw_parts_mut(start as *mut u8, len) };
        for (offset, count) in counters.iter_mut().enumerate() {
            if *count != 0 {
                hits.push(CoverageCounterHit {
                    id: base + offset as u64,
                    bucket: counter_hit_bucket(u64::from(*count)),
                });
                *count = 0;
            }
        }
        base += len as u64;
    }
    hits
}

#[cfg(all(coverage, not(sancov)))]
static LAST_COUNTERS: std::sync::Mutex<Vec<u64>> = std::sync::Mutex::new(Vec::new());

#[cfg(all(coverage, not(sancov)))]
pub fn counter_hit_buckets() -> Vec<CoverageCounterHit> {
    unsafe {
        let begin = __llvm_profile_begin_counters();
        let end = __llvm_profile_end_counters();
        if begin.is_null() || end.is_null() || end < begin {
            return Vec::new();
        }
        let len = end.offset_from(begin) as usize;
        let counters = std::slice::from_raw_parts(begin, len);
        let mut previous = LAST_COUNTERS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if previous.len() < len {
            previous.resize(len, 0);
        }

        let mut hits = Vec::new();
        for (index, count) in counters.iter().copied().enumerate() {
            let old = previous[index];
            previous[index] = count;

            // Coverage polling writes an interval profile and resets LLVM's
            // in-memory counters. If that happened since the last trace, the
            // current value is lower than our last snapshot; treat the current
            // value as this trace's post-reset delta rather than underflowing.
            let delta = count.checked_sub(old).unwrap_or(count);
            if delta != 0 {
                hits.push(CoverageCounterHit {
                    id: index as u64,
                    bucket: counter_hit_bucket(delta),
                });
            }
        }
        hits
    }
}

#[cfg(not(any(coverage, sancov)))]
pub fn counter_hit_buckets() -> Vec<CoverageCounterHit> {
    Vec::new()
}

#[cfg(any(coverage, sancov))]
fn counter_hit_bucket(count: u64) -> u8 {
    match count {
        0 => 0,
        1 => 1,
        2 => 2,
        3 => 3,
        4..=7 => 4,
        8..=15 => 5,
        16..=31 => 6,
        32..=127 => 7,
        _ => 8,
    }
}

#[cfg(coverage)]
fn flush_coverage_profile(path: &Path) -> Result<(), CoverageError> {
    // LLVM profile data is normally written at process exit. When this binary
    // is built with `-Cinstrument-coverage`, the profiling runtime exposes
    // symbols that let us write the current counters mid-run.
    //
    // Do not keep writing to the LLVM_PROFILE_FILE path. The default script uses
    // `%m`, which enables LLVM's online merge mode; repeatedly asking that mode
    // to merge the process' cumulative counters into the same raw profile can
    // eventually make the raw profile unreadable. Instead, write each poll to a
    // fresh .profraw file and reset the in-memory counters. Merging those
    // interval profiles gives the same covered/not-covered result for the run
    // without ever re-merging old counters into the same raw profile.
    let filename = std::ffi::CString::new(path.to_string_lossy().as_bytes()).map_err(|source| {
        CoverageError::ProfilePathNul {
            path: path.to_path_buf(),
            source,
        }
    })?;
    unsafe {
        __llvm_profile_set_filename(filename.as_ptr());
        let status = __llvm_profile_write_file();
        if status != 0 {
            return Err(CoverageError::ProfileWrite {
                path: path.to_path_buf(),
                status,
            });
        }
        __llvm_profile_reset_counters();
    }
    Ok(())
}

#[cfg(not(coverage))]
fn flush_coverage_profile(_path: &Path) -> Result<(), CoverageError> {
    Ok(())
}

fn collect_profraws(root: &Path) -> Result<Vec<PathBuf>, CoverageError> {
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut profraws = Vec::new();
    for entry in fs::read_dir(root).map_err(|source| CoverageError::Io {
        action: "read coverage profile directory",
        path: root.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| CoverageError::Io {
            action: "read coverage profile directory entry",
            path: root.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("profraw") {
            profraws.push(path);
        }
    }
    profraws.sort();
    Ok(profraws)
}

fn remove_profraws(root: &Path) -> Result<(), CoverageError> {
    for profraw in collect_profraws(root)? {
        fs::remove_file(&profraw).map_err(|source| CoverageError::Io {
            action: "remove stale coverage profile",
            path: profraw,
            source,
        })?;
    }
    Ok(())
}

fn covered(value: &serde_json::Value) -> Result<u64, CoverageError> {
    value
        .get("covered")
        .and_then(serde_json::Value::as_u64)
        .ok_or(CoverageError::MissingJsonField("covered"))
}

fn count(value: &serde_json::Value) -> Result<u64, CoverageError> {
    value
        .get("count")
        .and_then(serde_json::Value::as_u64)
        .ok_or(CoverageError::MissingJsonField("count"))
}

fn percent(covered: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        (covered as f64 / total as f64) * 100.0
    }
}

fn merge_command(profraws: &[PathBuf], output: &Path) -> Command {
    let mut command = Command::new("llvm-profdata");
    command.arg("merge").arg("-sparse");
    for profraw in profraws {
        command.arg(profraw);
    }
    command.arg("-o").arg(output);
    command
}

fn command_output(command: Command, name: &'static str) -> Result<String, CoverageError> {
    let output = run(command, name)?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        Err(CoverageError::CommandFailed {
            name,
            status: output.status.code(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

fn run(mut command: Command, name: &'static str) -> Result<std::process::Output, CoverageError> {
    command
        .output()
        .map_err(|source| CoverageError::CommandIo { name, source })
}

#[derive(Debug)]
pub enum CoverageError {
    CurrentExe {
        source: std::io::Error,
    },
    Io {
        action: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
    CommandIo {
        name: &'static str,
        source: std::io::Error,
    },
    ProfilePathNul {
        path: PathBuf,
        source: std::ffi::NulError,
    },
    ProfileWrite {
        path: PathBuf,
        status: i32,
    },
    CommandFailed {
        name: &'static str,
        status: Option<i32>,
        stderr: String,
    },
    Json(serde_json::Error),
    MissingJsonField(&'static str),
}

impl std::fmt::Display for CoverageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CurrentExe { source } => write!(f, "failed to find current executable: {source}"),
            Self::Io {
                action,
                path,
                source,
            } => write!(f, "failed to {action} {}: {source}", path.display()),
            Self::CommandIo { name, source } => write!(f, "failed to run {name}: {source}"),
            Self::ProfilePathNul { path, source } => write!(
                f,
                "coverage profile path contains a NUL byte {}: {source}",
                path.display()
            ),
            Self::ProfileWrite { path, status } => write!(
                f,
                "failed to write coverage profile {}: LLVM status {status}",
                path.display()
            ),
            Self::CommandFailed {
                name,
                status,
                stderr,
            } => write!(f, "{name} failed with status {status:?}: {stderr}"),
            Self::Json(source) => write!(f, "failed to parse llvm-cov JSON: {source}"),
            Self::MissingJsonField(field) => write!(f, "llvm-cov JSON missing field {field}"),
        }
    }
}

impl std::error::Error for CoverageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::CurrentExe { source } => Some(source),
            Self::Io { source, .. } => Some(source),
            Self::CommandIo { source, .. } => Some(source),
            Self::ProfilePathNul { source, .. } => Some(source),
            Self::Json(source) => Some(source),
            Self::CommandFailed { .. } | Self::ProfileWrite { .. } | Self::MissingJsonField(_) => {
                None
            }
        }
    }
}
