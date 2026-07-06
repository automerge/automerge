#!/usr/bin/env bash
set -euo pipefail

# Run the trace fuzzer with Rust/LLVM coverage instrumentation enabled.
#
# Examples:
#
#   ./run-trace-fuzz-coverage.sh --seed 1 --iterations 100000
#   ./run-trace-fuzz-coverage.sh --seed 1 --iterations 1000000000 --report-every 10000
#
# Optional environment variables:
#
#   TRACE_FUZZ_COVERAGE_DIR   Where reports are written. Defaults to target/trace-coverage.
#   LLVM_PROFILE_FILE         Where fallback .profraw files are written. Defaults to target/trace-fuzz-%p-%m.profraw.
#   RUSTFLAGS                 Extra rustc flags. -Cinstrument-coverage and --cfg coverage are appended if absent.

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir"

coverage_dir="${TRACE_FUZZ_COVERAGE_DIR:-target/trace-coverage}"
export LLVM_PROFILE_FILE="${LLVM_PROFILE_FILE:-$script_dir/target/trace-fuzz-%p-%m.profraw}"

case " ${RUSTFLAGS:-} " in
  *" -Cinstrument-coverage "*) ;;
  *) export RUSTFLAGS="${RUSTFLAGS:-} -Cinstrument-coverage" ;;
esac

case " ${RUSTFLAGS:-} " in
  *" --cfg coverage "*) ;;
  *) export RUSTFLAGS="${RUSTFLAGS:-} --cfg coverage" ;;
esac

has_arg() {
  local needle="$1"
  shift
  for arg in "$@"; do
    if [[ "$arg" == "$needle" || "$arg" == "$needle="* ]]; then
      return 0
    fi
  done
  return 1
}

arg_value() {
  local needle="$1"
  shift
  while [[ $# -gt 0 ]]; do
    case "$1" in
      "$needle")
        if [[ $# -lt 2 ]]; then
          return 1
        fi
        printf '%s\n' "$2"
        return 0
        ;;
      "$needle="*)
        printf '%s\n' "${1#*=}"
        return 0
        ;;
    esac
    shift
  done
  return 1
}

merge_profraws_tolerant() {
  local output="$1"
  shift
  local profraws=("$@")

  if llvm-profdata merge -sparse "${profraws[@]}" -o "$output"; then
    return 0
  fi

  echo "warning: llvm-profdata merge failed; checking individual .profraw files" >&2
  local bad_dir="$coverage_dir/bad-profraw"
  mkdir -p "$bad_dir"

  local good=()
  local tmp_profdata="$coverage_dir/.profraw-check.profdata"
  local tmp_stderr="$coverage_dir/.profraw-check.stderr"
  local profraw
  for profraw in "${profraws[@]}"; do
    if llvm-profdata merge -sparse "$profraw" -o "$tmp_profdata" > /dev/null 2> "$tmp_stderr"; then
      good+=("$profraw")
    else
      echo "warning: quarantining corrupt profile $profraw" >&2
      if [[ -e "$profraw" ]]; then
        mv -f "$profraw" "$bad_dir/$(basename "$profraw")" || true
      fi
    fi
  done
  rm -f "$tmp_profdata" "$tmp_stderr"

  if [[ ${#good[@]} -eq 0 ]]; then
    echo "error: no valid .profraw files remain after quarantine" >&2
    return 1
  fi

  llvm-profdata merge -sparse "${good[@]}" -o "$output"
}

args=(fuzz)
if has_arg "--coverage-dir" "$@"; then
  coverage_dir="$(arg_value "--coverage-dir" "$@")"
else
  args+=(--coverage-dir "$coverage_dir")
fi

profile_dir="$coverage_dir/profiles"
mkdir -p "$coverage_dir" "$profile_dir"
rm -f target/trace-fuzz-*.profraw "$profile_dir"/trace-fuzz-*.profraw

echo "running trace fuzzer with coverage: coverage_dir=$coverage_dir" >&2

echo "command: cargo run --bin trace_fuzz -- ${args[*]} $*" >&2
set +e
cargo run --bin trace_fuzz -- "${args[@]}" "$@"
status=$?
set -e

# The in-process coverage poller is best-effort. The authoritative final report
# is generated here, after the instrumented binary has exited and LLVM has
# flushed the runtime profile.
shopt -s nullglob
profraws=(target/trace-fuzz-*.profraw "$profile_dir"/trace-fuzz-*.profraw)
shopt -u nullglob

if [[ ${#profraws[@]} -gt 0 ]]; then
  if command -v llvm-profdata >/dev/null && command -v llvm-cov >/dev/null; then
    if ! merge_profraws_tolerant "$coverage_dir/final.profdata" "${profraws[@]}"; then
      echo "warning: unable to merge coverage profiles; skipping final coverage report" >&2
      exit "$status"
    fi
    llvm-cov export target/debug/trace_fuzz \
      --instr-profile="$coverage_dir/final.profdata" \
      --summary-only \
      --ignore-filename-regex='/.cargo/registry|/rustc/' \
      > "$coverage_dir/summary.json"
    llvm-cov export target/debug/trace_fuzz \
      --instr-profile="$coverage_dir/final.profdata" \
      --ignore-filename-regex='/.cargo/registry|/rustc/' \
      > "$coverage_dir/export.json"
    llvm-cov report target/debug/trace_fuzz \
      --instr-profile="$coverage_dir/final.profdata" \
      --ignore-filename-regex='/.cargo/registry|/rustc/' \
      | tee "$coverage_dir/report.txt"

    if command -v python3 >/dev/null; then
      python3 - "$coverage_dir/export.json" "$coverage_dir/summary-core.json" "$coverage_dir/report-core.txt" <<'PY'
import json
import sys
from pathlib import Path

export_path = Path(sys.argv[1])
summary_path = Path(sys.argv[2])
report_path = Path(sys.argv[3])

data = json.loads(export_path.read_text())
files = data.get("data", [{}])[0].get("files", [])

CORE_MARKERS = ("/rust/automerge/src/", "/rust/hexane/src/")
METRICS = ("regions", "functions", "lines", "branches")

def is_core(filename: str) -> bool:
    normalized = "/" + filename.lstrip("/")
    return any(marker in normalized for marker in CORE_MARKERS)

def empty_metric():
    return {"count": 0, "covered": 0, "notcovered": 0, "percent": 0.0}

def add_metric(total, metric):
    total["count"] += int(metric.get("count", 0))
    total["covered"] += int(metric.get("covered", 0))
    total["notcovered"] += int(metric.get("notcovered", 0))

core_files = [file for file in files if is_core(file.get("filename", ""))]
totals = {metric: empty_metric() for metric in METRICS}

for file in core_files:
    summary = file.get("summary", {})
    for metric in METRICS:
        if metric in summary:
            add_metric(totals[metric], summary[metric])

for metric in totals.values():
    count = metric["count"]
    metric["percent"] = (metric["covered"] / count * 100.0) if count else 0.0

summary = {
    "kind": "automerge-core-coverage",
    "includes": ["rust/automerge/src", "rust/hexane/src"],
    "totals": totals,
    "files": len(core_files),
}
summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")

rows = []
for file in core_files:
    file_summary = file.get("summary", {})
    lines = file_summary.get("lines", empty_metric())
    regions = file_summary.get("regions", empty_metric())
    functions = file_summary.get("functions", empty_metric())
    rows.append((
        file.get("filename", ""),
        regions.get("covered", 0), regions.get("count", 0), regions.get("percent", 0.0),
        functions.get("covered", 0), functions.get("count", 0), functions.get("percent", 0.0),
        lines.get("covered", 0), lines.get("count", 0), lines.get("percent", 0.0),
    ))

rows.sort(key=lambda row: row[0])
with report_path.open("w") as report:
    report.write("Automerge core coverage (rust/automerge/src + rust/hexane/src)\n")
    report.write("=" * 79 + "\n")
    report.write(f"{'File':100} {'Regions':>17} {'Funcs':>17} {'Lines':>17}\n")
    report.write("-" * 157 + "\n")
    for row in rows:
        filename, rc, rt, rp, fc, ft, fp, lc, lt, lp = row
        report.write(f"{filename:100} {rc:6}/{rt:<6} {rp:6.2f}% {fc:6}/{ft:<6} {fp:6.2f}% {lc:6}/{lt:<6} {lp:6.2f}%\n")
    report.write("-" * 157 + "\n")
    report.write(
        f"{'TOTAL':100} "
        f"{totals['regions']['covered']:6}/{totals['regions']['count']:<6} {totals['regions']['percent']:6.2f}% "
        f"{totals['functions']['covered']:6}/{totals['functions']['count']:<6} {totals['functions']['percent']:6.2f}% "
        f"{totals['lines']['covered']:6}/{totals['lines']['count']:<6} {totals['lines']['percent']:6.2f}%\n"
    )

print(
    "core coverage: "
    f"lines={totals['lines']['covered']}/{totals['lines']['count']} {totals['lines']['percent']:.2f}% "
    f"regions={totals['regions']['covered']}/{totals['regions']['count']} {totals['regions']['percent']:.2f}% "
    f"funcs={totals['functions']['covered']}/{totals['functions']['count']} {totals['functions']['percent']:.2f}%"
)
PY
    else
      echo "warning: python3 not found; skipping core coverage summary" >&2
    fi

    echo "coverage reports written to $coverage_dir"
  else
    echo "warning: llvm-profdata and/or llvm-cov not found; skipping final coverage report" >&2
  fi
else
  echo "warning: no .profraw files found; skipping final coverage report" >&2
fi

exit "$status"
