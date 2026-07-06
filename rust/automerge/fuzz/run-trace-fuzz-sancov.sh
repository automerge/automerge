#!/usr/bin/env bash
set -euo pipefail

# Run the trace fuzzer with LLVM SanitizerCoverage inline 8-bit counters.
#
# This is closer to AFL/libFuzzer-style edge feedback than Rust's source-based
# -Cinstrument-coverage counters. The fuzzer reads and resets the sancov counter
# array after each trace and uses hit-count buckets for feedback and scheduling.
#
# Optional environment variables:
#   TRACE_FUZZ_SANCOV_LEVEL         SanitizerCoverage level. Defaults to 3.
#                                  1 = function entry, 2 = basic blocks,
#                                  3 = blocks + critical edges.
#   TRACE_FUZZ_SANCOV_PRUNE_BLOCKS  Set to 0 to disable LLVM's block pruning.
#   TRACE_FUZZ_SANCOV_TRACE_CMPS    Set to 1 to enable comparison tracing.
#   TRACE_FUZZ_CMP_SAMPLE_RATE      With comparison tracing enabled, collect
#                                  operands for one trace in N. Defaults to 1.
#   TRACE_FUZZ_SANCOV_GATED_CMPS    Set to 1 to ask LLVM to gate trace-cmp
#                                  callback calls when not collecting.
#   RUSTFLAGS                       Extra rustc flags; sancov flags are appended.

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir"

target_triple="$(rustc -vV | awk '/^host:/ { print $2 }')"
target_env="CARGO_TARGET_${target_triple^^}_RUSTFLAGS"
target_env="${target_env//-/_}"

stub_dir="$script_dir/target/sancov-stubs"
mkdir -p "$stub_dir"
cat > "$stub_dir/sancov_stubs.c" <<'C'
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#define TRACE_FUZZ_MAX_CMP_OBSERVATIONS 4096

static _Atomic uint8_t trace_fuzz_cmp_enabled;
static _Atomic size_t trace_fuzz_cmp_count;
static _Atomic uint64_t trace_fuzz_cmp_observations[TRACE_FUZZ_MAX_CMP_OBSERVATIONS];

__attribute__((weak)) void __sanitizer_cov_8bit_counters_init(uint8_t *start, uint8_t *stop) {}
extern uint64_t __sancov_should_track __attribute__((weak));

static void trace_fuzz_cmp_set_llvm_gate(uint8_t enabled) {
  if (&__sancov_should_track) {
    __sancov_should_track = enabled != 0;
  }
}

static uint64_t trace_fuzz_cmp_encode(uint8_t kind, uint8_t width, uint64_t left, uint64_t right) {
  return ((uint64_t)(kind & 0xf) << 60)
       | ((uint64_t)(width & 0xf) << 56)
       | ((left & 0x0fffffffULL) << 28)
       | (right & 0x0fffffffULL);
}

static void trace_fuzz_cmp_record(uint8_t kind, uint8_t width, uint64_t left, uint64_t right) {
  if (!atomic_load_explicit(&trace_fuzz_cmp_enabled, memory_order_relaxed)) {
    return;
  }
  size_t index = atomic_fetch_add_explicit(&trace_fuzz_cmp_count, 1, memory_order_relaxed);
  if (index < TRACE_FUZZ_MAX_CMP_OBSERVATIONS) {
    atomic_store_explicit(
      &trace_fuzz_cmp_observations[index],
      trace_fuzz_cmp_encode(kind, width, left, right),
      memory_order_relaxed
    );
  } else {
    atomic_store_explicit(&trace_fuzz_cmp_enabled, 0, memory_order_relaxed);
  }
}

void trace_fuzz_cmp_begin(uint8_t enabled) {
  atomic_store_explicit(&trace_fuzz_cmp_count, 0, memory_order_relaxed);
  atomic_store_explicit(&trace_fuzz_cmp_enabled, enabled != 0, memory_order_relaxed);
  trace_fuzz_cmp_set_llvm_gate(enabled);
}

size_t trace_fuzz_cmp_take(uint64_t *out, size_t max) {
  atomic_store_explicit(&trace_fuzz_cmp_enabled, 0, memory_order_relaxed);
  trace_fuzz_cmp_set_llvm_gate(0);
  size_t count = atomic_exchange_explicit(&trace_fuzz_cmp_count, 0, memory_order_relaxed);
  if (count > TRACE_FUZZ_MAX_CMP_OBSERVATIONS) count = TRACE_FUZZ_MAX_CMP_OBSERVATIONS;
  if (count > max) count = max;
  for (size_t index = 0; index < count; index++) {
    out[index] = atomic_load_explicit(&trace_fuzz_cmp_observations[index], memory_order_relaxed);
  }
  return count;
}

void __sanitizer_cov_trace_cmp1(uint8_t a, uint8_t b) { trace_fuzz_cmp_record(0, 1, a, b); }
void __sanitizer_cov_trace_cmp2(uint16_t a, uint16_t b) { trace_fuzz_cmp_record(0, 2, a, b); }
void __sanitizer_cov_trace_cmp4(uint32_t a, uint32_t b) { trace_fuzz_cmp_record(0, 4, a, b); }
void __sanitizer_cov_trace_cmp8(uint64_t a, uint64_t b) { trace_fuzz_cmp_record(0, 8, a, b); }
void __sanitizer_cov_trace_const_cmp1(uint8_t a, uint8_t b) { trace_fuzz_cmp_record(1, 1, a, b); }
void __sanitizer_cov_trace_const_cmp2(uint16_t a, uint16_t b) { trace_fuzz_cmp_record(1, 2, a, b); }
void __sanitizer_cov_trace_const_cmp4(uint32_t a, uint32_t b) { trace_fuzz_cmp_record(1, 4, a, b); }
void __sanitizer_cov_trace_const_cmp8(uint64_t a, uint64_t b) { trace_fuzz_cmp_record(1, 8, a, b); }
void __sanitizer_cov_trace_switch(uint64_t val, uint64_t *cases) {
  if (!cases) return;
  uint64_t count = cases[0];
  uint8_t width = cases[1] > 15 ? 15 : (uint8_t)cases[1];
  if (count > 64) count = 64;
  for (uint64_t index = 0; index < count; index++) {
    trace_fuzz_cmp_record(2, width, val, cases[index + 2]);
  }
}
C
cc -c "$stub_dir/sancov_stubs.c" -o "$stub_dir/sancov_stubs.o"
ar rcs "$stub_dir/libsancov_stubs.a" "$stub_dir/sancov_stubs.o"

level="${TRACE_FUZZ_SANCOV_LEVEL:-3}"
prune="${TRACE_FUZZ_SANCOV_PRUNE_BLOCKS:-1}"
trace_cmps="${TRACE_FUZZ_SANCOV_TRACE_CMPS:-0}"
gated_cmps="${TRACE_FUZZ_SANCOV_GATED_CMPS:-0}"

sancov_flags=(
  "--cfg" "sancov"
  "-Cpasses=sancov-module"
  "-Cllvm-args=-sanitizer-coverage-level=$level"
  "-Cllvm-args=-sanitizer-coverage-inline-8bit-counters"
  "-Cllvm-args=-sanitizer-coverage-prune-blocks=$prune"
  "-L" "native=$stub_dir"
  "-l" "static=sancov_stubs"
)

if [[ "$trace_cmps" != "0" ]]; then
  sancov_flags+=("-Cllvm-args=-sanitizer-coverage-trace-compares")
  export TRACE_FUZZ_CMP_SAMPLE_RATE="${TRACE_FUZZ_CMP_SAMPLE_RATE:-1}"
  if [[ "$gated_cmps" != "0" ]]; then
    sancov_flags+=("-Cllvm-args=-sanitizer-coverage-gated-trace-callbacks")
  fi
fi

existing="${!target_env:-}"
printf -v appended ' %q' "${sancov_flags[@]}"
export "$target_env=$existing$appended"

args=(fuzz)
echo "running trace fuzzer with sanitizer coverage: level=$level prune=$prune trace_cmps=$trace_cmps gated_cmps=$gated_cmps cmp_sample_rate=${TRACE_FUZZ_CMP_SAMPLE_RATE:-1}" >&2
echo "command: cargo run --bin trace_fuzz -- ${args[*]} $*" >&2
cargo run --bin trace_fuzz -- "${args[@]}" "$@"
