# Batch application

Run `cargo bench -p automerge --bench apply_batch` from `rust/`.
Set `BENCH_OPS` to change the number of items (default: 10,000).

The benchmark reports median, 10th percentile, and 90th percentile application
times from 31 samples after five warmups. Fixture construction, input cloning,
and document destruction are outside the timer. Map inserts have no predecessors;
map updates reference an existing document; batch map/list updates reference
operations supplied in the same batch.

For comparisons, build both revisions with the same toolchain and profile, then
run their executables alternately while the machine is otherwise idle. These are
end-to-end timings, including operation import and insertion, not isolated
validation timings.

## Local comparison (2026-09-10)

Ryzen 7 7840S, Linux x86_64, Cargo 1.97.1, optimized bench profile with LTO.
All runs were pinned to CPU 2. Each implementation ran three times, rotating
execution order. Values below are medians of the three run medians, in milliseconds,
for 10,000 items. [Raw results](apply_batch_results.csv) include per-run percentiles.

- Original PR: commit `727066984e5f2f6725601de1116767d84e1d9a61`.
- Previous revision: combined traversal with a `seen` map for every incoming op.
- Revised: collect references during import, resolve them during the existing
  sorted traversal, and use the same hasher as the existing predecessor cache.

| Scenario | Original PR | Previous revision | Revised |
| --- | ---: | ---: | ---: |
| `insert_map` | 4.343 | 4.818 | 4.366 |
| `update_map` | 81.041 | 77.312 | 81.749 |
| `batch_map` | 16.741 | 17.791 | 15.357 |
| `batch_list` | 15.771 | 15.374 | 15.856 |

Run-to-run variation is substantial, so these results do not establish a general
speedup. The revised implementation is broadly comparable to the original PR on
these workloads. Its temporary validation map contains only referenced predecessors;
insert-only batches allocate no entries. Map keys for referenced predecessors are
owned copies so they remain valid while imported operations are moved and sorted.
The remaining stored-operation scan happens before history is recorded, allowing
validation failures to leave the document unchanged.
