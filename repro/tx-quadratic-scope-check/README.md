# Quadratic transaction cost from the isolation-scope check

A reproducer for a performance regression between automerge 0.9.0 and 0.11.0: the cost of a single
transaction became quadratic in its own length.

Reported by a user of [backstitch], where `commit_fs_changes` writes an entire filesystem snapshot
in one transaction.

[backstitch]: https://github.com/inkandswitch/backstitch/blob/8673ec8d92f984a15e80553bb49b75e561b869a1/rust/src/project/branch_db/commit.rs#L92

## Theory

Commit [`fb86fab75`] ("rust: reject object edits outside transaction scope") fixed a real bug: an
isolated transaction could edit an object that did not exist at its isolation heads, producing a
change which referenced actors absent from its own dependency history, which then made `fork_at`
panic.

The fix routes every mutating method through a new wrapper instead of calling `doc.exid_to_obj`
directly. `rust/automerge/src/transaction/inner.rs`:

```rust
fn exid_to_obj(&self, doc: &Automerge, id: &ExId) -> Result<ObjMeta, AutomergeError> {
    let obj = doc.exid_to_obj(id)?;
    let created_in_transaction = self.pending.iter().any(|op| op.id() == obj.id.0);
    if !obj.id.is_root()
        && !created_in_transaction
        && self.scope.as_ref().is_some_and(|scope| !scope.covers(&obj.id.0))
    {
        return Err(AutomergeError::InvalidObjId(id.to_string()));
    }
    Ok(obj)
}
```

The guard needs an exception for objects created inside the transaction itself, because those are
legitimately not covered by the isolation clock. That exception is implemented as a linear search
of `self.pending`.

Three things compound:

1. _The search is unconditional._ `&&` short-circuits, but `created_in_transaction` is a `let`
   binding evaluated before the `if`. A transaction with `scope: None` — which is every
   transaction from `Automerge::transaction()`, `AutoCommit`, and the whole Wasm/JS surface — can
   never fail this check, yet still pays for it on every mutating call.
2. _The search runs per operation, over all prior operations._ For a transaction of `n`
   externally-issued operations the total work is `O(n²)` comparisons.
3. _The scan direction is the worst case for the common access pattern._ `pending.iter()` starts
   at the front, while the idiomatic pattern is "create an object, then immediately write into
   it". The matching op is the newest, so nearly every lookup walks the entire list.

## Proposed fix

The exception can be answered in `O(1)` without any extra state. An object was created in this
transaction exactly when its `OpId` was minted by this transaction, and `next_id` mints ids
sequentially from `start_op` under the transaction's own actor:

```rust
pub(super) fn next_id(&self) -> OpId {
    OpId::new(self.start_op.get() + self.pending_ops() as u64, self.actor)
}
```

so membership is a comparison, not a search:

```rust
let created_in_transaction =
    obj.id.0.actor() == self.actor && obj.id.0.counter() >= self.start_op.get();
```

This is measured as the third column below. It passes the existing `tests/test.rs` suite (139
tests) unchanged, including the two isolation tests added by `fb86fab75`, plus the four semantic
guards in `tests/transaction_scaling.rs`.

A one-line alternative is to reorder the existing condition so the cheap, discriminating test gates
the scan:

```rust
if !obj.id.is_root()
    && self.scope.as_ref().is_some_and(|scope| !scope.covers(&obj.id.0))
    && !self.pending.iter().any(|op| op.id() == obj.id.0)
```

That fixes every non-isolated transaction, which is the reported problem, but leaves isolated
transactions quadratic. Prefer the `O(1)` form.

[`fb86fab75`]: https://github.com/automerge/automerge/commit/fb86fab75a10bea5e31d9bcb1520f02e6f047d35

## Which workloads are affected

Cost is quadratic in the number of operations in a _single_ transaction, not in document size.

_Affected_: any code path that issues many mutating calls before committing. Bulk imports,
filesystem snapshot commits, and `autosurgeon::reconcile` of a large structured value are the
obvious ones. `autosurgeon` is particularly exposed because it drives every nested `put` and
`put_object` through the public API, one call at a time.

_Not affected_: `splice`, `splice_text`, `update_text`, and `batch_create_object` resolve the
object once and then push operations internally. Reads (`get`, `get_all`, `keys`) never enter the
wrapper. Many small transactions are fine, because `pending` resets at each commit.

## Running it

```sh
cargo run --release
cargo run --release -- 1000 4000 16000 64000
```

The binary sweeps transaction sizes and reports, for each step, the local slope on a log-log plot.
Linear cost gives an exponent near 1.0 and quadratic cost near 2.0.

The crate is deliberately outside the `rust/` workspace and uses only `transaction`, `put_object`,
`put`, and `commit`, so the same source compiles against 0.9, 0.10, and 0.11. To A/B against an
older release, extract it and repoint the path dependency:

```sh
mkdir -p /tmp/am090
git -C /path/to/automerge archive rust/automerge-0.9.0 | tar -x -C /tmp/am090

cp -r repro/tx-quadratic-scope-check /tmp/repro-am090
sed -i 's|path = "../../rust/automerge"|path = "/tmp/am090/rust/automerge"|' /tmp/repro-am090/Cargo.toml
cd /tmp/repro-am090 && cargo run --release
```

The same recipe works for `git bisect run`.

## Measured

One transaction, three operations per entry, `--release`, single machine, same session.

| entries | ops | 0.9.0 | 0.11.0 | 0.11.0 + `O(1)` check |
|--------:|----:|------:|-------:|----------------------:|
| 4,000 | 12,000 | 0.16 s | 0.12 s | 0.11 s |
| 8,000 | 24,000 | 0.30 s | 0.33 s | 0.20 s |
| 16,000 | 48,000 | 0.61 s | 1.07 s | 0.41 s |
| 32,000 | 96,000 | 1.25 s | 3.26 s | 0.86 s |
| 64,000 | 192,000 | **2.59 s** | **16.72 s** | **1.73 s** |

Per operation, 0.9.0 holds flat at 12–13 µs and the fixed 0.11.0 at 8.3–9.0 µs. Stock 0.11.0
climbs from 10 µs to 87 µs and keeps going.

Fitted exponents: 0.9.0 = 1.05, stock 0.11.0 = 2.36, fixed 0.11.0 = 1.06.

Two things worth noting. The regression is invisible below roughly 5,000 operations, which is why
it survived review and why small projects report no problem. And once the scan is removed, 0.11.0
is _faster_ than 0.9.0 — the genuine optimisations in the release are real, and currently masked.

## Companion test

`rust/automerge/tests/transaction_scaling.rs` holds the regression test:

```sh
# semantic guards, always run
cargo test -p automerge --test transaction_scaling

# the timing assertion, opt-in
cargo test --release -p automerge --test transaction_scaling -- --ignored --nocapture
```

The timing test is `#[ignore]`d because wall-clock assertions do not belong in a default test run.
The four semantic tests are cheap and always run: they pin what a transaction is and is not allowed
to edit, so any faster implementation of the check has to preserve the behaviour rather than merely
be quick.

On this branch the timing test fails at 9.61x growth for a 4x size increase. With the `O(1)` check
it passes at 4.07x, and per-operation cost is flat at 7.4–7.6 µs across both sizes.
