> **Note (1.0.0-alpha.2):** the 0.2-era `ColumnData<C>` API described as
> "v0" below has been **removed**, and the v1 API now lives at the crate
> root (no `v1::` prefix).  This guide remains as a porting reference for
> code still written against 0.2.

# Migrating from v0 `ColumnData<C>` to v1 `PrefixColumn<T>` / `Column<T>`

## Why migrate?

v0 `ColumnData<C>` uses linear iterators for prefix-sum queries (`advance_acc_by`, `shift_acc`, `get_acc_delta`). These are O(n) per call.

v1 `PrefixColumn<T>` uses a slab-based Fenwick tree (BIT) for O(log n) prefix-sum queries (`get_prefix`, `get_index_for_total`, `sum_range`). When you only need values (no prefix sums), `Column<T>` gives O(log n) random access without the BIT overhead.

## Choosing the right v1 type

| Need prefix sums? | Use |
|-|-|
| Yes — counting, seeking by accumulated value, range sums | `hexane::PrefixColumn<T>` |
| No — just random access, iteration, splice | `hexane::Column<T>` |

## Type declarations

```rust
// Before
text: ColumnData<UIntCursor>,        // Option<u64> with prefix sums
top: ColumnData<BooleanCursor>,      // bool with prefix sums
visible: ColumnData<BooleanCursor>,  // bool, no prefix sums needed

// After
text: hexane::PrefixColumn<Option<u32>>, // prefix sums over Option<u32>
top: hexane::PrefixColumn<bool>,         // prefix sums over bool
visible: hexane::Column<bool>,           // values only
```

`PrefixColumn` requires `T: PrefixValue`. Implemented for: `bool`, `u32`, `u64`, `i64`, `Option<u32>`, `Option<u64>`, `Option<i64>`, `NonZeroU32`, etc.

## Building columns

`PrefixColumn` and `Column` both implement `FromIterator`, so `.collect()` works:

```rust
// Before
let text: ColumnData<UIntCursor> = widths.iter()
    .map(|w| if visible { Some(*w) } else { None })
    .collect();

// After
let text: PrefixColumn<Option<u32>> = widths.iter()
    .map(|w| if visible { Some(*w as u32) } else { None })
    .collect();
```

## Splice operations

```rust
// Before (delete)
col.splice::<u64, _>(pos, del, []);

// After (delete)
col.remove_n(pos, del);   // same arguments as splice(pos, del, [])

// Before (insert/replace)
col.splice(pos, 1, [Some(value)]);

// After — same, just match the new element type
col.splice(pos, 1, [Some(value as u32)]);
```

## API migration reference

### Getting a single value

```rust
// v0
let val = col.get(pos);              // → Option<Option<Cow<'_, T>>>

// v1 PrefixColumn
let val = col.values().get(pos);     // → Option<T>  (owned, no Cow)

// v1 Column
let val = col.get(pos);              // → Option<T>
```

### Iterating values in a range

```rust
// v0
let iter = col.iter_range(range);    // yields Option<Cow<'_, T>>

// v1 PrefixColumn
let iter = col.values().iter_range(range);  // yields T

// v1 Column
let iter = col.iter_range(range);           // yields T
```

### Prefix sum at a position (sum of items 0..pos)

```rust
// v0 — no direct call, must iterate

// v1
let sum = col.get_prefix(pos);      // O(log n), returns T::Prefix
```

### Prefix sum delta across a range

```rust
// v0
let (delta, item) = col.get_acc_delta(start, end);
let index = delta.as_usize();
let visible = item.is_some();

// v1
let index = col.sum_range(start..end) as usize;  // O(log n), same-slab fast path
let visible = col.values().get(end).is_some();
```

### Seeking by accumulated value — `advance_prefix`

The v0 `shift_acc(n)` call combines "advance past n units" and "tell me
where I landed".  The v1 equivalent is `advance_prefix(n)` on a `PrefixIter`:

```rust
// v0
let mut iter = col.iter_range(range).with_acc();
let start_acc = iter.acc();
let tx = iter.shift_acc(n)?;
let pos = tx.pos;
let consumed = (tx.acc - start_acc).as_usize();

// v1
let mut iter = col.iter_range(range);
let tx = iter.advance_prefix(n)?;
let pos = tx.pos;
let consumed = tx.delta as usize;
// iterator is now at pos+1, can continue with iter.next() etc.
```

Returns a `PrefixSeek { pos, delta, pv }` where:
- `pos` — position of the item landed on
- `delta` — prefix consumed over `[from, to)`, exclusive of the item
- `pv` — the item as a `PrefixedValue`: `.value`, `.prefix()` (exclusive),
  `.total()` (inclusive)

### Querying at a known position — `advance_to`

The v0 `get_acc_delta(start, pos)` returns the delta and value at a position.
The v1 equivalent is `advance_to(pos)` on a `PrefixIter`:

```rust
// v0
let (delta, item) = col.get_acc_delta(start, pos);
let visible = item.is_some();
let index = delta.as_usize();

// v1
let tx = col.delta(obj_range.start, pos).unwrap();
let visible = tx.pv.value.is_some();
let index = tx.delta as usize;
// (or iter.delta_nth(pos - start) on an existing PrefixIter)
```

### Convenience methods on `PrefixColumn`

For one-shot lookups without needing the iterator afterwards:

```rust
// Value + prefix context at a position, in one shot
let tx = col.delta(start, pos)?;
let value = tx.pv.value;
let consumed = tx.delta;   // sum over [start, pos)
```

`delta` creates a temporary iterator internally. Use the iterator methods
directly when you already have a range or need to continue iterating.

## Cross-check scaffolding pattern

When migrating, keep the old column around temporarily to validate:

```rust
// In the struct
text: hexane::PrefixColumn<Option<u32>>,
text_old: ColumnData<UIntCursor>,    // temporary, remove after validation

// At every usage site
let new_result = col.sum_range(start..end) as usize;
let (old_delta, _) = col_old.get_acc_delta(start, end);
assert_eq!(new_result, old_delta.as_usize(), "v1 vs v0 mismatch");

// Keep text_old in sync at every splice/insert/remove
```

Remove `_old` and all asserts once tests pass. Migrations done so far:
- `insert`: `ColumnData<BooleanCursor>` → `hexane::PrefixColumn<bool>` (done, scaffolding removed)
- `top`: `ColumnData<BooleanCursor>` → `hexane::PrefixColumn<bool>` (done, scaffolding removed)
- `visible`: `ColumnData<BooleanCursor>` → `hexane::Column<bool>` (done)
- `text`: `ColumnData<UIntCursor>` → `hexane::PrefixColumn<Option<u32>>` (in progress, scaffolding active)

## API summary

All methods return `PrefixSeek { pos, delta, pv }`:
- `delta` — exclusive sum from the seek's start to just before this item
- `pv` — the item as a `PrefixedValue` (`.value`, `.prefix()`, `.total()`)

### Iterator methods (preferred — use when you have a range)

| Method | Description |
|-|-|
| `iter.advance_prefix(n)` | Advance past `n` prefix units, return landed item |
| `iter.advance_to(pos)` | Jump to position `pos`, return item + prefix delta |

### Column convenience methods (one-shot lookups)

| Method | Description |
|-|-|
| `col.delta(from, to)` | Seek to `to`; `delta` carries the sum over `[from, to)` |

### Migration mapping

| v0 pattern | v1 replacement |
|-|-|
| `iter.shift_acc(n)` → pos + consumed | `iter.advance_prefix(n)` |
| `col.get_acc_delta(start, pos)` → delta + value | `col.delta(start, pos)` / `iter.delta_nth(n)` |
| `prefix_delta(range)` (sum only, no value) | `sum_range(range)` |

## Encoder migration status (`automerge`)

### `XYZCursor::encode_unless_empty` semantics gotcha

v0's `encode_unless_empty` only elides when **no values were appended at
all** (i.e. the iterator was empty AND the encoder state is empty). v1's
`save_to_unless(default)` elides whenever **all values equal the default**.
For nullable columns the two coincide (an "empty" column means all-null),
but for non-nullable columns they diverge — use v1's `encode_to` (no
`_unless`) for non-nullable columns to match v0's wire format.

### Migrated columns (v0 scaffolding removed — v1 is the source of truth)

| File | Column | Type | Notes |
|-|-|-|-|
| `op_set2/change.rs` | `obj_actor` | `Option<ActorIdx>` | With remap |
| `op_set2/change.rs` | `obj_ctr` | `Option<u64>` | |
| `op_set2/change.rs` | `key_actor` | `Option<ActorIdx>` | With remap |
| `op_set2/change.rs` | `key_str` | `Option<String>` | |
| `op_set2/change.rs` | `insert` | `bool` | Non-nullable, `encode_to` |
| `op_set2/change.rs` | `action` | `Action` | Non-nullable, `encode_to` |
| `op_set2/change.rs` | `value_meta` | `ValueMeta` | Non-nullable, `encode_to` |
| `op_set2/change.rs` | `pred_count` | `u32` | Non-nullable, `encode_to` |
| `op_set2/change.rs` | `pred_actor` | `ActorIdx` | Non-null, with remap |
| `op_set2/change.rs` | `key_ctr` | `Option<i64>` | Delta, `DeltaEncoder::encode_to_unless(None)` |
| `op_set2/change.rs` | `pred_ctr` | `i64` | Delta, `DeltaEncoder::encode_to` |
| `op_set2/change.rs` | `expand` | `bool` | Nullable-by-default |
| `op_set2/change.rs` | `mark_name` | `Option<String>` | |
| `op_set2/change/collector.rs::ProgressiveEncoder` | `key_ctr` | `Option<i64>` | Delta, `DeltaEncoder::save_to_unless(None)` |
| `op_set2/change/collector.rs::ProgressiveEncoder` | `pred_ctr` | `i64` | Delta, `DeltaEncoder::save_to` |
| `change_graph.rs` | `actor` | `ActorIdx` | Non-null, `encode_to` |
| `change_graph.rs` | `num_deps` | `usize` | Non-nullable, `encode_to` |
| `change_graph.rs` | `seq` | `usize` | Delta, `DeltaEncoder::encode_to` |
| `change_graph.rs` | `max_op` | `usize` | Delta, `DeltaEncoder::encode_to` |
| `change_graph.rs` | `deps` | `usize` | Delta, `DeltaEncoder::encode_to` |
| `storage/bundle/builder.rs::BundleChangeWriter` | `actor` | `ActorIdx` | Non-null, with remap via `save_to_and_remap` |
| `storage/bundle/builder.rs::BundleChangeWriter` | `message` | `Option<String>` | Via `append_owned` |
| `storage/bundle/builder.rs::BundleChangeWriter` | `dep_count` | `u32` | Non-nullable |
| `storage/bundle/builder.rs::BundleChangeWriter` | `extra_count` | `u32` | Non-nullable |
| `storage/bundle/builder.rs::BundleChangeWriter` | `seq` | `i64` | Delta (cross-checked) |
| `storage/bundle/builder.rs::BundleChangeWriter` | `start_op` | `i64` | Delta (cross-checked) |
| `storage/bundle/builder.rs::BundleChangeWriter` | `max_op` | `i64` | Delta (cross-checked) |
| `storage/bundle/builder.rs::BundleChangeWriter` | `timestamp` | `i64` | Delta (cross-checked) |
| `storage/bundle/builder.rs::BundleChangeWriter` | `deps` | `i64` | Delta (cross-checked) |
| `storage/bundle/builder.rs::BundleOpWriter` | `obj_actor` | `Option<ActorIdx>` | With remap via `save_to_unless_and_remap` |
| `storage/bundle/builder.rs::BundleOpWriter` | `key_actor` | `Option<ActorIdx>` | With remap via `save_to_unless_and_remap` |
| `storage/bundle/builder.rs::BundleOpWriter` | `key_str` | `Option<String>` | Via `append_owned` |
| `storage/bundle/builder.rs::BundleOpWriter` | `id_actor` | `ActorIdx` | Non-null, with remap via `save_to_and_remap` |
| `storage/bundle/builder.rs::BundleOpWriter` | `pred_actor` | `ActorIdx` | Non-null, with remap via `save_to_and_remap` |
| `storage/bundle/builder.rs::BundleOpWriter` | `insert` | `bool` | |
| `storage/bundle/builder.rs::BundleOpWriter` | `action` | `Action` | |
| `storage/bundle/builder.rs::BundleOpWriter` | `value_meta` | `ValueMeta` | |
| `storage/bundle/builder.rs::BundleOpWriter` | `pred_count` | `u32` | |
| `storage/bundle/builder.rs::BundleOpWriter` | `obj_ctr` | `Option<i64>` | Delta (cross-checked) |
| `storage/bundle/builder.rs::BundleOpWriter` | `key_ctr` | `Option<i64>` | Delta (cross-checked) |
| `storage/bundle/builder.rs::BundleOpWriter` | `id_ctr` | `i64` | Delta (cross-checked) |
| `storage/bundle/builder.rs::BundleOpWriter` | `pred_ctr` | `i64` | Delta (cross-checked) |
| `storage/bundle/builder.rs::BundleOpWriter` | `expand` | `bool` | |
| `storage/bundle/builder.rs::BundleOpWriter` | `mark_name` | `Option<String>` | Via `append_owned` |

### Remaining v0 encoders and migration blockers

| File | Column(s) | Encoder | Blocker |
|-|-|-|-|
| `op_set2/change.rs` | `value` | `RawCursor::encode_unless_empty` | **Raw encoding** — v1's `Column<Vec<u8>>` uses RLE which produces a different wire format. |
| `change_graph.rs` | `timestamps`, `messages`, `extra_bytes_meta` | `ColumnData<*>::save_to_unless_empty` | These are live v0 columns, not streaming encoders. See "Column storage migration" section. |
| `storage/bundle/builder.rs::BundleOpWriter` | `value` | `Encoder<'a, RawCursor>` | Raw. |

### Categories of work needed

1. **Raw encoding**: needs a `RawEncoder` in v1 (uncompressed byte concatenation). Affects `value` columns in `change.rs` and `bundle/builder.rs`, plus `extra_bytes_raw` in change_graph (already just a raw `Vec<u8>` blit).

Every other streaming encoder category (RLE, Bool, UInt, Delta) is now fully migrated.

### Reusable helpers added during the migration

- `hexane::Column::remap(|T| T)` — walks runs and re-emits each value through `f`, replacing the column. For `T: ColumnValueRef`.
- `hexane::RleEncoder::save_to_and_remap(out, |T| T)` / `save_to_unless_and_remap(out, unless, |T| T)` — like `save_to` / `save_to_unless` but applies `f` during save without a round-trip through `Column`. Walks the encoder's own buffer with `RleDecoder` and re-emits into a fresh encoder.
- `hexane::RleEncoder::append_owned(T)` — owned-value shorthand that complements `append(T::Get<'a>)`. Lets call sites with `Option<String>` or `String` values append without wrapping in `append_n_owned(v, 1)`.
- `hexane::DeltaEncoder<'a, T: DeltaValue>` — streaming delta encoder that mirrors `RleEncoder`'s interface (`append`, `append_n`, `extend`, `save`, `save_to`, static `encode` / `encode_to`). Byte-compatible with v0 `DeltaCursor` and v1 `DeltaColumn::from_values`. Supports all `DeltaValue` types: `u32`, `u64`, `i32`, `i64`, `usize` and their `Option<_>` variants.

## Column storage migration status (`automerge`)

Separate from the encoder migration above: automerge keeps live `ColumnData<*>`
columns in memory for the op set, the change graph, and a few indexes. These
are read/written via the v0 column API (`splice`, `iter`, `get`, etc.).

### Migrated columns (`hexane::Column` / `hexane::PrefixColumn`)

| File | Field | Type |
|-|-|-|
| `op_set2/columns.rs::Columns` | `key_str` | `hexane::Column<Option<String>>` |
| `op_set2/columns.rs::Columns` | `mark_name` | `hexane::Column<Option<String>>` |
| `op_set2/columns.rs::Columns` | `expand` | `hexane::Column<bool>` |
| `op_set2/columns.rs::Columns` | `insert` | `hexane::PrefixColumn<bool>` |
| `op_set2/columns.rs::Indexes` | `text` | `hexane::PrefixColumn<Option<u32>>` |
| `op_set2/columns.rs::Indexes` | `top` | `hexane::PrefixColumn<bool>` |
| `op_set2/columns.rs::Indexes` | `visible` | `hexane::Column<bool>` |
| `change_graph.rs::ChangeGraph` | `num_ops` | `hexane::Column<u64>` |
| `change_graph.rs::ChangeGraph` | `timestamps` | `hexane::DeltaColumn<i64>` |
| `change_graph.rs::ChangeGraph` | `messages` | `hexane::Column<Option<String>>` |
| `change_graph.rs::ChangeGraph` | `extra_bytes_meta` | `hexane::PrefixColumn<ValueMeta>` (via `PrefixValue for ValueMeta` → byte-length prefix) |
| `op_set2/columns.rs::Columns` | `value` | `hexane::RawColumn` — standalone byte arena with its own slab + Fenwick BIT, 4 KiB default `max_segments`; `splice_slice(&[u8])` + `splice(IntoIterator<Item = impl AsRef<[u8]>>)` |

### Remaining v0 `ColumnData<*>` columns and migration blockers

| File | Field | Type | Blocker |
|-|-|-|-|
| `op_set2/columns.rs::Columns` | `id_actor` | `ColumnData<ActorCursor>` | Trivial — `ActorCursor = RleCursor<64, ActorIdx>`, `RleValue for ActorIdx` already exists. Just hasn't been done. |
| `op_set2/columns.rs::Columns` | `id_ctr` | `ColumnData<DeltaCursor>` | **Delta encoding** — v1 has `DeltaColumn<i64>` but it's a different shape (no streaming/splice equivalent). The op set uses `splice`, `get_acc_delta`, `find_by_value`, etc. on this column, so the migration also needs the v1 delta column to support those operations. |
| `op_set2/columns.rs::Columns` | `obj_actor` | `ColumnData<ActorCursor>` | Trivial like `id_actor`. Used heavily in `iter_range`, `scope_to_value` (the latter requires sorted-ascending values which is true for obj_actor). |
| `op_set2/columns.rs::Columns` | `obj_ctr` | `ColumnData<UIntCursor>` | Trivial. Used in `scope_to_value(obj.counter(), ..)` — the v1 `Column<Option<u64>>::scope_to_value` we wrote already supports this pattern. |
| `op_set2/columns.rs::Columns` | `key_actor` | `ColumnData<ActorCursor>` | Trivial. |
| `op_set2/columns.rs::Columns` | `key_ctr` | `ColumnData<DeltaCursor>` | Delta — same blocker as `id_ctr`. |
| `op_set2/columns.rs::Columns` | `succ_count` | `ColumnData<UIntCursor>` | Uses `iter().with_acc()` and `calculate_acc()` for the prefix-sum semantics (text/list seek). The v1 equivalent is `PrefixColumn<u64>` — straightforward but needs callers updated to use `get_prefix`/`advance_prefix` instead of the v0 acc API. |
| `op_set2/columns.rs::Columns` | `succ_actor` | `ColumnData<ActorCursor>` | Sub-column (one entry per succ). Layout coupled to `succ_count`'s prefix sum. |
| `op_set2/columns.rs::Columns` | `succ_ctr` | `ColumnData<DeltaCursor>` | Delta + sub-column coupling. |
| `op_set2/columns.rs::Columns` | `action` | `ColumnData<ActionCursor>` | Trivial — `Action` is RLE and `RleValue for Action` already exists. |
| `op_set2/columns.rs::Columns` | `value_meta` | `ColumnData<MetaCursor>` | Trivial — `RleValue for ValueMeta` exists. **However** `MetaCursor` carries an `Acc` weight (`agg = item.length()`) used to compute byte offsets into the `value` column. v1 `PrefixColumn<ValueMeta>` would need the same — there's no aggregator hook on plain `Column`. |
| `op_set2/columns.rs::Indexes` | `inc` | `ColumnData<IntCursor>` | Signed integer column. v1 has `RleValue for i64` so the basic migration is trivial; just hasn't been done. |
| `op_set2/columns.rs::Indexes` | `mark` | `MarkIndexColumn` | Custom column type that wraps `ColumnData<MarkIndex>`. Has its own internal abstractions (start/end markers, lookup cache). Migration is a larger refactor, not a simple field swap. |
| `op_set2/op_set/mark_index.rs` | `MarkIndexColumn::data` | `ColumnData<MarkIndex>` | The custom `MarkIndex` cursor type would need a v1 encoding. Would also need to verify the prefix-sum semantics used by mark range queries. |

### Categories of work needed

1. **Trivial RLE migrations** (`id_actor`, `obj_actor`, `obj_ctr`, `key_actor`, `action`, `inc`, `num_ops`, `messages`): same pattern as the `text`/`top`/`visible` migrations already done. Each needs the field type changed plus all callers updated to use the v1 iterator/get/splice API.

2. **`succ_count` and friends** (`succ_count`, `succ_actor`, `succ_ctr`): the sub-column coupling means these three must migrate together. `succ_count` uses prefix-sum semantics so it becomes `PrefixColumn<u32>` (similar to `text`); the sub-columns are positioned by its prefix sum.

3. **Acc-weight columns** (`value_meta`): the v0 `MetaCursor` provides an `agg(item) = item.length()` that the column accumulates. Solved for `extra_bytes_meta` via `PrefixValue for ValueMeta { to_prefix(v) = v.length() as u128 }` — same impl applies to `value_meta`.

4. **Delta columns** (`id_ctr`, `key_ctr`, `succ_ctr`, `timestamps`): need a streaming `DeltaEncoder` and the splice/iter API on `DeltaColumn` to match `ColumnData<DeltaCursor>`. Largest single piece of v1 work.

5. **`MarkIndexColumn`**: standalone custom column with its own logic. Migration is its own project.
