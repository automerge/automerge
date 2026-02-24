# Hexane

Hexane is a columnar compression library that implements the encoding described in the
[Automerge Binary Format specification](https://automerge.org/automerge-binary-format-spec/).
It stores sequences of typed values using run-length encoding (RLE), delta encoding, and other
compact representations, organized in a slab-based B-tree structure for efficient random access
and in-place modification.

It was originally designed to serve [Automerge](https://github.com/automerge/automerge)'s
internal storage needs. Introducing it reduced Automerge's memory footprint to roughly 1% of
the previous implementation while keeping performance equal to or better than the old codebase
for most operations.

> **Note:** The public API is still maturing. A v1.0 redesign is planned that will give
> `ColumnData` a more `Vec`-like interface and remove some ergonomic rough edges. See
> [Planned v1.0 Changes](#planned-v10-changes) for details.

## Data Model

A `ColumnData<C>` stores a sequence of `Option<T>` values, where `T` is determined by the
cursor type `C`. `None` represents a null entry and is compressed efficiently as a null run.

Internally, data is held in a sequence of **slabs** — immutable, `Arc`-wrapped byte buffers
containing compressed runs of values. The slabs are organized in a `SpanTree` (a balanced
B-tree keyed by cumulative item count) that supports O(log n) positional seek, insert, and
splice. Each slab carries pre-computed metadata (item count, accumulator sum, min/max) so that
range queries and accumulator-based navigation can skip over slabs without decoding them.

## Encoding Types

The cursor type parameter of `ColumnData<C>` determines the encoding:

| Cursor type     | Item type | Encoding                           |
|-----------------|-----------|------------------------------------|
| `UIntCursor`    | `u64`     | RLE with unsigned LEB128 values    |
| `IntCursor`     | `i64`     | RLE with signed LEB128 values      |
| `StrCursor`     | `str`     | RLE with length-prefixed UTF-8     |
| `ByteCursor`    | `[u8]`    | RLE with length-prefixed bytes     |
| `BooleanCursor` | `bool`    | Boolean run-length encoding        |
| `DeltaCursor`   | `i64`     | Delta-encoded signed integers      |
| `RawCursor`     | `[u8]`    | Uncompressed raw bytes             |

For custom types, use `RleCursor<B, T>` directly where `T: Packable` and `B` is the maximum
slab size in bytes.

## Quick Start

```rust
use hexane::{ColumnData, IntCursor, UIntCursor, StrCursor};
use std::borrow::Cow;

// splice(index, delete_count, insert_iter) is the general-purpose mutation method.
let mut col: ColumnData<UIntCursor> = ColumnData::new();
col.splice(0, 0, [1u64, 2, 3, 4, 5]);

// Collect from an iterator — works with T or Option<T> interchangeably
let col2: ColumnData<UIntCursor> = [1u64, 2, 3].into_iter().collect();
assert_eq!(col2.to_vec(), vec![Some(1), Some(2), Some(3)]);

// Nullable columns: mix Some and None in the input
let col3: ColumnData<UIntCursor> = [Some(1u64), None, Some(3)].into_iter().collect();
assert_eq!(col3.to_vec(), vec![Some(1), None, Some(3)]);
```

## Reading Values

All read methods return `Option<Cow<'_, C::Item>>`:

- `Some(Cow::Owned(value))` — an in-bounds non-null value (copy types like `u64`, `i64`)
- `Some(Cow::Borrowed(&value))` — an in-bounds non-null value that borrows from the slab
  (used by `StrCursor` and `ByteCursor` to avoid copying)
- `None` — a null entry

`get(index)` returns an extra outer `Option` that is `None` when the index is out of bounds.

```rust
use hexane::{ColumnData, IntCursor};
use std::borrow::Cow;

let mut col: ColumnData<IntCursor> = ColumnData::new();
col.splice(0, 0, [1i64, 2, 3, 4, 5, 6, 7]);

// Random access — O(log n) seek + O(B) decode where B is runs-per-slab (small)
assert_eq!(col.get(0), Some(Some(Cow::Owned(1))));
assert_eq!(col.get(999), None); // out of bounds returns outer None

// Sequential iteration — most efficient; decoder state is amortized across a slab
let first_three: Vec<_> = col.iter().take(3).collect();
assert_eq!(first_three, vec![
    Some(Cow::Owned(1)),
    Some(Cow::Owned(2)),
    Some(Cow::Owned(3)),
]);

// Ranged iteration — O(log n) seek to start, then O(range) decode
let mid: Vec<_> = col.iter_range(3..5).collect();
assert_eq!(mid, vec![Some(Cow::Owned(4)), Some(Cow::Owned(5))]);
```

`StrCursor` and `ByteCursor` borrow directly from the internal slab where possible:

```rust
use hexane::{ColumnData, StrCursor};
use std::borrow::Cow;

let col: ColumnData<StrCursor> = ["hello", "world"].into_iter().collect();
assert_eq!(col.get(0), Some(Some(Cow::Borrowed("hello")))); // zero-copy
```

## Serialization

```rust
use hexane::{ColumnData, UIntCursor};

let mut col: ColumnData<UIntCursor> = ColumnData::new();
col.splice(0, 0, [1u64, 2, 3, 4, 5]);

// save() serializes to a new Vec<u8>
let bytes: Vec<u8> = col.save();

// save_to() appends to an existing buffer and returns the byte range written
let mut buf = vec![];
let range = col.save_to(&mut buf);

// load() deserializes; returns PackError if the data is malformed
let col2: ColumnData<UIntCursor> = ColumnData::load(&bytes).unwrap();
assert_eq!(col.to_vec(), col2.to_vec());

// load_unless_empty() treats an empty byte slice as a column of `len` null values
let col3: ColumnData<UIntCursor> = ColumnData::load_unless_empty(&[], 3).unwrap();
assert_eq!(col3.to_vec(), vec![None, None, None]);

// load_with() validates each decoded value with a callback; returns PackError on failure
let col4 = ColumnData::<UIntCursor>::load_with(&bytes, &|v| {
    match v {
      Some(n) if n > 100 => Some(format!("value {} too large", n)),
      _ => None,
    }
}).unwrap();
```

## Direct Encoding

For one-shot encoding without `ColumnData`, cursor types expose `encode` and
`encode_unless_empty` class methods that write directly to a byte buffer:  

```rust
use hexane::{StrCursor, BooleanCursor, Encoder};

let mut buf = vec![];
let word_range = StrCursor::encode(&mut buf, ["dog", "book", "bell"]);
assert_eq!(word_range, 0..15);

// encode_unless_empty writes nothing if all values are null/false
let bool_range = BooleanCursor::encode_unless_empty(&mut buf, [false, false, false]);
assert_eq!(bool_range, 15..15); // nothing written

// For incremental encoding, use Encoder directly
let mut encoder: Encoder<'_, StrCursor> = Encoder::default();
for word in ["dog", "book", "bell"] {
    encoder.append(word);
}
let _range = encoder.save_to(&mut buf);
```

## Advanced: Accumulators (`Acc`)

Several `ColumnData` and iterator methods deal with an `Acc` accumulator. `Acc` is a
monotonically non-decreasing `u64` — the cumulative sum of per-item `Agg` values. The
aggregate assigned to each item depends on the cursor type:

| Cursor          | `agg(item)`                               | Meaning of `Acc`             |
|-----------------|-------------------------------------------|------------------------------|
| `UIntCursor`    | the item value (clamped to `u32`)         | cumulative sum of values     |
| `IntCursor`     | the item value if it fits in `u32`        | cumulative sum of values     |
| `BooleanCursor` | 1 for `true`, 0 for `false`               | count of `true` entries      |
| `StrCursor`     | 0                                         | unused                       |
| `ByteCursor`    | 0                                         | unused                       |
| `DeltaCursor`   | the absolute item value (if fits in `u32`)| absolute position tracking   |

`get_acc(index)` returns the accumulated sum of all items *before* position `index`:

```rust
use hexane::{ColumnData, UIntCursor, Acc};

let col: ColumnData<UIntCursor> = ColumnData::from(vec![3u64, 3, 3]);
assert_eq!(col.get_acc(0), Acc::from(0usize));
assert_eq!(col.get_acc(1), Acc::from(3usize));
assert_eq!(col.get_acc(2), Acc::from(6usize));
```

`get_with_acc(index)` returns a `ColGroupItem { acc, pos, item }` bundling the pre-item
accumulator with the value.

`iter().with_acc()` returns a `ColGroupIter` that emits `ColGroupItem` values.

`iter().advance_acc_by(n)` skips forward until the cumulative accumulator has grown by `n`,
returning the number of items consumed. This enables O(log n + B) lookup by acc value:

```rust
use hexane::{ColumnData, UIntCursor};

// Column: 0, 1, 1, 0, 1, 1, 0  — acc grows at positions 1, 2, 4, 5
let col: ColumnData<UIntCursor> = ColumnData::from(vec![0u64, 1, 1, 0, 1, 1, 0]);
let mut iter = col.iter();
let items_consumed = iter.advance_acc_by(2u64); // skip until acc has grown by 2
// items_consumed == 4 (items 0,1,2 consumed, item 3 is the first with acc sum >= 2)
```

## Advanced: Ordered Value Lookup

For columns with sorted values, `scope_to_value` and the iterator's `seek_to_value` efficiently
locate the contiguous range of a specific value using B-tree binary search plus a linear scan:

```rust
use hexane::{ColumnData, UIntCursor};

// Values must be sorted for correct results
let col: ColumnData<UIntCursor> = [2u64, 2, 3, 3, 3, 4, 4].into_iter().collect();

let range = col.scope_to_value(Some(3u64), ..);
assert_eq!(range, 2..5);

// Restrict search to a sub-range
let range = col.scope_to_value(Some(3u64), 1..4);
assert_eq!(range, 2..4);
```

`iter().seek_to_value(value, range)` does the same and leaves the iterator positioned at
the start of the found range, ready to read further.

## Advanced: Iterator Suspension and Resumption

An iterator can be suspended and later resumed from the exact same position, provided
`ColumnData` has not been mutated since the suspend:

```rust
use hexane::{ColumnData, UIntCursor};

let col: ColumnData<UIntCursor> = [1u64, 2, 3, 4, 5].into_iter().collect();
let mut iter = col.iter();
iter.advance_by(2); // skip items 0 and 1

// Snapshot the iterator position
let state = iter.suspend();

// Resume — returns Err if `col` was mutated after the suspend
let mut resumed = state.try_resume(&col).unwrap();
assert_eq!(
    resumed.collect::<Vec<_>>(),
    iter.collect::<Vec<_>>()
);
```

## Performance Characteristics

- **Random access** (`get`): O(log n) B-tree lookup + O(B) slab scan, where B is the number
  of encoded runs in a slab (bounded and small by design).
- **Sequential access** (`iter`, `iter_range`): O(log n) initial seek; amortized O(1) per item
  — the decode state is carried across items within a slab.
- **Modification** (`splice`, `push`, `extend`): O(log n) B-tree lookup plus an O(slab-size)
  slab rewrite. The affected slab is replaced entirely; unaffected slabs are untouched.
- **Clone**: O(number of slabs) — slabs are `Arc`-wrapped, so cloning shares byte data.
- **Serialization** (`save`): O(n) — all slabs are read and re-encoded into a single buffer.

For bulk reads, strongly prefer `iter()` or `iter_range()` over repeated `get()` calls.
For bulk inserts at a known position, a single `splice()` is more efficient than many `push()`.

## Caveats

**Clone for rollback:** Because slabs are `Arc`-wrapped, cloning a `ColumnData` is cheap
(O(slab count)). Automerge exploits this by cloning before each transaction and dropping the
clone on rollback. This behaviour will likely be removed in v1.0 in favour of an explicit
inverse-splice rollback.

**Null semantics:** `is_empty()` considers a column "empty" if every value is `None` (or
`false` for `BooleanCursor`). This affects `save_to_unless_empty()` and friends.

**Slab size constant:** The `B` const parameter in `RleCursor<B, T>` controls the maximum
number of items per slab. Smaller values reduce the per-operation cost but increase B-tree
overhead. The defaults (64 or 128) are a reasonable starting point.

## Planned v1.0 Changes

- A `Vec`-like subscript API: `col[a..b]` as shorthand for `col.iter_range(a..b)`
- Remove pervasive `Cow` return types in favour of direct `&T` / owned `T`
- A cleaner `DeltaCursor` primitive based on `Delta { abs: i64, step: i64 }`
- Distinct cursor types for `Rle<u64>`, `Rle<Option<u64>>`, etc.
- Normalized min/max metadata (currently computed for all slabs, only needed for some)
- Transaction rollback via inverse splice rather than `Arc`-shared clone
- A SortedColumnData type so features like `seek_to_value` are not available on
  datasets that cant support it
