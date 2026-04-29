# Hexane — Columnar Storage API

Hexane stores sequences of typed values using run-length encoding (RLE), delta
encoding, and other compact representations. Columns are typed by the value
they hold (`Column<u64>`, `Column<Option<String>>`, `Column<bool>`, etc.) and
expose random access, iteration, and in-place splice.

Data is held in **slabs** — `Vec<u8>` buffers holding RLE + LEB128 encoded
runs. Slabs are indexed by a B-tree keyed on per-slab aggregates for O(log S)
positional access, where S is the number of slabs. Mutations happen in place on
the affected slab; slabs automatically split when they exceed a segment budget
and merge when they become too small.

## Column Types

| Type | Description |
|------|-------------|
| `Column<T>` | Core column — random access, insert, remove, splice, iterate |
| `PrefixColumn<T>` | Column with O(log S) prefix-sum queries |
| `DeltaColumn<T>` | Stores deltas, presents realized values; `find_by_value` / `find_by_range` via per-slab min/max |
| `RawColumn` | Uncompressed byte arena with its own slab + B-tree index |

## Supported Value Types

**Non-nullable:** `u32`, `u64`, `i64`, `usize`, `NonZeroU32`, `String`,
`Vec<u8>`, `bool`

**Nullable:** `Option<T>` for each of the above

**Delta types:** `u32`, `u64`, `i32`, `i64`, `usize` and their `Option<>`
variants

Read operations return zero-copy borrows where possible — `String` yields
`&str`, `Vec<u8>` yields `&[u8]`.

## Quick Start

```rust
use hexane::v1::Column;

// Build a column from values
let mut col = Column::<u64>::from_values(vec![10, 20, 30]);
assert_eq!(col.get(1), Some(20));

// Mutate
col.insert(1, 15);           // [10, 15, 20, 30]
col.remove(3);               // [10, 15, 20]
col.splice(0, 2, vec![99]);  // [99, 20]

// Iterate
for val in col.iter() {
    println!("{val}");
}

// Serialize / deserialize
let bytes = col.save();
let restored = Column::<u64>::load(&bytes).unwrap();
```

### Prefix Sums

```rust
use hexane::v1::PrefixColumn;

let col = PrefixColumn::<u64>::from_values(vec![5, 3, 7, 2]);

// Exclusive prefix sum (sum of items before `index`)
assert_eq!(col.get_prefix(3), 15);       // 5 + 3 + 7
assert_eq!(col.prefix_delta(1..3), 10);  // 3 + 7

// First index where the prefix sum reaches `target`
assert_eq!(col.get_index_for_prefix(10), 2);

// Walk items paired with their running prefix sum
for (prefix, value) in col.iter() {
    println!("{value} (prefix so far: {prefix})");
}
```

### Delta Encoding

```rust
use hexane::v1::DeltaColumn;

// Constant-stride sequences compress to a single RLE run of deltas
let col = DeltaColumn::<u64>::from_values(vec![100, 200, 300, 400]);
assert_eq!(col.get(2), Some(300));

// Internally stored as deltas: [100, 100, 100, 100]
// Realized values are recovered via prefix sum over deltas
```

By default `DeltaColumn` carries a per-slab `SlabAgg` (`len + total +
min_offset + max_offset`) that unlocks value-range queries via min/max
pruning:

```rust
let col = DeltaColumn::<u64>::from_values(vec![100, 150, 200, 250, 300]);
assert_eq!(col.find_first(200), Some(2));

let hits: Vec<usize> = col.find_by_range(150..250).collect();
assert_eq!(hits, vec![1, 2]);
```

For a smaller per-slab aggregate when value queries are not needed,
instantiate with `PrefixWeightFn<T::Inner>` as the second type parameter.

## Implementing a Custom Type

To store a custom type in a `Column`, implement two traits: `ColumnValue` and
`RleValue`. The RLE encoding layer handles runs, slab splitting, merging, and
serialization automatically — you only define how a single value maps to/from
bytes.

### Example: `ValueMeta`

`ValueMeta` packs a type code (low 4 bits) and a byte-length (high 60 bits)
into a single `u64`.

#### The type

```rust
#[derive(Copy, Clone, Debug, Default, PartialEq, PartialOrd)]
pub struct ValueMeta(u64);

impl ValueMeta {
    pub fn type_code(&self) -> u8 { (self.0 as u8) & 0x0f }
    pub fn length(&self) -> usize { (self.0 >> 4) as usize }
}
```

#### `ColumnValue` — declare the encoding

For `Copy` types, implement `ColumnValue` instead of `ColumnValueRef` directly.
The blanket impl provides `ColumnValueRef` and `AsColumnRef` automatically.

```rust
use hexane::v1::{ColumnValue, RleValue};
use hexane::v1::rle::RleEncoding;
use hexane::PackError;

impl ColumnValue for ValueMeta {
    type Encoding = RleEncoding<ValueMeta>;
}
```

#### `RleValue` — define the wire format for a single value

The only question is: how does one `ValueMeta` become bytes, and how do you
read it back?

```rust
impl RleValue for ValueMeta {
    fn try_unpack(data: &[u8]) -> Result<(usize, ValueMeta), PackError> {
        let mut buf = data;
        let start = buf.len();
        let v = leb128::read::unsigned(&mut buf)?;
        Ok((start - buf.len(), ValueMeta(v)))
    }

    fn pack(value: ValueMeta, out: &mut Vec<u8>) -> bool {
        leb128::write::unsigned(out, value.0).unwrap();
        true
    }
}
```

#### That's it

`ColumnValue` + `RleValue` is all you need. Everything else is provided by
blanket impls:

- `ColumnValueRef` and `AsColumnRef` — from `ColumnValue`
- `NULLABLE` — defaults to `false`
- `get_null` — defaults to panic (correct for non-nullable types)
- `value_len` and `unpack` — default to calling `try_unpack`
- `Option<ValueMeta>` — automatically works as a nullable column type

`Column<ValueMeta>` and `Column<Option<ValueMeta>>` are both fully functional:

```rust
let mut col = Column::<ValueMeta>::new();
col.insert(0, ValueMeta(0x63));
col.insert(1, ValueMeta(0x65));

assert_eq!(col.get(0), Some(ValueMeta(0x63)));

let bytes = col.save();
let restored = Column::<ValueMeta>::load(&bytes).unwrap();
```

### Trait Cheat Sheet

For a custom `Copy` type, you implement **two traits** and get everything else
from blanket impls:

| Trait | You implement | Provided by blanket |
|-------|--------------|-------------------|
| `ColumnValue` | `type Encoding` | `ColumnValueRef`, `AsColumnRef` |
| `RleValue` | `try_unpack`, `pack` | `NULLABLE` (false), `get_null` (panic), `value_len`, `unpack` |
| `Option<T>` | — | `ColumnValueRef`, `AsColumnRef`, `RleValue` (nullable) |

For non-`Copy` types like `String` or `Vec<u8>`, implement `ColumnValueRef`
directly (with a custom `Get<'a>` type) and add an `AsColumnRef` impl.

### Design Decisions

**Wire format:** `try_unpack` and `pack` define the byte encoding for a single
value. The RLE layer wraps these in run-length encoded segments (repeat runs,
literal runs, null runs) automatically. Choose a compact encoding — LEB128
varints are a good default for integer-like types.

**Overriding defaults:** Override `value_len` when skipping is cheaper than
decoding (e.g. `String` avoids UTF-8 validation). Override `unpack` when
post-load decoding can skip validation that `try_unpack` performs on load.

## Architecture

### Wire Format

All column types share the same RLE + LEB128 wire format:

- **Repeat run:** signed LEB128 count (> 0) + one encoded value
- **Literal run:** signed LEB128 count (< 0) + |count| encoded values
- **Null run:** 0 byte + unsigned LEB128 null count

Booleans use a specialized encoding: alternating unsigned LEB128 run-length
counts starting with `false`. No value bytes are stored — the boolean value is
implicit from position parity.

### Slab Management

Each slab tracks its item count, segment count, and (depending on the column
type) its prefix sum and min/max offsets. Slabs are kept within a segment
budget (default: 16 segments). When a mutation pushes a slab over budget it is
split; when adjacent slabs are small enough they are merged. This keeps both
sequential iteration and random access efficient.

### Slab Index

Slabs are indexed by a pluggable structure keyed on a per-slab aggregate. Two
concrete backings ship:

- **Fenwick BIT** — used by `Column` and `PrefixColumn`. Fast, cache-tight;
  requires an invertible aggregate.
- **Slab B-tree** — used by `DeltaColumn`. Supports non-invertible aggregates
  (min/max) that Fenwick can't handle; typically wins on compound prefix-sum
  queries.

## Module Map

| File | Contents |
|------|----------|
| `mod.rs` | `ColumnValue`, `ColumnValueRef`, `RleValue`, `AsColumnRef`, `Run<V>`, re-exports |
| `column.rs` | `Column<T, WF>`, `Slab`, `SlabWeight`, Fenwick tree, `Iter<T>` |
| `encoding.rs` | `ColumnEncoding` trait, `RunDecoder` trait |
| `rle/` | `RleEncoding<T>` — RLE codec for numeric/binary types |
| `bool.rs` | `BoolEncoding` — specialized boolean RLE codec |
| `prefix.rs` | `PrefixColumn<T>`, `PrefixValue` trait, `PrefixIter` |
| `delta/mod.rs` | `DeltaColumn<T>`, `DeltaValue` trait, streaming `DeltaEncoder` |
| `delta/indexed.rs` | `IndexedDeltaWeightFn<T>` (default WF on `DeltaColumn`), min/max-based value queries |
| `raw.rs` | `RawColumn` — uncompressed byte arena |
| `index.rs` | `ColumnIndex` trait, `BitIndex` (Fenwick backing) |
| `btree.rs` | `SlabBTree<A>`, `SlabAgg`, `PrefixSlabWeight` — B-tree backing |
| `load_opts.rs` | `LoadOpts<T>` — builder for deserialization options |
| `encoder.rs` | Streaming `RleEncoder` and friends |

## Migrating from Hexane 0.2

Hexane 0.2 exposed a cursor-parameterized `ColumnData<C>` type. Columns now
carry the value type directly, and prefix-sum queries move from linear
iterator-based scans to O(log n) tree-backed queries.

### Type mapping

| 0.2 | Current |
|-|-|
| `ColumnData<UIntCursor>` (values only) | `Column<Option<u64>>` |
| `ColumnData<IntCursor>` (values only) | `Column<Option<i64>>` |
| `ColumnData<StrCursor>` | `Column<Option<String>>` |
| `ColumnData<ByteCursor>` | `Column<Option<Vec<u8>>>` |
| `ColumnData<BooleanCursor>` (values only) | `Column<bool>` |
| `ColumnData<UIntCursor>` + `get_acc` / `advance_acc_by` | `PrefixColumn<Option<u64>>` |
| `ColumnData<BooleanCursor>` + `get_acc` | `PrefixColumn<bool>` |
| `ColumnData<DeltaCursor>` | `DeltaColumn<i64>` |
| `ColumnData<RawCursor>` | `RawColumn` |
| `RleCursor<B, T>` (custom) | `Column<T>` with `impl ColumnValue for T` |

The decision is "do I need prefix-sum queries on this column?" — if yes,
`PrefixColumn`; if no, `Column`. Delta and raw stay on their dedicated types.

### Reading a value

```rust
// 0.2
let val = col.get(pos);                 // Option<Option<Cow<'_, T>>>

// Current
let val = col.get(pos);                 // Option<T>
```

Read operations now return plain `Option<T>` (or `Option<&str>` / `Option<&[u8]>`
for borrowed types). Null handling is encoded in the value type: `Column<T>` is
non-nullable, `Column<Option<T>>` is nullable. The double-`Option`-plus-`Cow`
return of 0.2 is gone.

### Iterating

```rust
// 0.2
for item in col.iter_range(range) {      // yields Option<Cow<'_, T>>
    // ...
}

// Current
for item in col.iter_range(range) {      // yields T (or T::Get<'_>)
    // ...
}
```

### Prefix sums

In 0.2, prefix queries were iterator methods: `iter().with_acc()`,
`advance_acc_by(n)`, `shift_acc`, `get_acc_delta(start, end)`. They're linear
scans.

In the current API, `PrefixColumn<T>` answers the same questions in O(log n):

| 0.2 | Current |
|-|-|
| `iter.shift_acc(n)` | `iter.advance_prefix(n)` |
| `col.get_acc_delta(start, pos)` | `iter.advance_to(pos)` (on a `PrefixIter`) |
| `col.get_acc(pos)` | `col.get_prefix(pos)` |
| `iter.with_acc()` | `col.iter()` already yields `(prefix, value)` pairs |

Convenience shorthands `col.seek(start, n)` and `col.get_delta(start, pos)`
wrap iterator construction for one-shot lookups.

### Delta columns

0.2 `ColumnData<DeltaCursor>` only stored `i64`. The current `DeltaColumn<T>`
is generic over the value type (`u32`, `u64`, `i32`, `i64`, and their
`Option<>` variants), keeps the same wire format, and adds value-range
queries:

```rust
let col = DeltaColumn::<i64>::from_values(vec![10, 20, 30, 40, 50]);
let hits: Vec<usize> = col.find_by_range(15..35).collect();
// [1, 2] — indices where the realized value lies in [15, 35)
```

The streaming `DeltaEncoder<'a, T>` mirrors `RleEncoder`'s interface
(`append`, `append_n`, `extend`, `save`, `save_to`, static `encode` /
`encode_to`) and is byte-compatible with 0.2 `DeltaCursor`.

### Splice

```rust
// 0.2
col.splice::<u64, _>(pos, del, []);                 // typed empty delete
col.splice(pos, 1, [Some(value)]);

// Current
col.splice(pos, del, [] as [Option<u32>; 0]);       // typed empty delete
col.splice(pos, 1, [Some(value)]);
```

Insert iterators take `T` (or `Option<T>`) directly. No `Cow` wrapping needed.

### Encoder helpers

The current API adds a few helpers that do not exist in 0.2:

- `Column::remap(|T| T)` — walks runs and re-emits each value through `f`,
  replacing the column in place.
- `RleEncoder::save_to_and_remap` / `save_to_unless_and_remap` — like `save_to`
  / `save_to_unless` but applies a mapping function during save without a
  round-trip through `Column`.
- `RleEncoder::append_owned(T)` — owned-value shorthand that complements
  `append(T::Get<'a>)` for callers holding `String` or `Option<String>`
  values.

### Wire format compatibility

Bytes written by the current API load cleanly into 0.2 for all shared
encodings (RLE, boolean, delta, raw). The reverse is true as well — there is
no schema migration step. Column types differ only in which in-memory
aggregate they carry, not in how bytes are stored.
