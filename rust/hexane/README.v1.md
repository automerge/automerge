# Hexane v1 — Columnar Storage API

The `v1` module provides a value-type-parameterized API for hexane's columnar
data storage. Columns are typed by the value they hold (`Column<u64>`,
`Column<Option<String>>`, `Column<bool>`, etc.) rather than by cursor type as in
v0.

Data is stored in **slabs** — `Vec<u8>` buffers holding RLE + LEB128 encoded
runs. Slabs are organized in a Fenwick tree (binary indexed tree) for O(log S)
positional access, where S is the number of slabs. Mutations happen in-place on
the affected slab; slabs automatically split when they exceed a segment budget
and merge when they become too small.

## Column Types

| Type | Description |
|------|-------------|
| `Column<T>` | Core column — random access, insert, remove, splice, iterate |
| `PrefixColumn<T>` | Column with O(log S) prefix-sum queries |
| `DeltaColumn<T>` | Stores deltas, presents realized (cumulative) values |
| `IndexedDeltaColumn<T>` | Delta column with segment-tree for O(log n) range min/max |
| `MirroredColumn<T>` | Paired v0+v1 column for cross-validation (testing) |

## Supported Value Types

**Non-nullable:** `u64`, `i64`, `String`, `Vec<u8>`, `bool`

**Nullable:** `Option<u64>`, `Option<i64>`, `Option<String>`, `Option<Vec<u8>>`

**Delta types:** `u32`, `u64`, `i32`, `i64` and their `Option<>` variants

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

let mut col = PrefixColumn::<u64>::from_values(vec![5, 3, 7, 2]);

// Get item with its running prefix sum
let (value, prefix) = col.get_with_prefix(2);
// value = 7, prefix = 15 (5 + 3 + 7)

// Find first index where prefix sum >= target
let (index, overshoot) = col.find_prefix(10);
// index = 2 (prefix at index 2 is 15), overshoot = 5
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

## Implementing a Custom Type

To store a custom type in a `Column`, implement two traits: `ColumnValue` and
`RleValue`. The RLE encoding layer handles runs, slab splitting, merging, and
serialization automatically — you only define how a single value maps to/from
bytes.

### Example: `ValueMeta`

Automerge's `ValueMeta` packs a type code (low 4 bits) and a byte-length (high
60 bits) into a single `u64`. In v0 it implements `Packable` and uses
`RleCursor<64, ValueMeta>`. Here's how the same type would work in v1.

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
read it back? Since the inner `u64` is already encoded as an unsigned LEB128
varint in v0, we keep the same format.

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
col.insert(0, ValueMeta(0x63)); // type=Uleb, length=6
col.insert(1, ValueMeta(0x65)); // type=Float, length=6

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

Each slab tracks its item count, segment count, and (for prefix columns) its
aggregate sum and min/max. Slabs are kept within a segment budget (default: 16
segments). When a mutation pushes a slab over budget it is split; when adjacent
slabs are small enough they are merged. This keeps both sequential iteration and
random access efficient.

### Fenwick Tree

Slabs are indexed by a Fenwick tree (BIT) keyed on a weight type. For plain
columns, the weight is `usize` (item count). For prefix columns, the weight is a
compound `(count, prefix_sum)` pair, enabling O(log S) prefix queries without
scanning slab data.

## Module Map

| File | Contents |
|------|----------|
| `mod.rs` | `ColumnValue`, `ColumnValueRef`, `RleValue`, `AsColumnRef`, `Run<V>`, re-exports |
| `column.rs` | `Column<T, WF>`, `Slab`, `SlabWeight`, Fenwick tree, `Iter<T>` |
| `encoding.rs` | `ColumnEncoding` trait, `RunDecoder` trait |
| `rle.rs` | `RleEncoding<T>` — RLE codec for numeric/binary types |
| `bool_encoding.rs` | `BoolEncoding` — specialized boolean RLE codec |
| `prefix_column.rs` | `PrefixColumn<T>`, `PrefixValue` trait, `PrefixIter` |
| `delta_column.rs` | `DeltaColumn<T>`, `DeltaValue` trait |
| `indexed.rs` | `IndexedDeltaColumn<T>`, segment tree for range min/max |
| `load_opts.rs` | `LoadOpts<T>` — builder for deserialization options |
| `mirrored.rs` | `MirroredColumn<T>`, `Mirrorable` — v0/v1 cross-validation |
| `tests.rs` | Property-based and unit tests |
