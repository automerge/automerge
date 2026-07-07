# Hexane

**Columnar compression you can edit in place.**

Hexane stores sequences of typed values in the compressed column format of
the [Automerge binary
specification](https://automerge.org/automerge-binary-format-spec/) — and,
unusually for a columnar format, lets you *mutate* them there. Insert,
delete, and splice operate directly on the compressed bytes in
O(log n + affected bytes), without decompressing the column. A million
identical values cost three bytes at rest and still accept an insert at any
position in about a hundred nanoseconds.

It is the storage engine underneath [automerge](https://automerge.org), and
useful anywhere you want an editable, compressed, typed sequence.

## At a glance

| Type | What it adds | Reach for it when |
|------|--------------|-------------------|
| [`Column<T>`] | random access, splice, iteration | you just need an editable compressed sequence |
| [`PrefixColumn<T>`] | O(log n) running-sum queries | values are *sizes* or *counts* and you need offsets |
| [`DeltaColumn<T>`] | delta encoding, find-by-value | values are mostly sequential (IDs, counters) |
| [`RawColumn`] | uncompressed byte arena | variable-sized blobs addressed by byte range |

Supported value types: `u32`, `u64`, `i64`, `usize`, `NonZeroU32`, `String`,
`Vec<u8>`, `bool` — and `Option<T>` of each, stored as first-class nulls.
Reads are zero-copy where possible: a `Column<String>` hands out `&str`.

## Quick start

```rust
use hexane::Column;

let mut col = Column::<Option<String>>::new();
col.push(Some("hello"));        // &str accepted anywhere String is
col.push(None::<String>);       // nulls are part of the encoding
col.push(Some("world"));

assert_eq!(col.get(0), Some(Some("hello")));
assert_eq!(col.get(1), Some(None));

col.insert(1, Some("there"));   // splice into the compressed bytes
col.remove_n(2, 2);             // remove 2 items starting at index 2
assert_eq!(col.len(), 2);

// The wire format is the automerge binary column format.
let bytes = col.save();
let back = Column::<Option<String>>::load(&bytes).unwrap();
assert_eq!(back.get(1), Some(Some("there")));
```

## The column types

### `PrefixColumn<T>` — running sums as an index

When values are lengths or counts, the running sum *is* the interesting
number: it converts "record 2" into "bytes 8..15". `PrefixColumn` keeps
per-slab sums in its B-tree so any running sum is O(log n), and its
iterators answer both views of the accumulator with unambiguous names:
**`prefix()` is the sum *before* an item (exclusive), `total()` the sum
*through* it (inclusive).**

```rust
use hexane::PrefixColumn;

// Byte lengths of four records stored elsewhere.
let lens = PrefixColumn::<u32>::from_values(vec![5, 3, 7, 2]);

let pv = lens.get(2).unwrap();          // a PrefixedValue
assert_eq!(pv.value, 7);
assert_eq!(pv.prefix(), 8);             // bytes before record 2
assert_eq!(pv.total(), 15);             // bytes through record 2
// record 2 lives at pv.prefix()..pv.total()

// One-shot sums and inverse lookups:
assert_eq!(lens.get_prefix(3), 15);            // exclusive sum of 0..3
assert_eq!(lens.sum_range(1..3), 10);          // 3 + 7
assert_eq!(lens.get_index_for_total(10), 2);   // which record owns offset 10?

// Plain values, when you don't need sums:
assert_eq!(lens.values().get(1), Some(3));
```

### `DeltaColumn<T>` — sequential data, tiny bytes

Stores the *difference* between consecutive values, so mostly-sequential
data collapses into a few runs. A per-slab min/max index makes
`find_by_value` prune whole slabs at a time.

```rust
use hexane::DeltaColumn;

let ids = DeltaColumn::<u64>::from_values(vec![100, 101, 102, 103, 200]);
assert_eq!(ids.get(3), Some(103));
assert_eq!(ids.find_first(200), Some(4));
assert_eq!(ids.find_by_range(101..104).collect::<Vec<_>>(), vec![1, 2, 3]);

// Four sequential IDs + one jump = eight bytes on the wire.
assert!(ids.save().len() <= 8);
```

One contract to know: realized values must fit in a 2⁶³-wide range (for
unsigned types, `< 2⁶³`). This is not an implementation quirk — wire deltas
are `i64`, and deleting an element makes its neighbors adjacent, so *any*
pair of values may someday need their difference encoded. 2⁶³ is exactly
the boundary at which that stays representable. Writes outside the domain
panic; [`load`] rejects such data with an error; queries for unstorable
values return empty.

### `RawColumn` — the byte arena

For variable-sized payloads (automerge stores op values here), addressed by
byte ranges that a `PrefixColumn` of lengths typically provides. Splice
points become slab boundaries, so values never straddle slabs and reads
are zero-copy slices.

```rust
use hexane::RawColumn;

let mut raw = RawColumn::new();
raw.splice_slice(0, 0, b"hello");
raw.splice_slice(5, 0, b"world");
assert_eq!(raw.get(5..10), b"world");
```

## Streaming: encoders and decoders

When values arrive in order — building a change, parsing a file — skip the
column and stream:

```rust
use hexane::{Encoder, EncoderApi, DeltaEncoder};

// RLE bytes straight from an iterator:
let bytes = Encoder::<u64>::encode([7u64, 7, 7, 8]);

// ...and straight back out, no Column allocated:
let vals: Vec<u64> = hexane::decoder::<u64>(&bytes).collect();
assert_eq!(vals, vec![7, 7, 7, 8]);

// Delta-encode absolute values as they arrive:
let mut enc = DeltaEncoder::<u64>::new();
enc.append(100);
enc.append(101);
let delta_bytes = enc.save();
```

Encoders also know the sparse-file trick: `save_to_unless(out, sentinel)`
writes *nothing* when every value equals the sentinel, so an all-default
column costs zero bytes in a saved document.

## Loading untrusted data

`load` fully validates: every run header, every value, UTF-8 for strings,
delta running sums with overflow checks, domain checks per type. Only after
that validation do the unchecked hot paths ever touch the bytes.

```rust
use hexane::{Column, LoadOpts};

let bytes = Column::<u64>::from_values(vec![1, 2, 3]).save();

// Expect an exact length; treat empty input as "3 zeros".
let col = Column::<u64>::load_with(
    &bytes,
    LoadOpts::new().with_length(3).with_fill::<u64>(0),
).unwrap();
assert_eq!(col.len(), 3);
```

The failure-mode policy is uniform across the crate:

| Situation | Behavior |
|-----------|----------|
| Your code breaks a documented precondition (out-of-bounds splice, out-of-domain delta write, inverted range) | **panic** — it points at the bug |
| Untrusted bytes are malformed, hostile, or out of domain | **`Err(PackError)`** — `load` never panics |
| A query asks for a value the column could never contain | **empty result** — "not found" is the truthful answer |

## How it works

```text
values   ──RLE──▶  runs        7,7,7,8  →  [run 3×7][run 1×8]
runs     ──────▶  segments     one encoded run = one segment
segments ──────▶  slabs        ≤ max_segments per slab (default 64)
slabs    ──────▶  B-tree       per-slab aggregates: count, sum, min/max
```

Every mutation lands in exactly one slab (plus neighbors for big deletes):
find the slab in O(log S) through the B-tree, rewrite only the affected
runs with a memcpy-based byte splice, update one aggregate on the path
back up. Slabs split when they exceed the segment budget and merge with a
sibling when underfull, so both the tree and the slabs stay balanced under
arbitrary edit patterns.

| Operation | Cost |
|-----------|------|
| `get(i)`, `iter_range(a..b)` seek | O(log S + runs in slab) |
| `insert` / `remove` / `splice` | O(log S + bytes in slab) |
| prefix-sum / find-by-value queries | O(log S + runs in slab) |
| `save` | O(total bytes), merges boundary runs |
| `load` | O(total bytes), full validation |

## Rarely-used columns are almost free

A column that is *all default* — every value `None`, every flag `false` —
is one run, in one slab, forever:

```rust
use hexane::Column;

let flags = Column::<bool>::from_values(vec![false; 1_000_000]);
assert_eq!(flags.save().len(), 3);   // one run: a single LEB128 count

let mut sparse = Column::<Option<u32>>::from_values(vec![None; 1_000_000]);
sparse.splice(500_000, 0, [None::<u32>, None, None]);  // ~100ns: a count bump
assert_eq!(sparse.len(), 1_000_003);

// And save_to_unless elides it from the file entirely:
let mut out = Vec::new();
assert!(sparse.save_to_unless(&mut out, None::<u32>).is_empty());
```

This makes speculative schema — columns you *might* need — nearly free to
carry: sub-150ns per edit, a handful of bytes in memory, zero bytes on
disk. (Timings from the divan benches on an M-series laptop; run
`cargo bench` for yours.)

## Custom value types

Two small trait impls teach a `Copy` type the wire format, and both
`Column<T>` and `Column<Option<T>>` fall out — see the worked `ValueMeta`
example on [`ColumnValue`]. Implement [`PrefixValue`] too and
`PrefixColumn<T>` works as well; automerge's op metadata columns are built
exactly this way.

## Format stability

The byte format **is** the automerge document format, so it is frozen. The
test suite pins it with golden fixtures — byte-exact expected encodings for
every codec, captured against the original reference implementation. If a
change trips one of those tests, it isn't a test failure; it's a
compatibility break with every existing document.

Behind that sit the rest of the guarantees: property tests and fuzzers with
reference models for every column type, a structural invariant checker that
re-verifies every cached B-tree aggregate inside the fuzz loops, and
validate-on-load backing the one `unsafe` block in the crate.

## Development

```bash
cargo test -p hexane            # ~900 tests incl. fuzzers & golden fixtures
cargo bench -p hexane           # divan benches: column_ops, remap, ...
```

Feature flags: `wasm` (console logging via `web-sys`). MSRV 1.80.

License: MIT.

[`Column<T>`]: https://docs.rs/hexane/latest/hexane/struct.Column.html
[`PrefixColumn<T>`]: https://docs.rs/hexane/latest/hexane/struct.PrefixColumn.html
[`DeltaColumn<T>`]: https://docs.rs/hexane/latest/hexane/struct.DeltaColumn.html
[`RawColumn`]: https://docs.rs/hexane/latest/hexane/struct.RawColumn.html
[`ColumnValue`]: https://docs.rs/hexane/latest/hexane/trait.ColumnValue.html
[`PrefixValue`]: https://docs.rs/hexane/latest/hexane/trait.PrefixValue.html
[`load`]: https://docs.rs/hexane/latest/hexane/struct.DeltaColumn.html#method.load
