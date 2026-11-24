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
| [`AccColumn<T>`] | running sums in a type of your own | you add up something that is not a number |
| [`DeltaColumn<T>`] | delta encoding, find-by-value | values are mostly sequential (IDs, counters) |
| [`RawColumn`] | uncompressed byte arena | variable-sized blobs addressed by byte range |

Supported value types: `u32`, `u64`, `i32`, `i64`, `usize`, `NonZeroU32`,
`String`, `Vec<u8>`, `bool`, and `Option<T>` of each, stored as
first-class nulls; and types of your own (below). Reads are zero-copy where
possible: a `Column<String>` hands out `&str`, and takes one.

Every public function has a contract, which [Verus] has proved: see
[Formal verification](#formal-verification).

## Quick start

```rust
use hexane::Column;

let mut col = Column::<Option<String>>::new();
col.push(Some("hello"));        // a column takes and gives values borrowed
col.push(None);                 // nulls are part of the encoding
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
*through* it (inclusive).** A prefix column is a `Column` kept with sums
(`Column<T, Leb128, SumIndex>`): `iter` and `get` read its values,
`prefix_iter` and `get_prefixed` its values with their sums.

```rust
use hexane::PrefixColumn;

// Byte lengths of four records stored elsewhere.
let lens = PrefixColumn::<u32>::from_values(&[5, 3, 7, 2]);

let pv = lens.get_prefixed(2).unwrap(); // a PrefixedValue
assert_eq!(pv.value, 7);
assert_eq!(pv.prefix(), 8);             // bytes before record 2
assert_eq!(pv.total(), 15);             // bytes through record 2
// record 2 lives at pv.prefix()..pv.total()

// One-shot sums and inverse lookups:
assert_eq!(lens.get_prefix(3), 15);            // exclusive sum of 0..3
assert_eq!(lens.sum_range(1..3), 10);          // 3 + 7
assert_eq!(lens.get_index_for_total(10), 2);   // which record owns offset 10?

// Plain values, when you don't need sums:
assert_eq!(lens.get(1), Some(3));
```

The sums are kept in a type wide enough for them (`u64` for `u32` values,
`u128` for `u64`), and a column whose sums would not fit is refused on
load and panics on an edit. An [`AccColumn<T>`] sums in a type of your
own, which need not be a number (automerge's mark sets are kept this way).

### `DeltaColumn<T>` — sequential data, tiny bytes

Stores the *difference* between consecutive values, so mostly-sequential
data collapses into a few runs. A per-slab min/max index makes
`find_by_value` prune whole slabs at a time.

```rust
use hexane::DeltaColumn;

let ids = DeltaColumn::<u64>::from_values(&[100, 101, 102, 103, 200]);
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
panic; `load` rejects such data with an error; queries for unstorable
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
use hexane::{DeltaEncoder, Encoder};

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
delta running sums with overflow checks, domain checks per type. It accepts
exactly the canonical encodings of the values (each sequence of values has
one), so a column that loads saves back to the same bytes.

```rust
use hexane::{Column, LoadOpts};

let bytes = Column::<u64>::from_values(&[1, 2, 3]).save();

// Expect an exact length; treat empty input as "3 zeros".
let col = Column::<u64>::load_with(
    &bytes,
    LoadOpts::new().with_length(3).with_fill(0u64),
).unwrap();
assert_eq!(col.len(), 3);
```

Loads can also stream: `load_iter` yields the column's runs *during*
the decode pass, so a consumer that needs the data anyway reads it while
the column builds — one pass over the bytes instead of load-then-iterate.

```rust
use hexane::{Column, LoadOpts};

let bytes = Column::<u64>::from_values(&[1, 1, 2, 2, 2]).save();

let mut iter = Column::<u64>::load_iter(&bytes, LoadOpts::new());
let mut total = 0;
while let Some(run) = iter.try_next_run().unwrap() {
    total += run.value * run.count as u64;
}
let col = iter.finalize().unwrap();      // the fully-validated column
assert_eq!((col.len(), total), (5, 8));
```

The streaming decoders (`decoder`, `DeltaDecoder::new`) take bytes that
are a saved column, as an encoder or `save` in the same program writes
them. For bytes from outside, `try_decoder` and `DeltaDecoder::try_new`
check them first, as `load` does:

```rust
use hexane::{DeltaDecoder, PackError};

let bytes = hexane::DeltaEncoder::<u32>::encode(vec![5, 6, 7]);
let vals: Vec<u32> = DeltaDecoder::<u32>::try_new(&bytes)?.collect();
assert_eq!(vals, vec![5, 6, 7]);
assert!(hexane::try_decoder::<u64>(&[0x05]).is_err()); // a run with no value
assert!(DeltaDecoder::<u32>::try_new(&[0x7f, 0x7f]).is_err()); // -1 isn't a u32
# Ok::<(), PackError>(())
```

The failure-mode policy:

| Situation | Behavior |
|-----------|----------|
| Untrusted bytes are malformed, hostile, or out of domain | **`Err(PackError)`** — `load` never panics |
| A value the column cannot hold (a delta out of its domain, a sum that does not fit) | **panic**, before the column changes where the contract says so |
| A call outside its contract's `requires` (an out-of-bounds splice, a decoder of bytes that are no saved column) | a program checked by Verus cannot make one; any other has to keep the contract itself, and gets an unspecified result or a panic, never anything unsafe (the crate has no `unsafe`) |
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

let flags = Column::<bool>::from_values(&vec![false; 1_000_000]);
assert_eq!(flags.save().len(), 3);   // one run: a single LEB128 count

let mut sparse = Column::<Option<u32>>::fill(1_000_000, None);
sparse.splice(500_000, 0, [None, None, None]);  // ~100ns: a count bump
assert_eq!(sparse.len(), 1_000_003);

// And save_to_unless elides it from the file entirely:
let mut out = Vec::new();
assert!(sparse.save_to_unless(&mut out, None).is_empty());
```

This makes speculative schema — columns you *might* need — nearly free to
carry: sub-150ns per edit, a handful of bytes in memory, zero bytes on
disk.

## Custom value types

A `Copy` type of your own is stored as one of the built-in numbers
([`VNewtype`]): say which, and how to convert. A number that is no value
of the type is refused on load, so each value keeps one encoding. Both
`Column<T>` and `Column<Option<T>>` then work.

```rust
use hexane::{Column, PackError, VNewtype};

#[derive(Clone, Copy, PartialEq, Debug)]
enum Kind {
    Int,
    Str,
}

impl VNewtype for Kind {
    type Base = u64;

    fn to_base(self) -> u64 {
        match self {
            Kind::Int => 0,
            Kind::Str => 1,
        }
    }

    fn from_base(b: u64) -> Result<Self, PackError> {
        match b {
            0 => Ok(Kind::Int),
            1 => Ok(Kind::Str),
            _ => Err(PackError::InvalidValue(format!("not a kind: {b}"))),
        }
    }
}

let mut col = Column::<Option<Kind>>::new();
col.push(Some(Kind::Int));
col.push(None);
assert_eq!(col.get(0), Some(Some(Kind::Int)));
assert!(Column::<Kind>::load(&[2, 2]).is_err()); // 2 is no kind
```

A type with an encoding of its own implements [`RleValue`] instead, and a
codec of your own [`Codec`]. A type whose values add up to something in a
prefix column implements [`VAmount`] (a number) or [`VAdds`] (an
accumulator of your own, for an [`AccColumn<T>`]). Each of these traits
states laws that a client checked by Verus proves; any other client has
to keep them itself.

## Format stability

The byte format **is** the automerge document format, so it is frozen.
The proofs show that what a column saves is the canonical encoding of its
values, and that a load accepts exactly the canonical encodings; the tests
tie that encoding to the format automerge has always written. Those of
`tests/verified_api.rs` once ran the reference implementation beside
every check, and kept a digest of what it wrote and read; they check each
digest still (`tests/golden`). If a change trips one of them, it isn't a
test failure; it's a compatibility break with every existing document.

## Formal verification

Every public function of the crate has a contract, stated in terms of
the column's values as a sequence (`col@`), and [Verus] has proved that
the code keeps every one of them. The proofs live beside the code in
`verus!` blocks, and a normal build compiles them away. To check them,
run `cargo verus verify -p hexane --features bijou64,vstd/nonzero_internals`
with Verus installed.

A client that is itself checked by Verus can build its own proofs on the
contracts: that what it reads back is what it wrote, or that a column it
saved loads as the same values and saves back to the same bytes.
`src/verified/client_proof.rs` holds such clients, written as a user of
the API would write them, which the same run proves; `verus-client/`
proves them again as a crate outside hexane, from what hexane exports
alone.

The specification can be read apart from the code and the proofs: each
file of `src/verified` (and `src/raw.rs`) is the specification of what it
declares, every body there a call of a function proved in the file's
module `imp`. The module docs of `hexane::verified` ("Reviewing the
specification") say which files to read and how.

**Proved**, among the contracts:

* Each sequence of values has exactly one canonical encoding, for both
  codecs (LEB128 and bijou64); `save` writes it, a load accepts exactly
  the canonical encodings (and so a column that loads saves back to the
  same bytes), and a load that succeeds holds the values the bytes
  encode. So do the streaming encoders and decoders, and loads run by run.
* Every edit (`splice`, `insert`, `remove`, `remap`, `copy_ranges`, the
  edit cursors) leaves the column holding exactly the values it should,
  and the column's slabs, index and counts consistent, as a type
  invariant.
* Every read (`get`, the iterators, their moves, runs, searches, and
  suspending and resuming) gives exactly the column's values; a search of
  values in order finds exactly the places that hold the target.
* Prefix and accumulator columns: each sum is the sum of the values, and
  a search by sum finds the first place that reaches it; a delta column's
  values stay in its domain, and its finds and scans give exactly the
  places whose values lie in the range.

**Trusted**, besides Verus, Z3 and vstd's specifications of the standard
library, is all in one file, `src/trusted.rs`:

* the helpers that panic, each only in that it does not return, and the
  constructors of `PackError`;
* three edits of a range of a `Vec` (`splice_vec`, `splice_bytes`,
  the raw column's `drain_slabs`), whose specifications are assumed, and
  four helpers that claim nothing (`add`, `recycle_buf`, `carry_budget`,
  `take_buf`);
* for strings, the specification of `core::str::from_utf8`, and that two
  `str`s of the same characters are one.

Of what a client hands it, the crate asks: of an iterator, the laws Verus
asks of every iterator; and of a client's own value type, encoding,
codec, sum, accumulator or repositionable iterator, the laws of its trait
(`VNewtype`, `RleValue`, `Codec`, `VAmount`, `VAdds` and `VAccum`,
`Shiftable`). A client checked by Verus proves them; any other keeps
them itself.

**Not proved:** that a splice or an encoder's `extend` of a caller's
iterator ends. It ends when the iterator does; one of a slice is proved
to end.

## Development

```bash
cargo test -p hexane --features bijou64   # unit, regression and API tests
cargo verus verify -p hexane --features bijou64,vstd/nonzero_internals
```

Feature flags: `bijou64` (the second codec), `wasm` (console logging via
`web-sys`). MSRV 1.90.

License: MIT.

[Verus]: https://github.com/verus-lang/verus
[`Column<T>`]: https://docs.rs/hexane/latest/hexane/struct.Column.html
[`PrefixColumn<T>`]: https://docs.rs/hexane/latest/hexane/type.PrefixColumn.html
[`AccColumn<T>`]: https://docs.rs/hexane/latest/hexane/type.AccColumn.html
[`DeltaColumn<T>`]: https://docs.rs/hexane/latest/hexane/struct.DeltaColumn.html
[`RawColumn`]: https://docs.rs/hexane/latest/hexane/struct.RawColumn.html
[`VNewtype`]: https://docs.rs/hexane/latest/hexane/trait.VNewtype.html
[`RleValue`]: https://docs.rs/hexane/latest/hexane/trait.RleValue.html
[`Codec`]: https://docs.rs/hexane/latest/hexane/trait.Codec.html
[`VAmount`]: https://docs.rs/hexane/latest/hexane/trait.VAmount.html
[`VAdds`]: https://docs.rs/hexane/latest/hexane/trait.VAdds.html
