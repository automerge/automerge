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
    LoadOpts::new().with_length(3).with_fill(0u64),
).unwrap();
assert_eq!(col.len(), 3);
```

Loads can also stream: `load_iter` yields the column's runs *during*
the decode pass, so a consumer that needs the data anyway reads it while
the column builds — one pass over the bytes instead of load-then-iterate.
(This is how automerge builds its op indexes at load time.)

```rust
use hexane::{Column, LoadOpts};

let bytes = Column::<u64>::from_values(vec![1, 1, 2, 2, 2]).save();

let mut iter = Column::<u64>::load_iter(&bytes, LoadOpts::new());
let mut total = 0;
while let Some(run) = iter.try_next_run().unwrap() {
    total += run.value * run.count as u64;
}
let col = iter.finalize().unwrap();      // the fully-validated column
assert_eq!((col.len(), total), (5, 8));
```

The streaming decoders (`decoder`, `DeltaDecoder::new`) trust their
bytes, as they come from an encoder or `save` in the same program. For
bytes from outside, `try_decoder` and `DeltaDecoder::try_new` check them
first, as `load` does:

```rust
use hexane::{DeltaDecoder, PackError};

let bytes = hexane::DeltaEncoder::<u32>::encode(vec![5, 6, 7]);
let vals: Vec<u32> = DeltaDecoder::<u32>::try_new(&bytes)?.collect();
assert_eq!(vals, vec![5, 6, 7]);
assert!(hexane::try_decoder::<u64>(&[0x05]).is_err()); // a run with no value
assert!(DeltaDecoder::<u32>::try_new(&[0x7f, 0x7f]).is_err()); // -1 isn't a u32
# Ok::<(), PackError>(())
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
exactly this way. The encoding must be a code, so that each value's bytes
are its alone: [`RleValue`] spells out what that asks of an impl.

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

## Formal verification

Most of the crate's core is verified with [Verus](https://github.com/verus-lang/verus):
the proofs live beside the code in `verus!` blocks, and a normal build
compiles them away. To check them, run `cargo verus verify -p hexane
--features bijou64` with Verus installed.

**Proved:**

* The LEB128 and bijou64 codecs: each value has exactly one encoding.
* Loading: the RLE and bool loaders accept exactly the canonical encodings
  and build slabs with correct metadata holding the input's values. As the
  runs stream past, a slab is cut only at the end of a run, and a load's
  slab weights are accumulated from exactly the runs of each slab.
* The slab-level code: decoders, encoders, the edit rebuilds and slab
  merges, for both RLE and bool slabs.
* The slab indexes (the B-tree and the Fenwick tree).
* The column iterator and the column edit cursor, against the RLE and bool
  formats: every read yields exactly the column's values in the window
  (and an `nth` past the data stops at its end), and every edit leaves canonical slabs, with correct metadata, holding
  exactly the intended values. This includes delta columns, which store
  their deltas as an RLE column. Their index sums the deltas in `i64`, and
  while an edit is under way those sums can exceed `i64` (a delete that
  drops a whole slab joins its neighbours in the index before the delta
  that bridges them is written). The B-tree's invariant asks only that
  its item counts add up; its aggregates are exact wherever they are
  representable, which is all its queries need.
* Building a column: its index and length are computed from its slabs,
  checking that each slab's weight counts its items, and `Column::fill`
  makes a well-formed slab of the values asked for.
* Saving: `Column::save_to` writes the canonical encoding of the column's
  values, merging each slab onto those before it.
* The streaming encoders (`RleEncoder`, `BoolEncoder`): the bytes `save`
  returns are the canonical encoding of the values appended. With a segment
  budget, every slab they cut is well-formed, and together the slabs hold
  the values appended, in order. `save_to_unless` writes nothing only if
  every value equals the one given.
* Encoding into a caller's buffer (`encode_to`, `encode_to_unless`,
  `RleEncoderState`): the buffer's earlier bytes are left as they were,
  and followed by the canonical encoding of the values; the `_unless`
  variants write nothing only if every value equals the one given.
* The delta encoders (`DeltaEncoder`, `DeltaEncoderState`): the deltas
  they write realize exactly the values appended, or they panic on values
  spanning 2^63 or more.
* Delta columns' slab aggregates: a slab's sum of deltas and its least and
  greatest running sum are exact, both as `load` accumulates them run by
  run and as an edit or a build computes them from the slab, wherever the
  slab's sums fit `i64` (an edit can weigh a slab whose sums don't for a
  moment, and weighs it again once they do); and `load` accepts only data
  whose running sums all lie in the value domain, and whose values span
  less than 2^63.
  Merging two aggregates gives the aggregate of their deltas, as the
  B-tree's value search assumes.
* Reading a delta column (`DeltaIter`'s `next`, `nth` and `next_run`,
  and so `get`, `iter`, `iter_range` and `to_vec`): each value is the
  running sum of the deltas before it, or null, including after `nth`
  jumps to a far slab by the index's sums, given that the index holds
  each slab's aggregate and that the sums at slab boundaries fit `i64`.
  The running sum wraps as release arithmetic does, and is proved to be
  the deltas' sum modulo 2^64: the sum itself, wherever it fits.
* A delta column's value search (`find_by_value`, `find_by_range`,
  `find_first`): it yields exactly the items whose values lie in the
  range, in order, given the index's aggregates (as for reading). Slabs
  the index rules out hold none, and within a slab the items of a run of
  equal deltas are found by division, without overflow at the ends of
  `i64`.
* A delta column's sorted search (`DeltaColumn::scope_to_value`,
  `DeltaIter::seek_to_value`): where the values in the window are sorted
  (nulls first), it returns exactly the range of them equal to the
  target, or, if there are none, the empty range where the target would
  go, and leaves the iterator at its start; unsorted, it returns some
  range inside the window. It jumps by the index's prefix sums to the slab
  where the running sum first reaches the target, which skips no match
  when the window is sorted, whatever lies outside it. Given the index's
  aggregates, as for reading.
* A delta column's scans (`DeltaIter::scan_to_range` and
  `scan_to_value`): each returns the first item of the iterator's window,
  from its position, whose value lies in the range (or is null, for
  `scan_to_value(None)`), with the iterator just past it; or none, with
  the window used up. Slabs the index rules out hold no match. Given the
  index's aggregates, as for reading.
* The deltas a delta column's edits write (`splice`, `splice_runs`,
  `insert`, `remove` and the other splices, and the `DeltaEdit` cursor):
  the delta a value or a run of values needs after the running sum
  before it, and the cursor's account of what the values after it are
  owed. Each step of the cursor writes deltas realizing exactly the
  values inserted, the original values of the items it carries past or
  re-anchors, or nothing (a delete), and once it has paid what it owes,
  every original value after it is realized as it was; a splice's
  rewritten boundary delta does the same for the values after it, and
  the values it is computed from (the running sum before the edit, the
  nulls after it, and the value of the item after those) are what the
  column holds there. Over a model of the column as its sequence of
  deltas, with sums wrapping as the iterators read them.
* The value search of `Column` and its iterator (`scope_to_value`,
  `seek_to_value`): where the values in the window are sorted, it returns
  exactly the range of them equal to the target, or, if there are none,
  the empty range where the target would go. Unsorted, it returns some
  range inside the window.
* `RawColumn`.
* The splices `copy_ranges` takes: normalizing them checks their order
  and bounds, and gives a list in order in both columns, with every range
  inside the source and none doing nothing, which applied gives what the
  splices give one by one. Of the two ways to carry the list out, the
  direct one applies it to the destination with the edit cursor; the
  inverted one keeps the source, cuts out the gaps between its ranges and
  puts the destination's gaps back. For that one, the cuts leave exactly
  the ranges' items, and the gaps, as splices, are a normalized list that
  applied to those gives what the splices give. So both come down to the
  edit cursor applying a normalized list (over a model of the columns as
  sequences; that its seeks, deletes and run inserts, and `remove_n`, do
  that is glue).
* Prefix sums: a prefix column's slab weights hold each slab's sum, as an
  edit or a build computes them; `get_prefix` and `get_total` return the
  sum of the values before (through) the item, from the index's sum for
  the slabs before it; and the prefix iterator's running total
  (`PrefixIter`'s `next`, `nth`, `next_run` and `reset_prefix`), with its
  offset, is always the sum of the values before it, including after
  `nth` jumps to a far slab; and `sum_range(start..end)` is what added
  to the sum before `start` gives the sum before `end`.
* Prefix searches: `get_index_for_prefix` returns exactly the first index
  whose prefix sum reaches the target, or one past the end if none does
  (`get_index_for_total` is that less one); and `PrefixIter::advance_prefix(n)`
  lands on the first item in the iterator's window whose running sum,
  from the iterator's position, exceeds `n`, or else runs to the window's
  end and returns `None`. Both whenever the column's sum fits its prefix
  type, which loads and edits check.

**Trusted, not proved:** each value type's and codec's own encode and
decode, which the verified code reaches through traits (the contract on
`RleValue`); the thin glue that
connects the generic column types to the verified cores, and the closures
passed through it (including the slabs a new column is built from, which
come from the verified loaders, fills and encoders); that the weight a
column's index holds for each slab is the one its weight function
computes for the slab (the edit cursor is proved to keep them so, and a
load, which accumulates each slab's weight from the runs as they stream
past, is proved to give each slab exactly the runs that hold its values,
in order), so that accumulating a slab's runs gives what computing its
weight does; each prefix value type's `accumulate` and
`accumulate_run` (that they add its values, a run as its value
repeated) and `try_accumulate_run` (that when it succeeds it adds what
`accumulate_run` does), and that its prefix type's `+` is associative with `default`
its identity and `b + (a - b) == a`, as for wrapping integers; for the
prefix searches, that an unsigned prefix type's values are numbers, whose
sums (where they fit) and differences are its `+` and `-`, which its
`checked_add` adds when the sum fits, whose order is its `<=`, and which
its `/` and `try_into` divide and convert; the delta
value types' `to_opt`; that each value type's `Ord` agrees with its
`==`; and, for the delta columns' sorted search, that a delta value
type's `Ord` orders values as the `i64`s they are stored as, with nulls
first and values outside the domain last.

**Known limitations:**

* The prefix and delta columns' other logic is not verified: that a
  column's sum fits its prefix type, which the searches' results depend on
  (loads reject sums that would not, and edits panic, but those checks are
  not part of the proof); `delta`, which is glue over the verified
  iterator; the glue that drives a delta edit's verified arithmetic (that
  `splice` feeds its items to the verified plan in order, and that what
  the cursor reports carrying, deleting or jumping over is what it did);
  and that a
  delta edit leaves the values in the domain, so that the index's sums
  fit again, which `check_span` checks at run time. Its walk over the column is proved: it
  passes exactly when the values lie in the domain within a range less
  than 2^63 wide, and then every running sum fits `i64`. Its shortcut
  skips the walk where a bound on the values already fits: the values
  the column held (the index's root aggregate), widened by each value
  the edit writes. That is proved too: the edit cursor, `splice` and
  `from_values` keep the bound with their verified arithmetic
  (`SpanLedger`, `SpanPlan`, `ToDeltas`), and every value the column
  holds afterwards lies in it, so where it fits the walk would pass,
  given that the root aggregate bounds the values the column held.
* Where a precondition of the verified code cannot be shown from the
  specifications (a slab of `i64::MAX` bytes or more, or a column or
  encoder of nearly `usize::MAX` items), the code checks it and panics
  rather than assume it.

## Development

```bash
cargo test -p hexane            # ~525 tests incl. fuzzers & golden fixtures
cargo bench -p hexane           # divan benches: column_ops, load, remap, ...
```

Feature flags: `wasm` (console logging via `web-sys`). MSRV 1.80.

License: MIT.

[`Column<T>`]: https://docs.rs/hexane/latest/hexane/struct.Column.html
[`PrefixColumn<T>`]: https://docs.rs/hexane/latest/hexane/struct.PrefixColumn.html
[`DeltaColumn<T>`]: https://docs.rs/hexane/latest/hexane/struct.DeltaColumn.html
[`RawColumn`]: https://docs.rs/hexane/latest/hexane/struct.RawColumn.html
[`ColumnValue`]: https://docs.rs/hexane/latest/hexane/trait.ColumnValue.html
[`PrefixValue`]: https://docs.rs/hexane/latest/hexane/trait.PrefixValue.html
[`RleValue`]: https://docs.rs/hexane/latest/hexane/trait.RleValue.html
[`load`]: https://docs.rs/hexane/latest/hexane/struct.DeltaColumn.html#method.load
