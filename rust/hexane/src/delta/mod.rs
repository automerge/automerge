//! The verified core of a delta column: its weights (`agg`), the domain
//! of its values (`ddomain`), its edits, iterators, searches and copies.
//! The column itself is [`crate::DeltaColumn`].

pub(crate) mod agg;
pub(crate) mod dbound;
pub(crate) mod dcopy;
pub(crate) mod ddomain;
pub(crate) mod dedit;
pub(crate) mod dedit_core;
pub(crate) mod dfind;
pub(crate) mod diter;
pub(crate) mod dscan;
pub(crate) mod dseek;
pub(crate) mod stream;

// ── DeltaRun ────────────────────────────────────────────────────────────────

/// A run from a [`DeltaColumn`](crate::DeltaColumn) iterator: one run of identical deltas,
/// realizing the values `[prefix + delta, prefix + 2*delta, …,
/// prefix + count*delta]`.  A null run (`delta: None`) is `count` nulls
/// and leaves the running value at `prefix`.
///
/// Carrying `prefix` makes each run self-describing, so
/// [`DeltaColumn::splice_runs`](crate::DeltaColumn::splice_runs) can copy aligned runs verbatim and fix
/// up misaligned ones by peeling the first item.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeltaRun {
    /// Realized running value immediately before the run.
    pub prefix: i64,
    /// The per-item delta; `None` is a run of nulls.
    pub delta: Option<i64>,
    pub count: usize,
}
