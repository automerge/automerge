# Causal fragment selection

The [spec](selection_spec.rs) describes a subset of *fragment occurrences*,
not of heads or hashes. A subset is **closed** if each requirement of each
selected occurrence has a supplier in the subset. A supplier is any occurrence
that contains the required hash as a member.

The [production worklist](../../automerge/src/admission/selection.rs) returns an
exclusion mask. The proof checks that its complement is closed and **contains
every other closed subset**. It also checks termination and the safety of the
counter updates. Multiple occurrences of a hash contribute separately to the
counter but have no effect on the definition of a supplier.

## Trusted adapter

[`select_fragments`](../../automerge/src/admission.rs) builds the indexed view:

- One index per distinct hash and one index per input fragment occurrence.
- Each fragment supplies all its member hashes. Initial counts include every
  member occurrence, including repeats.
- A fragment requires each of its boundary hashes **except** those already in
  accepted history or already among its own members. An omitted requirement
  cannot make a selected set fail closure: it is already satisfied by history
  or by that fragment itself.
- A false entry of the worklist's mask selects that input fragment occurrence;
  a true entry leaves it pending.

The proof assumes this translation and the result of querying accepted history.
It does not prove that the incoming fragments were correctly authenticated or
that the selected fragment union passes the separate actor-equivocation check.

Run with the [same toolchain as the ancestry worklist](../incoming_topology/README.md):

```sh
verus --edition=2021 --crate-type=lib --cfg automerge_verus --no-cheating \
    rust/verification/fragment_selection/lib.rs
```
