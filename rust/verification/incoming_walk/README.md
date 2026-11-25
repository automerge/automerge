# Incoming dependency walks

[spec.rs](spec.rs) specifies the [worklist used by production](../../automerge/src/automerge/signing/incoming/walk.rs).
The contract is independent of the queue and visited set:

1. Start at the roots. A reachable node's parents become reachable only if
   that node is marked *expand*.
2. Return exactly the reachable nodes marked *keep*, without duplicates.
   Both flags are independent. A kept node need not be expanded, and the
   worklist never asks for the parents of a node that doesn't expand.

`reached` is written as the least set closed under rule 1: a node is reachable
if every set that contains the roots and is closed under the rule contains it.
There is no assumption that the input is acyclic. The verifier checks both
soundness and completeness, no repeated visits, and termination.

## Trusted adapters in `incoming.rs`

The callbacks must supply a consistent classification for all reachable nodes
and the correct parent list for nodes they classify as expandable. They must
have a finite universe containing the roots and every followed edge. The
standard HashSet specifications also require concrete, consistent key equality
and hashing (`ChangeHash` uses its bytes for both). The callbacks are not
verified by this harness. Review these narrow translations of the graph:

| Caller | Roots | Expands | Keeps |
| --- | --- | --- | --- |
| Intrinsic group validation | The proposed head | Every reached group member, following only in-group edges | Every reached group member |
| Canonical boundary | The head's parents | Nodes below the head's level | The first equal-or-higher nodes on the paths |
| Canonical members | The head | Nodes outside the boundary past | Nodes outside the boundary past |

For canonical members, [the cut lemma](proof.rs) shows that if the excluded
set is parent-closed and does not include the head, stopping at the cut gives
exactly the head's ancestry **minus** that set. This does not prove that
`boundary_past_for` and the two sequence clocks compute the right excluded
set; that is a separate obligation. The unique-head check, level checks and
comparison with the delivered group are also outside this proof.

Run with the [same toolchain as the ancestry worklist](../incoming_topology/README.md):

```sh
verus --edition=2021 --crate-type=lib --cfg automerge_verus --no-cheating \
    rust/verification/incoming_walk/lib.rs
```
