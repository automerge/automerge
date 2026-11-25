# Incoming author checks

[spec.rs](spec.rs) describes the two checks in the [production code](../../automerge/src/automerge/signing/incoming/author_checks.rs):

- **Predecessor lookup:** Return the author source of the first direct
  dependency with the required actor and sequence, or no source if none
  qualifies. An existing predecessor with no author is distinct from a missing
  predecessor. The caller supplies `seq - 1` and the lookup exposes only applied
  changes and already-valid candidate changes.
- **Cached proof reuse:** Reuse cached evidence only if every required frontier
  has an established author and a cached witness under that exact author.
  Missing authors don't match, even if both lookups return no author.

The callback contracts assume accurate, stable lookup results and exact
identity equality. They do not assume that a predecessor matches or that a
cache entry can be reused; those are the checks proved here. Reviewing the
call sites in [incoming.rs](../../automerge/src/automerge/signing/incoming.rs)
also establishes that the cache lookup is scoped to this delivery's verified
store and the exact frontier hash. The proof does not cover construction of
that store, computation of the frontier hashes, cryptography, or the complete
actor-author propagation loop.

Run with the [same toolchain as the ancestry worklist](../incoming_topology/README.md):

```sh
verus --edition=2021 --crate-type=lib --cfg automerge_verus --no-cheating \
    rust/verification/incoming_authors/lib.rs
```
