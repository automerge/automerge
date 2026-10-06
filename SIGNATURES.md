# Detached Change Signatures

## Status

This describes the experimental detached-signature support on this branch.
The wire format and the public API may change before release.

## Summary

A signature is an annotation on a change hash, stored beside the change
rather than inside it, so attaching one never alters the change, its hash or
its dependencies. Automerge retains and relays every record it is given, and
verifies exactly one scheme, Ed25519, where a record can authenticate history.

Signing happens outside Automerge, because private keys often live in Web
Crypto or hardware and signing is asynchronous. Verification happens inside
Automerge, synchronously and deterministically, so that two documents with the
same changes and the same records always agree on what is visible.

## Records

A record is a change hash plus a tagged byte string. The first byte names the
scheme:

```text
0x00 || 64-byte Ed25519 signature
0x01 || application-defined bytes
```

Records are deduplicated by their exact bytes and unioned on merge. Custom,
malformed, unknown-tag and cryptographically invalid records are all kept, so a
process running this code can relay records for schemes it does not
understand, and a valid Ed25519 record for the same change can arrive later.

Rust callers construct records with `SignatureInput::ed25519(hash, signature)`
and `SignatureInput::custom(hash, bytes)`; JavaScript callers pass
`{ hash, scheme: "ed25519" | "custom", signature }`. Both go through
`add_signatures` / `Automerge.addSignatures`.

In practice a caller passes what `signing_requests` asked for, which is this
document's own unsigned commits, and records from elsewhere arrive in chunk
streams instead. Nothing constrains it to that, because nothing needs to: a record
only authenticates by verifying under the author of the change it targets, so one
which is not this document's to add cannot make anything visible that a signature
from that author would not have. Records on history the document does not hold are
retained like any other, and relayed.

## Ed25519 verification

The author of an actor is the byte string carried by the actor's first change.
For Ed25519 it must be a 32-byte verifying key which is not a weak key. A
record verifies if its payload is a 64-byte signature which passes strict
Ed25519 verification over the raw 32-byte change hash under that key. There is
no domain separator: the change hash already covers the actor, sequence
number, dependencies, author and operations.

Verification of a record is permanent: the key it is checked against is fixed
by the change itself, so a record which verifies once is remembered as valid.
That memory is never serialized and is rebuilt on demand.

## Modes

A document is created or loaded in one of two modes and forks inherit it. The mode
is part of the document's configuration, alongside its text encoding, because both
decide what its existing history means and so neither can change afterwards:

```rust,ignore
let doc = Automerge::new_with_config(
    Config::new()
        .text_encoding(TextEncoding::Utf8CodeUnit)
        .signature_mode(SignatureMode::Authenticated),
);

let loaded = Automerge::load_with_options(
    &bytes,
    LoadOptions::new()
        .config(Config::new().signature_mode(SignatureMode::Authenticated))
        .author(author)
        .own_storage(true),
)?;
```

In **legacy** mode nothing changes: received changes are applied as they
always have been and records are merely carried along.

In **authenticated** mode a document holds every chunk it receives, apart from
its own commits, until valid Ed25519 signatures from the relevant authors cover
it and its dependencies are met. Only then do its changes enter the document.
Local commits are visible immediately, since signing them may take a round trip
to an external key.

Records must accompany the history they authenticate. A chunk therefore only ever
waits for an author to become reachable — never for bytes which may never arrive —
and a peer which strips records is refused rather than silently withheld. Where
the author is already reachable, the record must actually verify, so a stripped or
forged record fails at import.

The chunk stream is the only import path: `load_incremental`, which `addCommits`
and `addFragments` use. `apply_changes` and `merge` hand over decoded changes with
no stream to read boundaries or records from, and are refused, as is sync.

## Coverage

Authentication is decided per chunk, where a chunk is one unit of transport: a
change chunk is a level-0 commit, a bundle chunk is a fragment, and a
whole-document chunk is a document. A chunk declares what it needs — one record
which verifies at each actor maximum it contains — and is held whole until it has
them, then admitted whole. Nothing is authenticated change by change, so no change
becomes visible while the rest of its chunk waits.

One record per actor suffices because a record's target hash commits to that
change's entire causal history. That holds only if the actor's other changes in
the chunk really are in the maximum's history, so a chunk is checked for exactly
that when it is decoded, following dependencies within the chunk. Without the
check an attacker could pair a genuine signed `C(A, n)` with a fabricated
low-sequence change by `A` outside its history: that change's dependencies are the
attacker's to choose, so it would become causally ready and visible on its own
while the genuine history it displaced waited behind a sequence conflict.

Confining the walk to the chunk is exact rather than approximate. A fragment's
members are causally closed on `(boundary, head]`, so any change causally between
two members is itself a member; a change below the boundary is already applied and
was authenticated by whichever chunk carried it.

A record covers only its own actor's changes. A dependency made by another actor
needs its own record, even when both actors share an author, because the key is
fixed by each actor's own first change.

A held chunk waits in the same queue as a change whose dependencies are missing,
because the two waits are one wait. A chunk's records are verified under the
authors of the actors it contains, and an actor's author is declared by its first
change, which is in the causal history of everything that actor later writes. A
chunk whose key cannot be found yet is one whose dependencies have not arrived,
and a chunk whose dependencies have all arrived always has its keys. So the queue
has one readiness test, and runs the record check at the moment a chunk would
otherwise be released. A chunk which requires signatures is released whole, so
no change in it becomes visible while the rest waits; a chunk which requires
none, which is every chunk in legacy mode, releases change by change as it always
has. A chunk whose record turns out not to verify once its author is reachable is
dropped: nothing will ever make that record verify, and holding it would only
hide the failure. The same history may arrive again properly signed.

Held chunks are untrusted. One actor may not claim the same sequence twice inside
a single chunk, since no record could then say unambiguously which change it
covered, and such a chunk is refused outright rather than held. Across chunks the
claims may conflict; whichever is admitted first wins.

A save writes every queued chunk, whether it waits for a signature or a
dependency: both are history the document holds and cannot apply yet, and neither
is necessarily re-fetchable. Each goes back *after* the document chunk, as the
chunk it arrived as, so a load makes the same admission decision over again rather
than taking it on trust — a trailing chunk is never trusted by association with
the document chunk before it. Writing a chunk's changes individually instead would
lose it: records cover a chunk's per-actor maxima, so regrouped history would need
records the document never had, and the reload would refuse it. That is as true of
an authenticated chunk waiting on a dependency as of one waiting on a signature,
which is one reason the two share a queue.

## Signing and export

A signature at an actor's greatest sequence authenticates that actor's whole
prefix, which is everything admission asks for. Export asks for more. A fragment
travels alone, and its receiver can only check the record on the exact hash it is
given, so every fragment boundary needs a signature of its own. `signing_requests`
therefore reports two things: changes above an actor's signed frontier, which is
where a fresh local commit sits, and the actor maxima of fragments in the index
which carry no record of their own. Without the second, signing only the latest
change would lift the frontier over the boundaries below it and leave them
unexportable with nothing ever asked for to fix it.

A fragment whose maxima are not all signed is withheld rather than exported
without them, because a receiver refuses such a chunk whole. For history received
rather than authored this does not arise: a chunk is admitted only with records at
its own actor maxima, and the fragments a document can form are the fragments it
received, so the records it holds are the ones its cover needs.


Fragments are the unit of export, and a fragment is authenticated by records
on its actor maxima, the highest-sequence change of each actor it contains.
`signing_requests()` cannot read the fragment cover, because the cover is exactly
the history which does *not* need signing. It lists the changes above each actor's
signed frontier whose actor has an Ed25519 author, with the exact bytes to sign:

```rust
for request in doc.signing_requests() {
    let signature = signer.sign(request.bytes_to_sign()).await;
    doc.add_signatures([SignatureInput::ed25519(request.change_hash(), signature)])?;
}
```

A request carries the change hash, the bytes to sign, and the author — which is the
verifying key, and so what selects the private half to sign with. It deliberately
does not name the actor whose chain the change belongs to: several actors may share
an author, so it would not narrow the choice of key.

Actors without an author, or with an author that is not an Ed25519 key, cannot
be signed and are not reported.

Every fragment the document offers is exportable. That is an invariant, not a
filter: a fragment is not formed until the history it contains is authenticated,
and enumeration starts from the authenticated frontier rather than the document's
heads. An unsigned local commit is in the change graph and visible immediately, but
it takes no part in fragment processing until a record covers it.

The invariant matters for more than tidiness. A fragment is exported whole, and
forming one supersedes the lower-level fragments it absorbs. A change whose hash
begins with a zero byte — one in 256 — forms a higher-level fragment, so an
unsigned local commit which was allowed to form one would absorb the signed history
beneath it into something unexportable, withdrawing history which was exportable a
moment earlier. Deferring formation is what prevents that; without it a single
unsigned commit can make an entire document unexportable until it is signed.

Deferred formation happens in arrival order when the records arrive, so the index
ends up as it would have done had nothing been deferred.

A document which replays or reconstructs history before it knows what is
authenticated — a fork at heads, or loading a document chunk — builds its index as
though all of it were, which would offer an unsigned commit's fragment as
exportable. Those paths rebuild the index once the mode and the records are in
place, producing the index the history would have produced. Bundled fragments carry the records at their
actor maxima as a trailing signature chunk, so a fragment travels with its
evidence. The JavaScript `getCommits` and `getFragments` produce those bytes
and `addCommits` and `addFragments` accept them.

Encoding a signature chunk is not exposed. Deciding which records a chunk needs is
what this module is for, so a caller assembling a stream by hand would be
reimplementing that decision, and the streams it would be assembling are the ones
the fragment export already produces, records included. Nor is the amount of
history a document holds back exposed: held history is simply history which is not
there yet, and a document reports it the way it reports anything absent.

## Storage

Records are written as a trailing chunk of type 4 after the document chunk
and any change chunks:

```text
version (1 byte)
count of target hashes (LEB128)
for each target: hash (32 bytes), record count (LEB128),
    for each record: length (LEB128), bytes
```

Records are sorted by hash and then by bytes, so the encoding of a set of
records is canonical. The chunk checksum detects corruption; validity is only
ever established by Ed25519 verification.

`save` writes every record, and in authenticated mode it writes every change too:
a save is storage, not an export, and `own_storage` is how it comes back.

The incremental saves are exports, because they produce bytes to send. In
authenticated mode `save_after(heads)` and `AutoCommit::save_incremental` therefore
fall back to the fragments carrying anything since `heads`, rather than loose
change chunks. That is the only authenticated way out — a loose change has no
fragment boundary and so no signature set — and it means an incremental save is
gated exactly as any other export: unsigned history is simply not in an exportable
fragment yet, so it is withheld with no special case, and goes out once it is
signed. A fragment straddling `heads` is sent whole, so the receiver sees changes
it already has and skips them; in the common case the recent tail is still loose
level-0 commits of one change each and nothing is oversent. `save_incremental`
advances its cursor only past history which actually went out, or signing a change
after the cursor passed it would strand that change.

In legacy mode the incremental saves emit loose changes as they always have, and
`AutoCommit::save_incremental` also writes records which arrived since the last
save, so a signature added after its change travels once without repeating it.

## Loading

Both modes read a stream through one reader, which keeps chunk boundaries rather
than flattening them, because in authenticated mode the chunk is the unit of
authentication: what signatures a chunk needs is a property of the chunk, not of
each change in it. A legacy load ignores the grouping and applies everything it
read, so the two modes part at one point on the way in — whether a chunk must be
covered before any of it is applied — rather than running separate loaders. A
document chunk whose heads are all present is skipped in both modes, and a corrupt
or truncated tail leaves the prefix of whole chunks before it, which an
incremental load keeps and `load` accepts or refuses according to
`OnPartialLoad`.

An authenticated `load` requires a signature for every actor's greatest sequence
in a leading document chunk. One record there covers that actor's whole chain, and
a document carries its own authors, so whether a document chunk is admissible is
always decidable on arrival: an inadmissible one is refused rather than held,
exactly as an uncovered change or bundle chunk is. The records needed may appear
later in the same stream, which is why `load` reads them ahead before deciding.
When they cover it, the document reconstructed from the chunk is kept rather than
decoded a second time.

A document the application saved itself is the one case where that rule is too
strong. Its own unsigned commits were visible when it saved them and have nothing
to verify yet, so the rule would make its own storage unloadable. That is what
`LoadOptions::own_storage(true)` (`ownStorage` in JavaScript) is for: it admits
unsigned history assigned the author given to `LoadOptions::author`, which is how
an unsigned draft survives a restart and can still be signed afterwards. The
author has to be known as the chunk is checked, which is why it is a load option
and not something set on the document afterwards.

`own_storage` is an assertion about where the bytes came from, and only the
application can make it. It cannot be inferred from the bytes, because an author
is a public key asserted by the chain which claims it, so any chain can claim any
author. Passing it for bytes from a peer would let that peer put unsigned history
into the document under the application's own author — and have it offered back
through `signing_requests`. History by any other author still needs its signature
either way.

This is also what upholds the export invariant. The only history a document holds
without a signature is history it authored, and a record at an actor's sequence `n`
covers that actor's whole prefix, so unsigned history is always a suffix of the
document's own chain. Nothing covered can depend on it, so no fragment which has
formed can contain it, and every fragment the document offers is one it can
export. Were unsigned history by another author admitted, a fragment could form
over it as soon as a local commit above it was signed, and that fragment would be
offered for export while carrying members no signature covers.

A document chunk is a legitimate member of a stream: `save` concatenated with
`save_incremental` is exactly that shape, and loading the two together works.
Change chunks following a document chunk are held on their own merits: they were
not visible when the snapshot was saved, so `own_storage` says nothing about them.

## Not provided

Sync does not carry records yet, and an authenticated document refuses to
sync rather than exchange messages forever with a peer whose changes it will
never apply. There is no authorization, key discovery, rotation or
revocation of keys, no protection against the holder of a valid private key,
no custom verification hook, and no limit on how many held changes or records
a document keeps. View revocation remains the way to hide the changes of an
author which policy no longer permits.
