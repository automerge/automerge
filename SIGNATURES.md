# Change Signatures Plan

## Goal

Use an Ed25519 public key as the Automerge author ID and sign each change so that a receiver can verify that every change attributed to an author was authorized by that author's key.

At the same time, preserve the compression benefits of the columnar document format by not requiring every signature to be stored in saved document chunks.

## Key constraints

1. `ActorId` remains the CRDT actor/op namespace.
2. `Author` is a cryptographic identity, represented by an Ed25519 public key.
3. Every standalone change should be signed by its author.
4. Signatures must not be part of the hashed change payload.
5. Saved/compressed document chunks may omit signatures that can be transitively validated from retained descendant signatures.
6. Reconstructing changes from a saved document chunk must produce the same change hashes whether or not omitted signatures are available.
7. Automerge must not own private keys.
8. Signing may be asynchronous, but Automerge's core APIs should remain synchronous.
9. Signing should have no effect on documents that do not opt in.
10. For signed documents, unsigned local changes are visible locally but must not be flushed to storage or sent over the network until the required signatures are available.

## Hash/signature model

The change hash continues to be derived from the existing change payload: dependencies, actor, sequence number, start op, timestamp, message, other actors, op columns, and hashed extra metadata such as the author assignment.

The signature signs the change hash, with domain separation, for example:

```text
automerge-change-signature-v1 || change_hash
```

The signature is attached to the change but is not included in `Change::hash()` or any other change-identity hash derivation.

This gives us:

- stable hashgraph reconstruction from compressed data,
- signatures over the exact commit identity,
- no circularity between hash and signature.

## Authorship model

The author ID is the Ed25519 public key. The existing `author` bookmark currently stores author assignment in change extra bytes, only on `seq == 1`. That is a good starting point because the author assignment is covered by the change hash.

The effective author of a change is derived from the actor's assigned author. Initially:

- actor author is assigned by a hashed author footer on the actor's first change (`seq == 1`),
- later changes by that actor inherit the same author.

If we later support explicit author changes for an actor, the same signature plan still applies as long as effective author is a hash-covered property of each change.

## API and reconciliation model

Signing and verification should both be reconciliation processes managed by the application or orchestration layer, not by Automerge itself. Automerge knows which payloads need signatures, which signatures need verification, and how verified signatures fit into the hashgraph. Automerge does not hold private keys, does not mandate a particular signature implementation, and does not perform asynchronous work.

Use a single external signature state with two kinds of work:

1. signing requests for local authored changes,
2. verification requests for incoming signatures/proofs.

The rough JavaScript shape is:

```javascript
const signatures = new Automerge.SignatureState()

// After making local changes, receiving remote data, or periodically:
Automerge.reconcileSignatures(doc, signatures)

for (const request of signatures.pendingSigningRequests()) {
  request.markStarted()
  doSigning(request.bytesToSign).then(
    signature => request.complete(signature),
    error => request.fail(error),
  )
}

for (const request of signatures.pendingVerificationRequests()) {
  request.markStarted()
  doVerify(request.bytesToVerify, request.signature, request.author).then(
    valid => valid ? request.markValid() : request.markInvalid(),
    error => request.markFailed(error),
  )
}

// After signatures or verification results complete, or periodically:
Automerge.reconcileSignatures(doc, signatures)
```

A signing request should contain only public/stable data:

```typescript
type SigningRequest = {
  hash: Uint8Array
  author: Uint8Array       // author public key / verifier identity bytes
  algorithm: "ed25519"     // or an application-level algorithm identifier
  bytesToSign: Uint8Array  // domain-separated payload
}
```

A verification request similarly contains opaque signature bytes plus the stable payload to verify:

```typescript
type VerificationRequest = {
  hash: Uint8Array
  author: Uint8Array
  algorithm: "ed25519"
  signature: Uint8Array
  bytesToVerify: Uint8Array
}
```

`reconcileSignatures` should:

1. scan the document for local authored changes that require direct signatures,
2. create missing signing requests in the external signature state,
3. consume completed local signatures and attach them once policy allows (for example after external verification, or directly if the signing provider is trusted),
4. scan pending incoming verification units for retained direct signatures that require verification,
5. create missing verification requests for those retained direct signatures,
6. consume completed verification results,
7. run Automerge's structural/transitive validation once the necessary direct signature results are available,
8. atomically accept valid pending incoming changes or reject invalid pending input.

The signature state should track request lifecycle, for example:

- pending,
- started/in-flight,
- completed valid/invalid,
- failed/retryable,
- attached/accepted.

This lets higher-level code decide how to schedule signing and verification, retry failures, talk to hardware keys, use WebCrypto, prompt users, delegate signing to a service, or use an application-specific verification policy.

## Rust API shape

The Rust API should mirror the JavaScript model without introducing async methods:

```rust
let mut signatures = SignatureState::new();

doc.reconcile_signatures(&mut signatures)?;

for request in signatures.pending_signing_requests() {
    let hash = request.hash();
    let payload = request.bytes_to_sign().to_vec();
    let signature = external_sign(payload)?;
    signatures.complete_signing(hash, signature);
}

for request in signatures.pending_verification_requests() {
    let id = request.id();
    let valid = external_verify(
        request.bytes_to_verify(),
        request.signature(),
        request.author(),
    )?;
    signatures.complete_verification(id, valid);
}

doc.reconcile_signatures(&mut signatures)?;
```

Possible core types:

```rust
pub struct SignatureState { /* external reconciliation state */ }

pub struct SigningRequest<'a> {
    pub hash: ChangeHash,
    pub author: &'a Author,
    pub algorithm: SignatureAlgorithm,
    pub bytes_to_sign: Cow<'a, [u8]>,
}

pub struct VerificationRequest<'a> {
    pub id: VerificationRequestId,
    pub hash: ChangeHash,
    pub author: &'a Author,
    pub algorithm: SignatureAlgorithm,
    pub signature: Cow<'a, [u8]>,
    pub bytes_to_verify: Cow<'a, [u8]>,
}

pub enum SignatureAlgorithm {
    Ed25519,
    Other(Cow<'static, str>),
}

pub struct SignatureReport {
    pub signing_requested: usize,
    pub signatures_attached: usize,
    pub verification_requested: usize,
    pub verification_accepted: usize,
    pub verification_rejected: usize,
}

impl Automerge {
    pub fn reconcile_signatures(
        &mut self,
        signatures: &mut SignatureState,
    ) -> Result<SignatureReport, SignatureError>;
}
```

The exact ownership can be tuned. The important point is that `SignatureState` is not part of the CRDT state and is safe for applications to persist, discard, retry, or rebuild.

### Enabling signing

Signing should be opt-in and construction-time only. A document is either operating in signed mode or it is not; toggling signed mode after the document has been used is hard to define because it changes import/export and visibility invariants.

Examples:

```rust
let doc = Automerge::new().with_author(author).with_signing();
let doc = Automerge::load_with_options(bytes, LoadOptions::new().author(author).signing())?;
```

The same principle applies to JavaScript:

```javascript
const doc = Automerge.init({ author, signing: true })
const doc = Automerge.load(bytes, { author, signing: true })
```

Forks should inherit signed mode from the source document unless an explicit construction-time fork option says otherwise.

If signing is not enabled, existing commit/save/sync APIs should behave as they do today.

If signing is enabled:

- local commits still succeed synchronously,
- local unsigned commits are visible in the document immediately,
- reconciliation creates signing requests for them,
- storage and change-export APIs on signing-mode documents must refuse or omit changes until required signatures are attached,
- sync must not advertise locally-created unsigned changes, but it may send any received or local bytes when explicitly requested and may advertise remote hashes it has already received while they await verification.

### Export and storage gating

Signing mode should be a property of the document, not a separate save API. Normal legacy APIs keep working for non-signing users. When the document is in signing mode, the normal export/storage/sync APIs should include retained signatures. Change-list exports such as `get_changes` should not fail; they should behave as though changes awaiting required local signatures are not present yet.

Normal snapshot saves on signing-mode documents emit a dependency-closed filtered document when locally-created changes are still awaiting required signature coverage. The save contains all exportable changes plus retained signatures, omits locally blocked changes and any descendants whose dependencies were omitted, and filters op columns directly, including successor references to omitted ops. This preserves the synchronous/infallible `save()` API without silently persisting signing-incomplete local changes. Lower-level strict helpers such as `try_save_signed` may still report `MissingSignature`/`SigningIncomplete` instead of filtering.

Applications can call `reconcile_signatures` and save again later to include newly signed local changes. This separates local visibility from durability/exportability without requiring separate `saveSigned`/`save_signed` APIs.

### Sync gating and bundles

Sync should continue to operate over the received change hash graph. Signing mode adds visibility/export gates, but it should not create a second signed-only sync protocol or prevent peers from converging on which hashes have been received.

In signed mode there are two relevant graph views:

1. **Advertised local graph**: locally-created changes that are not waiting for signatures. This is the graph advertised in sync heads/have messages.
2. **Received sync graph**: hashes this peer has received from the remote side, including changes that are still pending verification or were rejected by verification. This graph may also be advertised in sync heads/have messages so the sender does not resend the same bytes forever.

Sending is less restrictive than advertising. If a peer explicitly requests an unsigned or unverified hash that this peer has received or created, sync may send the bytes. Signature state controls local visibility, not byte transport. The key local restriction is that changes still waiting for signatures are not proactively advertised as this peer's heads/have state.

For example:

```text
A(signed) -> B(verified/exportable) -> C(unsigned local)
```

Local document heads are `[C]`, but sync must not advertise `C` until `C` receives a signature. The sync-visible local frontier is `[B]`; `reconcile_signatures` can therefore make sync progress possible even though no CRDT operation changed.

For remote input, the behavior is different. If a signing-mode peer receives an unsigned, pending, or eventually rejected change `R`, it should keep `R` invisible locally, but it should still remember that `R` was received and advertise `R` to that peer. This keeps ordinary hash-graph sync convergent: signing affects whether a change is visible/verified, not whether the transport can acknowledge receipt.

Snapshot/document sync sends are allowed in signing mode only when the snapshot can include the required retained signatures. Otherwise sync should fall back to change-oriented sends. Those sends may include unsigned dependencies or unverified received changes when requested; receivers keep them invisible until verification policy accepts them.

Signed sync can send verification units, not necessarily individual changes. A verification unit is one of:

1. a standalone change with a valid direct signature,
2. a bundle whose included changes validate transitively,
3. a document chunk whose included changes validate transitively.

This matters because a document loaded from compressed storage may only have retained frontier signatures. It may be able to prove that a historical change is valid, but not be able to serve that historical change as a directly signed standalone change.

Therefore signed sync should not require every transmitted commit to carry its own direct signature. Instead, when a v2 peer needs a hash for which we do not have a direct signature, sync sends a proof bundle around that hash.

For a missing change `H`, the proof bundle should include enough same-author descendants to reach a retained/direct signature:

```text
H -> same-author child -> ... -> signed same-author frontier
```

The bundle may also need additional missing dependencies so the recipient can apply the changes. Those dependencies are separate from the same-author signature proof: they must be present, included directly with their own signatures/proofs, or included as unsigned/unverified bytes which remain invisible until accepted. The current implementation expands proof bundles over available dependency changes, so a requested cross-actor change is sent with its known dependencies where available.

The sync protocol implementation should evolve from "messages contain a list of changes" to "messages contain a list of chunks/proofs", conceptually:

```rust
enum SyncChunk {
    Change(Change),
    Bundle(Bundle),
    Document(Vec<u8>),
}
```

Each chunk should report the change hashes it covers so sync state such as `sent_hashes`, `shared_heads`, and `last_sent_heads` can be updated in terms of hashes that were actually transported or acknowledged as received.

## Storage model

### Standalone change chunks

A standalone change should continue to use `ChunkType::Change`, but the change body should become:

```text
canonical_change_body || optional_signature_field
```

where `canonical_change_body` is exactly the old hashed change payload and `optional_signature_field` is excluded from the change hash.

This is intentionally not forward-compatible with old readers. Old readers either include the signature bytes in `extra_bytes` and derive the wrong hash/checksum, or reject the chunk. The alternatives are not much better: a new chunk type also breaks old readers, and putting the signature in the hashed payload breaks detached signing. We should instead keep unsigned data compatible and make signed data a new format that new readers can parse.

New reader invariants:

```text
change.hash() == hash(canonical_change_body)
```

not:

```text
hash(canonical_change_body || signature_field)
```

The chunk header still carries the full encoded chunk length so readers can skip to the next chunk. However, for signed change chunks, the checksum/hash used for Automerge change identity must be computed from the canonical body only. Signature bytes are protected by the external signature verification result rather than by the chunk checksum.

Because the existing change parser treats the remainder of the body as `extra_bytes`, the standalone signature field should be detectable from the end of the chunk. A concrete shape could be:

```text
canonical_change_body
signature_bytes
signature_len: fixed-width u32le
signature_version: u8
signature_magic: b"AMSG"
```

Parsing then proceeds as:

1. parse the outer `ChunkType::Change` header to find the full body length,
2. inspect the body suffix for `signature_magic` and `signature_version`,
3. if present, read `signature_len` and split the body into canonical bytes and signature bytes,
4. parse the canonical bytes as the existing change body,
5. compute the `ChangeHash` from the canonical bytes only,
6. store the optional opaque `ChangeSignature` alongside the parsed `Change`.

The trailer format should be versioned and length-delimited. Unknown signature trailer versions should be rejected in strict signed mode and may be ignored in permissive legacy modes.

### Document and bundle chunks

Document chunks and bundle chunks should not store signatures by repeating change hashes. They already contain an ordered table of changes, so signatures can refer to change rows by index. This avoids storing 32-byte hashes for every retained signature and should compress well because retained signatures are sparse and row indexes are monotonic.

Conceptually, add a dedicated signatures table outside the column blocks:

```text
signature_table_magic
signature_table_version
signature_count
for each retained signature:
  change_index_delta: uleb128 // index into the chunk's change table
  signature_len:      uleb128
  signature_bytes
```

Signature bytes are effectively random, so storing them as columnar data is unlikely to help and can waste time/space if compressed. The useful compression win is avoiding repeated 32-byte change hashes; a simple row-indexed table gets that benefit while leaving signature bytes uncompressed and opaque. If the initial implementation only supports opaque Ed25519 signatures, an algorithm field can be omitted and added in a later table version.

The in-memory `ChangeGraph` may retain all known signatures, but document/bundle encoding should only emit signatures needed for transitive validation of the encoded graph. In practice this means retaining signatures for author frontiers: changes with no same-author child in the encoded graph.

When loading a document or bundle, Automerge reconstructs the changes, maps signature table rows to change hashes by row index, creates verification requests for the retained signatures, and then performs same-author transitive validation over the reconstructed graph after the application reports verification results.

## Transitive validation rule

A change is author-valid if either:

1. it has a retained signature which verifies under its effective author key, or
2. it has at least one author-valid child with the same effective author.

Equivalently, every unsigned change must have a same-author path to a retained signature.

A signature by a different author does not validate the parent author's claim. It only proves that the child author linked to the parent hash.

Example that must **not** validate Alice's parent:

```text
P(author = Alice, unsigned) -> C(author = Bob, signed by Bob)
```

Bob's signature covers `hash(C)`, and `hash(C)` covers `hash(P)`, but Bob could have fabricated `P` claiming Alice as author. Therefore Alice's change still needs either Alice's direct signature or a same-author signed descendant.

## Which signatures to retain when saving

When encoding a document chunk, retain a signature for every change that has no same-author child in the saved graph.

That includes:

- document heads,
- branch tips for each author,
- the final change in an author's run before all outgoing edges go to different authors.

This is more precise than "retain signatures where the author changes": the signature required is on the old author's frontier, not merely on the new author's first change.

Algorithm sketch:

1. Compute effective author for every change row.
2. Build child adjacency from dependency edges.
3. For each change `n`, check whether any child `c` has `author(c) == author(n)`.
4. Emit `signature(n)` if no such same-author child exists.
5. Optionally emit additional signatures for debugging, compatibility, or stricter policy.

If a required signature is missing from memory when saving, the save operation should either:

- fail in a strict signed-document mode, or
- save without signature validation guarantees in a permissive/legacy mode.

## Validation algorithm for document load

After parsing a document chunk and reconstructing the change graph:

1. Determine effective author for every change.
2. Verify each retained signature directly against `author(change)` and `change.hash()`.
3. Walk changes in reverse topological order.
4. Mark a change valid if:
   - it has a valid retained direct signature, or
   - it has a same-author child already marked valid.
5. Reject the document if any change requiring signed-author validation is not marked valid.

Pseudo-code:

```rust
for n in reverse_topological_order {
    valid[n] = direct_signature_valid[n]
        || children[n].iter().any(|c| author[*c] == author[n] && valid[*c]);
}

if valid.iter().any(|v| !v) {
    return Err(SignatureError::UnsignedAuthorChain);
}
```

This proves that every change is covered by a same-author signed descendant, and the hashgraph binds each descendant signature to all ancestors on that same-author path.

## Applying and loading signed data

For incoming standalone changes from sync, load-incremental, or `apply_changes`, signed mode should not expose a separate ingest API. The normal ingest APIs should parse the input and hold unverified changes in an internal pending queue. `Automerge::reconcile_signatures` then creates verification requests in the existing `SignatureState` state machine and accepts or rejects pending input when the application reports verification results.

For bundles and document chunks, verify at the chunk/bundle level using the transitive validation rule. Do not require every contained change to have a direct signature.

Automerge should not require the application to understand bundle/document graph structure. For each retained direct signature, Automerge creates a verification request containing the author bytes, opaque signature bytes, and domain-separated payload. Once the application reports which direct signatures are valid, Automerge performs the graph validation itself:

```text
valid(change) = direct_signature_valid(change)
             || same_author_valid_child(change)
```

Remote unverified changes should not become visible locally and should not advance verified document state. They should be held as pending verification units until enough verification results are available. Sync receipt state is separate: the peer may still advertise those hashes as received so hash-graph sync converges and the sender does not resend them indefinitely. This is different from local unsigned changes, which are visible immediately but not exportable and should not be advertised to peers until signed.

This gives us an important internal invariant:

> Once a change has been accepted into the change graph by signed `load`, `load_incremental`, `receive_sync_message`, or `apply_changes` reconciliation, signed-mode code may treat it as verified. Merely parsing or queuing a change is not acceptance.

Implementation-wise, this may require distinguishing parsed changes from accepted changes. Current bundle/document loading often reconstructs a `Vec<Change>` and then applies it. In signed mode, that reconstruction should produce or be accompanied by a verification result, for example:

```rust
struct VerifiedChangeBatch {
    changes: Vec<Change>,
    proof: VerificationProof,
}
```

or an internal marker that is never exposed as cryptographic proof but preserves the invariant that the document and orphan queue only contain verified changes.

Queued orphan changes need the same treatment: if they came from a verified bundle/message but cannot yet apply due to missing dependencies, they should remain marked/known as verified while waiting.

For sync specifically, peer state should distinguish receipt from verification. `receive_sync_message` can store pending sync input and advance receipt/acknowledgement state without making the change visible. `reconcile_signatures` later creates verification requests and, when verification succeeds, accepts pending input into the visible/verified document state. If verification rejects a received change, the hash may remain acknowledged as received for that peer while remaining invisible.

A permissive mode may be needed for legacy data, tests, or migration. Non-signing documents should not pay this cost or see behavior changes.

## Verification modes

Introduce an explicit policy enum, for example:

```rust
pub enum SignatureVerification {
    None,
    Permissive,
    RequireDirectForLooseChanges,
    RequireTransitiveForDocuments,
    RequireAllDirect,
}
```

Expected defaults:

- non-signing documents: `None`, preserving existing behavior,
- signed loose-change export/sync send: require direct signatures,
- signed loose-change import/sync receive: accept bytes into pending receipt state, but require direct or transitive verification before visibility,
- signed document load: require transitive validation,
- migration/testing APIs: allow `Permissive` when explicitly requested.

## Implementation touch points

Likely files:

- `rust/automerge/src/types.rs`
  - add `Author`/public-key type if not already present,
  - add `ChangeSignature` type.

- `rust/automerge/src/change.rs`
  - expose `author()`, `signature()`, and signature verification helpers.

- `rust/automerge/src/storage/change.rs`
  - parse/write the optional non-hashed signature field for standalone change chunks,
  - split full chunk bodies into canonical change bytes plus signature bytes,
  - ensure signatures do not affect `Change::hash()`.

- `rust/automerge/src/storage/chunk.rs`
  - allow change chunk headers to cover full encoded bytes while change identity/checksum uses canonical bytes.

- `rust/automerge/src/change_graph.rs`
  - store signatures in memory,
  - encode only required frontier signatures,
  - load row-indexed signature tables,
  - implement transitive validation.

- `rust/automerge/src/op_set2/change.rs`
- `rust/automerge/src/op_set2/change/collector.rs`
  - carry signature metadata when reconstructing changes.

- `rust/automerge/src/op_set2/change/batch.rs`
  - verify incoming loose changes before apply.

- `rust/automerge/src/storage/bundle/*`
  - add row-indexed signature tables/proofs to bundles,
  - verify bundles transitively as atomic verification units,
  - support building minimal proof bundles around requested hashes.

- `rust/automerge/src/sync*`
  - advertise received pending/rejected remote hashes so ordinary hash-graph sync converges,
  - gate locally-created signed sync output on exportable/verified heads rather than local visible heads,
  - allow sync messages to carry verification units such as changes, bundles, and document chunks,
  - update `sent_hashes` and related state using hashes covered by each verification unit.

- signature reconciliation API
  - add `SignatureState`, `SigningRequest`, `VerificationRequest`, `SignatureReport`, and `SignatureError`,
  - add `Automerge::reconcile_signatures`,
  - make normal export/storage/sync APIs gate on required signatures and pending verification when signing mode is enabled.

- wasm/javascript bindings
  - expose author public key configuration,
  - expose `SignatureState`, `SigningRequest`, and `VerificationRequest` lifecycle,
  - expose `reconcileSignatures(doc, signatures)`,
  - expose verification errors from normal export/sync APIs in signing mode,
  - do not require Automerge APIs to be async.

## Open questions

1. Should saved documents fail if a required frontier signature is unavailable, or should that depend on save options?
2. Are there remaining cases where proof bundle construction should include more than the exportable dependency closure, for example to reduce round trips around partial availability?
3. How should legacy unsigned actors interact with signed authors during migration?
4. Should `SignatureState` be purely application-owned, or should Automerge offer a serializable helper type for convenience?
5. Should standalone signature trailers include an algorithm field now, or should the first version assume Ed25519 and leave algorithm agility to a later trailer version?
