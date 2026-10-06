# Signature Feature Design: Follow-up Questions

## Current direction

- Signatures are opaque, application-defined byte strings.
- They attest authorship, not authorization.
- Signatures are detached from change hashes.
- A signature over an actor's latest change authenticates that actor's preceding hash chain.
- Signing and verification are asynchronous reconciliation processes.
- Automerge accepts unsigned data, while applications control its visibility.
- Signatures may be compacted, retaining only signatures needed at actor/frontier boundaries.
- The first version can focus on fragments rather than sync.

## Remaining design questions

1. **Cross-document replay:** Signing only the change hash does not bind a change to a document; Automerge changes generally have no document identity. Should cross-document replay be dropped from the threat model, or must the signed payload include an application-provided domain/document identifier?

The change is bound to the document via its hash link. No need for us to do that.

2. **Chain attestation:** Can we state the central guarantee as:

   > A valid signature over change `C` authenticates `C` and every preceding change in the same actor's sequence through which `C` is chained.

   It would not authenticate dependencies created by other actors.

Yep that's correct

3. **Actor forks:** If malformed or hostile input creates multiple maximal chains for one actor, should each independently signed maximum be retained, or should such a fork invalidate authentication for that actor?

I guess we require independently signed maxima. In practice we reject commits with equivocating actor IDs so it wont' arise right now, but that implementation
could change I suppose so the general rule is to require all maxima be signed.

4. **Author discovery:** A fragment might contain a later actor change but omit the actor's first change, which carries its author. Must independently consumable fragments include enough metadata/history to recover the actor-to-author association?

I think we don't need anything extra here. We can validate that the fragment contains signatures that attest to all the actor chains in it. If some actor chain in the fragment doesn't actually ground out in an initial commit with the correct author then it will be ignored.

5. **Signature retention:** Globally retaining only the latest signature per actor makes older fragments impossible to authenticate independently. Should we instead retain a signature for each actor at each independently exportable fragment boundary?

Yep that seems wise.

6. **Fragment coverage:** May a fragment be accompanied by a signature over a later change not contained in that fragment, relying on the hash chain to authenticate the fragment? Or must the signed change be included in the exported material?

I think we probably would prefer that the change only be in the fragment but I guess there's no reason to restrict ourselves to that.

7. **Incoming visibility:** Should Automerge have a dedicated authenticated frontier per actor, making changes beyond that frontier invisible until verified? Or should Automerge expose verification results and require the application to translate them into calls to the existing revocation API?

I think we probably want to handle the authenticated frontier internally.

8. **Avoiding temporary visibility:** If unsigned changes are applied before asynchronous verification, they could briefly produce patches or visible state. Must incoming unauthenticated changes begin hidden and only become visible after successful verification?

Unsigned changes shouldn't be applied before verification

9. **Local visibility versus export:** Should locally created unsigned changes:
   - Be immediately visible locally
   - Be included in ordinary `save()`
   - Be included in fragment exports
   - Be included in sync

   My reading is "visible locally, but excluded from authenticated fragment export until covered by a signature."

I think that's the right read yes

10. **Compatibility default:** Existing unsigned applications must keep working. Should transmission gating therefore be explicitly enabled by an authenticated-export option rather than becoming the default behavior?

I think we need it to be the default option. We should just assume that legacy clients won't use signatures.

11. **Verification decisions:** Should verification results be stored in the document, or should they remain ephemeral application policy that must be recomputed after loading? Because verification is application-specific and may change, ephemeral results seem safer.

Ephemeral I think. Though I can see a future where it is useful to say "I have verified up to these heads" locally to avoid redoing verification work (though note that verification work scales with the total number of actors, not commits).

12. **Opaque signature identity:** For multiple signatures over one hash, should Automerge assign each blob an internal ID so the application can report verification results for the exact blob? Any algorithm, signer, or key identifiers would remain encoded inside the opaque application-defined bytes.

I think that makes sense yes.

13. **Invalid signatures:** Should failed signature blobs remain stored and exportable, or may Automerge discard them after the application reports failure?

Let's retain them for now.

14. **Reconciliation API:** Would a lower-level API be acceptable instead of a mutable `SigningState`, along these lines?

   ```text
   doc.signing_requests()
     -> [{ actor, author, hash }]

   doc.add_signature(hash, bytes)

   doc.verification_requests()
     -> [{ signature_id, actor, author, hash, bytes }]

   doc.set_verification(signature_id, valid)
   ```

   A higher-level `reconcile_signatures()` helper could later wrap this flow.

Yep I think that's fine

15. In question 32 of the original questionnaire, I assume "discard internal commits" means **discard signatures on internal/sub-maximal commits**, not discard the commits themselves. Is that correct?

That's correct
