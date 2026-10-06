# Signature Feature Design Questions

## 1. Goals and threat model

1. What is the primary purpose of signatures?
   - Prove who authored a change
   - Detect tampering
   - Enforce authorization
   - Support auditing/non-repudiation
   - Something else

The primary goal here is to prove who authored a change

2. Which attacks must the feature protect against?
   - Forged changes
   - Modified stored changes
   - A sync peer stripping signatures
   - Replay across documents
   - Compromised author keys
   - Malicious but correctly signed changes

The first 4. Compromised keys and malicious changes are handled at a higher 
level with the revocation mechanism being all that's needed in Automerge.

3. Are signatures only evidence of authorship, or should Automerge reject changes from unauthorized authors?

They are only evindence of authorship. The revocation option should be used to
reject changes from unauthorized authors.

4. What should signatures explicitly **not** attempt to solve?

All kinds of authorization should be left to higher layers

## 2. Authors and keys

5. What does the existing author field identify?
   - A public key
   - A hash/fingerprint of a public key
   - An application-level identity independent of keys

Any of these things. I don't want Automerge to have an opinion. Hence the 
author ID being just a byte array.

6. How is an author bound to the key allowed to sign for it?

That is up to the application. In our first stab we are using ed25519 signing
keys as the author ID.

7. Can one author have multiple active signing keys?

In our case no, but I don't think we should say in general that this is not
possible.

8. Must key rotation be supported? If so, who authorizes a replacement key?

Key rotation is out of scope.

9. Should compromised signing keys be revocable independently of the existing change-revocation feature?

No, deciding what to do about data signed with revoked keys is up to the
application

10. May multiple actor IDs belong to one author and use the same key?

Yes

11. Should anonymous changes remain possible?

In general yes, though most applications will likely choose to reject 
unauthenticated changes

## 3. What is signed?

12. Should every individual Automerge change be signed?

Not exactly. For any given set of changes we need the latest change from each
_actor_ to be signed. We maintain an invariant that changes have monotonic
sequence numbers and that only the first change by a given actor has the author
set and so the latest signature for each actor is sufficient to attest to
authorship.

This is pretty importan because we need to be able to throw away most
signatures in order to maintain good compression. 

This does bring up some thorny questions around how to transmit small parts of
the document (e.g. a single) commit when we have thrown away the signatures.


13. Exactly which data should the signature cover?
    - Change hash
    - Canonical encoded change bytes
    - Author
    - Actor ID and sequence
    - Dependencies
    - Document identity
    - Protocol/version domain separator

The change hash is sufficient

14. Should a signature be portable between documents containing the same change, or bound to one document?

I think it's fine to be portable. The hash chain does the binding work.

15. Should signatures cover fragments, bundles, or sync messages in addition to changes?

See above re handling of signatures in these contexts. We _don't_ need to 
cover sync messages.

16. Should the change hash include the signature, or should signatures remain detached so adding one does not alter change identity?

Signatures should be outside the hash envelope. Otherwise we can't discard
them.

17. Can a change have multiple signatures—for example, author plus approvers?

In theory yes I suppose so. I don't think we should worry too much about this

## 4. Validation semantics

18. What should happen when receiving a change that is:
    - Properly signed
    - Unsigned
    - Signed by an unknown key
    - Signed by the wrong key
    - Malformed
    - Signed using an unsupported algorithm

In all the sad-path cases we should just continue considering the change to be
unauthenticated. I.e. it should always be possible to accept unauthenticated
changes - the application should be able to choose not to reify such things 
using the revocation mechanism.

19. Should validation behavior be configurable, such as:
    - Permissive: accept unsigned changes
    - Verified-only: expose only valid changes
    - Strict: reject invalid or unsigned changes

This is kind of already possible via the revocation mechanism

20. Should invalid changes be rejected permanently, quarantined, or retained pending keys/policy information?

Application specific via the revocation policy

21. Can signatures arrive after their changes and make previously pending changes usable?

I think we do need to allow this yes. If we're able to discard signatures on
internal (sub-maximal actor sequence) commits then we need to be able to 
transmit those commits and the following external commits separately and have
the following signatures attest to the authorship of the internal commits.

22. If a signature is removed in transit, should the receiver still retain the unsigned change?

I think so yes

23. Should a valid signature imply authorization, or should applications run a separate authorization policy?

That's up to the application

24. Should verification happen before storing a change, before applying it, or lazily when reading it?

I think when applying it?

## 5. Creating signatures

25. Must locally created changes be signed before they are committed?

I think we want the change to be visible in the document immediately, but not
transmitted over the network.

I'm actually unclear on what good semantics are here. But we should think about
the fact that most signing infrastructure will be asynchronous and commits are
created on a per-keystroke basis in many applications. Delaying UI until
signing is complete for every keystroke is a non-starter.

26. What should happen when a document requires signatures but no signer is available?

I think we should just not transmit the commits (maybe?)

27. Where should private keys live?
    - Inside Automerge
    - In an application-provided signer
    - OS/browser key store
    - Hardware-backed signer

This is all up to the applications.

28. Does signing need to support asynchronous signers such as WebCrypto or hardware wallets?

Yes

29. If signing is asynchronous, may APIs such as `change()` become asynchronous, or should signing be a separate follow-up step?

No, signing should be a separate thing. As much as possible Automerge should
not have an opinion on what a signature is or how it is obtained.

30. Should applications be able to import an already signed change without access to its private key?

yes

## 6. Storage and synchronization

31. Where should signatures be stored?
    - In each encoded change
    - In a detached signature table
    - In a separate bundle section
    - Entirely outside Automerge

They need to be stored outside the hash envelope and due to needing to be able
to receive them separately from the commits I think we might even want a
separate chunk type for signatures.

32. Must signatures survive all existing save/load and incremental-save paths?

No. We can discard internal commits.

33. Should sync automatically exchange missing signatures separately from missing changes?

I _think_ so.

34. Can peers request a specific signature or all signatures for a change?

No. We might not still have the signatures. I think the best we can do is 
provide the actor seq maximal signatures.

35. How should duplicate or conflicting signatures be handled?

I think probably we just hold on to both signatures and it's up to the
application what to do. 

36. Is it important that old implementations can read documents containing signatures?

Ideally we would allow this but I don't see a way to do it and we definitely
shouldn't block on it.

37. Should signatures be included when exporting changes and fragments by default?

I think so

## 7. Interaction with revocation

38. Is the existing revocation feature strictly about hiding an author’s changes after an epoch, or should it also express distrust in their key?

It's just about hiding the changes. Trust/distrust is handled in another layer

39. Must revocation declarations themselves be signed or authorized?

No, revocation itself is out of scope

40. If an author is revoked and later unrevoked, do their old valid signatures remain valid?

That's up to the application. Something I have perhaps not indicated clearly
is that because "what is a signature" is up to the application I want "verify
this signature" to also be a very generic (and possibly asynchronous)
mechanism too.

41. Should key compromise support a cutoff—“this key is valid before change/time X”—similar to the current author revocation clock?

No

42. Should cryptographic key revocation and document-view change revocation use clearly separate terminology and APIs?

I suppose so. Key revocation is just completely out of scope here. Possibly
view revocation is poorly named, but that's a battle for another day.

## 8. Cryptography

43. Do you have a required signature algorithm, such as Ed25519?

No, we should explicitly avoid had coding any such thing

44. Should the format be algorithm-agile from the beginning?

Yes, in particular we should not specify any kind of algorithm whatsoever. A 
signature is just a bunch of bytes from automerges perspective

45. May applications register custom signing and verification algorithms?

Yes, but not in those terms. I think Automerge should just asy "here's a change
hash and the bytes of a signature, are they valid?". Signing algorithms are
then entirely up to the application.

46. Is deterministic canonical encoding required across Rust, JavaScript, and future implementations?

I guess? I mean it's just a bunch of bytes anyway.

47. Are there external standards or key formats we should interoperate with?

No

## 9. Public APIs and observability

48. Which APIs need first-class support: Rust, JavaScript/WASM, both, or others?

Yes thos

49. What should the simplest signing workflow look like in pseudocode?

I think something like

```
doc.put("foo", "bar")
doc.commit()

signingState = new SigningState()
doc.reconcileSigs(signingState)
for toSign in signingState
    sign(toSign)
doc.reconcileSigs(signingState)
```

50. What should the simplest verification workflow look like?

```
doc.put("foo", "bar")
doc.commit()

signingState = new SigningState()
doc.reconcileSigs(signingState)
for toVerify in signginState
    signingState.verify(isValid(toVerify))
doc.reconcileSigs(signingState)
```

51. Do callers need APIs to inspect:
    - Signature bytes
        yes
    - Signer/key identity
        no, the signature and author are just bytes (which can be inspected)
        everything else is up to the application
    - Verification status
        yes
    - Missing signatures
        yes
    - Rejection reasons
        I don't know what this would refer to? I guess yes?


52. Should patch callbacks or sync events report that changes became visible after signature reconciliation?

No. The patches should be emitted, but there's no need to label them

53. Should verification failures be returned as errors, surfaced as status records, logged, or some combination?

probably just make a note of them on some state in the verification flow so 
that the application can log them?

## 10. Compatibility and rollout

54. Must existing unsigned documents continue working without configuration changes?

Yes

55. Can the binary document/change format be extended, or must signatures fit existing extension points?

If at all possible we should fit in existing extension points but I think we 
probably do need to introduce a new chunk type

56. Is this experimental API allowed to break before release?

Yes

57. Should signed and unsigned peers be able to synchronize safely?

Yes

58. What minimum success criteria would make the first version complete?

We are able to sign individual commits on a per-keystroke with a ed25519 key
and then transmit the saved sedimentree (fragments API) of the document with
the signatures and the signed changes are visible. Unsigned changes are not.

I would accept a solution which does not bother transmitting signatures at all
via the sync protocol or for individual commits but only on fragments. We are
moving to a world in which all synchronization is done via the fragments API
and so this would be fine.

## Example scenarios

Finally, describe two concrete scenarios:

- **Happy path:** Who creates, signs, synchronizes, and verifies a change?

Signer is a web crypto signer in the browser. Commits are created per keystroke
signed asynchronously and transmitted via sedimentree once signatures are
available. Server ignores signatures entirely. Another client in another
browser loads the document checks the latest signature for each actor attests
to authorship and reifies the doc.

- **Failure path:** What happens when a peer sends an unsigned or incorrectly signed change?

The unauthenticated changes are ignored (by the view revocation configuration)
and so nothing user visible appears
