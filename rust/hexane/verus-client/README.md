# The acceptance clients, as a crate outside hexane

`src/verified/client_proof.rs` holds the acceptance clients of the
verified API (`hexane::verified`): functions written as a user of the API
would write them, each with a specification that Verus proves from the
API's specifications alone. They are a module of hexane, so that every
Verus run of the crate checks them.

But a module of hexane is told more than a user's crate is. It can name
`pub(crate)` items, and Verus shows it the bodies of the closed
specification functions of the modules above it. So that the clients are
proved there does not yet say that a user can prove the same.

This directory makes the same clients into a crate of their own, which
depends on hexane as a user's crate does. Verus then proves them again
from what hexane exports and nothing else, and the crate's tests run
them.

## Running it

With Verus installed (the version the crate's `vstd` dependency names),
from this directory:

    python3 gen.py
    cd ../../target/verus-client
    cargo verus verify --features vstd/nonzero_internals
    cargo test

`gen.py` writes the crate to `rust/target/verus-client` (or to the
directory it is given). The first command after it verifies hexane
(with `bijou64`, which the crate's default feature of that name asks
for) and then the clients. It prints a line
`verification results:: N verified, 0 errors` for each crate it
verifies: `vstd` and hexane the first time, and after that only what
has changed since. The last such line is the clients'. The second
command runs the clients.

Run it after a change to the public API of `hexane::verified`, or to the
clients.

## What is in the crate

* `src/lib.rs` is `client_proof.rs` word for word, but for what a crate
  outside hexane has to say differently: the imports name `hexane`
  instead of `super` and `crate`, the items are public (so that the tests
  can call them), and a direct `from_iter` call is written as `collect()`
  (see below). `gen.py` makes those changes and stops if it meets an
  import or a `from_iter` call it does not know how to rewrite. The file
  is generated: change `client_proof.rs` or `gen.py`, not it.
* `tests/run.rs` is this directory's `run.rs`: it calls the clients with
  arguments that meet their `requires`. A client's proof says nothing of
  whether it panics on a capacity limit or returns; the tests run each
  one.
* `Cargo.toml` depends on hexane by path and on the same `vstd`, and
  declares a workspace of its own, so that the crate is not taken for a
  member of the workspace it lies in. Its feature `bijou64` is hexane's:
  the clients of the second codec are compiled and verified with it,
  and it is on unless `--no-default-features` is given.

## What only this check has found

* A type that an associated type of one of the model traits names (the
  state under `EncoderState`, for one) has to be `pub`, in a
  module that is not. With `pub(crate)` hexane verifies and builds, and
  Verus panics on every function of a crate that depends on it.
* Verus panics on a direct call of `from_iter` where the `FromIterator`
  is of another verified crate. `collect()` is the same call and is
  accepted, which is why `gen.py` rewrites the one into the other.
* A public type that the module did not re-export
  (`EncoderState`, before it was first committed).

## What it does not check

It is a check of the clients that exist. A specification that no client
uses is not made any more usable by it; the mutation tests of each piece
(see the commit messages) are what ties each clause of a specification to
a client that needs it.
