automerge-c exposes an API to C that can either be used directly or as a basis
for other language bindings that have good support for calling into C functions.

# Building

See the main README for instructions on getting your environment set up, then
you can use `./scripts/ci/cmake-build Release static` to build automerge-c.

It will output two files:

- ./build/Cargo/target/include/automerge-c/automerge.h
- ./build/Cargo/target/release/libautomerge.a

To use these in your application you must arrange for your C compiler to find
these files, either by moving them to the right location on your computer, or
by configuring the compiler to reference these directories.

- `export LDFLAGS=-L./build/Cargo/target/release -lautomerge`
- `export CFLAGS=-I./build/Cargo/target/include`

If you'd like to cross compile the library for different platforms you can do so
using [cross](https://github.com/cross-rs/cross). For example:

- `cross build --manifest-path rust/automerge-c/Cargo.toml -r --target aarch64-unknown-linux-gnu`

This will output a shared library in the directory `rust/target/aarch64-unknown-linux-gnu/release/`.

You can replace `aarch64-unknown-linux-gnu` with any [cross supported targets](https://github.com/cross-rs/cross#supported-targets). The targets below are known to work, though other targets are expected to work too:

- `x86_64-apple-darwin`
- `aarch64-apple-darwin`
- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`

As a caveat, the header file is currently 32/64-bit dependant. You can re-use it
for all 64-bit architectures, but you must generate a specific header for 32-bit
targets.

# Usage

For full reference, read through `automerge.h`, or to get started quickly look
at the
[examples](https://github.com/automerge/automerge-rs/tree/main/rust/automerge-c/examples).

Almost all operations in automerge-c act on an AMdoc struct which you can get
from `AMcreate()` or `AMload()`. Operations on a given doc are not thread safe
so you must use a mutex or similar to avoid calling more than one function with
the same AMdoc pointer concurrently.

As with all functions that either allocate memory, or could fail if given
invalid input, `AMcreate()` returns an `AMresult`. The `AMresult` contains the
returned doc (or error message), and must be freed with `AMfree()` after you are
done to avoid leaking memory.

```
#include <automerge-c/automerge.h>
#include <stdio.h>

int main(int argc, char** argv) {
  AMresult *docResult = AMcreate(NULL);

  if (AMresultStatus(docResult) != AM_STATUS_OK) {
    printf("failed to create doc: %s", AMerrorMessage(docResult).src);
    goto cleanup;
  }

  AMdoc *doc = AMresultValue(docResult).doc;

  // useful code goes here!

cleanup:
  AMfree(docResult);
}
```

If you are writing code in C directly, you can use the `AMpush()` helper
function to reduce the boilerplate of error handling and freeing for you (see
examples/quickstart.c).

If you are wrapping automerge-c in another language, particularly one that has a
garbage collector, you can call `AMfree` within a finalizer to ensure that memory
is reclaimed when it is no longer needed.

An AMdoc wraps an automerge document which are very similar to JSON documents.
Automerge documents consist of a mutable root, which is always a map from string
keys to values. Values can have the following types:

- A number of type double / int64_t / uint64_t
- An explicit true / false / nul
- An immutable utf-8 string (AMbyteSpan)
- An immutable array of arbitrary bytes (AMbyteSpan)
- A mutable map from string keys to values (AMmap)
- A mutable list of values (AMlist)
- A mutable string (AMtext)

If you read from a location in the document with no value a value with
`.tag == AM_VALUE_VOID` will be returned, but you cannot write such a value explicitly.

Under the hood, automerge references mutable objects by the internal object id,
and `AM_ROOT` is always the object id of the root value.

There is a function to put each type of value into either a map or a list, and a
function to read the current value from a list. As (in general) collaborators
may edit the document at any time, you cannot guarantee that the type of the
value at a given part of the document will stay the same. As a result reading
from the document will return an `AMvalue` union that you can inspect to
determine its type.

Strings in automerge-c are represented using an `AMbyteSpan` which contains a
pointer and a length. Strings must be valid utf-8 and may contain null bytes.
As a convenience you can use `AMstr()` to get the representation of a
null-terminated C string as an `AMbyteSpan`.

Putting all of that together, to read and write from the root of the document
you can do this:

```
#include <automerge-c/automerge.h>
#include <stdio.h>

int main(int argc, char** argv) {
  // ...previous example...
  AMdoc *doc = AMresultValue(docResult).doc;

  AMresult *putResult = AMmapPutStr(doc, AM_ROOT, AMstr("key"), AMstr("value"));
  if (AMresultStatus(putResult) != AM_STATUS_OK) {
    printf("failed to put: %s", AMerrorMessage(putResult).src);
    goto cleanup;
  }

  AMresult *getResult = AMmapGet(doc, AM_ROOT, AMstr("key"), NULL);
  if (AMresultStatus(getResult) != AM_STATUS_OK) {
    printf("failed to get: %s", AMerrorMessage(getResult).src);
    goto cleanup;
  }

  AMvalue got = AMresultValue(getResult);
  if (got.tag != AM_VALUE_STR) {
    printf("expected to read a string!");
    goto cleanup;
  }

  printf("Got %zu-character string `%s`", got.str.count, got.str.src);

cleanup:
  AMfree(getResult);
  AMfree(putResult);
  AMfree(docResult);
}
```

Functions that do not return an `AMresult` (for example `AMmapItemValue()`) do
not allocate memory, but continue to reference memory that was previously
allocated. It's thus important to keep the original `AMresult` alive (in this
case the one returned by `AMmapRange()`) until after you are done with the return
values of these functions.

Beyond that, good luck!
