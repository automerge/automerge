# Overview

automerge-c exposes a C API that can either be used directly or as the basis
for other language bindings that have good support for calling C functions.

# Installing

## Prerequisites

* Cargo >= 1.71.0
* CMake >= 3.25
* CMocka >= 1.1.5
* Doxygen >= 1.9.1
* Ninja >= 1.10.1

See the main README for instructions on getting your environment set up and then
you can build the automerge-c library and install its constituent files within
a root directory of your choosing (e.g. "/usr/local") like so:

```shell
cmake -E make_directory automerge-c/build
cmake -S automerge-c -B automerge-c/build
cmake --build automerge-c/build
cmake --install automerge-c/build --prefix "/usr/local"
```

Installation is important because the name, location and structure of CMake's
out-of-source build subdirectory is subject to change based on the platform and
the release version; generated headers like `automerge-c/config.h` and
`automerge-c/utils/enum_string.h` are only sure to be found within their
installed locations.

It's not obvious because they are versioned but the `Cargo.toml` and
`cbindgen.toml` configuration files are also generated in order to ensure that
the project name, project version and library name that they contain match those
specified within the top-level `CMakeLists.txt` file.

If you'd like to cross compile the library for different platforms you can do so
using [cross](https://github.com/cross-rs/cross). For example:

- `cross build --manifest-path rust/automerge-c/Cargo.toml -r --target aarch64-unknown-linux-gnu`

This will output a shared library in the directory `rust/target/aarch64-unknown-linux-gnu/release/`.

You can replace `aarch64-unknown-linux-gnu` with any
[cross supported targets](https://github.com/cross-rs/cross#supported-targets).
The targets below are known to work, though other targets are expected to work
too:

- `x86_64-apple-darwin`
- `aarch64-apple-darwin`
- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`

As a caveat, CMake generates the `automerge.h` header file in terms of the
processor architecture of the computer on which it was built so, for example,
don't use a header generated for a 64-bit processor if your target is a 32-bit
processor.

# Unicode indexing

By default automerge-c expects string indices to be given in terms of UTF-8 byte
offsets so, for example, the length of "😀" (U+1F600) is 4.

If instead you need string indices to be given in terms of Unicode code point
offsets such that the length of "😀" (U+1F600) will be 1, build it like so:

`cmake -S automerge-c -B automerge-c/build -DUTF32_INDEXING=true`

Regardless of the specified encoding for character indices, automerge-c always
requires a string to be provided as an `AMbyteSpan` struct that references an
array of valid UTF-8 code points.

# Usage

You can build and view the C API's HTML reference documentation like so:

```shell
cmake -E make_directory automerge-c/build
cmake -S automerge-c -B automerge-c/build
cmake --build automerge-c/build --target automerge_docs
firefox automerge-c/build/docs/html/index.html
```

To get started quickly, look at the
[examples](https://github.com/automerge/automerge/tree/main/rust/automerge-c/examples).

Almost all operations in automerge-c act on an Automerge document
(`AMdoc` struct) which is structurally similar to a JSON document.

You can get a document by calling either `AMcreate()` or `AMload()`. Operations
on a given document are not thread-safe so you must use a mutex or similar to
avoid calling more than one function on the same one concurrently.

A C API function that could succeed or fail returns a result (`AMresult` struct)
containing a status code (`AMstatus` enum) and either a sequence of at least one
item (`AMitem` struct) or a read-only view onto a UTF-8 error message string
(`AMbyteSpan` struct).
An item contains up to three components: an index within its parent object
(`AMbyteSpan` struct or `size_t`), a unique identifier (`AMobjId` struct) and a
value.
The result of a successful function call that doesn't produce any values will
contain a single item that is void (`AM_VAL_TYPE_VOID`).
A returned result **must** be passed to `AMresultFree()` once the item(s) or
error message it contains is no longer needed in order to avoid a memory leak.

```
#include <stdio.h>
#include <stdlib.h>
#include <automerge-c/automerge.h>
#include <automerge-c/utils/string.h>

int main(int argc, char** argv) {
  AMresult *docResult = AMcreate(NULL);

  if (AMresultStatus(docResult) != AM_STATUS_OK) {
    char* const err_msg = AMstrdup(AMresultError(docResult), NULL);
    printf("failed to create doc: %s", err_msg);
    free(err_msg);
    goto cleanup;
  }

  AMdoc *doc;
  AMitemToDoc(AMresultItem(docResult), &doc);

  // useful code goes here!

cleanup:
  AMresultFree(docResult);
}
```

If you are writing an application in C, the `AMstackItem()`, `AMstackItems()`
and `AMstackResult()` functions enable the lifetimes of anonymous results to be
centrally managed and allow the same validation logic to be reused without
relying upon the `goto` statement (see examples/quickstart.c).

If you are wrapping automerge-c in another language, particularly one that has a
garbage collector, you can call the `AMresultFree()` function within a finalizer
to ensure that memory is reclaimed when it is no longer needed.

An Automerge document consists of a mutable root which is always a map from
string keys to values. A value can be one of the following types:

- A number of type double / int64_t / uint64_t
- An explicit true / false / null
- An immutable UTF-8 string (`AMbyteSpan` struct).
- An immutable array of arbitrary bytes (`AMbyteSpan` struct).
- A mutable map from string keys to values.
- A mutable list of values.
- A mutable UTF-8 string.

If you read from a location in the document with no value, an item with type
`AM_VAL_TYPE_VOID` will be returned, but you cannot write such a value
explicitly.

Under the hood, automerge references a mutable object by its object identifier
where `AM_ROOT` signifies a document's root map object.

There are functions to put each type of value into either a map or a list, and
functions to read the current or a historical value from a map or a list. As
(in general) collaborators may edit the document at any time, you cannot
guarantee that the type of the value at a given part of the document will stay
the same. As a result, reading from the document will return an `AMitem` struct
that you can inspect to determine the type of value that it contains.

Strings in automerge-c are represented using an `AMbyteSpan` struct which
contains a pointer and a length. Strings must be valid UTF-8 and may contain
NUL (`0`) characters.
For your convenience, you can call `AMstr()` to get an `AMbyteSpan` struct
referencing a null-terminated byte string or `AMstrdup()` to get the
representation of an `AMbyteSpan` struct as a null-terminated byte string
wherein its NUL characters have been removed/replaced as you choose.

Putting all of that together, to read and write from the root of the document
you can do this:

```
#include <stdio.h>
#include <stdlib.h>
#include <automerge-c/automerge.h>
#include <automerge-c/utils/string.h>

int main(int argc, char** argv) {
  // ...previous example...
  AMdoc *doc;
  AMitemToDoc(AMresultItem(docResult), &doc);

  AMresult *putResult = AMmapPutStr(doc, AM_ROOT, AMstr("key"), AMstr("value"));
  if (AMresultStatus(putResult) != AM_STATUS_OK) {
    char* const err_msg = AMstrdup(AMresultError(putResult), NULL);
    printf("failed to put: %s", err_msg);
    free(err_msg);
    goto cleanup;
  }

  AMresult *getResult = AMmapGet(doc, AM_ROOT, AMstr("key"), NULL);
  if (AMresultStatus(getResult) != AM_STATUS_OK) {
    char* const err_msg = AMstrdup(AMresultError(getResult), NULL);
    printf("failed to get: %s", err_msg);
    free(err_msg);
    goto cleanup;
  }

  AMbyteSpan got;
  if (AMitemToStr(AMresultItem(getResult), &got)) {
    char* const c_str = AMstrdup(got, NULL);
    printf("Got %zu-character string \"%s\"", got.count, c_str);
    free(c_str);
  } else {
    printf("expected to read a string!");
    goto cleanup;
  }


cleanup:
  AMresultFree(getResult);
  AMresultFree(putResult);
  AMresultFree(docResult);
}
```

Functions that do not return an `AMresult` (for example `AMitemKey()`) do
not allocate memory but rather reference memory that was previously
allocated. It's therefore important to keep the original `AMresult` alive (in
this case the one returned by `AMmapRange()`) until after you are finished with
the items that it contains. However, the memory for an individual `AMitem` can
be shared with a new `AMresult` by calling `AMitemResult()` on it. In other
words, a select group of items can be filtered out of a collection and only each
one's corresponding `AMresult` must be kept alive from that point forward; the
originating collection's `AMresult` can be safely freed.

Beyond that, good luck!
