# automerge-node-api

A [Node-API](https://nodejs.org/api/n-api.html) binding to the Rust
implementation of Automerge, built with [napi-rs](https://napi.rs).

This is work in progress and only covers a small part of the low-level API.
See https://github.com/automerge/automerge/pull/1099 for background.

## Principles

- **The public JavaScript API does not change.** What `@automerge/automerge`
  exports, and what is observable through it (values, patches, error classes
  and messages), is the same whichever binding is underneath. The end-to-end
  tests in `javascript/test`, which exercise the public API, must pass
  unchanged against both bindings.
- **Everything below the public API is in scope.** That includes the
  low-level `API` interface (`javascript/src/low_level.ts`), the internals of
  the JS package and how it divides work with the native side.
- **The WebAssembly binding stays.** `automerge-wasm` remains a first-class
  binding built on `wasm-bindgen`. Bridging WebAssembly through Node-API
  (e.g. via napi-rs' own wasm target) is not something we are pursuing for
  now.
- **Aligned where it makes sense, not at all costs.** Code with no dependency
  on a JS engine should live in one place and be used by both bindings. Code
  that builds or reads JS values should use the primitives of its binding
  generator idiomatically. When that pulls the two bindings apart, we
  abstract on the JS side and let each binding provide its own
  implementation, rather than forcing one binding into the shape of the
  other.

## Where the bindings meet the JS package

The JS package selects its backend through `UseApi` in
`javascript/src/low_level.ts`. Today that seam assumes `wasm-bindgen` in a
few places, which is where a JS-side abstraction would go:

- **Materialisation.** `implementation.ts` calls `handle.materialize`,
  `handle.applyPatches` and `handle.applyAndReturnPatches` and expects fully
  built, optionally frozen documents carrying hidden metadata. This is the
  main candidate for an internal abstraction with a per-binding
  implementation.
- **Object metadata.** `constants.ts` defines `STATE`
  (`Symbol.for("_am_meta")`) and `OBJECT_ID` (`Symbol.for("_am_objectId")`),
  which `internal_state.ts` and `proxies.ts` read from objects that the
  binding wrote them onto.
- **Handle identity.** `proxies.ts` compares `handle.__wbg_ptr`, a
  `wasm-bindgen` internal, to tell whether a value belongs to the same
  document.
- **Types.** `wasm_types.d.ts` is copied from the `wasm-bindgen` output by
  `scripts/build.mjs` and imported throughout `javascript/src`. See
  [Types](#types) below.
- **Loading.** `initializeWasm` and the files in `src/entrypoints` are about
  instantiating a wasm module.

## Building and testing

```
yarn install
yarn build:debug
yarn test
```

## WebAssembly workarounds to revisit

The existing binding is shaped in places by constraints of WebAssembly and
`wasm-bindgen`. When porting, the first goal is parity, so these are ported
as they are. They are listed here so we can come back and use what Node-API
offers instead. Entries are candidates found by reading the code; none of
them have been measured yet.

| Area | What `automerge-wasm` does | Why | What Node-API could do |
| --- | --- | --- | --- |
| Materialize key strings | `ExportCache::ensure_key` / `ensure_datatype` keep a `HashMap` of `JsString`s so each map key is only encoded once per export (`src/export_cache.rs`) | Every Rust `&str` crossing into JS is copied out of linear memory and decoded | Create property keys directly (`napi_create_string_utf8`, or `node_api_create_property_key_utf16` for internalised keys) and re-measure whether the cache is still needed |
| Materialize / patch application | Objects are built one `Reflect::set` / `Object.defineProperty` / `Object.freeze` call at a time, each one a call out of wasm into JS glue | No direct access to the engine | Direct engine calls (`napi_set_named_property`, `napi_define_properties` to set several hidden props in one call, `napi_object_freeze`) |
| Object metadata | Object id, datatype and meta are stored on every materialised object as non-enumerable `Symbol.for("_am_objectId")`, `_am_datatype_`, `_am_raw_value_`, `_am_meta` properties and read back in `unwrap_object` | The only way to associate Rust-side data with a JS object | `napi_wrap` / `napi_type_tag_object` could attach native data. The JS package reads `_am_objectId` and `_am_meta` too, so this needs the lookup in `internal_state.ts` to go through a per-binding implementation |
| Binary data | `Uint8Array::to_vec()` on the way in and `Uint8Array::from(slice)` on the way out (`load`, `save`, changes, sync messages, byte scalars) | JS cannot hold references into linear memory, which may move when it grows | Borrow the incoming `ArrayBuffer` for the duration of the call, and hand ownership of an outgoing `Vec<u8>` to an external `ArrayBuffer`/`Buffer` without copying |
| Memory ceiling | 32-bit linear memory, 4GB at most | wasm32 | None needed. This is one of the motivations for the binding, and means sizes should not be assumed to fit in `u32` |
| Lifetime of handles | `Automerge` and `SyncState` must be released with `.free()`; `javascript/src/implementation.ts` calls it explicitly | wasm-bindgen objects are not collected by the JS GC | napi-rs classes are finalised by the GC. `free()` has to keep existing on the low-level API, but can become a no-op or an eager release |
| Untyped arguments | Most methods take `JsValue` and validate by hand; 49 of them are `skip_typescript` with types written in a `typescript_custom_section` | `wasm-bindgen` cannot express optional or union parameters | napi-rs can express `Option<T>` and `Either<A, B>` and generates the `.d.ts`, so this binding should use typed parameters. It does not need to generate the same types as `wasm-bindgen`, see [Types](#types) |
| Error classes | Each error enum is converted to a JS `RangeError`/`Error` by hand | `wasm-bindgen` only throws what it is given | napi-rs throws plain `Error` by default, so the binding needs explicit conversions to keep the same classes (see the TODO in `simple.test.ts`) |
| Start-up | Async instantiation (`initializeWasm`, base64 / bundler / workerd entrypoints in `javascript/src/entrypoints`), plus an `on_start` abort hook because a trapped instance is unusable afterwards | Module loading and trap semantics of wasm | Synchronous `require`, and a Rust panic can be caught and thrown as a normal exception |
| Text indexing | `utf16-indexing` feature and `TextEncoding::Utf16CodeUnit` | Not a wasm limitation: JS strings are UTF-16 | Keep. This binding must use the same setting to stay compatible |

## Types

Three kinds of types come out of `wasm_types.d.ts` today, and only the first
is part of the public API:

- **Data shapes**, such as `Patch`, `Mark`, `Cursor`, `Span`, `Heads` and
  `DecodedChange`. `javascript/src` re-exports these, so they are public.
  They are not generated from Rust signatures though: they are hand-written
  TypeScript in a `typescript_custom_section` string in
  `automerge-wasm/src/lib.rs`.
- **The `API` interface and the `Automerge` handle class.** These are
  exported from the JS package, but only through `use()` and `getBackend()`,
  which are both marked `@hidden`.
- **Method signatures generated by `wasm-bindgen`**, which only reach users
  through the handle class above.

The direction we want is for the JS package to own its types rather than for
both generators to produce identical output:

- The public data shapes move into `javascript/src` as ordinary TypeScript.
  This is mostly relocating what is already hand-written.
- The JS package declares the internal backend interface it needs. Each
  binding is checked against it at compile time where it is plugged in, with
  a thin adapter where the generated shape is not a direct fit.
- Each binding is then free to use whatever its generator produces
  idiomatically.

Open point: `@automerge/automerge-wasm` is published as its own package with
these types, so they either stay there as well or that package re-exports
them.

## Possible future strategy: lazy materialisation

Instead of building a whole document eagerly, a read could return a `Proxy`
whose handlers call into native code when a property is accessed. This would
save memory and work when only a small part of a large document is read. The
JS package already does this inside `change()` (`rootProxy` in
`javascript/src/proxies.ts`), and it is not specific to Node-API: a read-only
variant would work over either binding.

It is postponed, and would be an opt-in addition to the public API rather
than a replacement for the default, because a plain object tree is
observable today:

- `Object.isFrozen(doc)` is asserted by `javascript/test/basic_test.ts` when
  `freeze` is on.
- `structuredClone(doc)` and `postMessage(doc)` throw on a `Proxy`.
- Patch application leaves untouched subtrees as the same object, which
  consumers rely on for memoisation. A lazy proxy would need its own cache to
  preserve that.
- A `Doc` is an immutable view at fixed heads while the native document keeps
  changing, so every lazy read is a read at pinned heads and every old
  snapshot keeps the native document alive.
- Each property access becomes a proxy trap plus a native call, so code that
  walks a whole document gets slower.

What we do now is keep this possible: the internal materialiser interface
should not assume that the result is an eagerly built plain object. In
particular, object id and state lookups should go through functions rather
than reading symbols off the object directly. The cost of eager
materialisation on large documents has not been measured yet, and that
number should decide the priority of this.
