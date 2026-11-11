# automerge-node-api

A [Node-API](https://nodejs.org/api/n-api.html) binding to the Rust
implementation of Automerge, built with [napi-rs](https://napi.rs).

This is work in progress and only covers a small part of the low-level API.
See https://github.com/automerge/automerge/pull/1099 for background.

## Principles

- **The public JavaScript API does not change.** This crate is an alternative
  backend for the low-level `API` interface that `@automerge/automerge`
  already consumes (`javascript/src/low_level.ts`, `UseApi`). Anything
  observable through the public package (values, patches, error classes and
  messages) must match the WebAssembly binding.
- **The WebAssembly binding stays.** `automerge-wasm` remains a first-class
  binding built on `wasm-bindgen`. Bridging WebAssembly through Node-API
  (e.g. via napi-rs' own wasm target) is not something we are pursuing for
  now.
- **Share logic, not JS glue.** Code with no dependency on a JS engine should
  live in one place and be used by both bindings. Code that builds or reads
  JS values may legitimately differ per binding, see below.

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
| Object metadata | Object id, datatype and meta are stored on every materialised object as non-enumerable `Symbol.for("_am_objectId")`, `_am_datatype_`, `_am_raw_value_`, `_am_meta` properties and read back in `unwrap_object` | The only way to associate Rust-side data with a JS object | `napi_wrap` / `napi_type_tag_object` could attach native data. Note that `javascript/src` reads these symbols too, so changing this is a JS-package change and not only a binding change |
| Binary data | `Uint8Array::to_vec()` on the way in and `Uint8Array::from(slice)` on the way out (`load`, `save`, changes, sync messages, byte scalars) | JS cannot hold references into linear memory, which may move when it grows | Borrow the incoming `ArrayBuffer` for the duration of the call, and hand ownership of an outgoing `Vec<u8>` to an external `ArrayBuffer`/`Buffer` without copying |
| Memory ceiling | 32-bit linear memory, 4GB at most | wasm32 | None needed. This is one of the motivations for the binding, and means sizes should not be assumed to fit in `u32` |
| Lifetime of handles | `Automerge` and `SyncState` must be released with `.free()`; `javascript/src/implementation.ts` calls it explicitly | wasm-bindgen objects are not collected by the JS GC | napi-rs classes are finalised by the GC. `free()` has to keep existing on the low-level API, but can become a no-op or an eager release |
| Untyped arguments | Most methods take `JsValue` and validate by hand; 49 of them are `skip_typescript` with types written in a `typescript_custom_section` | `wasm-bindgen` cannot express optional or union parameters | napi-rs can express `Option<T>` and `Either<A, B>` and generates the `.d.ts`. The hand-written wasm types remain the source of truth for the shared `API` interface, and the generated ones should be checked against them |
| Error classes | Each error enum is converted to a JS `RangeError`/`Error` by hand | `wasm-bindgen` only throws what it is given | napi-rs throws plain `Error` by default, so the binding needs explicit conversions to keep the same classes (see the TODO in `simple.test.ts`) |
| Start-up | Async instantiation (`initializeWasm`, base64 / bundler / workerd entrypoints in `javascript/src/entrypoints`), plus an `on_start` abort hook because a trapped instance is unusable afterwards | Module loading and trap semantics of wasm | Synchronous `require`, and a Rust panic can be caught and thrown as a normal exception |
| Text indexing | `utf16-indexing` feature and `TextEncoding::Utf16CodeUnit` | Not a wasm limitation: JS strings are UTF-16 | Keep. This binding must use the same setting to stay compatible |
