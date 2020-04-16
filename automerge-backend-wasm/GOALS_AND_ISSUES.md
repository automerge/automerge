
# WASM Goals and Issues

We set out with this project to see if we could create a backend implementation 
for Automerge that could serve as a basis for native ports to many different
languages but also replace the javascript backend of the current implementation 
without any compromises.

We chose Rust as the basis of this project.  It has the same performance 
characteristics as C and C++ making it ideal for implementing a database-like
tool. It also has safety guarantees C and C++ which will protect us from
synchronization issues and data races that plague projects like this.  Rust
also has a very mature WASM integration suite of tools.

Our goal was to create a zero compromise implementation of the backend.  We
almost achieved this goal.  Here are the details of the compromises we found.

## Problem: WASM memory and garbage collection

Memory allocated in WASM needs to be explicitly freed.  And there is no feature
(yet) in javascript to alert you when an object has been collected by the
GC.  This makes immutable API's undoable since you need the GC to collect old
versions of objects.

Also this means that an Automerge backend would need to be explicitly freed at the 
end of its life.  Under normal circumstances a backend will live indefinitely so this 
would not require a change but in situations where many small databases are being 
created and thrown away this requires an API change.

## Solution

The performance branch of Automerge has made some small but important adjustments to 
the Frontend/Backend API.  These now assume the backends to be long lived and possibly
mutable and disallows creating divergent histories with old handles to the backend.
A `clone` function was added to allow this behavior if it was intentional and a `free`
that can do cleanup.

```js
    let doc1 = Automerge.init();
    let doc2 = Automerge.clone(doc1);
    Automerge.free(doc1);
```

## Problem: WASM in fundamentally async - Automerge is sync

WASM's love of all things async was surely the largest thorn in our side was dealing with this.  It basically boils down to this...

1. ### Loading WASM requires IO - IO is async
  
  WASM binaries are not js - loading them from JS is async (with the notable exception of node's `readFileSync()`)

2. ### WebAssembly.Module(buffer) has a 4k limit on the render thread in browsers
  
  Even if you can synchronously load and compile the wasm, most browsers impose a 4k limit on synchronous (but not asynchronous) WASM compilation in the render thread.  This is not an issue in node applications or in web workers.

## Solutions

1. ### Compile Rust to ASM.js - (no problems except it's big and slow)

  Now it's javascript.  All the strangeness of WASM goes away.  Webpack will happily inline the code into a bundle.  The only downside, 400k of WASM becomes 5M of js and it runs 3 times slower.
  
2. ### Inline the WASM as a base64 encoded string - (no problems except the render thread)

  This is actually surprisingly effective.  The sized added to the js bundle is reasonable and the decode time is trivial.  The only issue is, it still wont work in the render thread
  
3. ### Wait for top level await (no problems - someday)

  There is a proposal for top level await support in js modules.  This would allow us to insert an internal await into the backend module and hide the async load from users.  Unfortunately its not in JS yet... 
  
4. ### Change Automerge.init to be async (no problems except a breaking api change)

  All of the async strangeness can be boiled down to the Automerge.init() call. This would require introducing an api change that has no purpose in the JS only implementation and represents a non-trivial compromise in adopting WASM
  ```js
  const doc = Automerge.init();
  // becomes 
  const doc = await Automerge.init();
  ```

