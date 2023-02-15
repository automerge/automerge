## Automerge

Automerge is a library of data structures for building collaborative
applications, this package is the javascript implementation.

Detailed documentation is available at [automerge.org](http://automerge.org/)
but see the following for a short getting started guid.

## Quickstart

First, install the library.

```
yarn add @automerge/automerge
```

If you're writing a `node` application, you can skip straight to [Make some
data](#make-some-data). If you're in a browser you need a bundler

### Bundler setup

`@automerge/automerge` is a wrapper around a core library which is written in
rust, compiled to WebAssembly and distributed as a separate package called
`@automerge/automerge-wasm`. Browsers don't currently support WebAssembly
modules taking part in ESM module imports, so you must use a bundler to import
`@automerge/automerge` in the browser. There are a lot of bundlers out there, we
have examples for common bundlers in the `examples` folder. Here is a short
example using Webpack 5.

Assuming a standard setup of a new webpack project, you'll need to enable the
`asyncWebAssembly` experiment. In a typical webpack project that means adding
something like this to `webpack.config.js`

```javascript
module.exports = {
  ...
  experiments: { asyncWebAssembly: true },
  performance: {       // we dont want the wasm blob to generate warnings
     hints: false,
     maxEntrypointSize: 512000,
     maxAssetSize: 512000
  }
};
```

### Make some data

Automerge allows to separate threads of execution to make changes to some data
and always be able to merge their changes later.

```javascript
import * as automerge from "@automerge/automerge"
import * as assert from "assert"

let doc1 = automerge.from({
  tasks: [
    { description: "feed fish", done: false },
    { description: "water plants", done: false },
  ],
})

// Create a new thread of execution
let doc2 = automerge.clone(doc1)

// Now we concurrently make changes to doc1 and doc2

// Complete a task in doc2
doc2 = automerge.change(doc2, d => {
  d.tasks[0].done = true
})

// Add a task in doc1
doc1 = automerge.change(doc1, d => {
  d.tasks.push({
    description: "water fish",
    done: false,
  })
})

// Merge changes from both docs
doc1 = automerge.merge(doc1, doc2)
doc2 = automerge.merge(doc2, doc1)

// Both docs are merged and identical
assert.deepEqual(doc1, {
  tasks: [
    { description: "feed fish", done: true },
    { description: "water plants", done: false },
    { description: "water fish", done: false },
  ],
})

assert.deepEqual(doc2, {
  tasks: [
    { description: "feed fish", done: true },
    { description: "water plants", done: false },
    { description: "water fish", done: false },
  ],
})
```

## Development

See [HACKING.md](./HACKING.md)

## Meta

Copyright 2017–present, the Automerge contributors. Released under the terms of the
MIT license (see `LICENSE`).
