# How it works

Automerge is implemented as a Rust library which provides a low level key/value
interface to the CRDT data structure. In order to expose this in JavaScript we
do two things:

1. We compile the Rust code to WebAssembly with a thin JavaScript binding
   around it. This is implemented in the rust codebase in `../rust/automerge-wasm`
2. We implement a higher level, more idiomatic JavaScript API which exposes the
   automerge document as a POJO using JavaScript proxies. This is the code
   implemented in this directory.

Due to the complexity of WebAssembly loading we have to do some fiddly things
to handle loading WebAssembly in different environments. This is taken care of
with a combination of subpath exports in `package.json` and the platform
specific entry points in `src/entrypoints/*`. Read the rest of this document
for more details.

## Building and testing

If you just want to build and test the package you need to do this:

```
yarn install
yarn run build
yarn run test
```

Any time you change the rust code in `../rust/*` you'll need to re-run the
`yarn run build` command.

Read on to understand what the `build` step is doing.

## Packaging the WebAssembly

The `automerge-wasm` rust code uses `wasm-bindgen` to produce a WebAssembly
file plus a javascript wrapper which handles the complications of passing
objects across the WebAssembly/JavaScript boundary efficienty. `wasm-bindgen`
actually produces an entire NPM package which you can publish and depend on, we
used to do that but we don't do it any more. Instead we copy the generated
WebAssembly and it's JavaScript wrapper into this directory in the
`src/wasm_bindgen_output` directory. This is done by the `scripts/build.mjs`
script.

Why do we do this? The main reason is that we want to be flexible about how
the WebAssembly file is loaded. `wasm-bindgen` assumes that there will be some
kind of bundler in the picture which will take care of loading the WebAssembly
but in practice people want to use Automerge in all sorts of environments and
they don't always have a bundler available. What we aim to do is provide a
package which works with bundlers when they are available but provides fallback
options. We achieve this using [subpath exports](https://nodejs.org/api/packages.html#subpath-exports).

### Conditional exports

[!NOTE]
This is a brief note about how conditional exports work, feel free to skip it
if you already know.

Subpath exports are specified in the `exports` field of `package.json`. This
object maps paths to files in the package. For example:

```json
{
    ...
    exports: {
        ".": "./dist/index.js",
        "./nested": "./dist/nested/index.js"
    }
    ...
}
```

Would mean that `import * from "<package>"` would resolve to `./dist/index.js`
and `import * from "<package>/nested"` would resolve to `./dist/nested/index.js`.

Subpath exports can be "conditional", this means that instead of a simple map
from path to file, you can have a two level map from "condition" to path to
file. For example:

```json
{
    ...
    exports: {
        ".": {
            "node": {
                ".": "./dist/node/index.js",
                "./nested": "./dist/node/nested/index.js"
            },
            "browser": {
                ".": "./dist/node/index.js",
                "./nested": "./dist/node/nested/index.js"
            }
        }
    }
    ...
}
```

The `"node"` and `"browser"` keys are the conditions. These are reasonably
well supported by the various module loaders in the ecosystem. The example here
would mean that if you are in `node` then `import * from "<package>/nested"`
would resolve to `./dist/node/nested/index.js` but if you are in the browser it
would resolve to `./dist/browser/nested/index.js`. We can use this to provide
different implementations of the same module for different environments.

### Automerge's subpath exports

Our objective is to provide conditional exports which do the following:

- If you are using a bundler which supports WebAssembly (e.g. Webpack) then
  import the webassembly file as an ES module and allow the bundler to handle
  initializing it
- If you are in node or a cloudflare worker, directly import the WebAssembly
  file as these platforms support WebAssembly as modules
- Otherwise, provide an alternative import which allows you to load the
  WebAssembly yourself.

We need to do this in a manner which allows libraries to depend on automerge
without forcing their users into any particular initialization strategy. We
can do this with conditional exports on the "." path.

To allow the client to choose their own initialization strategy we also provide
a `/slim` subpath export. This export only includes the javascript code and
allows the user to figure out how to load the WebAssembly themselves. This is
also the path which libraries should depend on.

Finally, we also want to make it easy for the user to obtain the WebAssembly
file from this package, so we expose two subpath exports, one which provides
the WebAssembly file directly and another which exposes a base64 encoded
version of it.

Altogether then we have the following exports:

- `/`: The full package with WebAssembly initialization
- `/slim`: Only the JavaScript code, no WebAssembly initialization
- `/automerge.wasm`: The WebAssembly file
- `/automerge.wasm.base64`: The WebAssembly file as a module with a single
  export which is a base64 encoded string

These subpath exports are then mapped - using conditional exports - to platform
specific files in the `src/entrypoints` directory. For example, here are the
mappings for the `"."` export:

```json
  "exports": {
    ".": {
      "types": "./dist/index.d.ts",
      "workerd": {
        "import": "./dist/mjs/entrypoints/fullfat_workerd.js",
        "require": "./dist/cjs/fullfat_base64.cjs"
      },
      "node": {
        "import": "./dist/mjs/entrypoints/fullfat_node.js",
        "require": "./dist/cjs/fullfat_node.cjs"
      },
      "browser": {
        "import": "./dist/mjs/entrypoints/fullfat_bundler.js",
        "require": "./dist/cjs/fullfat_base64.cjs"
      },
      "import": "./dist/mjs/entrypoints/fullfat_base64.js",
      "require": "./dist/cjs/fullfat_base64.cjs"
    },
    ...
  }
```

### Testing

All of this packaging magic is quite fragile, it depends a lot on the various
module loader conventions. In order to make sure that this stuff continues to
work we have a test suite which builds a package for each (subpath, platform)
combination and tests that the package loads the WebAssembly correctly. This
is implemented in `./packaging_tests/run.mjs`.
