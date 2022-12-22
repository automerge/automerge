# Vite + Automerge

There are three things you need to do to get WASM packaging working with vite:

1. Install the top level await plugin
2. Install the `vite-plugin-wasm` plugin
3. Exclude `automerge-wasm` from the optimizer

First, install the packages we need:

```bash
yarn add vite-plugin-top-level-await
yarn add vite-plugin-wasm
```

In `vite.config.js`

```javascript
import { defineConfig } from "vite"
import wasm from "vite-plugin-wasm"
import topLevelAwait from "vite-plugin-top-level-await"

export default defineConfig({
  plugins: [topLevelAwait(), wasm()],

  // This is only necessary if you are using `SharedWorker` or `WebWorker`, as
  // documented in https://vitejs.dev/guide/features.html#import-with-constructors
  worker: {
    format: "es",
    plugins: [topLevelAwait(), wasm()],
  },

  optimizeDeps: {
    // This is necessary because otherwise `vite dev` includes two separate
    // versions of the JS wrapper. This causes problems because the JS
    // wrapper has a module level variable to track JS side heap
    // allocations, initializing this twice causes horrible breakage
    exclude: ["@automerge/automerge-wasm"],
  },
})
```

Now start the dev server:

```bash
yarn vite
```

## Running the example

```bash
yarn install
yarn dev
```
