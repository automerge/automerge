import alias from "@rollup/plugin-alias"
import nodeResolve from "@rollup/plugin-node-resolve"
import replace from "@rollup/plugin-replace"
import wasm from "@rollup/plugin-wasm"
import { defineConfig } from "rollup"
import tla from "rollup-plugin-tla"

const inPageConfig = {
  input: "src/index.js",
  output: {
    file: "public/main.js",
  },
  // Fixes the `this` instance at top level in automerge's `stable.js` file
  context: "globalThis",
  plugins: [
    // Force automerge-wasm to resolve to its web version
    alias({
      entries: [
        {
          find: "@automerge/automerge-wasm",
          replacement: "node_modules/@automerge/automerge-wasm/web/index.js",
        },
      ],
    }),
    // Ensure we call the `WASM` loading function created by `@rollup/plugin-wasm`
    // See https://www.npmjs.com/package/@rollup/plugin-wasm#using-with-wasm-bindgen-and-wasm-pack
    // The `initSync` function doesn't need awaiting, unlike the `init` of the plugin's example
    replace({
      preventAssignment: true,
      delimiters: ["", ""],
      values: {
        "initSync(WASM);": "initSync(await WASM());",
      },
    }),
    nodeResolve({
      browser: true,
    }),
    wasm({
      targetEnv: "browser",
    }),
  ],
}

const inServiceWorkerConfig = {
  // When running inside a service worker automerge needs to be bundled on its own
  // then brought in the worker script with `importScripts`.
  // If bundled inside the script, automerge's code would be hoisted at the top,
  // always delaying the registration of the script's event listeners.
  // These listeners are critical for telling the page when the worker
  // has loaded automerge-wasm and is actually ready to roll
  input: "@automerge/automerge",
  // Not all browsers support ES module Service Workers
  // so we'll bundle to an IIFE that'll set up an `AutomergeAPI` global variable.
  // It'll be a Promise that'll resolve once the wasm is loaded.
  output: {
    format: "iife",
    name: "AutomergeAPI",
    file: "public/in-service-worker/automerge.js",
  },
  // Fixes the `this` instance at top level in automerge's `stable.js` file
  context: "globalThis",
  plugins: [
    // Force automerge-wasm to resolve to its web version
    alias({
      entries: [
        {
          find: "@automerge/automerge-wasm",
          replacement: "node_modules/@automerge/automerge-wasm/web/index.js",
        },
      ],
    }),
    // Ensure we call the `WASM` loading function created by `@rollup/plugin-wasm`
    // See https://www.npmjs.com/package/@rollup/plugin-wasm#using-with-wasm-bindgen-and-wasm-pack
    // The `initSync` function doesn't need awaiting, unlike the `init` of the plugin's example
    replace({
      preventAssignment: true,
      delimiters: ["", ""],
      values: {
        "initSync(WASM);": "initSync(await WASM());",
      },
    }),
    // The `initSync(await WASM();` call is a top-level await,
    // which is only supported inside ES Modules (not supported by Firefox)
    // This is what makes `AutomergeAPI` be a Promise
    tla(),
    nodeResolve({
      browser: true,
    }),
    wasm({
      targetEnv: "browser",
      // Set the correct prefix to the path
      // used for loading the `.wasm` file
      publicPath: "/in-service-worker/",
    }),
  ],
}

export default defineConfig(() => {
  return [inPageConfig, inServiceWorkerConfig]
})
