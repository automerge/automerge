import { defineConfig } from "vite"
import wasm from "vite-plugin-wasm"
import topLevelAwait from "vite-plugin-top-level-await"

export default defineConfig({
    plugins: [topLevelAwait(), wasm()],

    optimizeDeps: {
        // This is necessary because otherwise `vite dev` includes two separate
        // versions of the JS wrapper. This causes problems because the JS
        // wrapper has a module level variable to track JS side heap
        // allocations, initializing this twice causes horrible breakage
        exclude: ["@automerge/automerge-wasm"]
    }
})
