import { UseApi } from "../low_level.js"
import * as api from "../wasm_bindgen_output/bundler/automerge_wasm.js"
UseApi(api)

export * from "../index.js"
