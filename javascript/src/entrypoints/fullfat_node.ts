import { UseApi } from "../low_level.js"
import * as api from "../wasm_bindgen_output/nodejs/automerge_wasm.cjs"
UseApi(api)

export * from "../stable.js"
export * as next from "../next_slim.js"
