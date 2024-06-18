import { UseApi } from "../low_level.js"
import * as api from "../wasm_bindgen_output/bundler/automerge_wasm.js"
//@ts-ignore
UseApi(api)

export * from "../next_slim.js"
