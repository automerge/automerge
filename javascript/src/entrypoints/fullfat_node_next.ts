import { UseApi } from "../low_level.js"
import * as api from "../wasm_bindgen_output/nodejs/automerge_wasm.cjs"
//@ts-ignore
UseApi(api)

export * from "../next_slim.js"

