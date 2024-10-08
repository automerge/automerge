import { UseApi } from "../low_level.js"
import * as api from "../wasm_bindgen_output/workerd/automerge_wasm.js"
// @ts-ignore
import wasm from "../wasm_bindgen_output/workerd/automerge_wasm_bg.wasm"
api.initSync({ module: wasm })
//@ts-ignore
UseApi(api)

export * from "../stable.js"
export * as next from "../next_slim.js"
