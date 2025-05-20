import { UseApi } from "../low_level.js"
import * as api from "../wasm_bindgen_output/workerd/automerge_wasm.js"
// @ts-expect-error wasm module is un typed
import wasm from "../wasm_bindgen_output/workerd/automerge_wasm_bg.wasm"
api.initSync({ module: wasm })
UseApi(api)

export * from "../index.js"
