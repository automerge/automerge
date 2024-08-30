import { UseApi } from "../low_level.js"
import * as api from "../wasm_bindgen_output/web/index.js"
//@ts-ignore
UseApi(api)

import * as next from "../next_slim.js"

declare global {
  interface Window {
    Automerge: typeof next
  }
}
window.Automerge = next
