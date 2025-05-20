import { UseApi } from "../low_level.js"
import * as api from "../wasm_bindgen_output/web/index.js"
UseApi(api)

import * as Automerge from "../index.js"

declare global {
  interface Window {
    Automerge: typeof Automerge
  }
}
window.Automerge = Automerge
