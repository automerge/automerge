export {
  loadDoc as load,
  create,
  encodeChange,
  decodeChange,
  initSyncState,
  encodeSyncMessage,
  decodeSyncMessage,
  encodeSyncState,
  decodeSyncState,
  exportSyncState,
  importSyncState,
} from "./bindgen.js"
import {
  loadDoc as load,
  create,
  encodeChange,
  decodeChange,
  initSyncState,
  encodeSyncMessage,
  decodeSyncMessage,
  encodeSyncState,
  decodeSyncState,
  exportSyncState,
  importSyncState,
} from "./bindgen.js"

let api = {
  load,
  create,
  encodeChange,
  decodeChange,
  initSyncState,
  encodeSyncMessage,
  decodeSyncMessage,
  encodeSyncState,
  decodeSyncState,
  exportSyncState,
  importSyncState
}

import wasm_init from "./bindgen.js"

export function init() {
  return new Promise((resolve,reject) => wasm_init().then(() => {
    resolve({ ... api, load, create })
  }))
}

