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

import init from "./bindgen.js"
export default function() {
  return new Promise((resolve,reject) => init().then(() => {
    resolve({ ... api, load, create, foo: "bar" })
  }))
}
