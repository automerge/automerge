import type {
  API,
  Automerge,
  Change,
  DecodedChange,
  SyncMessage,
  SyncState,
  JsSyncState,
  DecodedSyncMessage,
  ChangeToEncode,
  LoadOptions,
  InitOptions,
} from "./wasm_types.js"
export type { ChangeToEncode } from "./wasm_types.js"
import { default as initWasm } from "./wasm_bindgen_output/web/automerge_wasm.js"
import * as WasmApi from "./wasm_bindgen_output/web/automerge_wasm.js"

let _initialized = false
let _initializeListeners: (() => void)[] = []

export function UseApi(api: API) {
  for (const k in api) {
    // eslint-disable-next-line @typescript-eslint/no-extra-semi,@typescript-eslint/no-explicit-any
    ;(ApiHandler as any)[k] = (api as any)[k]
  }
  _initialized = true
  for (const listener of _initializeListeners) {
    listener()
  }
}

/* eslint-disable */
export const ApiHandler: API = {
  create(options?: InitOptions): Automerge {
    throw new RangeError("Automerge.use() not called")
  },
  load(data: Uint8Array, options?: LoadOptions): Automerge {
    throw new RangeError("Automerge.use() not called (load)")
  },
  encodeChange(change: ChangeToEncode): Change {
    throw new RangeError("Automerge.use() not called (encodeChange)")
  },
  decodeChange(change: Change): DecodedChange {
    throw new RangeError("Automerge.use() not called (decodeChange)")
  },
  initSyncState(): SyncState {
    throw new RangeError("Automerge.use() not called (initSyncState)")
  },
  encodeSyncMessage(message: DecodedSyncMessage): SyncMessage {
    throw new RangeError("Automerge.use() not called (encodeSyncMessage)")
  },
  decodeSyncMessage(msg: SyncMessage): DecodedSyncMessage {
    throw new RangeError("Automerge.use() not called (decodeSyncMessage)")
  },
  encodeSyncState(state: SyncState): Uint8Array {
    throw new RangeError("Automerge.use() not called (encodeSyncState)")
  },
  decodeSyncState(data: Uint8Array): SyncState {
    throw new RangeError("Automerge.use() not called (decodeSyncState)")
  },
  exportSyncState(state: SyncState): JsSyncState {
    throw new RangeError("Automerge.use() not called (exportSyncState)")
  },
  importSyncState(state: JsSyncState): SyncState {
    throw new RangeError("Automerge.use() not called (importSyncState)")
  },
}
/* eslint-enable */

export function initializeWasm(
  wasmBlob: Uint8Array | Request | Promise<Uint8Array> | string,
): Promise<void> {
  return initWasm(wasmBlob).then(_ => {
    UseApi(WasmApi)
  })
}

export function initializeBase64Wasm(wasmBase64: string): Promise<void> {
  return initializeWasm(Uint8Array.from(atob(wasmBase64), c => c.charCodeAt(0)))
}

export function wasmInitialized(): Promise<void> {
  if (_initialized) return Promise.resolve()
  return new Promise(resolve => {
    _initializeListeners.push(resolve)
  })
}

export function isWasmInitialized(): boolean {
  return _initialized
}
