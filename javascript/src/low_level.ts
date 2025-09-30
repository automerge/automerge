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
  DecodedBundle,
} from "./wasm_types.js"
export type { ChangeToEncode } from "./wasm_types.js"
import { default as initWasm } from "./wasm_bindgen_output/web/automerge_wasm.js"
import * as WasmApi from "./wasm_bindgen_output/web/automerge_wasm.js"

let _initialized = false
let _initializeListeners: (() => void)[] = []

export function UseApi(api: API) {
  for (const k in api) {
    // eslint-disable-next-line no-extra-semi
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
  readBundle(data: Uint8Array): DecodedBundle {
    throw new RangeError("Automerge.use() not called (readBundle)")
  },
}
/* eslint-enable */

/**
 * Initialize the wasm module
 *
 * @param wasmBlob - The wasm module as a Uint8Array, Request, Promise<Uint8Array> or string. If this argument is a string then it is assumed to be a URL and the library will attempt to fetch the wasm module from that URL.
 *
 * @remarks
 * If you are using the `/slim` subpath export then this function must be
 * called before any other functions in the library. If you are using any of
 * the other subpath exports then it will have already been called for you.
 */
export function initializeWasm(
  wasmBlob: Uint8Array | Request | Promise<Uint8Array> | string,
): Promise<void> {
  return initWasm({ module_or_path: wasmBlob }).then(_ => {
    UseApi(WasmApi)
  })
}

/**
 * Initialize the wasm module from a base64 encoded string
 *
 * @param wasmBase64 - The bytes of the wasm file as a base64 encoded string
 */
export function initializeBase64Wasm(wasmBase64: string): Promise<void> {
  return initializeWasm(Uint8Array.from(atob(wasmBase64), c => c.charCodeAt(0)))
}

/**
 * A promise which resolves when the web assembly module has been initialized
 * (or immediately if it has already been initialized)
 */
export function wasmInitialized(): Promise<void> {
  if (_initialized) return Promise.resolve()
  return new Promise(resolve => {
    _initializeListeners.push(resolve)
  })
}

/**
 * Check if the wasm module has been initialized
 *
 * @returns true if the wasm module has been initialized
 */
export function isWasmInitialized(): boolean {
  return _initialized
}
