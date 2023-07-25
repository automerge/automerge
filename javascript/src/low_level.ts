import {
  type API,
  Automerge,
  type Change,
  type DecodedChange,
  SyncState,
  type SyncMessage,
  type JsSyncState,
  type DecodedSyncMessage,
  type ChangeToEncode,
  type LoadOptions,
  type InitOptions,
} from "@automerge/automerge-wasm"
export type { ChangeToEncode } from "@automerge/automerge-wasm"

export function UseApi(api: API) {
  for (const k in api) {
    // eslint-disable-next-line @typescript-eslint/no-extra-semi,@typescript-eslint/no-explicit-any
    ;(ApiHandler as any)[k] = (api as any)[k]
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
