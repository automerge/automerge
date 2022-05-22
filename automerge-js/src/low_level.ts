
import { Automerge, Change, DecodedChange, Actor, SyncState, SyncMessage, JsSyncState, DecodedSyncMessage }  from "automerge-wasm"
import { API as LowLevelApi }  from "automerge-wasm"
export { API as LowLevelApi }  from "automerge-wasm"

export function UseApi(api: LowLevelApi) {
  for (const k in api) {
    ApiHandler[k] = api[k]
  }
}

/* eslint-disable */
export const ApiHandler : LowLevelApi = {
  create(actor?: Actor): Automerge { throw new RangeError("Automerge.use() not called") },
  load(data: Uint8Array, actor?: Actor): Automerge { throw new RangeError("Automerge.use() not called") },
  encodeChange(change: DecodedChange): Change { throw new RangeError("Automerge.use() not called") },
  decodeChange(change: Change): DecodedChange { throw new RangeError("Automerge.use() not called") },
  initSyncState(): SyncState { throw new RangeError("Automerge.use() not called") },
  encodeSyncMessage(message: DecodedSyncMessage): SyncMessage { throw new RangeError("Automerge.use() not called") },
  decodeSyncMessage(msg: SyncMessage): DecodedSyncMessage { throw new RangeError("Automerge.use() not called") },
  encodeSyncState(state: SyncState): Uint8Array { throw new RangeError("Automerge.use() not called") },
  decodeSyncState(data: Uint8Array): SyncState { throw new RangeError("Automerge.use() not called") },
  exportSyncState(state: SyncState): JsSyncState { throw new RangeError("Automerge.use() not called") },
  importSyncState(state: JsSyncState): SyncState { throw new RangeError("Automerge.use() not called") },
}
/* eslint-enable */
