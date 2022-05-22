
export { Actor as ActorId, Value, Prop, ObjID, Change, DecodedChange, Heads, Automerge } from "automerge-wasm"
export { JsSyncState as SyncState, SyncMessage, DecodedSyncMessage } from "automerge-wasm"

export { Text } from "./text"
export { Counter  } from "./counter"
export { Int, Uint, Float64  } from "./numbers"

export type UnknownObject = Record<string | number | symbol, unknown>;
export type Dictionary<T> = Record<string, T>;

import { Counter } from "./counter"

export type AutomergeValue = ScalarValue | { [key: string]: AutomergeValue } | Array<AutomergeValue>
export type MapValue =  { [key: string]: AutomergeValue }
export type ListValue = Array<AutomergeValue> 
export type TextValue = Array<AutomergeValue>
export type ScalarValue = string | number | null | boolean | Date | Counter | Uint8Array
