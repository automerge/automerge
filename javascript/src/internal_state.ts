import { type ObjID, type Heads, Automerge } from "@automerge/automerge-wasm"

import { STATE, OBJECT_ID, CLEAR_CACHE, TRACE, IS_PROXY } from "./constants.js"

import { ApiHandler } from "./low_level.js"

import type { Doc, PatchCallback } from "./types.js"

export interface InternalState<T> {
  handle: Automerge
  heads: Heads | undefined
  freeze: boolean
  mostRecentPatch: any // TODO: type this
  patchCallback?: PatchCallback<T>
  textV2: boolean
}

export function _state<T>(doc: Doc<T>, checkroot = true): InternalState<T> {
  if (typeof doc !== "object") {
    throw new RangeError("must be the document root")
  }
  let state = ApiHandler.getObjMetadata(doc);
  if (state === undefined) {
    let proxy_state = Reflect.get(doc, STATE);
    if (proxy_state !== undefined) {
        return proxy_state as InternalState<T>
    }
  }
  if (state === undefined || (checkroot && state.obj !== "_root")) {
    throw new RangeError("must be the document root")
  }
  return state.user_data as InternalState<T>
}

export function _clear_cache<T>(doc: Doc<T>): void {
  Reflect.set(doc, CLEAR_CACHE, true)
}

export function _trace<T>(doc: Doc<T>): string | undefined {
  return Reflect.get(doc, TRACE) as string
}

export function _obj<T>(doc: Doc<T>): ObjID | null {
  if (!(typeof doc === "object") || doc === null) {
    return null
  }
  const obj_metadata = ApiHandler.getObjMetadata(doc);
  if (obj_metadata !== undefined) {
    return obj_metadata.obj || null
  } else {
    // try proxy
    return Reflect.get(doc, OBJECT_ID) as ObjID
  }
}

export function _is_proxy<T>(doc: Doc<T>): boolean {
  return !!Reflect.get(doc, IS_PROXY)
}
