import { type ObjID, type Heads, Automerge } from "@automerge/automerge-wasm"

import { OBJ_META, CLEAR_CACHE, TRACE } from "./constants.js"

import { ApiHandler } from "./low_level.js"

import type { ObjMetadata, Doc, PatchCallback } from "./types.js"

export interface InternalState<T> {
  handle: Automerge
  heads: Heads | undefined
  freeze: boolean
  mostRecentPatch: any // TODO: type this
  patchCallback?: PatchCallback<T>
  textV2: boolean
}

export function _strict_meta<T>(
  doc: Doc<T>,
  checkroot = true,
): ObjMetadata<InternalState<T>> {
  if (typeof doc !== "object") {
    throw new RangeError("must be the document root")
  }
  let meta = _meta(doc)
  if (meta === null || (checkroot && meta?.obj !== "_root")) {
    throw new RangeError("must be the document root")
  }
  return meta
}

export function _clear_cache<T>(doc: Doc<T>): void {
  Reflect.set(doc, CLEAR_CACHE, true)
}

export function _trace<T>(doc: Doc<T>): string | undefined {
  return Reflect.get(doc, TRACE) as string
}

export const META = new WeakMap()

export function _state<T>(doc: Doc<T>, checkroot?: boolean): InternalState<T> {
  return _strict_meta(doc, checkroot).user_data
}

export function _obj<T>(doc: Doc<T>): ObjID | null {
  return _meta(doc)?.obj || null
}

export function _meta<T>(doc: Doc<T>): ObjMetadata<InternalState<T>> | null {
  if (!(typeof doc === "object") || doc === null) {
    return null
  }
  const obj_metadata = ApiHandler.getObjMetadata<InternalState<T>>(META, doc)
  if (obj_metadata !== undefined) {
    return obj_metadata || null
  } else {
    return (Reflect.get(doc, OBJ_META) as ObjMetadata<InternalState<T>>) || null
  }
}

export function _is_proxy<T>(doc: Doc<T>): boolean {
  return !!(
    (Reflect.get(doc, OBJ_META) as ObjMetadata<InternalState<T>>) || undefined
  )?.proxy
}
