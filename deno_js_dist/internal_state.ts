import { type ObjID, type Heads, Automerge } from "https://deno.land/x/automerge_wasm@0.11.0/automerge_wasm.js";

import { STATE, OBJECT_ID, CLEAR_CACHE, TRACE, IS_PROXY } from "./constants.ts"

import type { Doc, PatchCallback } from "./types.ts"

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
  const state = Reflect.get(doc, STATE) as InternalState<T>
  if (
    state === undefined ||
    state == null ||
    (checkroot && _obj(doc) !== "_root")
  ) {
    throw new RangeError("must be the document root")
  }
  return state
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
  return Reflect.get(doc, OBJECT_ID) as ObjID
}

export function _is_proxy<T>(doc: Doc<T>): boolean {
  return !!Reflect.get(doc, IS_PROXY)
}
