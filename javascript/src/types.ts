export { Counter } from "./counter.js"
export { Int, Uint, Float64 } from "./numbers.js"

import { Counter } from "./counter.js"
import type { Patch } from "./wasm_types.js"
export type {
  ConflictPatch,
  Cursor,
  CursorPosition,
  MoveCursor,
  MapObjType,
  MarkSet,
  Mark,
  MarkRange,
  MarkPatch,
  Patch,
  UnmarkPatch,
} from "./wasm_types.js"

export type MarkValue = string | number | null | boolean | Date | Uint8Array

/**
 * An automerge document.
 * @typeParam T - The type of the value contained in this document
 *
 * Note that this provides read only access to the fields of the value. To
 * modify the value use {@link change}
 */
export type Doc<T> = { readonly [P in keyof T]: T[P] }

export type PatchSource =
  | "from"
  | "emptyChange"
  | "change"
  | "changeAt"
  | "merge"
  | "loadIncremental"
  | "applyChanges"
  | "receiveSyncMessage"
export type PatchInfo<T> = {
  before: Doc<T>
  after: Doc<T>
  source: PatchSource
}

/**
 * Callback which is called by various methods in this library to notify the
 * user of what changes have been made.
 * @param patch - A description of the changes made
 * @param info - An object that has the "before" and "after" document state, and the "from" and "to" heads
 */
export type PatchCallback<T> = (
  patches: Array<Patch>,
  info: PatchInfo<T>,
) => void

import { ImmutableString } from "./immutable_string.js"
export { ImmutableString } from "./immutable_string.js"

export type AutomergeValue =
  | ScalarValue
  | { [key: string]: AutomergeValue }
  | Array<AutomergeValue>
export type MapValue = { [key: string]: AutomergeValue }
export type ListValue = Array<AutomergeValue>
export type ScalarValue =
  | string
  | number
  | null
  | boolean
  | Date
  | Counter
  | Uint8Array
  | ImmutableString
