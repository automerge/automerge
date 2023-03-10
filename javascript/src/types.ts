export { Text } from "./text"
import { Text } from "./text"
export { Counter } from "./counter"
export { Int, Uint, Float64 } from "./numbers"

import { Counter } from "./counter"
import type { Patch, PatchInfo } from "@automerge/automerge-wasm"
export type { Patch, Mark } from "@automerge/automerge-wasm"

export type AutomergeValue =
  | ScalarValue
  | { [key: string]: AutomergeValue }
  | Array<AutomergeValue>
  | Text
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

/**
 * An automerge document.
 * @typeParam T - The type of the value contained in this document
 *
 * Note that this provides read only access to the fields of the value. To
 * modify the value use {@link change}
 */
export type Doc<T> = { readonly [P in keyof T]: T[P] }

/**
 * Callback which is called by various methods in this library to notify the
 * user of what changes have been made.
 * @param patch - A description of the changes made
 * @param info - An object that has the "before" and "after" document state, and the "from" and "to" heads
 */
export type PatchCallback<T> = (
  patches: Array<Patch>,
  info: PatchInfo<T>
) => void
