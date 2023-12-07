import { Counter } from "./types.js"

export {
  Counter,
  type Doc,
  Int,
  Uint,
  Float64,
  type Patch,
  type MapObjType,
  type PatchCallback,
  type Mark,
  type MarkSet,
  type MarkRange,
  type MarkValue,
  type Cursor,
  type PatchInfo,
  type PatchSource,
} from "./types.js"

import { RawString } from "./raw_string.js"
export { RawString } from "./raw_string.js"

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
  | RawString
