import { Counter } from "./types.ts"

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
} from "./types.ts"

import { RawString } from "./raw_string.ts"
export { RawString } from "./raw_string.ts"

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
