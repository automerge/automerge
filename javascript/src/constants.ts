// Properties of the document root object

export const STATE = Symbol.for("_am_meta") // symbol used to hide application metadata on automerge objects
export const TRACE = Symbol.for("_am_trace") // used for debugging
export const OBJECT_ID = Symbol.for("_am_objectId") // symbol used to hide the object id on automerge objects
export const IS_PROXY = Symbol.for("_am_isProxy") // symbol used to test if the document is a proxy object
export const CLEAR_CACHE = Symbol.for("_am_clearCache") // symbol used to tell a proxy object to clear its cache

export const UINT = Symbol.for("_am_uint")
export const INT = Symbol.for("_am_int")
export const F64 = Symbol.for("_am_f64")
export const COUNTER = Symbol.for("_am_counter")
export const TEXT = Symbol.for("_am_text")
export const IMMUTABLE_STRING = Symbol.for("_am_immutableString")
