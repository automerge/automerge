// Properties of the document root object

export const OBJ_META = Symbol.for("_am_obj_meta") // symbol used to hide automerge metadata on the proxy object

export const TRACE = Symbol.for("_am_trace") // used for debugging
export const CLEAR_CACHE = Symbol.for("_am_clearCache") // symbol used to tell a proxy object to clear its cache

export const UINT = Symbol.for("_am_uint")
export const INT = Symbol.for("_am_int")
export const F64 = Symbol.for("_am_f64")
export const COUNTER = Symbol.for("_am_counter")
export const TEXT = Symbol.for("_am_text")
