// Properties of the document root object
//const OPTIONS   = Symbol('_options')   // object containing options passed to init()
//const CACHE     = Symbol('_cache')     // map from objectId to immutable object
export const STATE      = Symbol.for('_am_state')     // object containing metadata about current state (e.g. sequence numbers)
export const HEADS      = Symbol.for('_am_heads')     // object containing metadata about current state (e.g. sequence numbers)
export const OBJECT_ID  = Symbol.for('_am_objectId')     // object containing metadata about current state (e.g. sequence numbers)
export const READ_ONLY  = Symbol.for('_am_readOnly')     // object containing metadata about current state (e.g. sequence numbers)
export const FROZEN     = Symbol.for('_am_frozen')     // object containing metadata about current state (e.g. sequence numbers)

export const UINT     = Symbol.for('_am_uint')
export const INT      = Symbol.for('_am_int')
export const F64      = Symbol.for('_am_f64')
export const COUNTER  = Symbol.for('_am_counter')
export const TEXT     = Symbol.for('_am_text')

// Properties of all Automerge objects
//const OBJECT_ID = Symbol('_objectId')  // the object ID of the current object (string)
//const CONFLICTS = Symbol('_conflicts') // map or list (depending on object type) of conflicts
//const CHANGE    = Symbol('_change')    // the context object on proxy objects used in change callback
//const ELEM_IDS  = Symbol('_elemIds')   // list containing the element ID of each list element


