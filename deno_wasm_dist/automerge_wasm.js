/// <reference types="./index.d.ts" />


const heap = new Array(128).fill(undefined);

heap.push(undefined, null, true, false);

function getObject(idx) { return heap[idx]; }

let heap_next = heap.length;

function dropObject(idx) {
    if (idx < 132) return;
    heap[idx] = heap_next;
    heap_next = idx;
}

function takeObject(idx) {
    const ret = getObject(idx);
    dropObject(idx);
    return ret;
}

const cachedTextDecoder = (typeof TextDecoder !== 'undefined' ? new TextDecoder('utf-8', { ignoreBOM: true, fatal: true }) : { decode: () => { throw Error('TextDecoder not available') } } );

if (typeof TextDecoder !== 'undefined') { cachedTextDecoder.decode(); };

let cachedUint8Memory0 = null;

function getUint8Memory0() {
    if (cachedUint8Memory0 === null || cachedUint8Memory0.byteLength === 0) {
        cachedUint8Memory0 = new Uint8Array(wasm.memory.buffer);
    }
    return cachedUint8Memory0;
}

function getStringFromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return cachedTextDecoder.decode(getUint8Memory0().subarray(ptr, ptr + len));
}

function addHeapObject(obj) {
    if (heap_next === heap.length) heap.push(heap.length + 1);
    const idx = heap_next;
    heap_next = heap[idx];

    heap[idx] = obj;
    return idx;
}

let WASM_VECTOR_LEN = 0;

const cachedTextEncoder = (typeof TextEncoder !== 'undefined' ? new TextEncoder('utf-8') : { encode: () => { throw Error('TextEncoder not available') } } );

const encodeString = function (arg, view) {
    return cachedTextEncoder.encodeInto(arg, view);
};

function passStringToWasm0(arg, malloc, realloc) {

    if (realloc === undefined) {
        const buf = cachedTextEncoder.encode(arg);
        const ptr = malloc(buf.length, 1) >>> 0;
        getUint8Memory0().subarray(ptr, ptr + buf.length).set(buf);
        WASM_VECTOR_LEN = buf.length;
        return ptr;
    }

    let len = arg.length;
    let ptr = malloc(len, 1) >>> 0;

    const mem = getUint8Memory0();

    let offset = 0;

    for (; offset < len; offset++) {
        const code = arg.charCodeAt(offset);
        if (code > 0x7F) break;
        mem[ptr + offset] = code;
    }

    if (offset !== len) {
        if (offset !== 0) {
            arg = arg.slice(offset);
        }
        ptr = realloc(ptr, len, len = offset + arg.length * 3, 1) >>> 0;
        const view = getUint8Memory0().subarray(ptr + offset, ptr + len);
        const ret = encodeString(arg, view);

        offset += ret.written;
    }

    WASM_VECTOR_LEN = offset;
    return ptr;
}

function isLikeNone(x) {
    return x === undefined || x === null;
}

let cachedInt32Memory0 = null;

function getInt32Memory0() {
    if (cachedInt32Memory0 === null || cachedInt32Memory0.byteLength === 0) {
        cachedInt32Memory0 = new Int32Array(wasm.memory.buffer);
    }
    return cachedInt32Memory0;
}

let cachedFloat64Memory0 = null;

function getFloat64Memory0() {
    if (cachedFloat64Memory0 === null || cachedFloat64Memory0.byteLength === 0) {
        cachedFloat64Memory0 = new Float64Array(wasm.memory.buffer);
    }
    return cachedFloat64Memory0;
}

function debugString(val) {
    // primitive types
    const type = typeof val;
    if (type == 'number' || type == 'boolean' || val == null) {
        return  `${val}`;
    }
    if (type == 'string') {
        return `"${val}"`;
    }
    if (type == 'symbol') {
        const description = val.description;
        if (description == null) {
            return 'Symbol';
        } else {
            return `Symbol(${description})`;
        }
    }
    if (type == 'function') {
        const name = val.name;
        if (typeof name == 'string' && name.length > 0) {
            return `Function(${name})`;
        } else {
            return 'Function';
        }
    }
    // objects
    if (Array.isArray(val)) {
        const length = val.length;
        let debug = '[';
        if (length > 0) {
            debug += debugString(val[0]);
        }
        for(let i = 1; i < length; i++) {
            debug += ', ' + debugString(val[i]);
        }
        debug += ']';
        return debug;
    }
    // Test for built-in
    const builtInMatches = /\[object ([^\]]+)\]/.exec(toString.call(val));
    let className;
    if (builtInMatches.length > 1) {
        className = builtInMatches[1];
    } else {
        // Failed to match the standard '[object ClassName]'
        return toString.call(val);
    }
    if (className == 'Object') {
        // we're a user defined class or Object
        // JSON.stringify avoids problems with cycles, and is generally much
        // easier than looping through ownProperties of `val`.
        try {
            return 'Object(' + JSON.stringify(val) + ')';
        } catch (_) {
            return 'Object';
        }
    }
    // errors
    if (val instanceof Error) {
        return `${val.name}: ${val.message}\n${val.stack}`;
    }
    // TODO we could test for more things here, like `Set`s and `Map`s.
    return className;
}

function _assertClass(instance, klass) {
    if (!(instance instanceof klass)) {
        throw new Error(`expected instance of ${klass.name}`);
    }
    return instance.ptr;
}
/**
* @param {any} options
* @returns {Automerge}
*/
export function create(options) {
    try {
        const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
        wasm.create(retptr, addHeapObject(options));
        var r0 = getInt32Memory0()[retptr / 4 + 0];
        var r1 = getInt32Memory0()[retptr / 4 + 1];
        var r2 = getInt32Memory0()[retptr / 4 + 2];
        if (r2) {
            throw takeObject(r1);
        }
        return Automerge.__wrap(r0);
    } finally {
        wasm.__wbindgen_add_to_stack_pointer(16);
    }
}

/**
* @param {Uint8Array} data
* @param {any} options
* @returns {Automerge}
*/
export function load(data, options) {
    try {
        const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
        wasm.load(retptr, addHeapObject(data), addHeapObject(options));
        var r0 = getInt32Memory0()[retptr / 4 + 0];
        var r1 = getInt32Memory0()[retptr / 4 + 1];
        var r2 = getInt32Memory0()[retptr / 4 + 2];
        if (r2) {
            throw takeObject(r1);
        }
        return Automerge.__wrap(r0);
    } finally {
        wasm.__wbindgen_add_to_stack_pointer(16);
    }
}

/**
* @param {any} change
* @returns {Uint8Array}
*/
export function encodeChange(change) {
    try {
        const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
        wasm.encodeChange(retptr, addHeapObject(change));
        var r0 = getInt32Memory0()[retptr / 4 + 0];
        var r1 = getInt32Memory0()[retptr / 4 + 1];
        var r2 = getInt32Memory0()[retptr / 4 + 2];
        if (r2) {
            throw takeObject(r1);
        }
        return takeObject(r0);
    } finally {
        wasm.__wbindgen_add_to_stack_pointer(16);
    }
}

/**
* @param {Uint8Array} change
* @returns {any}
*/
export function decodeChange(change) {
    try {
        const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
        wasm.decodeChange(retptr, addHeapObject(change));
        var r0 = getInt32Memory0()[retptr / 4 + 0];
        var r1 = getInt32Memory0()[retptr / 4 + 1];
        var r2 = getInt32Memory0()[retptr / 4 + 2];
        if (r2) {
            throw takeObject(r1);
        }
        return takeObject(r0);
    } finally {
        wasm.__wbindgen_add_to_stack_pointer(16);
    }
}

/**
* @returns {SyncState}
*/
export function initSyncState() {
    const ret = wasm.initSyncState();
    return SyncState.__wrap(ret);
}

/**
* @param {any} state
* @returns {SyncState}
*/
export function importSyncState(state) {
    try {
        const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
        wasm.importSyncState(retptr, addHeapObject(state));
        var r0 = getInt32Memory0()[retptr / 4 + 0];
        var r1 = getInt32Memory0()[retptr / 4 + 1];
        var r2 = getInt32Memory0()[retptr / 4 + 2];
        if (r2) {
            throw takeObject(r1);
        }
        return SyncState.__wrap(r0);
    } finally {
        wasm.__wbindgen_add_to_stack_pointer(16);
    }
}

/**
* @param {SyncState} state
* @returns {any}
*/
export function exportSyncState(state) {
    _assertClass(state, SyncState);
    const ret = wasm.exportSyncState(state.__wbg_ptr);
    return takeObject(ret);
}

/**
* @param {any} message
* @returns {Uint8Array}
*/
export function encodeSyncMessage(message) {
    try {
        const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
        wasm.encodeSyncMessage(retptr, addHeapObject(message));
        var r0 = getInt32Memory0()[retptr / 4 + 0];
        var r1 = getInt32Memory0()[retptr / 4 + 1];
        var r2 = getInt32Memory0()[retptr / 4 + 2];
        if (r2) {
            throw takeObject(r1);
        }
        return takeObject(r0);
    } finally {
        wasm.__wbindgen_add_to_stack_pointer(16);
    }
}

/**
* @param {Uint8Array} msg
* @returns {any}
*/
export function decodeSyncMessage(msg) {
    try {
        const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
        wasm.decodeSyncMessage(retptr, addHeapObject(msg));
        var r0 = getInt32Memory0()[retptr / 4 + 0];
        var r1 = getInt32Memory0()[retptr / 4 + 1];
        var r2 = getInt32Memory0()[retptr / 4 + 2];
        if (r2) {
            throw takeObject(r1);
        }
        return takeObject(r0);
    } finally {
        wasm.__wbindgen_add_to_stack_pointer(16);
    }
}

/**
* @param {SyncState} state
* @returns {Uint8Array}
*/
export function encodeSyncState(state) {
    _assertClass(state, SyncState);
    const ret = wasm.encodeSyncState(state.__wbg_ptr);
    return takeObject(ret);
}

/**
* @param {Uint8Array} data
* @returns {SyncState}
*/
export function decodeSyncState(data) {
    try {
        const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
        wasm.decodeSyncState(retptr, addHeapObject(data));
        var r0 = getInt32Memory0()[retptr / 4 + 0];
        var r1 = getInt32Memory0()[retptr / 4 + 1];
        var r2 = getInt32Memory0()[retptr / 4 + 2];
        if (r2) {
            throw takeObject(r1);
        }
        return SyncState.__wrap(r0);
    } finally {
        wasm.__wbindgen_add_to_stack_pointer(16);
    }
}

function handleError(f, args) {
    try {
        return f.apply(this, args);
    } catch (e) {
        wasm.__wbindgen_exn_store(addHeapObject(e));
    }
}
/**
* How text is represented in materialized objects on the JS side
*/
export const TextRepresentation = Object.freeze({
/**
* As an array of characters and objects
*/
Array:0,"0":"Array",
/**
* As a single JS string
*/
String:1,"1":"String", });

const AutomergeFinalization = new FinalizationRegistry(ptr => wasm.__wbg_automerge_free(ptr >>> 0));
/**
*/
export class Automerge {

    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(Automerge.prototype);
        obj.__wbg_ptr = ptr;
        AutomergeFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }

    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        AutomergeFinalization.unregister(this);
        return ptr;
    }

    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_automerge_free(ptr);
    }
    /**
    * @param {string | undefined} actor
    * @param {number} text_rep
    * @returns {Automerge}
    */
    static new(actor, text_rep) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            var ptr0 = isLikeNone(actor) ? 0 : passStringToWasm0(actor, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            var len0 = WASM_VECTOR_LEN;
            wasm.automerge_new(retptr, ptr0, len0, text_rep);
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return Automerge.__wrap(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {string | undefined} actor
    * @returns {Automerge}
    */
    clone(actor) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            var ptr0 = isLikeNone(actor) ? 0 : passStringToWasm0(actor, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            var len0 = WASM_VECTOR_LEN;
            wasm.automerge_clone(retptr, this.__wbg_ptr, ptr0, len0);
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return Automerge.__wrap(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {string | undefined} actor
    * @param {any} heads
    * @returns {Automerge}
    */
    fork(actor, heads) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            var ptr0 = isLikeNone(actor) ? 0 : passStringToWasm0(actor, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            var len0 = WASM_VECTOR_LEN;
            wasm.automerge_fork(retptr, this.__wbg_ptr, ptr0, len0, addHeapObject(heads));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return Automerge.__wrap(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @returns {any}
    */
    pendingOps() {
        const ret = wasm.automerge_pendingOps(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
    * @param {string | undefined} message
    * @param {number | undefined} time
    * @returns {any}
    */
    commit(message, time) {
        var ptr0 = isLikeNone(message) ? 0 : passStringToWasm0(message, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len0 = WASM_VECTOR_LEN;
        const ret = wasm.automerge_commit(this.__wbg_ptr, ptr0, len0, !isLikeNone(time), isLikeNone(time) ? 0 : time);
        return takeObject(ret);
    }
    /**
    * @param {Automerge} other
    * @returns {Array<any>}
    */
    merge(other) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            _assertClass(other, Automerge);
            wasm.automerge_merge(retptr, this.__wbg_ptr, other.__wbg_ptr);
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @returns {number}
    */
    rollback() {
        const ret = wasm.automerge_rollback(this.__wbg_ptr);
        return ret;
    }
    /**
    * @param {any} obj
    * @param {Array<any> | undefined} heads
    * @returns {Array<any>}
    */
    keys(obj, heads) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_keys(retptr, this.__wbg_ptr, addHeapObject(obj), isLikeNone(heads) ? 0 : addHeapObject(heads));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} obj
    * @param {Array<any> | undefined} heads
    * @returns {string}
    */
    text(obj, heads) {
        let deferred2_0;
        let deferred2_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_text(retptr, this.__wbg_ptr, addHeapObject(obj), isLikeNone(heads) ? 0 : addHeapObject(heads));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            var r3 = getInt32Memory0()[retptr / 4 + 3];
            var ptr1 = r0;
            var len1 = r1;
            if (r3) {
                ptr1 = 0; len1 = 0;
                throw takeObject(r2);
            }
            deferred2_0 = ptr1;
            deferred2_1 = len1;
            return getStringFromWasm0(ptr1, len1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_free(deferred2_0, deferred2_1, 1);
        }
    }
    /**
    * @param {any} obj
    * @param {number} start
    * @param {number} delete_count
    * @param {any} text
    */
    splice(obj, start, delete_count, text) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_splice(retptr, this.__wbg_ptr, addHeapObject(obj), start, delete_count, addHeapObject(text));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            if (r1) {
                throw takeObject(r0);
            }
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} obj
    * @param {any} value
    * @param {any} datatype
    */
    push(obj, value, datatype) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_push(retptr, this.__wbg_ptr, addHeapObject(obj), addHeapObject(value), addHeapObject(datatype));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            if (r1) {
                throw takeObject(r0);
            }
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} obj
    * @param {any} value
    * @returns {string | undefined}
    */
    pushObject(obj, value) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_pushObject(retptr, this.__wbg_ptr, addHeapObject(obj), addHeapObject(value));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            var r3 = getInt32Memory0()[retptr / 4 + 3];
            if (r3) {
                throw takeObject(r2);
            }
            let v1;
            if (r0 !== 0) {
                v1 = getStringFromWasm0(r0, r1).slice();
                wasm.__wbindgen_free(r0, r1 * 1);
            }
            return v1;
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} obj
    * @param {number} index
    * @param {any} value
    * @param {any} datatype
    */
    insert(obj, index, value, datatype) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_insert(retptr, this.__wbg_ptr, addHeapObject(obj), index, addHeapObject(value), addHeapObject(datatype));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            if (r1) {
                throw takeObject(r0);
            }
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} obj
    * @param {number} index
    * @param {any} value
    * @returns {string | undefined}
    */
    insertObject(obj, index, value) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_insertObject(retptr, this.__wbg_ptr, addHeapObject(obj), index, addHeapObject(value));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            var r3 = getInt32Memory0()[retptr / 4 + 3];
            if (r3) {
                throw takeObject(r2);
            }
            let v1;
            if (r0 !== 0) {
                v1 = getStringFromWasm0(r0, r1).slice();
                wasm.__wbindgen_free(r0, r1 * 1);
            }
            return v1;
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} obj
    * @param {any} prop
    * @param {any} value
    * @param {any} datatype
    */
    put(obj, prop, value, datatype) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_put(retptr, this.__wbg_ptr, addHeapObject(obj), addHeapObject(prop), addHeapObject(value), addHeapObject(datatype));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            if (r1) {
                throw takeObject(r0);
            }
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} obj
    * @param {any} prop
    * @param {any} value
    * @returns {any}
    */
    putObject(obj, prop, value) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_putObject(retptr, this.__wbg_ptr, addHeapObject(obj), addHeapObject(prop), addHeapObject(value));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} obj
    * @param {any} prop
    * @param {any} value
    */
    increment(obj, prop, value) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_increment(retptr, this.__wbg_ptr, addHeapObject(obj), addHeapObject(prop), addHeapObject(value));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            if (r1) {
                throw takeObject(r0);
            }
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} obj
    * @param {any} prop
    * @param {Array<any> | undefined} heads
    * @returns {any}
    */
    get(obj, prop, heads) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_get(retptr, this.__wbg_ptr, addHeapObject(obj), addHeapObject(prop), isLikeNone(heads) ? 0 : addHeapObject(heads));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} obj
    * @param {any} prop
    * @param {Array<any> | undefined} heads
    * @returns {any}
    */
    getWithType(obj, prop, heads) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_getWithType(retptr, this.__wbg_ptr, addHeapObject(obj), addHeapObject(prop), isLikeNone(heads) ? 0 : addHeapObject(heads));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} obj
    * @param {any} arg
    * @param {Array<any> | undefined} heads
    * @returns {Array<any>}
    */
    getAll(obj, arg, heads) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_getAll(retptr, this.__wbg_ptr, addHeapObject(obj), addHeapObject(arg), isLikeNone(heads) ? 0 : addHeapObject(heads));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} enable
    * @returns {any}
    */
    enableFreeze(enable) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_enableFreeze(retptr, this.__wbg_ptr, addHeapObject(enable));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} datatype
    * @param {any} _function
    */
    registerDatatype(datatype, _function) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_registerDatatype(retptr, this.__wbg_ptr, addHeapObject(datatype), addHeapObject(_function));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            if (r1) {
                throw takeObject(r0);
            }
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} object
    * @param {any} meta
    * @returns {any}
    */
    applyPatches(object, meta) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_applyPatches(retptr, this.__wbg_ptr, addHeapObject(object), addHeapObject(meta));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} object
    * @param {any} meta
    * @returns {any}
    */
    applyAndReturnPatches(object, meta) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_applyAndReturnPatches(retptr, this.__wbg_ptr, addHeapObject(object), addHeapObject(meta));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @returns {Array<any>}
    */
    diffIncremental() {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_diffIncremental(retptr, this.__wbg_ptr);
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    */
    updateDiffCursor() {
        wasm.automerge_updateDiffCursor(this.__wbg_ptr);
    }
    /**
    */
    resetDiffCursor() {
        wasm.automerge_resetDiffCursor(this.__wbg_ptr);
    }
    /**
    * @param {Array<any>} before
    * @param {Array<any>} after
    * @returns {Array<any>}
    */
    diff(before, after) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_diff(retptr, this.__wbg_ptr, addHeapObject(before), addHeapObject(after));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {Array<any>} heads
    */
    isolate(heads) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_isolate(retptr, this.__wbg_ptr, addHeapObject(heads));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            if (r1) {
                throw takeObject(r0);
            }
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    */
    integrate() {
        wasm.automerge_integrate(this.__wbg_ptr);
    }
    /**
    * @param {any} obj
    * @param {Array<any> | undefined} heads
    * @returns {number}
    */
    length(obj, heads) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_length(retptr, this.__wbg_ptr, addHeapObject(obj), isLikeNone(heads) ? 0 : addHeapObject(heads));
            var r0 = getFloat64Memory0()[retptr / 8 + 0];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            var r3 = getInt32Memory0()[retptr / 4 + 3];
            if (r3) {
                throw takeObject(r2);
            }
            return r0;
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} obj
    * @param {any} prop
    */
    delete(obj, prop) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_delete(retptr, this.__wbg_ptr, addHeapObject(obj), addHeapObject(prop));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            if (r1) {
                throw takeObject(r0);
            }
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @returns {Uint8Array}
    */
    save() {
        const ret = wasm.automerge_save(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
    * @returns {Uint8Array}
    */
    saveIncremental() {
        const ret = wasm.automerge_saveIncremental(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
    * @param {Array<any>} heads
    * @returns {Uint8Array}
    */
    saveSince(heads) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_saveSince(retptr, this.__wbg_ptr, addHeapObject(heads));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @returns {Uint8Array}
    */
    saveNoCompress() {
        const ret = wasm.automerge_saveNoCompress(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
    * @returns {Uint8Array}
    */
    saveAndVerify() {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_saveAndVerify(retptr, this.__wbg_ptr);
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {Uint8Array} data
    * @returns {number}
    */
    loadIncremental(data) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_loadIncremental(retptr, this.__wbg_ptr, addHeapObject(data));
            var r0 = getFloat64Memory0()[retptr / 8 + 0];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            var r3 = getInt32Memory0()[retptr / 4 + 3];
            if (r3) {
                throw takeObject(r2);
            }
            return r0;
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} changes
    */
    applyChanges(changes) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_applyChanges(retptr, this.__wbg_ptr, addHeapObject(changes));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            if (r1) {
                throw takeObject(r0);
            }
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} have_deps
    * @returns {Array<any>}
    */
    getChanges(have_deps) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_getChanges(retptr, this.__wbg_ptr, addHeapObject(have_deps));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} hash
    * @returns {any}
    */
    getChangeByHash(hash) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_getChangeByHash(retptr, this.__wbg_ptr, addHeapObject(hash));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {Automerge} other
    * @returns {Array<any>}
    */
    getChangesAdded(other) {
        _assertClass(other, Automerge);
        const ret = wasm.automerge_getChangesAdded(this.__wbg_ptr, other.__wbg_ptr);
        return takeObject(ret);
    }
    /**
    * @returns {Array<any>}
    */
    getHeads() {
        const ret = wasm.automerge_getHeads(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
    * @returns {string}
    */
    getActorId() {
        let deferred1_0;
        let deferred1_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_getActorId(retptr, this.__wbg_ptr);
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            deferred1_0 = r0;
            deferred1_1 = r1;
            return getStringFromWasm0(r0, r1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
    * @returns {any}
    */
    getLastLocalChange() {
        const ret = wasm.automerge_getLastLocalChange(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
    */
    dump() {
        wasm.automerge_dump(this.__wbg_ptr);
    }
    /**
    * @param {Array<any> | undefined} heads
    * @returns {Array<any>}
    */
    getMissingDeps(heads) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_getMissingDeps(retptr, this.__wbg_ptr, isLikeNone(heads) ? 0 : addHeapObject(heads));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {SyncState} state
    * @param {Uint8Array} message
    */
    receiveSyncMessage(state, message) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            _assertClass(state, SyncState);
            wasm.automerge_receiveSyncMessage(retptr, this.__wbg_ptr, state.__wbg_ptr, addHeapObject(message));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            if (r1) {
                throw takeObject(r0);
            }
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {SyncState} state
    * @returns {any}
    */
    generateSyncMessage(state) {
        _assertClass(state, SyncState);
        const ret = wasm.automerge_generateSyncMessage(this.__wbg_ptr, state.__wbg_ptr);
        return takeObject(ret);
    }
    /**
    * @param {any} meta
    * @returns {any}
    */
    toJS(meta) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_toJS(retptr, this.__wbg_ptr, addHeapObject(meta));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} obj
    * @param {Array<any> | undefined} heads
    * @param {any} meta
    * @returns {any}
    */
    materialize(obj, heads, meta) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_materialize(retptr, this.__wbg_ptr, addHeapObject(obj), isLikeNone(heads) ? 0 : addHeapObject(heads), addHeapObject(meta));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} obj
    * @param {number} index
    * @param {Array<any> | undefined} heads
    * @returns {string}
    */
    getCursor(obj, index, heads) {
        let deferred2_0;
        let deferred2_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_getCursor(retptr, this.__wbg_ptr, addHeapObject(obj), index, isLikeNone(heads) ? 0 : addHeapObject(heads));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            var r3 = getInt32Memory0()[retptr / 4 + 3];
            var ptr1 = r0;
            var len1 = r1;
            if (r3) {
                ptr1 = 0; len1 = 0;
                throw takeObject(r2);
            }
            deferred2_0 = ptr1;
            deferred2_1 = len1;
            return getStringFromWasm0(ptr1, len1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_free(deferred2_0, deferred2_1, 1);
        }
    }
    /**
    * @param {any} obj
    * @param {any} cursor
    * @param {Array<any> | undefined} heads
    * @returns {number}
    */
    getCursorPosition(obj, cursor, heads) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_getCursorPosition(retptr, this.__wbg_ptr, addHeapObject(obj), addHeapObject(cursor), isLikeNone(heads) ? 0 : addHeapObject(heads));
            var r0 = getFloat64Memory0()[retptr / 8 + 0];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            var r3 = getInt32Memory0()[retptr / 4 + 3];
            if (r3) {
                throw takeObject(r2);
            }
            return r0;
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {string | undefined} message
    * @param {number | undefined} time
    * @returns {any}
    */
    emptyChange(message, time) {
        var ptr0 = isLikeNone(message) ? 0 : passStringToWasm0(message, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len0 = WASM_VECTOR_LEN;
        const ret = wasm.automerge_emptyChange(this.__wbg_ptr, ptr0, len0, !isLikeNone(time), isLikeNone(time) ? 0 : time);
        return takeObject(ret);
    }
    /**
    * @param {any} obj
    * @param {any} range
    * @param {any} name
    * @param {any} value
    * @param {any} datatype
    */
    mark(obj, range, name, value, datatype) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_mark(retptr, this.__wbg_ptr, addHeapObject(obj), addHeapObject(range), addHeapObject(name), addHeapObject(value), addHeapObject(datatype));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            if (r1) {
                throw takeObject(r0);
            }
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} obj
    * @param {any} range
    * @param {any} name
    */
    unmark(obj, range, name) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_unmark(retptr, this.__wbg_ptr, addHeapObject(obj), addHeapObject(range), addHeapObject(name));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            if (r1) {
                throw takeObject(r0);
            }
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} obj
    * @param {Array<any> | undefined} heads
    * @returns {any}
    */
    marks(obj, heads) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.automerge_marks(retptr, this.__wbg_ptr, addHeapObject(obj), isLikeNone(heads) ? 0 : addHeapObject(heads));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            var r2 = getInt32Memory0()[retptr / 4 + 2];
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
}

const SyncStateFinalization = new FinalizationRegistry(ptr => wasm.__wbg_syncstate_free(ptr >>> 0));
/**
*/
export class SyncState {

    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(SyncState.prototype);
        obj.__wbg_ptr = ptr;
        SyncStateFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }

    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        SyncStateFinalization.unregister(this);
        return ptr;
    }

    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_syncstate_free(ptr);
    }
    /**
    * @returns {any}
    */
    get sharedHeads() {
        const ret = wasm.syncstate_sharedHeads(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
    * @returns {any}
    */
    get lastSentHeads() {
        const ret = wasm.syncstate_lastSentHeads(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
    * @param {any} heads
    */
    set lastSentHeads(heads) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.syncstate_set_lastSentHeads(retptr, this.__wbg_ptr, addHeapObject(heads));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            if (r1) {
                throw takeObject(r0);
            }
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @param {any} hashes
    */
    set sentHashes(hashes) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.syncstate_set_sentHashes(retptr, this.__wbg_ptr, addHeapObject(hashes));
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            if (r1) {
                throw takeObject(r0);
            }
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @returns {SyncState}
    */
    clone() {
        const ret = wasm.syncstate_clone(this.__wbg_ptr);
        return SyncState.__wrap(ret);
    }
}

const imports = {
    __wbindgen_placeholder__: {
        __wbindgen_object_drop_ref: function(arg0) {
            takeObject(arg0);
        },
        __wbindgen_error_new: function(arg0, arg1) {
            const ret = new Error(getStringFromWasm0(arg0, arg1));
            return addHeapObject(ret);
        },
        __wbindgen_string_get: function(arg0, arg1) {
            const obj = getObject(arg1);
            const ret = typeof(obj) === 'string' ? obj : undefined;
            var ptr1 = isLikeNone(ret) ? 0 : passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            var len1 = WASM_VECTOR_LEN;
            getInt32Memory0()[arg0 / 4 + 1] = len1;
            getInt32Memory0()[arg0 / 4 + 0] = ptr1;
        },
        __wbindgen_object_clone_ref: function(arg0) {
            const ret = getObject(arg0);
            return addHeapObject(ret);
        },
        __wbindgen_string_new: function(arg0, arg1) {
            const ret = getStringFromWasm0(arg0, arg1);
            return addHeapObject(ret);
        },
        __wbindgen_number_get: function(arg0, arg1) {
            const obj = getObject(arg1);
            const ret = typeof(obj) === 'number' ? obj : undefined;
            getFloat64Memory0()[arg0 / 8 + 1] = isLikeNone(ret) ? 0 : ret;
            getInt32Memory0()[arg0 / 4 + 0] = !isLikeNone(ret);
        },
        __wbindgen_is_undefined: function(arg0) {
            const ret = getObject(arg0) === undefined;
            return ret;
        },
        __wbindgen_boolean_get: function(arg0) {
            const v = getObject(arg0);
            const ret = typeof(v) === 'boolean' ? (v ? 1 : 0) : 2;
            return ret;
        },
        __wbindgen_is_null: function(arg0) {
            const ret = getObject(arg0) === null;
            return ret;
        },
        __wbindgen_number_new: function(arg0) {
            const ret = arg0;
            return addHeapObject(ret);
        },
        __wbindgen_is_function: function(arg0) {
            const ret = typeof(getObject(arg0)) === 'function';
            return ret;
        },
        __wbindgen_is_string: function(arg0) {
            const ret = typeof(getObject(arg0)) === 'string';
            return ret;
        },
        __wbindgen_json_serialize: function(arg0, arg1) {
            const obj = getObject(arg1);
            const ret = JSON.stringify(obj === undefined ? null : obj);
            const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            const len1 = WASM_VECTOR_LEN;
            getInt32Memory0()[arg0 / 4 + 1] = len1;
            getInt32Memory0()[arg0 / 4 + 0] = ptr1;
        },
        __wbg_new_abda76e883ba8a5f: function() {
            const ret = new Error();
            return addHeapObject(ret);
        },
        __wbg_stack_658279fe44541cf6: function(arg0, arg1) {
            const ret = getObject(arg1).stack;
            const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            const len1 = WASM_VECTOR_LEN;
            getInt32Memory0()[arg0 / 4 + 1] = len1;
            getInt32Memory0()[arg0 / 4 + 0] = ptr1;
        },
        __wbg_error_f851667af71bcfc6: function(arg0, arg1) {
            let deferred0_0;
            let deferred0_1;
            try {
                deferred0_0 = arg0;
                deferred0_1 = arg1;
                console.error(getStringFromWasm0(arg0, arg1));
            } finally {
                wasm.__wbindgen_free(deferred0_0, deferred0_1, 1);
            }
        },
        __wbindgen_is_object: function(arg0) {
            const val = getObject(arg0);
            const ret = typeof(val) === 'object' && val !== null;
            return ret;
        },
        __wbindgen_jsval_loose_eq: function(arg0, arg1) {
            const ret = getObject(arg0) == getObject(arg1);
            return ret;
        },
        __wbg_String_91fba7ded13ba54c: function(arg0, arg1) {
            const ret = String(getObject(arg1));
            const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            const len1 = WASM_VECTOR_LEN;
            getInt32Memory0()[arg0 / 4 + 1] = len1;
            getInt32Memory0()[arg0 / 4 + 0] = ptr1;
        },
        __wbindgen_bigint_from_i64: function(arg0) {
            const ret = arg0;
            return addHeapObject(ret);
        },
        __wbindgen_bigint_from_u64: function(arg0) {
            const ret = BigInt.asUintN(64, arg0);
            return addHeapObject(ret);
        },
        __wbg_set_20cbc34131e76824: function(arg0, arg1, arg2) {
            getObject(arg0)[takeObject(arg1)] = takeObject(arg2);
        },
        __wbg_getRandomValues_37fa2ca9e4e07fab: function() { return handleError(function (arg0, arg1) {
            getObject(arg0).getRandomValues(getObject(arg1));
        }, arguments) },
        __wbg_randomFillSync_dc1e9a60c158336d: function() { return handleError(function (arg0, arg1) {
            getObject(arg0).randomFillSync(takeObject(arg1));
        }, arguments) },
        __wbg_crypto_c48a774b022d20ac: function(arg0) {
            const ret = getObject(arg0).crypto;
            return addHeapObject(ret);
        },
        __wbg_process_298734cf255a885d: function(arg0) {
            const ret = getObject(arg0).process;
            return addHeapObject(ret);
        },
        __wbg_versions_e2e78e134e3e5d01: function(arg0) {
            const ret = getObject(arg0).versions;
            return addHeapObject(ret);
        },
        __wbg_node_1cd7a5d853dbea79: function(arg0) {
            const ret = getObject(arg0).node;
            return addHeapObject(ret);
        },
        __wbg_msCrypto_bcb970640f50a1e8: function(arg0) {
            const ret = getObject(arg0).msCrypto;
            return addHeapObject(ret);
        },
        __wbg_require_8f08ceecec0f4fee: function() { return handleError(function () {
            const ret = module.require;
            return addHeapObject(ret);
        }, arguments) },
        __wbg_log_1d3ae0273d8f4f8a: function(arg0) {
            console.log(getObject(arg0));
        },
        __wbg_log_576ca876af0d4a77: function(arg0, arg1) {
            console.log(getObject(arg0), getObject(arg1));
        },
        __wbg_get_44be0491f933a435: function(arg0, arg1) {
            const ret = getObject(arg0)[arg1 >>> 0];
            return addHeapObject(ret);
        },
        __wbg_length_fff51ee6522a1a18: function(arg0) {
            const ret = getObject(arg0).length;
            return ret;
        },
        __wbg_new_898a68150f225f2e: function() {
            const ret = new Array();
            return addHeapObject(ret);
        },
        __wbg_newnoargs_581967eacc0e2604: function(arg0, arg1) {
            const ret = new Function(getStringFromWasm0(arg0, arg1));
            return addHeapObject(ret);
        },
        __wbg_next_526fc47e980da008: function(arg0) {
            const ret = getObject(arg0).next;
            return addHeapObject(ret);
        },
        __wbg_next_ddb3312ca1c4e32a: function() { return handleError(function (arg0) {
            const ret = getObject(arg0).next();
            return addHeapObject(ret);
        }, arguments) },
        __wbg_done_5c1f01fb660d73b5: function(arg0) {
            const ret = getObject(arg0).done;
            return ret;
        },
        __wbg_value_1695675138684bd5: function(arg0) {
            const ret = getObject(arg0).value;
            return addHeapObject(ret);
        },
        __wbg_iterator_97f0c81209c6c35a: function() {
            const ret = Symbol.iterator;
            return addHeapObject(ret);
        },
        __wbg_get_97b561fb56f034b5: function() { return handleError(function (arg0, arg1) {
            const ret = Reflect.get(getObject(arg0), getObject(arg1));
            return addHeapObject(ret);
        }, arguments) },
        __wbg_call_cb65541d95d71282: function() { return handleError(function (arg0, arg1) {
            const ret = getObject(arg0).call(getObject(arg1));
            return addHeapObject(ret);
        }, arguments) },
        __wbg_new_b51585de1b234aff: function() {
            const ret = new Object();
            return addHeapObject(ret);
        },
        __wbg_length_b3de0c6587c18245: function(arg0) {
            const ret = getObject(arg0).length;
            return ret;
        },
        __wbg_set_502d29070ea18557: function(arg0, arg1, arg2) {
            getObject(arg0)[arg1 >>> 0] = takeObject(arg2);
        },
        __wbg_from_d7c216d4616bb368: function(arg0) {
            const ret = Array.from(getObject(arg0));
            return addHeapObject(ret);
        },
        __wbg_isArray_4c24b343cb13cfb1: function(arg0) {
            const ret = Array.isArray(getObject(arg0));
            return ret;
        },
        __wbg_push_ca1c26067ef907ac: function(arg0, arg1) {
            const ret = getObject(arg0).push(getObject(arg1));
            return ret;
        },
        __wbg_unshift_de5fe028746b7cec: function(arg0, arg1) {
            const ret = getObject(arg0).unshift(getObject(arg1));
            return ret;
        },
        __wbg_instanceof_ArrayBuffer_39ac22089b74fddb: function(arg0) {
            let result;
            try {
                result = getObject(arg0) instanceof ArrayBuffer;
            } catch {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_new_d258248ed531ff54: function(arg0, arg1) {
            const ret = new Error(getStringFromWasm0(arg0, arg1));
            return addHeapObject(ret);
        },
        __wbg_call_01734de55d61e11d: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = getObject(arg0).call(getObject(arg1), getObject(arg2));
            return addHeapObject(ret);
        }, arguments) },
        __wbg_instanceof_Date_91c15a742581fc41: function(arg0) {
            let result;
            try {
                result = getObject(arg0) instanceof Date;
            } catch {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_getTime_5e2054f832d82ec9: function(arg0) {
            const ret = getObject(arg0).getTime();
            return ret;
        },
        __wbg_new_cd59bfc8881f487b: function(arg0) {
            const ret = new Date(getObject(arg0));
            return addHeapObject(ret);
        },
        __wbg_instanceof_Object_3daa8298c86298be: function(arg0) {
            let result;
            try {
                result = getObject(arg0) instanceof Object;
            } catch {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_assign_999478a110912e87: function(arg0, arg1) {
            const ret = Object.assign(getObject(arg0), getObject(arg1));
            return addHeapObject(ret);
        },
        __wbg_defineProperty_2db1bcb00cd9da96: function(arg0, arg1, arg2) {
            const ret = Object.defineProperty(getObject(arg0), getObject(arg1), getObject(arg2));
            return addHeapObject(ret);
        },
        __wbg_entries_e51f29c7bba0c054: function(arg0) {
            const ret = Object.entries(getObject(arg0));
            return addHeapObject(ret);
        },
        __wbg_freeze_29fdec6af26e40df: function(arg0) {
            const ret = Object.freeze(getObject(arg0));
            return addHeapObject(ret);
        },
        __wbg_keys_386d224d0262f361: function(arg0) {
            const ret = Object.keys(getObject(arg0));
            return addHeapObject(ret);
        },
        __wbg_values_88acfd44c7ef08d7: function(arg0) {
            const ret = Object.values(getObject(arg0));
            return addHeapObject(ret);
        },
        __wbg_new_732c49c4e03c98a1: function(arg0, arg1) {
            const ret = new RangeError(getStringFromWasm0(arg0, arg1));
            return addHeapObject(ret);
        },
        __wbg_apply_f9ecfcbfefaf7349: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = Reflect.apply(getObject(arg0), getObject(arg1), getObject(arg2));
            return addHeapObject(ret);
        }, arguments) },
        __wbg_deleteProperty_568c26e52bc57565: function() { return handleError(function (arg0, arg1) {
            const ret = Reflect.deleteProperty(getObject(arg0), getObject(arg1));
            return ret;
        }, arguments) },
        __wbg_ownKeys_391189872004f097: function() { return handleError(function (arg0) {
            const ret = Reflect.ownKeys(getObject(arg0));
            return addHeapObject(ret);
        }, arguments) },
        __wbg_set_092e06b0f9d71865: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = Reflect.set(getObject(arg0), getObject(arg1), getObject(arg2));
            return ret;
        }, arguments) },
        __wbg_buffer_085ec1f694018c4f: function(arg0) {
            const ret = getObject(arg0).buffer;
            return addHeapObject(ret);
        },
        __wbg_concat_c15e9c7e01e04001: function(arg0, arg1) {
            const ret = getObject(arg0).concat(getObject(arg1));
            return addHeapObject(ret);
        },
        __wbg_slice_0a0d91ee629d603a: function(arg0, arg1, arg2) {
            const ret = getObject(arg0).slice(arg1 >>> 0, arg2 >>> 0);
            return addHeapObject(ret);
        },
        __wbg_for_2256af0eb4ac96ac: function(arg0, arg1) {
            const ret = Symbol.for(getStringFromWasm0(arg0, arg1));
            return addHeapObject(ret);
        },
        __wbg_toString_bb5de5cb12268093: function(arg0) {
            const ret = getObject(arg0).toString();
            return addHeapObject(ret);
        },
        __wbg_self_1ff1d729e9aae938: function() { return handleError(function () {
            const ret = self.self;
            return addHeapObject(ret);
        }, arguments) },
        __wbg_window_5f4faef6c12b79ec: function() { return handleError(function () {
            const ret = window.window;
            return addHeapObject(ret);
        }, arguments) },
        __wbg_globalThis_1d39714405582d3c: function() { return handleError(function () {
            const ret = globalThis.globalThis;
            return addHeapObject(ret);
        }, arguments) },
        __wbg_global_651f05c6a0944d1c: function() { return handleError(function () {
            const ret = global.global;
            return addHeapObject(ret);
        }, arguments) },
        __wbg_newwithbyteoffsetandlength_6da8e527659b86aa: function(arg0, arg1, arg2) {
            const ret = new Uint8Array(getObject(arg0), arg1 >>> 0, arg2 >>> 0);
            return addHeapObject(ret);
        },
        __wbg_new_8125e318e6245eed: function(arg0) {
            const ret = new Uint8Array(getObject(arg0));
            return addHeapObject(ret);
        },
        __wbg_set_5cf90238115182c3: function(arg0, arg1, arg2) {
            getObject(arg0).set(getObject(arg1), arg2 >>> 0);
        },
        __wbg_length_72e2208bbc0efc61: function(arg0) {
            const ret = getObject(arg0).length;
            return ret;
        },
        __wbg_instanceof_Uint8Array_d8d9cb2b8e8ac1d4: function(arg0) {
            let result;
            try {
                result = getObject(arg0) instanceof Uint8Array;
            } catch {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_newwithlength_e5d69174d6984cd7: function(arg0) {
            const ret = new Uint8Array(arg0 >>> 0);
            return addHeapObject(ret);
        },
        __wbg_subarray_13db269f57aa838d: function(arg0, arg1, arg2) {
            const ret = getObject(arg0).subarray(arg1 >>> 0, arg2 >>> 0);
            return addHeapObject(ret);
        },
        __wbindgen_debug_string: function(arg0, arg1) {
            const ret = debugString(getObject(arg1));
            const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            const len1 = WASM_VECTOR_LEN;
            getInt32Memory0()[arg0 / 4 + 1] = len1;
            getInt32Memory0()[arg0 / 4 + 0] = ptr1;
        },
        __wbindgen_throw: function(arg0, arg1) {
            throw new Error(getStringFromWasm0(arg0, arg1));
        },
        __wbindgen_memory: function() {
            const ret = wasm.memory;
            return addHeapObject(ret);
        },
    },

};

const wasm_url = new URL('automerge_wasm_bg.wasm', import.meta.url);
let wasmCode = '';
switch (wasm_url.protocol) {
    case 'file:':
    wasmCode = await Deno.readFile(wasm_url);
    break
    case 'https:':
    case 'http:':
    wasmCode = await (await fetch(wasm_url)).arrayBuffer();
    break
    default:
    throw new Error(`Unsupported protocol: ${wasm_url.protocol}`);
}

const wasmInstance = (await WebAssembly.instantiate(wasmCode, imports)).instance;
const wasm = wasmInstance.exports;

