
export type Actor = string;
export type ObjID = string;
export type Change = Uint8Array;
export type SyncMessage = Uint8Array;
export type Prop = string | number;
export type Hash = string;
export type Heads = Hash[];
export type ObjectType = string; // opaque ??
export type Value = string | number | boolean | null | Date | Uint8Array | ObjectType;
export type FullValue =
  ["str", string] |
  ["int", number] |
  ["uint", number] |
  ["f64", number] |
  ["boolean", boolean] |
  ["timestamp", Date] |
  ["counter", number] |
  ["bytes", Uint8Array] |
  ["null", Uint8Array] |
  ["map", ObjID] |
  ["list", ObjID] |
  ["text", ObjID] |
  ["table", ObjID]

export const LIST : ObjectType;
export const MAP : ObjectType;
export const TABLE : ObjectType;
export const TEXT : ObjectType;

export enum ObjTypeName {
  list = "list",
  map = "map",
  table = "table",
  text = "text",
}

export type Datatype =
  "boolean" |
  "str" |
  "int" |
  "uint" |
  "f64" |
  "null" |
  "timestamp" |
  "counter" |
  "bytes";

export type DecodedSyncMessage = {
  heads: Heads,
  need: Heads,
  have: any[]
  changes: Change[]
}

export type DecodedChange = {
  actor: Actor,
  seq: number
  startOp: number,
  time: number,
  message: string | null,
  deps: Heads,
  hash: Hash,
  ops: Op[]
}

export type Op = {
  action: string,
  obj: ObjID,
  key: string,
  value?: string | number | boolean,
  datatype?: string,
  pred: string[],
}

export function create(actor?: Actor): Automerge;
export function loadDoc(data: Uint8Array, actor?: Actor): Automerge;
export function encodeChange(change: DecodedChange): Change;
export function decodeChange(change: Change): DecodedChange;
export function initSyncState(): SyncState;
export function encodeSyncMessage(message: DecodedSyncMessage): SyncMessage;
export function decodeSyncMessage(msg: SyncMessage): DecodedSyncMessage;
export function encodeSyncState(state: SyncState): Uint8Array;
export function decodeSyncState(data: Uint8Array): SyncState;

export class Automerge {
  set(obj: ObjID, prop: Prop, value: Value, datatype?: Datatype): ObjID | undefined;
  make(obj: ObjID, prop: Prop, value: ObjectType): ObjID;
  insert(obj: ObjID, index: number, value: Value, datatype?: Datatype): ObjID | undefined;
  push(obj: ObjID, value: Value, datatype?: Datatype): ObjID | undefined;
  splice(obj: ObjID, start: number, delete_count: number, text: string | Array<Value | FullValue>): ObjID[] | undefined;
  inc(obj: ObjID, prop: Prop, value: number): void;
  del(obj: ObjID, prop: Prop): void;
  mark(obj: ObjID, name: string, range: string, value: Value, datatype?: Datatype): void;

  // returns a single value - if there is a conflict return the winner
  value(obj: ObjID, prop: any, heads?: Heads): FullValue | null;
  // return all values in case of a conflict
  values(obj: ObjID, arg: any, heads?: Heads): FullValue[];
  keys(obj: ObjID, heads?: Heads): string[];
  text(obj: ObjID, heads?: Heads): string;
  length(obj: ObjID, heads?: Heads): number;
  spans(obj: ObjID): any;

  commit(message?: string, time?: number): Heads;
  getActorId(): Actor;
  pendingOps(): number;
  rollback(): number;

  // save and load to local store
  save(): Uint8Array;
  saveIncremental(): Uint8Array;
  loadIncremental(data: Uint8Array): number;

  // sync over network
  receiveSyncMessage(state: SyncState, message: SyncMessage): void;
  generateSyncMessage(state: SyncState): SyncMessage;

  // low level change functions
  applyChanges(changes: Change[]): void;
  getChanges(have_deps: Heads): Change[];
  getChangesAdded(other: Automerge): Change[];
  getHeads(): Heads;
  getLastLocalChange(): Change;
  getMissingDeps(heads?: Heads): Heads;

  // memory management
  free(): void;
  clone(actor?: string): Automerge;

  // dump internal state to console.log
  dump(): void;

  // dump internal state to a JS object
  toJS(): any;
}

export class SyncState {
  free(): void;
  clone(): SyncState;
  lastSentHeads: any;
  sentHashes: any;
  readonly sharedHeads: any;
}

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
  readonly memory: WebAssembly.Memory;
  readonly __wbg_automerge_free: (a: number) => void;
  readonly automerge_new: (a: number, b: number, c: number) => void;
  readonly automerge_clone: (a: number, b: number, c: number, d: number) => void;
  readonly automerge_free: (a: number) => void;
  readonly automerge_pendingOps: (a: number) => number;
  readonly automerge_commit: (a: number, b: number, c: number, d: number, e: number) => number;
  readonly automerge_rollback: (a: number) => number;
  readonly automerge_keys: (a: number, b: number, c: number, d: number, e: number) => void;
  readonly automerge_text: (a: number, b: number, c: number, d: number, e: number) => void;
  readonly automerge_splice: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => void;
  readonly automerge_push: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => void;
  readonly automerge_insert: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number) => void;
  readonly automerge_set: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number) => void;
  readonly automerge_inc: (a: number, b: number, c: number, d: number, e: number, f: number) => void;
  readonly automerge_value: (a: number, b: number, c: number, d: number, e: number, f: number) => void;
  readonly automerge_values: (a: number, b: number, c: number, d: number, e: number, f: number) => void;
  readonly automerge_length: (a: number, b: number, c: number, d: number, e: number) => void;
  readonly automerge_del: (a: number, b: number, c: number, d: number, e: number) => void;
  readonly automerge_save: (a: number, b: number) => void;
  readonly automerge_saveIncremental: (a: number) => number;
  readonly automerge_loadIncremental: (a: number, b: number, c: number) => void;
  readonly automerge_applyChanges: (a: number, b: number, c: number) => void;
  readonly automerge_getChanges: (a: number, b: number, c: number) => void;
  readonly automerge_getChangesAdded: (a: number, b: number, c: number) => void;
  readonly automerge_getHeads: (a: number) => number;
  readonly automerge_getActorId: (a: number, b: number) => void;
  readonly automerge_getLastLocalChange: (a: number, b: number) => void;
  readonly automerge_dump: (a: number) => void;
  readonly automerge_getMissingDeps: (a: number, b: number, c: number) => void;
  readonly automerge_receiveSyncMessage: (a: number, b: number, c: number, d: number) => void;
  readonly automerge_generateSyncMessage: (a: number, b: number, c: number) => void;
  readonly automerge_toJS: (a: number) => number;
  readonly create: (a: number, b: number, c: number) => void;
  readonly loadDoc: (a: number, b: number, c: number, d: number) => void;
  readonly encodeChange: (a: number, b: number) => void;
  readonly decodeChange: (a: number, b: number) => void;
  readonly initSyncState: () => number;
  readonly importSyncState: (a: number, b: number) => void;
  readonly exportSyncState: (a: number) => number;
  readonly encodeSyncMessage: (a: number, b: number) => void;
  readonly decodeSyncMessage: (a: number, b: number) => void;
  readonly encodeSyncState: (a: number, b: number) => void;
  readonly decodeSyncState: (a: number, b: number) => void;
  readonly __wbg_list_free: (a: number) => void;
  readonly __wbg_map_free: (a: number) => void;
  readonly __wbg_text_free: (a: number) => void;
  readonly __wbg_table_free: (a: number) => void;
  readonly __wbg_syncstate_free: (a: number) => void;
  readonly syncstate_sharedHeads: (a: number) => number;
  readonly syncstate_lastSentHeads: (a: number) => number;
  readonly syncstate_set_lastSentHeads: (a: number, b: number, c: number) => void;
  readonly syncstate_set_sentHashes: (a: number, b: number, c: number) => void;
  readonly syncstate_clone: (a: number) => number;
  readonly __wbindgen_malloc: (a: number) => number;
  readonly __wbindgen_realloc: (a: number, b: number, c: number) => number;
  readonly __wbindgen_add_to_stack_pointer: (a: number) => number;
  readonly __wbindgen_free: (a: number, b: number) => void;
  readonly __wbindgen_exn_store: (a: number) => void;
}

/**
* If `module_or_path` is {RequestInfo} or {URL}, makes a request and
* for everything else, calls `WebAssembly.instantiate` directly.
*
* @param {InitInput | Promise<InitInput>} module_or_path
*
* @returns {Promise<InitOutput>}
*/

export default function init (module_or_path?: InitInput | Promise<InitInput>): Promise<InitOutput>;
