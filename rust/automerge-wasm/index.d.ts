export type Actor = string;
export type ObjID = string;
export type Change = Uint8Array;
export type SyncMessage = Uint8Array;
export type Prop = string | number;
export type Hash = string;
export type Heads = Hash[];
export type Value = string | number | boolean | null | Date | Uint8Array
export type MaterializeValue = { [key:string]: MaterializeValue } | Array<MaterializeValue> | Value
export type ObjType = string | Array<ObjType | Value> | { [key: string]: ObjType | Value }
export type FullValue =
  ["str", string] |
  ["int", number] |
  ["uint", number] |
  ["f64", number] |
  ["boolean", boolean] |
  ["timestamp", Date] |
  ["counter", number] |
  ["bytes", Uint8Array] |
  ["null", null] |
  ["map", ObjID] |
  ["list", ObjID] |
  ["text", ObjID] |
  ["table", ObjID]

export type FullValueWithId =
  ["str", string, ObjID ] |
  ["int", number, ObjID ] |
  ["uint", number, ObjID ] |
  ["f64", number, ObjID ] |
  ["boolean", boolean, ObjID ] |
  ["timestamp", Date, ObjID ] |
  ["counter", number, ObjID ] |
  ["bytes", Uint8Array, ObjID ] |
  ["null", null, ObjID ] |
  ["map", ObjID ] |
  ["list", ObjID] |
  ["text", ObjID] |
  ["table", ObjID]

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
  "bytes" |
  "map" |
  "text" |
  "list";

export type SyncHave = {
  lastSync: Heads,
  bloom: Uint8Array,
}

export type DecodedSyncMessage = {
  heads: Heads,
  need: Heads,
  have: SyncHave[]
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

type PartialBy<T, K extends keyof T> = Omit<T, K> & Partial<Pick<T, K>>
export type ChangeToEncode = PartialBy<DecodedChange, 'hash'>

export type Op = {
  action: string,
  obj: ObjID,
  key: string,
  value?: string | number | boolean,
  datatype?: string,
  pred: string[],
}

export type Patch =  PutPatch | DelPatch | SpliceTextPatch | IncPatch | InsertPatch;

export type PutPatch = {
  action: 'put'
  path: Prop[],
  value: Value
  conflict: boolean
}

export type IncPatch = {
  action: 'inc'
  path: Prop[],
  value: number
}

export type DelPatch = {
  action: 'del'
  path: Prop[],
  length?: number,
}

export type SpliceTextPatch = {
  action: 'splice'
  path: Prop[],
  value: string,
}

export type InsertPatch = {
  action: 'insert'
  path: Prop[],
  values: Value[],
}

export function encodeChange(change: ChangeToEncode): Change;
export function create(text_v2: boolean, actor?: Actor): Automerge;
export function load(data: Uint8Array, text_v2: boolean, actor?: Actor): Automerge;
export function decodeChange(change: Change): DecodedChange;
export function initSyncState(): SyncState;
export function encodeSyncMessage(message: DecodedSyncMessage): SyncMessage;
export function decodeSyncMessage(msg: SyncMessage): DecodedSyncMessage;
export function encodeSyncState(state: SyncState): Uint8Array;
export function decodeSyncState(data: Uint8Array): SyncState;
export function exportSyncState(state: SyncState): JsSyncState;
export function importSyncState(state: JsSyncState): SyncState;

export interface API {
  create(text_v2: boolean, actor?: Actor): Automerge;
  load(data: Uint8Array, text_v2: boolean, actor?: Actor): Automerge;
  encodeChange(change: ChangeToEncode): Change;
  decodeChange(change: Change): DecodedChange;
  initSyncState(): SyncState;
  encodeSyncMessage(message: DecodedSyncMessage): SyncMessage;
  decodeSyncMessage(msg: SyncMessage): DecodedSyncMessage;
  encodeSyncState(state: SyncState): Uint8Array;
  decodeSyncState(data: Uint8Array): SyncState;
  exportSyncState(state: SyncState): JsSyncState;
  importSyncState(state: JsSyncState): SyncState;
}

export class Automerge {
  // change state
  put(obj: ObjID, prop: Prop, value: Value, datatype?: Datatype): void;
  putObject(obj: ObjID, prop: Prop, value: ObjType): ObjID;
  insert(obj: ObjID, index: number, value: Value, datatype?: Datatype): void;
  insertObject(obj: ObjID, index: number, value: ObjType): ObjID;
  push(obj: ObjID, value: Value, datatype?: Datatype): void;
  pushObject(obj: ObjID, value: ObjType): ObjID;
  splice(obj: ObjID, start: number, delete_count: number, text?: string | Array<Value>): ObjID[] | undefined;
  increment(obj: ObjID, prop: Prop, value: number): void;
  delete(obj: ObjID, prop: Prop): void;

  // returns a single value - if there is a conflict return the winner
  get(obj: ObjID, prop: Prop, heads?: Heads): Value | undefined;
  getWithType(obj: ObjID, prop: Prop, heads?: Heads): FullValue | null;
  // return all values in case of a conflict
  getAll(obj: ObjID, arg: Prop, heads?: Heads): FullValueWithId[];
  keys(obj: ObjID, heads?: Heads): string[];
  text(obj: ObjID, heads?: Heads): string;
  length(obj: ObjID, heads?: Heads): number;
  materialize(obj?: ObjID, heads?: Heads, metadata?: unknown): MaterializeValue;
  toJS(): MaterializeValue;

  // transactions
  commit(message?: string, time?: number): Hash | null;
  emptyChange(message?: string, time?: number): Hash;
  merge(other: Automerge): Heads;
  getActorId(): Actor;
  pendingOps(): number;
  rollback(): number;

  // patches
  enablePatches(enable: boolean): boolean;
  enableFreeze(enable: boolean): boolean;
  registerDatatype(datatype: string, callback: Function): void;
  popPatches(): Patch[];

  // save and load to local store
  save(): Uint8Array;
  saveIncremental(): Uint8Array;
  loadIncremental(data: Uint8Array): number;

  // sync over network
  receiveSyncMessage(state: SyncState, message: SyncMessage): void;
  generateSyncMessage(state: SyncState): SyncMessage | null;

  // low level change functions
  applyChanges(changes: Change[]): void;
  getChanges(have_deps: Heads): Change[];
  getChangeByHash(hash: Hash): Change | null;
  getChangesAdded(other: Automerge): Change[];
  getHeads(): Heads;
  getLastLocalChange(): Change | null;
  getMissingDeps(heads?: Heads): Heads;

  // memory management
  free(): void; // only needed if weak-refs are unsupported
  clone(actor?: string): Automerge; // TODO - remove, this is dangerous
  fork(actor?: string, heads?: Heads): Automerge;

  // dump internal state to console.log - for debugging
  dump(): void;

  // experimental api can go here
  applyPatches<Doc>(obj: Doc, meta?: unknown, callback?: (patch: Array<Patch>, before: Doc, after: Doc) => void): Doc;
}

export interface JsSyncState {
    sharedHeads: Heads;
    lastSentHeads: Heads;
    theirHeads: Heads | undefined;
    theirHeed: Heads | undefined;
    theirHave: SyncHave[] | undefined;
    sentHashes: Heads;
}

export class SyncState {
  free(): void;
  clone(): SyncState;
  lastSentHeads: Heads;
  sentHashes: Heads;
  readonly sharedHeads: Heads;
}
