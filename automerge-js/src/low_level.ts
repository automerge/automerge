
export type Actor = string;
export type ObjID = string;
export type Change = Uint8Array;
export type SyncMessage = Uint8Array;
export type Prop = string | number;
export type Hash = string;
export type Heads = Hash[];
export type Value = string | number | boolean | null | Date | Uint8Array
export type ObjType = string | Array<Value> | Object
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

export type Op = {
  action: string,
  obj: ObjID,
  key: string,
  value?: string | number | boolean,
  datatype?: string,
  pred: string[],
}

export type Patch = {
  obj: ObjID
  action: 'assign' | 'insert' | 'delete'
  key: Prop
  value: Value
  datatype: Datatype
  conflict: boolean
}

export interface LowLevelApi {
  create(actor?: Actor): Automerge;
  load(data: Uint8Array, actor?: Actor): Automerge;
  encodeChange(change: DecodedChange): Change;
  decodeChange(change: Change): DecodedChange;
  initSyncState(): SyncState;
  encodeSyncMessage(message: DecodedSyncMessage): SyncMessage;
  decodeSyncMessage(msg: SyncMessage): DecodedSyncMessage;
  encodeSyncState(state: SyncState): Uint8Array;
  decodeSyncState(data: Uint8Array): SyncState;
  exportSyncState(state: SyncState): JsSyncState;
  importSyncState(state: JsSyncState): SyncState;
}

export function UseApi(api: LowLevelApi) {
  for (const k in api) {
    ApiHandler[k] = api[k]
  }
}

export const ApiHandler : LowLevelApi = {
  create(actor?: Actor): Automerge { throw new RangeError("Automerge.use() not called") },
  load(data: Uint8Array, actor?: Actor): Automerge { throw new RangeError("Automerge.use() not called") },
  encodeChange(change: DecodedChange): Change { throw new RangeError("Automerge.use() not called") },
  decodeChange(change: Change): DecodedChange { throw new RangeError("Automerge.use() not called") },
  initSyncState(): SyncState { throw new RangeError("Automerge.use() not called") },
  encodeSyncMessage(message: DecodedSyncMessage): SyncMessage { throw new RangeError("Automerge.use() not called") },
  decodeSyncMessage(msg: SyncMessage): DecodedSyncMessage { throw new RangeError("Automerge.use() not called") },
  encodeSyncState(state: SyncState): Uint8Array { throw new RangeError("Automerge.use() not called") },
  decodeSyncState(data: Uint8Array): SyncState { throw new RangeError("Automerge.use() not called") },
  exportSyncState(state: SyncState): JsSyncState { throw new RangeError("Automerge.use() not called") },
  importSyncState(state: JsSyncState): SyncState { throw new RangeError("Automerge.use() not called") },
}

export interface Automerge {
  // change state
  put(obj: ObjID, prop: Prop, value: Value, datatype?: Datatype): undefined;
  putObject(obj: ObjID, prop: Prop, value: ObjType): ObjID;
  insert(obj: ObjID, index: number, value: Value, datatype?: Datatype): undefined;
  insertObject(obj: ObjID, index: number, value: ObjType): ObjID;
  push(obj: ObjID, value: Value, datatype?: Datatype): undefined;
  pushObject(obj: ObjID, value: ObjType): ObjID;
  splice(obj: ObjID, start: number, delete_count: number, text?: string | Array<Value>): ObjID[] | undefined;
  increment(obj: ObjID, prop: Prop, value: number): void;
  delete(obj: ObjID, prop: Prop): void;

  // returns a single value - if there is a conflict return the winner
  get(obj: ObjID, prop: any, heads?: Heads): FullValue | null;
  // return all values in case of a conflict
  getAll(obj: ObjID, arg: any, heads?: Heads): FullValue[];
  keys(obj: ObjID, heads?: Heads): string[];
  text(obj: ObjID, heads?: Heads): string;
  length(obj: ObjID, heads?: Heads): number;
  materialize(obj?: ObjID, heads?: Heads): any;

  // transactions
  commit(message?: string, time?: number): Hash;
  merge(other: Automerge): Heads;
  getActorId(): Actor;
  pendingOps(): number;
  rollback(): number;

  // patches
  enablePatches(enable: boolean): void;
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
  getLastLocalChange(): Change;
  getMissingDeps(heads?: Heads): Heads;

  // memory management
  free(): void;
  clone(actor?: string): Automerge;
  fork(actor?: string): Automerge;
  forkAt(heads: Heads, actor?: string): Automerge;

  // dump internal state to console.log
  dump(): void;

  // dump internal state to a JS object
  toJS(): any;
}

export interface JsSyncState {
  lastSentHeads: any;
  sentHashes: any;
  readonly sharedHeads: any;
}

export interface SyncState extends JsSyncState {
  free(): void;
  clone(): SyncState;
}

