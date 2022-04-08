
export type Actor = string;
export type ObjID = string;
export type Change = Uint8Array;
export type SyncMessage = Uint8Array;
export type Prop = string | number;
export type Hash = string;
export type Heads = Hash[];
export type Value = string | number | boolean | null | Date | Uint8Array
export type ObjType = string | Array | Object
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

export type Patch = {
  obj: ObjID
  action: 'assign' | 'insert' | 'delete'
  key: Prop
  value: Value
  datatype: Datatype
  conflict: boolean
}

export function create(actor?: Actor): Automerge;
export function load(data: Uint8Array, actor?: Actor): Automerge;
export function encodeChange(change: DecodedChange): Change;
export function decodeChange(change: Change): DecodedChange;
export function initSyncState(): SyncState;
export function encodeSyncMessage(message: DecodedSyncMessage): SyncMessage;
export function decodeSyncMessage(msg: SyncMessage): DecodedSyncMessage;
export function encodeSyncState(state: SyncState): Uint8Array;
export function decodeSyncState(data: Uint8Array): SyncState;

export class Automerge {
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
  value(obj: ObjID, prop: any, heads?: Heads): FullValue | null;
  // return all values in case of a conflict
  values(obj: ObjID, arg: any, heads?: Heads): FullValue[];
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

export class SyncState {
  free(): void;
  clone(): SyncState;
  lastSentHeads: any;
  sentHashes: any;
  readonly sharedHeads: any;
}

//export default function init (module_or_path?: InitInput | Promise<InitInput>): Promise<InitOutput>;
export default function init (): Promise<()>;
