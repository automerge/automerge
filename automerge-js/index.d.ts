import { API as LowLevelApi } from "automerge-types";
import { Actor as ActorId, Prop, ObjID, Change, DecodedChange, Heads,  MaterializeValue } from "automerge-types";
import { JsSyncState as SyncState, SyncMessage, DecodedSyncMessage } from "automerge-types";

export { API as LowLevelApi } from "automerge-types";
export { Actor as ActorId, Prop, ObjID, Change, DecodedChange, Heads, Automerge, MaterializeValue } from "automerge-types";
export { JsSyncState as SyncState, SyncMessage, DecodedSyncMessage } from "automerge-types";

export type ChangeOptions = {
    message?: string;
    time?: number;
};

export class Int {
    value: number;
    constructor(value: number);
}

export class Uint {
    value: number;
    constructor(value: number);
}

export class Float64 {
    value: number;
    constructor(value: number);
}

export class Counter {
    value: number;
    constructor(value?: number);
    valueOf(): number;
    toString(): string;
    toJSON(): number;
}

export class Text {
    elems: AutomergeValue[];
    constructor(text?: string | string[]);
    get length(): number;
    get(index: number): AutomergeValue | undefined;
    [index: number]: AutomergeValue | undefined;
    [Symbol.iterator](): {
        next(): {
            done: boolean;
            value: AutomergeValue;
        } | {
            done: boolean;
            value?: undefined;
        };
    };
    toString(): string;
    toSpans(): AutomergeValue[];
    toJSON(): string;
    set(index: number, value: AutomergeValue): void;
    insertAt(index: number, ...values: AutomergeValue[]): void;
    deleteAt(index: number, numDelete?: number): void;
    map<T>(callback: (e: AutomergeValue) => T): void;
}

export type Doc<T> = {
    readonly [P in keyof T]: T[P];
};

export type ChangeFn<T> = (doc: T) => void;

export interface State<T> {
    change: DecodedChange;
    snapshot: T;
}

export type ScalarValue = string | number | null | boolean | Date | Counter | Uint8Array;

export type AutomergeValue = ScalarValue | {[key: string]: AutomergeValue;} | Array<AutomergeValue>;

type Conflicts = {
    [key: string]: AutomergeValue;
};

export function use(api: LowLevelApi): void;
export function getBackend<T>(doc: Doc<T>) : Automerge;
export function init<T>(actor?: ActorId): Doc<T>;
export function clone<T>(doc: Doc<T>): Doc<T>;
export function free<T>(doc: Doc<T>): void;
export function from<T>(initialState: T | Doc<T>, actor?: ActorId): Doc<T>;
export function change<T>(doc: Doc<T>, options: string | ChangeOptions | ChangeFn<T>, callback?: ChangeFn<T>): Doc<T>;
export function emptyChange<T>(doc: Doc<T>, options: ChangeOptions): unknown;
export function load<T>(data: Uint8Array, actor?: ActorId): Doc<T>;
export function save<T>(doc: Doc<T>): Uint8Array;
export function merge<T>(local: Doc<T>, remote: Doc<T>): Doc<T>;
export function getActorId<T>(doc: Doc<T>): ActorId;
export function getConflicts<T>(doc: Doc<T>, prop: Prop): Conflicts | undefined;
export function getLastLocalChange<T>(doc: Doc<T>): Change | undefined;
export function getObjectId<T>(doc: Doc<T>): ObjID;
export function getChanges<T>(oldState: Doc<T>, newState: Doc<T>): Change[];
export function getAllChanges<T>(doc: Doc<T>): Change[];
export function applyChanges<T>(doc: Doc<T>, changes: Change[]): [Doc<T>];
export function getHistory<T>(doc: Doc<T>): State<T>[];
export function equals<T>(val1: Doc<T>, val2: Doc<T>): boolean;
export function encodeSyncState(state: SyncState): Uint8Array;
export function decodeSyncState(state: Uint8Array): SyncState;
export function generateSyncMessage<T>(doc: Doc<T>, inState: SyncState): [SyncState, SyncMessage | null];
export function receiveSyncMessage<T>(doc: Doc<T>, inState: SyncState, message: SyncMessage): [Doc<T>, SyncState, null];
export function initSyncState(): SyncState;
export function encodeChange(change: DecodedChange): Change;
export function decodeChange(data: Change): DecodedChange;
export function encodeSyncMessage(message: DecodedSyncMessage): SyncMessage;
export function decodeSyncMessage(message: SyncMessage): DecodedSyncMessage;
export function getMissingDeps<T>(doc: Doc<T>, heads: Heads): Heads;
export function getHeads<T>(doc: Doc<T>): Heads;
export function dump<T>(doc: Doc<T>): void;
export function toJS<T>(doc: Doc<T>): MaterializeValue;
export function uuid(): string;
