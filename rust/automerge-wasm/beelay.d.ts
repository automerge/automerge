/* tslint:disable */
/* eslint-disable */
export function parseBeelayDocId(val: string): DocumentId;

export type Config = {
  storage: StorageAdapter;
  signer: Signer;
};

export type StorageKey = string[];
export type PeerId = string;
export type DocumentId = string;

export interface Signer {
  verifyingKey: Uint8Array;
  sign(message: Uint8Array): Promise<Uint8Array>;
}

export interface StorageAdapter {
  load(key: string[]): Promise<Uint8Array | undefined>;
  loadRange(prefix: string[]): Promise<Map<StorageKey, Uint8Array>>;
  save(key: string[], data: Uint8Array): Promise<void>;
  remove(key: string[]): Promise<void>;
  listOneLevel(prefix: string[]): Promise<Array<string[]>>;
}

export type Audience =
  | { type: "peerId"; peerId: PeerId }
  | { type: "serviceName"; serviceName: string };

export type StreamConfig =
  | { direction: "accepting"; receiveAudience?: string | null }
  | { direction: "connecting"; remoteAudience: Audience };

export interface Stream {
  on(event: "message", f: (msg: Uint8Array) => void): void;
  off(event: "message", f: (msg: Uint8Array) => void): void;
  on(event: "disconnect", f: () => void): void;
  off(event: "disconnect", f: () => void): void;
  closed(): Promise<void>;
  recv(msg: Uint8Array): Promise<void>;
  disconnect(): void;
}

export type CommitHash = string;

export type Commit = {
  hash: CommitHash;
  parents: CommitHash[];
  contents: Uint8Array;
};

export type Bundle = {
  start: CommitHash;
  end: CommitHash;
  checkpoints: CommitHash[];
  contents: Uint8Array;
};

export type CommitOrBundle =
  | ({ type: "commit" } & Commit)
  | ({ type: "bundle" } & Bundle);

export type BundleSpec = {
  doc: DocumentId;
  start: CommitHash;
  end: CommitHash;
  checkpoints: CommitHash[];
};

export type Access = "pull" | "read" | "write" | "admin";

export type HexContactCard = string;
export type Membered =
  | { type: "group"; id: PeerId }
  | { type: "document"; id: DocumentId };
export type KeyhiveEntity =
  | { type: "individual"; contactCard: HexContactCard }
  | { type: "public" }
  | Membered;

export type CreateDocArgs = {
  initialCommit: Commit;
  otherParents?: Array<KeyhiveEntity>;
};
export type CreateGroupArgs = {
  otherParents?: Array<KeyhiveEntity>;
};
export type AddMemberArgs =
  | { groupId: PeerId; member: KeyhiveEntity; access: Access }
  | { docId: DocumentId; member: KeyhiveEntity; access: Access };
export type RemoveMemberArgs =
  | { groupId: PeerId; member: KeyhiveEntity }
  | { docId: DocumentId; member: KeyhiveEntity };
export type AddCommitArgs = {
  docId: DocumentId;
  commits: Commit[];
};
export type AddBundleArgs = {
  docId: DocumentId;
  bundle: Bundle;
};

interface BeelayEvents {
  "peer-sync-state": { peerId: PeerId; status: "listening" | "connected" };
  "doc-event": { docId: DocumentId; data: CommitOrBundle };
}
interface Beelay {
  on<T extends keyof BeelayEvents>(
    eventName: T,
    handler: (args: BeelayEvents[T]) => void,
  ): void;
  off<T extends keyof BeelayEvents>(
    eventName: T,
    handler: (args: BeelayEvents[T]) => void,
  ): void;
  createGroup(args?: CreateGroupArgs): Promise<PeerId>;
}

export class Beelay {
  private constructor();
  free(): void;
  static load(config: Config): Promise<Beelay>;
  createContactCard(): Promise<HexContactCard>;
  createDoc(args: CreateDocArgs): Promise<DocumentId>;
  addMember(args: AddMemberArgs): Promise<void>;
  removeMember(args: RemoveMemberArgs): Promise<void>;
  addCommits(args: AddCommitArgs): Promise<BundleSpec[]>;
  addBundle(args: AddBundleArgs): Promise<void>;
  loadDocument(doc_id: any): Promise<Array<CommitOrBundle> | null>;
  createStream(config: StreamConfig): Stream;
  stop(): void;
  version(): string;
  waitUntilSynced(peer_id: PeerId): Promise<void>;
  isStopped(): boolean;
  readonly peerId: PeerId;
}
export class MemorySigner {
  free(): void;
  constructor(signing_key?: Uint8Array | null);
  sign(message: Uint8Array): Promise<Uint8Array>;
  readonly verifyingKey: Uint8Array;
  readonly signingKey: Uint8Array;
}
export class MemoryStorageAdapter {
  free(): void;
  constructor();
  load(key: any): Promise<any>;
  loadRange(prefix: any): Promise<any>;
  save(key: any, data: any): Promise<void>;
  remove(key: any): Promise<void>;
  listOneLevel(prefix: any): Promise<any>;
}
export class StreamHandle {
  private constructor();
  free(): void;
  on(event: any, callback: any): void;
  recv(msg: any): void;
  disconnect(): void;
}
