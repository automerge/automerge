export class Beelay {
  receiveMessage(message: Message): any;
  createDocument(): any;
  addCommits(docId: DocumentId, commits: Commit[]): any;
  addBundle(
    docId: DocumentId,
    start: CommitHash,
    end: CommitHash,
    checkpoints: CommitHash[],
    data: Uint8Array,
  ): any;
  addLink(from: DocumentId, to: DocumentId): any;
  loadDocument(docId: DocumentId): any;
  syncDoc(docId: DocumentId): any;
  listen(peerId: PeerId, snapshot: SnapshotId): any;
  peerId(): PeerId;

  loadRangeComplete(
    taskId: string,
    result: { key: string[]; data: Uint8Array | undefined }[],
  ): any;
  loadComplete(taskId: string, result: Uint8Array | undefined): any;
  putComplete(taskId: string): any;
  deleteComplete(taskId: string): any;
  askComplete(taskId: string, peers: PeerId[]): any;
}

export type DocumentId = string;
export type CommitHash = string;
export type PeerId = string;
export type SnapshotId = string;

export type Commit = {
  parents: CommitHash[];
  hash: CommitHash;
  contents: Uint8Array;
};

export type Bundle = {
  start: CommitHash;
  end: CommitHash;
  checkpoints: CommitHash[];
  contents: Uint8Array;
};

type StorageAdapter = {
  load(key: string[]): Promise<Uint8Array | undefined>;
  save(key: string[], data: Uint8Array): Promise<void>;
  remove(key: string[]): Promise<void>;
  loadRange(
    prefix: string[],
  ): Promise<{ key: string[]; data: Uint8Array | undefined }[]>;
};

export type CommitOrBundle =
  | ({ type: "commit" } & Commit)
  | ({ type: "bundle" } & Bundle);

export type DocEvent = {
  docId: DocumentId;
  peer: PeerId;
  data: CommitOrBundle;
};

export type Message = {
  sender: PeerId;
  recipient: PeerId;
  message: Uint8Array;
};

export type LogLevel = "trace" | "debug" | "info" | "warn" | "error";

export declare function init_logging(level: LogLevel): void;
