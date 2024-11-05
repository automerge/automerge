type LowLevel = {
  construct(config: { peerId: PeerId }): LowLevelImplementation
  inspectMessage(message: Uint8Array): any
}

type LowLevelImplementation = {
  receiveMessage(message: Message): any
  createDocument(): any
  addCommits(docId: DocumentId, commits: Commit[]): any
  addBundle(
    docId: DocumentId,
    start: CommitHash | null,
    end: CommitHash,
    checkpoints: CommitHash[],
    data: Uint8Array,
  ): any
  addLink(from: DocumentId, to: DocumentId): any
  loadDocument(docId: DocumentId): any
  syncDoc(docId: DocumentId, peerId: PeerId): any
  listen(peerId: PeerId, snapshot: SnapshotId): any
  peerId(): PeerId

  loadRangeComplete(
    taskId: string,
    result: { key: string[]; data: Uint8Array | undefined }[],
  ): any
  loadComplete(taskId: string, result: Uint8Array | undefined): any
  putComplete(taskId: string): any
  deleteComplete(taskId: string): any
  askComplete(taskId: string, peers: PeerId[]): any
}

type IoTask =
  | { action: "load"; id: string; key: string[] }
  | { action: "load_range"; id: string; prefix: string[] }
  | { action: "put"; key: string[]; id: string; data: Uint8Array }
  | { action: "delete"; id: string; key: string[] }
  | { action: "ask"; id: string; docId: DocumentId }

export type DocumentId = string
export type CommitHash = string
export type SnapshotId = string

export type Commit = {
  parents: CommitHash[]
  hash: CommitHash
  contents: Uint8Array
}

export type Bundle = {
  start: CommitHash
  end: CommitHash
  checkpoints: CommitHash[]
  contents: Uint8Array
}

export interface StorageAdapter {
  load(key: string[]): Promise<Uint8Array | undefined>
  save(key: string[], data: Uint8Array): Promise<void>
  remove(key: string[]): Promise<void>
  loadRange(
    prefix: string[],
  ): Promise<{ key: string[]; data: Uint8Array | undefined }[]>
}

type PeerId = string

export type CommitOrBundle =
  | ({ type: "commit" } & Commit)
  | ({ type: "bundle" } & Bundle)

export type Message = {
  sender: PeerId
  recipient: PeerId
  message: Uint8Array
}

export type BeelayEvents = {
  message: { message: Message }
  bundleRequired: {
    docId: DocumentId
    start: CommitHash | null
    end: CommitHash
    checkpoints: CommitHash[]
  }
  docEvent: {
    docId: DocumentId
    peer: PeerId
    data: CommitOrBundle
  }
  docRequested: {
    docId: DocumentId
    fromPeer: PeerId
  }
}

type OnMessage = (args: { message: Message }) => void
type OnBundleRequired = (args: {
  docId: DocumentId
  start: CommitHash
  end: CommitHash
  checkpoints: CommitHash[]
}) => void
type OnDocEvent = (args: {
  docId: DocumentId
  peer: PeerId
  data: CommitOrBundle
}) => void

let lowLevel: LowLevel | undefined

export function init(impl: LowLevel) {
  lowLevel = impl
}

type TurnEvents = {
  new_messages: Message[]
  new_tasks: IoTask[]
  requested_docs: { peerId: PeerId; docId: DocumentId }[]
  completed_stories:
    | {
        [key: string]:
          | { story_type: "create_document"; document_id: DocumentId }
          | {
              story_type: "add_commits"
              new_bundles_required: {
                start: CommitHash
                end: CommitHash
                checkpoints: CommitHash[]
              }[]
            }
          | { story_type: "sync_doc"; found: boolean; snapshotId: SnapshotId }
      }
    | { story_type: "load_document"; commits: Commit[] }
    | { story_type: "add_link" }
  notifications: {
    docId: DocumentId
    peer: PeerId
    data: CommitOrBundle
  }[]
}

export function inspectMessage(message: Uint8Array): any {
  return lowLevel!.inspectMessage(message)
}

export type RequestPolicy = (args: { docId: DocumentId }) => Promise<PeerId[]>

export class Beelay {
  #lowLevel: LowLevelImplementation
  #storage: StorageAdapter
  #messageListeners: OnMessage[] = []
  #bundleRequiredListeners: OnBundleRequired[] = []
  #docEventListeners: OnDocEvent[] = []
  #awaitingDocCreation: { [key: string]: (docId: DocumentId) => void } = {}
  #awaitingAddcommits: { [key: string]: () => void } = {}
  #awaitingAddBundle: { [key: string]: () => void } = {}
  #awaitingLoadDoc: {
    [key: string]: (commits: (Commit | Bundle)[] | null) => void
  } = {}
  #awaitingSyncDoc: {
    [key: string]: (result: { snapshot: SnapshotId; found: boolean }) => void
  } = {}
  #awaitingAddLink: { [key: string]: () => void } = {}
  #awaitingListen: { [key: string]: () => void } = {}
  #requestPolicy: RequestPolicy

  get peerId() {
    return this.#lowLevel.peerId()
  }

  constructor({
    peerId,
    storage,
    requestPolicy,
  }: {
    peerId: PeerId
    storage: StorageAdapter
    requestPolicy?: RequestPolicy
  }) {
    this.#storage = storage
    this.#lowLevel = lowLevel!.construct({ peerId })
    if (requestPolicy != null) {
      this.#requestPolicy = requestPolicy
    } else {
      this.#requestPolicy = () => Promise.resolve([])
    }
  }

  receiveMessage({ message }: { message: Message }) {
    const [_, events] = this.#lowLevel.receiveMessage(message) as [
      null,
      TurnEvents,
    ]
    this.processEvents(events)
  }

  addCommits({
    docId: doc,
    commits,
  }: {
    docId: DocumentId
    commits: Commit[]
  }) {
    const [storyId, events] = this.#lowLevel.addCommits(doc, commits) as [
      string,
      TurnEvents,
    ]
    const result = new Promise<void>((resolve, _reject) => {
      this.#awaitingAddcommits[storyId] = resolve
    })
    this.processEvents(events)
    return result
  }

  addBundle({
    docId,
    start,
    end,
    checkpoints,
    data,
  }: {
    docId: DocumentId
    start: CommitHash | null
    end: CommitHash
    checkpoints: CommitHash[]
    data: Uint8Array
  }): Promise<void> {
    const [storyId, events] = this.#lowLevel.addBundle(
      docId,
      start,
      end,
      checkpoints,
      data,
    ) as [string, TurnEvents]
    const result = new Promise<void>((resolve, _reject) => {
      this.#awaitingAddBundle[storyId] = resolve
    })
    this.processEvents(events)
    return result
  }

  createDocument(): Promise<DocumentId> {
    const [storyId, events] = this.#lowLevel.createDocument() as [
      string,
      TurnEvents,
    ]
    const result = new Promise<DocumentId>((resolve, _reject) => {
      this.#awaitingDocCreation[storyId] = resolve
    })
    this.processEvents(events)
    return result
  }

  loadDocument(docId: DocumentId): Promise<(Commit | Bundle)[] | null> {
    const [storyId, events] = this.#lowLevel.loadDocument(docId) as [
      string,
      TurnEvents,
    ]
    const result = new Promise<(Commit | Bundle)[] | null>((resolve, _) => {
      this.#awaitingLoadDoc[storyId] = resolve
    })
    this.processEvents(events)
    return result
  }

  syncDoc(
    docId: DocumentId,
    peerId: PeerId,
  ): Promise<{ snapshot: SnapshotId; found: boolean }> {
    const [storyId, events] = this.#lowLevel.syncDoc(docId, peerId) as [
      string,
      TurnEvents,
    ]
    const result = new Promise<{ snapshot: SnapshotId; found: boolean }>(
      (resolve, _reject) => {
        this.#awaitingSyncDoc[storyId] = resolve
      },
    )
    this.processEvents(events)
    return result
  }

  listen(peerId: PeerId, snapshotId: SnapshotId): Promise<void> {
    const listenResult = this.#lowLevel.listen(peerId, snapshotId) as [
      // const [storyId, events] = this.#lowLevel.listen(peerId, snapshotId) as [
      string,
      TurnEvents,
    ]
    const [storyId, events] = listenResult
    const result = new Promise<void>((resolve, _reject) => {
      this.#awaitingListen[storyId] = resolve
    })
    this.processEvents(events)
    return result
  }

  cancelListens(_peerId: PeerId) {
    // TODO
  }

  addLink({ from, to }: { from: DocumentId; to: DocumentId }) {
    const [storyId, events] = this.#lowLevel.addLink(from, to) as [
      string,
      TurnEvents,
    ]
    const result = new Promise<void>((resolve, _) => {
      this.#awaitingAddLink[storyId] = resolve
    })
    this.processEvents(events)
    return result
  }

  on<E extends keyof BeelayEvents>(
    eventName: E,
    callback: (args: BeelayEvents[E]) => void,
  ) {
    if (eventName === "message") {
      this.#messageListeners.push(callback as OnMessage)
    } else if (eventName === "bundleRequired") {
      this.#bundleRequiredListeners.push(callback as OnBundleRequired)
    } else if (eventName === "docEvent") {
      this.#docEventListeners.push(callback as OnDocEvent)
    } else {
      throw new Error(`Unknown event name: ${eventName}`)
    }
  }

  off<E extends keyof BeelayEvents>(
    event: "message" | "bundleRequired",
    callback: (args: BeelayEvents[E]) => void,
  ): void {
    if (event === "message") {
      this.#messageListeners = this.#messageListeners.filter(l => l != callback)
    } else if (event === "bundleRequired") {
      this.#bundleRequiredListeners = this.#bundleRequiredListeners.filter(
        l => l != callback,
      )
    } else if (event === "onDocEvent") {
      this.#docEventListeners = this.#docEventListeners.filter(
        l => l != callback,
      )
    } else {
      throw new Error("unknown event type")
    }
  }

  processEvents(events: TurnEvents) {
    // console.log(JSON.stringify(events, null, 2))
    for (const message of events.new_messages) {
      this.#messageListeners.forEach(l => l({ message }))
    }

    for (const evt of events.notifications) {
      this.#docEventListeners.forEach(f => f(evt))
    }

    for (const task of events.new_tasks) {
      if (task.action === "load") {
        this.#storage.load(task.key).then(result => {
          const [_, events] = this.#lowLevel.loadComplete(task.id, result) as [
            null,
            TurnEvents,
          ]
          this.processEvents(events)
        })
      } else if (task.action === "load_range") {
        this.#storage.loadRange(task.prefix).then(result => {
          const [_, events] = this.#lowLevel.loadRangeComplete(
            task.id,
            result,
          ) as [null, TurnEvents]
          this.processEvents(events)
        })
      } else if (task.action === "put") {
        this.#storage.save(task.key, task.data).then(() => {
          const [_, events] = this.#lowLevel.putComplete(task.id) as [
            null,
            TurnEvents,
          ]
          this.processEvents(events)
        })
      } else if (task.action === "delete") {
        this.#storage.remove(task.key).then(() => {
          const [_, events] = this.#lowLevel.deleteComplete(task.id) as [
            null,
            TurnEvents,
          ]
          this.processEvents(events)
        })
      } else if (task.action == "ask") {
        this.#requestPolicy({ docId: task.docId }).then(result => {
          const [_, events] = this.#lowLevel.askComplete(task.id, result) as [
            null,
            TurnEvents,
          ]
          this.processEvents(events)
        })
      } else {
        throw new Error("Unknown task type")
      }
    }

    for (const [storyId, event] of Object.entries(events.completed_stories)) {
      if (event.story_type === "create_document") {
        this.#awaitingDocCreation[storyId](event.document_id)
        delete this.#awaitingDocCreation[storyId]
      } else if (event.story_type === "add_commits") {
        this.#awaitingAddcommits[storyId]()
        for (const bundle of event.new_bundles_required) {
          this.#bundleRequiredListeners.forEach(l => l(bundle))
        }
        delete this.#awaitingAddcommits[storyId]
      } else if (event.story_type === "load_document") {
        this.#awaitingLoadDoc[storyId](event.commits)
        delete this.#awaitingLoadDoc[storyId]
      } else if (event.story_type === "sync_doc") {
        this.#awaitingSyncDoc[storyId]({
          snapshot: event.snapshotId,
          found: event.found,
        })
        delete this.#awaitingSyncDoc[storyId]
      } else if (event.story_type === "add_link") {
        this.#awaitingAddLink[storyId]()
        delete this.#awaitingAddLink[storyId]
      } else if (event.story_type === "add_bundle") {
        this.#awaitingAddBundle[storyId]()
        delete this.#awaitingAddBundle[storyId]
      } else if (event.story_type === "listen") {
        this.#awaitingListen[storyId]()
        delete this.#awaitingListen[storyId]
      } else {
        throw new Error("Unknown story type")
      }
    }
  }
}

/**
 * @hidden
 */
export function initialize(impl: LowLevel) {
  lowLevel = impl
}
