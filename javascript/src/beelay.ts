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
    start: CommitHash,
    end: CommitHash,
    checkpoints: CommitHash[],
    data: Uint8Array,
  ): any
  addLink(from: DocumentId, to: DocumentId): any
  loadDocument(docId: DocumentId): any
  syncCollection(docId: DocumentId): any
  peerConnected(peerId: PeerId): any
  peerDisconnected(peerId: PeerId): any
  peerId(): PeerId

  loadRangeComplete(
    taskId: string,
    result: { key: string[]; data: Uint8Array | undefined }[],
  ): any
  loadComplete(taskId: string, result: Uint8Array | undefined): any
  putComplete(taskId: string): any
  deleteComplete(taskId: string): any
}

type IoTask =
  | { action: "load"; id: string; key: string[] }
  | { action: "load_range"; id: string; prefix: string[] }
  | { action: "put"; key: string[]; id: string; data: Uint8Array }
  | { action: "delete"; id: string; key: string[] }

export type DocumentId = string
export type CommitHash = string

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
    start: CommitHash
    end: CommitHash
    checkpoints: CommitHash[]
  }
  docEvent: {
    docId: DocumentId
    data: CommitOrBundle
  }
}

type OnMessage = (args: { message: Message }) => void
type OnBundleRequired = (args: {
  start: CommitHash
  end: CommitHash
  checkpoints: CommitHash[]
}) => void
type OnDocEvent = (args: { docId: DocumentId; data: CommitOrBundle }) => void

let lowLevel: LowLevel | undefined

export function init(impl: LowLevel) {
  lowLevel = impl
}

type TurnEvents = {
  new_messages: Message[]
  new_tasks: IoTask[]
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
          | { story_type: "sync_collection"; documents: DocumentId[] }
      }
    | { story_type: "load_document"; commits: Commit[] }
    | { story_type: "add_link" }
  notifications: {
    docId: DocumentId
    data: CommitOrBundle
  }[]
}

export function inspectMessage(message: Uint8Array): any {
  return lowLevel!.inspectMessage(message)
}

export class Beelay {
  #lowLevel: LowLevelImplementation
  #storage: StorageAdapter
  #messageListeners: OnMessage[] = []
  #bundleRequiredListeners: OnBundleRequired[] = []
  #docEventListeners: OnDocEvent[] = []
  #awaitingDocCreation: { [key: string]: (docId: DocumentId) => void } = {}
  #awaitingAddcommits: { [key: string]: () => void } = {}
  #awaitingAddBundle: { [key: string]: () => void } = {}
  #awaitingLoadDoc: { [key: string]: (commits: (Commit | Bundle)[]) => void } =
    {}
  #awaitingSyncCollection: { [key: string]: (docs: DocumentId[]) => void } = {}
  #awaitingAddLink: { [key: string]: () => void } = {}

  get peerId() {
    return this.#lowLevel.peerId()
  }

  constructor({
    peerId,
    storage,
  }: {
    peerId: PeerId
    storage: StorageAdapter
  }) {
    this.#storage = storage
    this.#lowLevel = lowLevel!.construct({ peerId })
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
    const result = new Promise<void>((resolve, reject) => {
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
    start: CommitHash
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
    const result = new Promise<void>((resolve, reject) => {
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
    const result = new Promise<DocumentId>((resolve, reject) => {
      this.#awaitingDocCreation[storyId] = resolve
    })
    this.processEvents(events)
    return result
  }

  loadDocument(docId: DocumentId): Promise<(Commit | Bundle)[]> {
    const [storyId, events] = this.#lowLevel.loadDocument(docId) as [
      string,
      TurnEvents,
    ]
    const result = new Promise<(Commit | Bundle)[]>((resolve, reject) => {
      this.#awaitingLoadDoc[storyId] = resolve
    })
    this.processEvents(events)
    return result
  }

  syncCollection(docId: DocumentId): Promise<DocumentId[]> {
    const [storyId, events] = this.#lowLevel.syncCollection(docId) as [
      string,
      TurnEvents,
    ]
    const result = new Promise<DocumentId[]>((resolve, reject) => {
      this.#awaitingSyncCollection[storyId] = resolve
    })
    this.processEvents(events)
    return result
  }

  syncDoc(_docId: DocumentId): Promise<{
    present_locally: boolean
    present_remotely: boolean
    snapshot_id: string | null
  }> {
    return new Promise(() => {
      return
    })
  }

  *listen(_docId: DocumentId, _snapshotId: string): Generator<CommitOrBundle> {
    // TODO
  }

  addLink({ from, to }: { from: DocumentId; to: DocumentId }) {
    const [storyId, events] = this.#lowLevel.addLink(from, to) as [
      string,
      TurnEvents,
    ]
    const result = new Promise<void>((resolve, reject) => {
      this.#awaitingAddLink[storyId] = resolve
    })
    this.processEvents(events)
    return result
  }

  peerConnected(peerId: PeerId) {
    const [_, events] = this.#lowLevel.peerConnected(peerId) as [
      null,
      TurnEvents,
    ]
    this.processEvents(events)
  }

  peerDisconnected(peerId: PeerId) {
    const [_, events] = this.#lowLevel.peerDisconnected(peerId) as [
      null,
      TurnEvents,
    ]
    this.processEvents(events)
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
    console.log(JSON.stringify(events, null, 2))
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
      } else if (event.story_type === "sync_collection") {
        this.#awaitingSyncCollection[storyId](event.documents)
        delete this.#awaitingSyncCollection[storyId]
      } else if (event.story_type === "add_link") {
        this.#awaitingAddLink[storyId]()
        delete this.#awaitingAddLink[storyId]
      } else if (event.story_type === "add_bundle") {
        this.#awaitingAddBundle[storyId]()
        delete this.#awaitingAddBundle[storyId]
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
