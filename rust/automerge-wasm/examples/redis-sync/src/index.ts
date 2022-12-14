import { createClient } from 'redis';
import type { RedisClientType, RedisClientOptions } from 'redis'
import { Automerge, create, load } from "@automerge/automerge-wasm"

export type RedisSyncOptions<T> = {
    redis: string,
    docId: string,
    clientId: string,
    init: (doc: Automerge) => void,
    update?: (doc: any) => void,
}

export class RedisSync<T> {
  client: RedisClientType
  subscriber: RedisClientType
  doc: null | Automerge
  docId: string
  clientId: string
  cursor: number
  leader: string | null
  init: (doc: Automerge) => void
  update?: (doc: any) => void
  interval: ReturnType<typeof setInterval>
  save_bytes: number
  incremental_save_bytes: number

  constructor(options: RedisSyncOptions<T>) {
    this.doc = null
    this.docId = options.docId
    this.client = createClient({ url: options.redis })
    this.clientId = options.clientId
    this.cursor = 0
    this.init = options.init
    this.update = options.update
    this.interval = setInterval(() => this.tick(), 10 * 1000)  // force a save every 10 seconds
    this.leader = null
    this.save_bytes = 0
    this.incremental_save_bytes = 0

    this.client.on('error', (err) => console.log('Redis Client Error', err));
    this.subscriber = this.client.duplicate()
  }

  iAmLeader() : boolean {
    return this.leader == this.clientId
  }

  get _saved_() : string { return `${this.docId}:saved` }
  get _saved_cursor_() : string { return `${this.docId}:saved:cursor` }
  get _changes_() : string { return `${this.docId}:changes` }
  get _leader_() : string { return `${this.docId}:leader` }
  get _notify_() : string { return `${this.docId}:notify` }

  async change(f: (doc: Automerge) => void) {
    if (this.doc === null) {
      throw new RangeError("cannot call change - doc not initalized")
    }
    let heads = this.doc.getHeads()
    f(this.doc)
    let changes = this.doc.getChanges(heads)
    if (changes.length > 0) {
      for (let i in changes) {
          let change_str = Buffer.from(changes[i].buffer).toString("hex");
          await this.client.rPush(this._changes_, change_str)
      }
      await this.notify_peers()
      this.notify_local()
    }
  }

  async tick() {
    let itsMe = await this.determineLeader()
    if (this.doc && itsMe) {
      if (this.incremental_save_bytes > this.save_bytes * 10) {
        console.log("  ::as leader - doing a full save")
        let save_all = this.doc.save() 
        this.incremental_save_bytes += save_all.length
        let save_all_str = Buffer.from(save_all.buffer).toString("hex");
        await this.client.multi()
          .rPush(this._saved_, save_all_str)
          .set(this._saved_cursor_,this.cursor)
          .exec()
      } else {
        let next_chunk = this.doc.saveIncremental() 
        if (next_chunk && next_chunk.length > 0) {
          console.log("  ::as leader - doing an incremental save")
          this.incremental_save_bytes += next_chunk.length
          let next_chunk_str = Buffer.from(next_chunk.buffer).toString("hex");
          await this.client.multi()
            .rPush(this._saved_, next_chunk_str)
            .set(this._saved_cursor_,this.cursor)
            .exec()
        }
      }
    }
  }

  toJS() : T | null {
    if (this.doc) {
      return this.doc.materialize("/") as T
    } else {
      return null
    }
  }

  async loadDocumentFromRedis() {
    console.log("  ::Loading document...");
    let saved = await this.client.lRange(this._saved_,0,-1)
    let cursor = await this.client.get(this._saved_cursor_)
    if (Array.isArray(saved) && typeof cursor === 'string') {
      let first_chunk_str = saved.shift();
      if (first_chunk_str) {
        let first_chunk = Buffer.from(first_chunk_str,'hex')
        let doc = load(first_chunk)
        this.save_bytes = first_chunk.length;
        for (let i in saved) {
          let chunk = Buffer.from(saved[i],'hex')
          doc.loadIncremental(chunk)
          this.incremental_save_bytes += chunk.length
        }
        this.cursor = parseInt(cursor);
        await this.fastForward(doc)
        this.doc = doc
        this.notify_local()
        console.log(`  ::done! cursor=${cursor}`);
        return
      }
    }
    console.log("  ::no document found in redis");
  }

  async fastForward(doc: Automerge) {
      if (doc !== null) {
          const changes = await this.client.lRange(this._changes_,this.cursor,-1)
          if (Array.isArray(changes)) {
            for (let i in changes) {
                const change = Buffer.from(changes[i],'hex')
                doc.loadIncremental(change)
                this.cursor += 1
                this.notify_local()
            }
          }
      }
  }

  async handleMessage(message:string, channel: string) {
      if (message !== this.clientId && this.doc) {
        await this.fastForward(this.doc)
      }
  }

  async connect() {
    await this.client.connect()
    await this.subscriber.connect()
    await this.subscriber.pSubscribe(this._notify_, (m,c) => this.handleMessage(m,c))
    await this.loadDocumentFromRedis()

    const itsMe = await this.determineLeader()

    if (itsMe && this.doc === null) {
      await this.resetDocumentState()
    }
  }

  async resetDocumentState() {
    console.log("  ::resetting document state")
    let doc = create()
    this.init(doc)
    let saved = doc.save()
    if (!saved || saved.length === 0) {
      throw new RangeError("initalized document is blank");
    }
    this.doc = doc;
    this.save_bytes = saved.length;
    this.incremental_save_bytes = 0;
    let saved_str = Buffer.from(saved.buffer).toString("hex");
    await this.client.del(this._changes_)
    await this.client.del(this._saved_)
    await this.client.rPush(this._saved_, saved_str)
    await this.client.set(this._saved_cursor_,0)
    this.notify_local()
  }

  async determineLeader() : Promise<boolean> {
    await this.client.setNX(this._leader_, this.clientId)
    this.leader = await this.client.get(this._leader_)
    const itsMe = this.leader == this.clientId
    if (itsMe) {
      this.client.expire(this._leader_, 60)
    }
    return itsMe
  }
  
  async notify_peers() {
    await this.client.publish(this._notify_, this.clientId)
  }

  notify_local() {
    if (this.doc && this.update) {
        this.update(this.toJS())
    }
  }

  async disconnect() {
    await this.tick() // save any data
    clearInterval(this.interval)
    await this.client.disconnect();
    await this.subscriber.disconnect();
  }
}
