import {
  type Beelay,
  type MemoryStorageAdapter,
  type MemorySigner,
  type Config,
} from "./beelay_types.js"
import { ApiHandler } from "./low_level.js"

export type {
  Beelay,
  StorageAdapter,
  Signer,
  Config,
  Stream,
  Commit,
  CommitOrBundle,
  Bundle,
  BundleSpec,
} from "./beelay_types.js"

export function loadBeelay(config: Config): Promise<Beelay> {
  return ApiHandler["Beelay"].load(config)
}

export function createMemoryStorageAdapter(): MemoryStorageAdapter {
  return new ApiHandler["MemoryStorageAdapter"]()
}

export function createMemorySigner(
  signingKey?: Uint8Array | null,
): MemorySigner {
  return new ApiHandler["MemorySigner"](signingKey)
}

export function parseBeelayDocId(docId: string): string | null {
  return ApiHandler["parseBeelayDocId"](docId)
}
