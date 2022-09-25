import { Automerge as VanillaAutomerge } from "automerge-types"

export * from "automerge-types"
export { default } from "automerge-types"

export class Automerge extends VanillaAutomerge {
  // experimental api can go here
  applyPatches<Doc>(obj: Doc, meta?: JsValue, callback?: Function): Doc;

  // override old methods that return automerge
  clone(actor?: string): Automerge;
  fork(actor?: string): Automerge;
  forkAt(heads: Heads, actor?: string): Automerge;
}

export function create(actor?: Actor): Automerge;
export function load(data: Uint8Array, actor?: Actor): Automerge;
