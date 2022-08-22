import { Automerge as VanillaAutomerge } from "automerge-types"

export * from "automerge-types"
export { default } from "automerge-types"

export class Automerge extends VanillaAutomerge {
  // experimental spans api - unstable!
  mark(obj: ObjID, name: string, range: string, value: Value, datatype?: Datatype): void;
  unmark(obj: ObjID, mark: ObjID): void;
  spans(obj: ObjID): any;
  raw_spans(obj: ObjID): any;
  blame(obj: ObjID, baseline: Heads, changeset: Heads[]): ChangeSet[];
  attribute(obj: ObjID, baseline: Heads, changeset: Heads[]): ChangeSet[];
  attribute2(obj: ObjID, baseline: Heads, changeset: Heads[]): ChangeSet[];

  // override old methods that return automerge
  clone(actor?: string): Automerge;
  fork(actor?: string): Automerge;
  forkAt(heads: Heads, actor?: string): Automerge;
}

export function create(actor?: Actor): Automerge;
export function load(data: Uint8Array, actor?: Actor): Automerge;
