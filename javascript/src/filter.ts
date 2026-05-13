/**
 * Visibility filters.
 *
 * A {@link Filter} controls which changes a document renders. Changes the
 * filter rejects are still stored in the document and synced to peers, but
 * they do not appear in the rendered state. Setting the filter rebuilds the
 * op-set index in one pass; reads stay at full speed afterwards.
 *
 * A filter is a small set of {@link Rule}s, each scoped to one of three
 * places:
 *
 * - the document **default** ({@link Filter.default}),
 * - a particular **author** ({@link Filter.authors}, keyed by the hex
 *   author identifier — see {@link getAuthor}), or
 * - a particular **actor** ({@link Filter.actors}, keyed by an
 *   {@link Actor} ID).
 *
 * Resolution is most-specific-wins: an actor rule overrides an author rule,
 * which overrides the default.
 *
 * # Examples
 *
 * Hide every change made by an author after a known set of heads. This is
 * the "revoke writes by Alice after we last trusted her" pattern:
 *
 * ```ts
 * import * as Automerge from "@automerge/automerge"
 *
 * doc = Automerge.updateFilter(doc, f => {
 *   f.authors ??= {}
 *   f.authors[alice] = Automerge.allowUpTo(trustedHeads)
 * })
 * ```
 *
 * Render only a validated prefix, but keep accepting changes from the local
 * actor (the schema-validation pattern):
 *
 * ```ts
 * doc = Automerge.setFilter(doc, {
 *   default: Automerge.allowUpTo(validatedHeads),
 *   actors: { [Automerge.getActorId(doc)]: Automerge.allow },
 * })
 * ```
 *
 * Drop the rule for one author without disturbing the rest of the filter:
 *
 * ```ts
 * doc = Automerge.updateFilter(doc, f => { delete f.authors?.[alice] })
 * ```
 *
 * Clear every rule:
 *
 * ```ts
 * doc = Automerge.setFilter(doc, {})
 * ```
 *
 * @module
 */

import { _state } from "./internal_state.js"
import type { ApplyOptions } from "./implementation.js"
import { progressDocument } from "./implementation.js"
import type { Doc } from "./types.js"
import type { Filter, Heads, Rule } from "./wasm_types.js"

export type { Filter, Rule } from "./wasm_types.js"

/**
 * The {@link Rule} that accepts every matching change.
 *
 * Equivalent to the literal `"allow"` — exported so that idiomatic builder
 * code reads naturally:
 *
 * ```ts
 * doc = Automerge.updateFilter(doc, f => {
 *   f.actors ??= {}
 *   f.actors[peer] = Automerge.allow
 * })
 * ```
 */
export const allow: Rule = "allow"

/**
 * The {@link Rule} that rejects every matching change.
 *
 * Equivalent to the literal `"deny"`. See {@link allow} for the rationale
 * behind exporting both as named constants.
 */
export const deny: Rule = "deny"

/**
 * Build the {@link Rule} that accepts only changes which are ancestors of
 * `heads`. Heads do not need to be present in the document at the time the
 * rule is set: a head referenced here that arrives later is honoured as
 * soon as it lands.
 */
export function allowUpTo(heads: Heads): Rule {
  return { allowUpTo: [...heads] }
}

/**
 * Get the visibility filter currently in effect for `doc`.
 *
 * The returned object is a fresh copy; mutating it does not affect the
 * document. Use {@link setFilter} or {@link updateFilter} to install a new
 * filter.
 */
export function getFilter<T>(doc: Doc<T>): Filter {
  return _state(doc).handle.getFilter()
}

/**
 * Replace the visibility filter for `doc` and return the resulting
 * document.
 *
 * Changes the filter accepts are rendered; rejected changes remain stored
 * and continue to sync, they just do not appear in the rendered state.
 *
 * Pass `{}` to clear every rule.
 *
 * @param doc - The document to update.
 * @param filter - The new filter. Any field omitted is treated as empty
 *     (no rules in that scope, default `"allow"`).
 * @param opts - Standard {@link ApplyOptions}: pass a `patchCallback` to
 *     observe the patches caused by changes that newly became visible or
 *     newly became hidden.
 * @returns A new document handle reflecting the new filter.
 */
export function setFilter<T>(
  doc: Doc<T>,
  filter: Filter,
  opts?: ApplyOptions<T>,
): Doc<T> {
  const state = _state(doc)
  if (state.heads) {
    throw new RangeError(
      "Attempting to change an outdated document. Use Automerge.clone() if you wish to make a writable copy.",
    )
  }
  const heads = state.handle.getHeads()
  state.handle.setFilter(filter)
  return progressDocument(
    doc,
    "setFilter",
    heads,
    opts?.patchCallback ?? state.patchCallback,
  )
}

/**
 * Read-modify-write helper around {@link setFilter}.
 *
 * `fn` is called with a mutable copy of the current filter and may add,
 * remove, or change rules in place. Once it returns the modified filter is
 * installed.
 *
 * This is the most ergonomic way to add or remove a single rule:
 *
 * ```ts
 * doc = Automerge.updateFilter(doc, f => {
 *   f.authors ??= {}
 *   f.authors[alice] = Automerge.allowUpTo(trustedHeads)
 * })
 * ```
 *
 * `opts` matches {@link setFilter}.
 */
export function updateFilter<T>(
  doc: Doc<T>,
  fn: (filter: Filter) => void,
  opts?: ApplyOptions<T>,
): Doc<T> {
  const filter = getFilter(doc)
  fn(filter)
  return setFilter(doc, filter, opts)
}
