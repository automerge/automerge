/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `npm run deploy` to publish your worker
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */

import  * as Automerge  from "@automerge/automerge"

// NOTE: Attempting to create a document at the top level will cause an
// obscure error about being unable to generate random values. This is because
// cloudflare workers are only allowed to generate random values in a handler,
// which is why we initialize the document in the fetch handler
let doc

// Export a default object containing event handlers
export default {
  // The fetch handler is invoked when this worker receives a HTTP(S) request
  // and should return a Response (optionally wrapped in a Promise)
  async fetch(_request, _env, _ctx) {
	if (!doc) {
		doc = Automerge.from({message: "hello workerd"})
	}
	return new Response(JSON.stringify(doc))
  }
}
