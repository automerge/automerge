// Firefox is not heavy on forwarding logs from Service Workers
// to the main window, so instead, we'll use a BroadcastChannel
// to help debugging. A bit blunt, but that'll do
const channel = new BroadcastChannel("service-worker-log")
const originalLog = console.log
console.log = function (...args) {
  try {
    channel.postMessage(args)
  } catch (error) {
    // There'll be one point where I'll try to log something
    // that's not serialisable for `postMessage` so that'll
    // avoid crashing.
    originalLog(...args)
  }
}

// Import the bundled script for automerge, which will add an `AutomergePromise`
// allowing to wait for the automerge API once all the event listeners
// of the service worker have been set up synchronously
self.importScripts("/in-service-worker/automerge.js")

// Little trickery to have a way to resolve the promise only late
// With that pattern, we get a reference we can pass to `waitUntil`
// or use as a way to delay responses to `isReady`, while delaying
// when the Promise is resolved till the Automerge API has been loaded
// Thankfully the function passed to the Promise constructor
// runs synchronously, allowing to register the listeners OK
const ready = new Promise(markReady => {
  self.addEventListener("install", event => {
    console.log("Installing service worker", new Date().toISOString())
    self.skipWaiting()

    event.waitUntil(ready)
  })

  self.addEventListener("activate", () => {
    console.log("Service worker activated")
  })

  self.addEventListener("message", event => {
    console.log("Message from page", event)
    const handler = MESSAGE_HANDLERS[event.data.action]
    if (handler) {
      handler(event)
    }
  })

  const MESSAGE_HANDLERS = {
    async isReady(event) {
      console.log("Checking if server is ready", ready)
      await ready
      event.source.postMessage({ action: "ready" })
    },
  }

  // This need to happen only after all event listeners are set up
  // This needs to run in the main script as there's no controlling
  // when the service worker script will be torn down and started again
  // Without this, when we close the browser, `automerge` and `doc`
  // would be undefined, which is less than ideal :(
  ;(async function () {
    let automerge = await AutomergeAPI
    let doc = automerge.from({
      value: 1,
    })

    // You'd likely want to add some persistence here, to IndexedDB most likely
    // as it's the storage available inside service workers

    Object.assign(MESSAGE_HANDLERS, {
      getValue(event) {
        event.source.postMessage({ action: "setValue", value: doc.value })
      },
      increment(event) {
        const { value } = event.data
        doc = automerge.change(doc, draft => {
          draft.value += value
        })
        console.log("Sending back", doc.value)
        event.source.postMessage({ action: "setValue", value: doc.value })
      },
    })

    console.log("Marking server as ready")
    markReady()
  })()
})
