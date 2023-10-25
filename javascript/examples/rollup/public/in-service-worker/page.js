const registration = await navigator.serviceWorker.register(
  "/in-service-worker/service-worker.js",
)
await navigator.serviceWorker.ready
// At this point, the service worker is active
// See https://web.dev/articles/service-worker-lifecycle

// However, active, doesn't mean ready for our purpose.
// When the browser gets closed, it loses all that was stored in memory
// Once it relauches, it needs to restore the availability of the Automerge API
// (and the content of the document used for storing the state if persisted).
// That's asynchronous so we need to wait for that to have happened before we actually
// start working with the Service Worker.
// This is done through a little "isReady", "ready" message handshake
await new Promise(resolve => {
  registration.active.postMessage({ action: "isReady" })
  navigator.serviceWorker.addEventListener("message", handleResponseMessage)

  function handleResponseMessage(event) {
    if (event.data.action == "ready") {
      navigator.serviceWorker.removeEventListener(
        "message",
        handleResponseMessage,
      )
      resolve()
    }
  }
})

// Now the service worker is properly ready

// Ask for the new value, prompting the worker to respond with a `setValue`
// So it can be displayed on the page
registration.active.postMessage({
  action: "getValue",
})

// ADd the click handler
document.addEventListener("click", () => {
  // Only handle clicks on the buttons
  if (event.target.dataset.value) {
    registration.active.postMessage({
      action: "increment",
      value: parseInt(event.target.dataset.value),
    })
  }
})

// Handle messages coming from the ServiceWorker
//
navigator.serviceWorker.addEventListener("message", ({ data }) => {
  console.log("Message from service worker!", data)
  const handler = MESSAGE_HANDLERS[data.action]
  if (handler) {
    handler(data)
  }
})

const MESSAGE_HANDLERS = {
  setValue({ value }) {
    document.querySelector("output").value = value
  },
}
