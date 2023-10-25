# Rollup + Automerge

Example implementation of Automerge bundled with [Rollup](https://rollupjs.org/).

This example illustrates two situations:
- automerge running inside the page itself
- automerge running inside a service worker

## Running the example

```bash
yarn install
yarn start
```

## Inside the page

This example is [the homepage](http://localhost:3000).

Check the [`inPageConfig` variable of `rollup.config.js`]() for the commented Rollup's configuration.

## In a service worker

This example is [at `/in-service-worker/`](http://localhost:3000/in-service-worker/).

Check the [`inServiceWorkerConfig` variable of `rollup.config.js`](rollup.config.js/#L8) for the Rollup's configuration.

Running inside a service worker not only affects the bundling, but also how the page and the worker coordinate to wait for the worker to have loaded automerge-wasm. Both the [page script](public/in-service-worker/page.js) and [service worker](public/in-service-worker/service-worker.js) have comments explaining this coordination. 


