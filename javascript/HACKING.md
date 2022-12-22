## Architecture

The `@automerge/automerge` package is a set of
[`Proxy`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Proxy)
objects which provide an idiomatic javascript interface built on top of the
lower level `@automerge/automerge-wasm` package (which is in turn built from the
Rust codebase and can be found in `~/automerge-wasm`). I.e. the responsibility
of this codebase is

- To map from the javascript data model to the underlying `set`, `make`,
  `insert`, and `delete` operations of Automerge.
- To expose a more convenient interface to functions in `automerge-wasm` which
  generate messages to send over the network or compressed file formats to store
  on disk

## Building and testing

Much of the functionality of this package depends on the
`@automerge/automerge-wasm` package and frequently you will be working on both
of them at the same time. It would be frustrating to have to push
`automerge-wasm` to NPM every time you want to test a change but I (Alex) also
don't trust `yarn link` to do the right thing here. Therefore, the `./e2e`
folder contains a little yarn package which spins up a local NPM registry. See
`./e2e/README` for details. In brief though:

To build `automerge-wasm` and install it in the local `node_modules`

```bash
cd e2e && yarn install && yarn run e2e buildjs
```

NOw that you've done this you can run the tests

```bash
yarn test
```

If you make changes to the `automerge-wasm` package you will need to re-run
`yarn e2e buildjs`
