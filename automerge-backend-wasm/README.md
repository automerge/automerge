## automerge-backend-wasm

This is a npm wrapper for the rust implementation of [automerge-backend](https://github.com/automerge/automerge-rs/tree/master/automerge-backend).
It is currently experimental and in development against the Automerge performance branch.  

### building

Make sure you have the latest rust compiler installed  (1.42.0 or later).

```sh
cargo install wasm-pack
yarn install
```

Then build the debug version with

```sh
yarn build
```
or the release build with 

```sh
yarn release
```

