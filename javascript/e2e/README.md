#End to end testing for javascript packaging

The network of packages and bundlers we rely on to get the `automerge` package
working is a little complex. We have the `automerge-wasm` package, which the
`automerge` package depends upon, which means that anyone who depends on
`automerge` needs to either a) be using node or b) use a bundler in order to
load the underlying WASM module which is packaged in `automerge-wasm`.

The various bundlers involved are complicated and capricious and so we need an
easy way of testing that everything is in fact working as expected. To do this
we run a custom NPM registry (namely [Verdaccio](https://verdaccio.org/)) and
build the `automerge-wasm` and `automerge` packages and publish them to this
registry. Once we have this registry running we are able to build the example
projects which depend on these packages and check that everything works as
expected.

## Usage

First, install everything:

```
yarn install
```

### Build `automerge-js`

This builds the `automerge-wasm` package and then runs `yarn build` in the
`automerge-js` project with the `--registry` set to the verdaccio registry. The
end result is that you can run `yarn test` in the resulting `automerge-js`
directory in order to run tests against the current `automerge-wasm`.

```
yarn e2e buildjs
```

### Build examples

This either builds or the examples in `automerge-js/examples` or just a subset
of them. Once this is complete you can run the relevant scripts (e.g. `vite dev`
for the Vite example) to check everything works.

```
yarn e2e buildexamples
```

Or, to just build the webpack example

```
yarn e2e buildexamples -e webpack
```

### Run Registry

If you're experimenting with a project which is not in the `examples` folder
you'll need a running registry. `run-registry` builds and publishes
`automerge-js` and `automerge-wasm` and then runs the registry at
`localhost:4873`.

```
yarn e2e run-registry
```

You can now run `yarn install --registry http://localhost:4873` to experiment
with the built packages.

## Using the `dev` build of `automerge-wasm`

All the commands above take a `-p` flag which can be either `release` or
`debug`. The `debug` builds with additional debug symbols which makes errors
less cryptic.
