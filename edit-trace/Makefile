rust:
	cargo run --release

build-wasm: ../automerge-wasm ../automerge
	cd ../automerge-wasm && yarn
	cd ../automerge-wasm && yarn release

wasm: build-wasm
	node automerge-wasm.js

build-js: build-wasm
	cd ../automerge-js && yarn
	cd ../automerge-js && yarn link "automerge-wasm"

js: build-js
	node automerge-js.js

baseline:
	node baseline.js
