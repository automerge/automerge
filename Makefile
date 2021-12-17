rust:
	cd automerge && cargo test

wasm:
	cd automerge-wasm && yarn
	cd automerge-wasm && yarn build
	cd automerge-wasm && yarn test
	cd automerge-wasm && yarn link

js: wasm
	cd automerge-js && yarn
	cd automerge-js && yarn link "automerge-wasm"
	cd automerge-js && yarn test
