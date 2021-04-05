#! /usr/bin/env bash

if [[ -e "../automerge" ]]
then
    echo "Automerge repo exists. Skipping cloning..."
else
    echo "Cloning automerge repo to ../../automerge"
    git clone https://github.com/automerge/automerge.git ../../automerge
fi

ORIGINAL_PWD=$PWD
cd ../../automerge
yarn install
env WASM_BACKEND_PATH="$ORIGINAL_PWD/build" yarn testwasm
