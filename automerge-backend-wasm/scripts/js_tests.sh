#! /usr/bin/env bash

if [[ -e "../../automerge" ]]
then
    echo "Automerge repo exists. Skipping cloning..."
else
    echo "Cloning automerge repo to ../../automerge"
    git clone https://github.com/automerge/automerge.git ../../automerge
fi

ORIGINAL_PWD=$PWD
cd ../../automerge
git checkout 81079ff75d2234b47cb912bad728158f2e71c527
yarn install
env WASM_BACKEND_PATH="$ORIGINAL_PWD/build" yarn testwasm
