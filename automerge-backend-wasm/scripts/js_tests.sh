#! /usr/bin/env bash
set -e

ORIGINAL_PWD=$PWD
if [[ -z $AUTOMERGE_JS_DIR ]]; then
    COMMIT_HASH=ee09eead68d572725b344bd7ab94bfa3d6d29889
    AUTOMERGE_JS_DIR="./automerge-js-temp"
    echo "'AUTOMERGE_JS_DIR' var not set. Using temporary dir: $AUTOMERGE_JS_DIR & commit hash: $COMMIT_HASH"
    if [[ -d $AUTOMERGE_JS_DIR ]]; then
        echo "Dir found, skipping clone"
        cd $AUTOMERGE_JS_DIR
        git fetch --all
        if ! git cat-file -e $COMMIT_HASH; then
            echo "Commit hash: $COMMIT_HASH not found in $AUTOMERGE_JS_DIR"
            exit 1
        fi
    else
        git clone https://github.com/automerge/automerge.git $AUTOMERGE_JS_DIR
    fi
    cd $ORIGINAL_PWD
    cd $AUTOMERGE_JS_DIR
    git checkout $COMMIT_HASH
else
    # if the env var is set, assume the user is using an existing checkout of automerge
    echo "Using $AUTOMERGE_JS_DIR"
    if [[ ! -d $AUTOMERGE_JS_DIR ]]; then
        echo "$AUTOMERGE_JS_DIR dir not found."
        exit 1
    fi
fi

cd $ORIGINAL_PWD
cd $AUTOMERGE_JS_DIR

WASM_BACKEND_PATH="$ORIGINAL_PWD/build"
if [[ ! -d $WASM_BACKEND_PATH ]]; then
    echo "$WASM_BACKEND_PATH does not exist. Run 'yarn dev' or 'yarn release' to build WASM backend"
    exit 1
fi
yarn install
WASM_BACKEND_PATH=$WASM_BACKEND_PATH yarn testwasm
