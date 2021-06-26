#! /usr/bin/python3

import msgpack
import os

DIR = "./examples/example-data"
for path in os.listdir(DIR):
    fpath = f"{DIR}/{path}"
    if fpath.endswith(".json"):
        continue
    bs = None
    with open(fpath, "rb") as f:
        bs = f.read()
    print(f"Reading: {fpath}")
    try:
        obj = msgpack.unpackb(bs)
        #print(obj)
    except Exception as e:
        print(f"File: {fpath} had error: {e}")
