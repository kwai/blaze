#!/bin/bash

export SHIM="${SHIM:-spark241kwaiae}"
export MODE="${MODE:-release-lto}"

docker-compose -f dev/docker-build/docker-compose.yml up
