#!/bin/bash

export SHIM="${SHIM:-spark-3.0}"
export MODE="${MODE:-release}"

docker-compose -f dev/docker-build/docker-compose.yml up
