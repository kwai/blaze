#!/bin/bash

export SHIM="${SHIM:-spark-3.0}"
export MODE="${MODE:-release}"
export JAVA_VERSION="${JAVA_VERSION:-8}"
export SCALA_VERSION="${SCALA_VERSION:-2.12}"

docker-compose -f dev/docker-build/docker-compose.yml up
