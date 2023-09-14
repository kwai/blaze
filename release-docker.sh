#!/bin/bash

export SHIM="${SHIM:-spark303}"
export MODE="${MODE:-release}"

docker-compose -f dev/docker-build/docker-compose.yml up
