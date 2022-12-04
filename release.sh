#!/bin/bash

cd "$(dirname "$0")"
./gradlew build --no-daemon -Pshim=spark303 -Pmode=release-lto
