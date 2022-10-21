#!/bin/bash

cd "$(dirname "$0")"
./gradlew build --offline --no-daemon -Pmode=release-lto
