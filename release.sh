#!/bin/bash

cd "$(dirname "$0")"
./gradlew  --offline build --no-daemon -Pmode=release-lto
