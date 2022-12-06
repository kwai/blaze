#!/bin/bash

set -x

cd "$(dirname "$0")" && ./gradlew build --no-daemon -Pmode=release-lto $@
