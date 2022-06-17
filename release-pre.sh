#!/bin/bash

cd "$(dirname "$0")"
./gradlew build --no-daemon -Pmode=pre
