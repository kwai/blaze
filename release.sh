#!/bin/bash

set -x

cd "$(dirname "$0")" && ./gradlew build \
    --no-daemon \
    --parallel \
    --build-cache \
    --offline \
    --console=verbose \
    $@
