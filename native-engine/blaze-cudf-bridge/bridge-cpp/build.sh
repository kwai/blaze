#!/bin/bash

set -x
set -e

ROOT_DIR="$(cd `dirname "$0"` && pwd)"

if [ ! -d "$VCPKG_ROOT" ]; then
    echo "invalid VCPKG_ROOT: $VCPKG_ROOT" >&2
    exit -1
fi

cmake --preset=vcpkg \
  -DCMAKE_BUILD_TYPE=Release \
  -DVCPKG_BUILD_TYPE=release \
  -DVCPKG_OVERLAY_PORTS=./vcpkg_ports \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=1

cmake --build build
