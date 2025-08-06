#!/usr/bin/env bash

set -e

# Preserve the calling directory
_CALLING_DIR="$(pwd)"

PROJECT_DIR="$(cd "`dirname "$0"`/../.."; pwd)"
cd "$PROJECT_DIR"

profile="$1"
features_arg=""
if [ -n "$2" ]; then
    features_arg="--features $2"
fi

libname=libblaze
if [ "$(uname)" == "Darwin" ]; then
    libsuffix=dylib
elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
    libsuffix=so
elif [ "$(expr substr $(uname -s) 1 10)" == "MINGW64_NT" ]; then
    libname=blaze
    libsuffix=dll
else
    echo "Unsupported platform $(uname)"
    exit 1
fi

libpath="target/$profile/$libname.$libsuffix"

checksum() {
    # Determine whether to use md5sum or md5
    if command -v md5sum >/dev/null 2>&1; then
        hash_cmd="md5sum"
    elif command -v md5 >/dev/null 2>&1; then
        hash_cmd="md5 -r" # Use md5 -r for macOS to match md5sum format
    else
        echo "Neither md5sum nor md5 is available."
        exit 1
    fi

    echo "$features_arg" | $hash_cmd | awk '{print $1}'

    find Cargo.toml Cargo.lock native-engine "$libpath" | \
        xargs $hash_cmd 2>&1 | \
        sort -k1 | \
        $hash_cmd
}

checksum_cache_file="./.build-checksum_$profile-"$libsuffix".cache"
if [ -f "$libpath" ]; then
  old_checksum="$(cat "$checksum_cache_file" 2>&1 || true)"
  new_checksum="$(checksum)"

  echo -e "old build-checksum: \n$old_checksum\n========"
  echo -e "new build-checksum: \n$new_checksum\n========"
fi

if [ ! -f "$libpath" ] || [ "$new_checksum" != "$old_checksum" ]; then
    export RUSTFLAGS=${RUSTFLAGS:-"-C target-cpu=native"}
    echo "Running cargo fix..."
    cargo fix --all --allow-dirty --allow-staged --allow-no-vcs  2>&1

    echo "Running cargo fmt..."
    cargo fmt --all -q -- 2>&1

    echo "Building native with [$profile] profile..."
    cargo build --profile="$profile" $features_arg --verbose --locked --frozen 2>&1
else
    echo "native-engine source code and built libraries not modified, no need to rebuild"
fi

mkdir -p native-engine/_build/$profile
rm -rf native-engine/_build/$profile/*
cp "$libpath" native-engine/_build/$profile

new_checksum="$(checksum)"
echo "build-checksum updated: $new_checksum"
echo "$new_checksum" >"$checksum_cache_file"

echo "Finished native building"

cd "${_CALLING_DIR}"
