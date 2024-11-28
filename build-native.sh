#!/usr/bin/env bash

set -e

cd "$(dirname "$0")"
profile="$1"

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

    find Cargo.toml Cargo.lock native-engine target/$profile/$libname."$libsuffix" | \
        xargs $hash_cmd 2>&1 | \
        sort -k1 | \
        $hash_cmd
}

checksum_cache_file="./.build-checksum_$profile-"$libsuffix".cache"
old_checksum="$(cat "$checksum_cache_file" 2>&1 || true)"
new_checksum="$(checksum)"

echo "old build-checksum: $old_checksum"
echo "new build-checksum: $new_checksum"

if [ "$new_checksum" != "$old_checksum" ]; then
    export RUSTFLAGS=${RUSTFLAGS:-"-C target-cpu=native"}
    echo "Running cargo fix..."
    cargo fix --all --allow-dirty --allow-staged --allow-no-vcs

    echo "Running cargo fmt..."
    cargo fmt --all -q --

    echo "Building native with [$profile] profile..."
    cargo build --profile="$profile" --verbose --locked --frozen
else
    echo "native-engine source code and built libraries not modified, no need to rebuild"
fi

mkdir -p dev/mvn-build-helper/assembly/target/classes
rm -f dev/mvn-build-helper/assembly/target/classes/$libname.{dylib,so,dll}
cp target/$profile/$libname."$libsuffix" dev/mvn-build-helper/assembly/target/classes

new_checksum="$(checksum)"
echo "build-checksum updated: $new_checksum"
echo "$new_checksum" >"$checksum_cache_file"

echo "Finished native building"
