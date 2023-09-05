#!/usr/bin/env bash

set -e

cd "$(dirname "$0")"
profile="$1"

if [ "$(uname)" == "Darwin" ]; then
    libsuffix=dylib
elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
    libsuffix=so
else
    echo "Unsupported platform $(uname)"
    exit 1
fi

checksum() {
    find Cargo.toml Cargo.lock native-engine target/$profile/libblaze."$libsuffix" | \
        xargs md5sum 2>&1 | \
        sort -k1 | \
        md5sum
}

checksum_cache_file="./.build-checksum_$profile-"$libsuffix".cache"
old_checksum="$(cat "$checksum_cache_file" 2>&1 || true)"
new_checksum="$(checksum)"

echo "old build-checksum: $old_checksum"
echo "new build-checksum: $new_checksum"

if [ "$new_checksum" != "$old_checksum" ]; then
    echo "Running cargo fix..."
    cargo fix --all --allow-dirty --allow-staged

    echo "Running cargo fmt..."
    cargo fmt --all -q --

    echo "Building native with [$profile] profile..."
    cargo build --profile="$profile" --verbose --locked --frozen
else
    echo "native-engine source code and built libraries not modified, no need to rebuild"
fi

mkdir -p dev/mvn-build-helper/assembly/target/classes
rm -f dev/mvn-build-helper/assembly/target/classes/libblaze.{dylib,so}
cp target/$profile/libblaze."$libsuffix" dev/mvn-build-helper/assembly/target/classes

new_checksum="$(checksum)"
echo "build-checksum updated: $new_checksum"
echo "$new_checksum" >"$checksum_cache_file"

echo "Finished native building"
