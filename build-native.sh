#!/usr/bin/env bash

profile="$1"

echo "Building native with [$profile] profile..."
cargo +nightly build --features=mm --profile="$profile"

rt=$?
if [[ "$rt" != 0 ]]; then
  echo "Cargo build failed, aborting...."
  exit $rt
fi
rm -rf `pwd`/../lib/*
if [ "$(uname)" == "Darwin" ]; then
  mkdir -p `pwd`/../lib/ && cp `pwd`/../target/$profile/*.dylib `pwd`/../lib/
elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
  mkdir -p `pwd`/../lib/ && cp `pwd`/../target/$profile/*.so `pwd`/../lib/
else
    echo "Unsupported platform $(uname)"
    exit 1
fi
echo "Finished native building"
