#!/usr/bin/env bash

if [[ "$1" == "debug" ]]; then
  echo "Building native with debug mode..."
  cargo +nightly-2022-05-22 build
else
  echo "Building native with release mode..."
  cargo +nightly-2022-05-22 build --release --features=mm
fi
rt=$?
if [[ "$rt" != 0 ]]; then
  echo "Cargo build failed, aborting...."
  exit $rt
fi
rm -rf `pwd`/../lib/*
if [[ "$1" == "debug" ]]; then
  if [ "$(uname)" == "Darwin" ]; then
      mkdir -p `pwd`/../lib/ && cp `pwd`/../target/debug/*.dylib `pwd`/../lib/
  elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
      mkdir -p `pwd`/../lib/ && cp `pwd`/../target/debug/*.so `pwd`/../lib/
  fi
else
  if [ "$(uname)" == "Darwin" ]; then
      mkdir -p `pwd`/../lib/ && cp `pwd`/../target/release/*.dylib `pwd`/../lib/
  elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
      mkdir -p `pwd`/../lib/ && cp `pwd`/../target/release/*.so `pwd`/../lib/
  fi
fi
echo "Finished native building"
