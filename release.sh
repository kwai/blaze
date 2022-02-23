#!/usr/bin/env bash

RUSTFLAGS='-C target-cpu=native' cargo +nightly run --release
