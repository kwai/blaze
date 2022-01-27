<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Blaze

[![test](https://github.com/blaze-init/blaze-rs/actions/workflows/rust.yml/badge.svg)](https://github.com/blaze-init/blaze-rs/actions/workflows/Build.yml)
[![codecov](https://codecov.io/gh/blaze-init/blaze-rs/branch/master/graph/badge.svg?token=T0J6196MLB)](https://codecov.io/gh/blaze-init/blaze-rs)

The blaze-rs project aims to provide Spark with a high-performance, low-cost native execution layer.

We seek to solve a series of performance bottlenecks in the current JVM-based Task execution of Spark SQL,
such as high fluctuations in performance due to GC, high memory overhead, and inability to accelerate computation directly with SIMD instructions.

This repo is under active development and is not ready for production (or even development) use, but stay tuned for updates! ☺️


## Overview


## For developers

- [Architectural Overview](./dev/doc/architectural_overview.md)
