<!--
- Licensed to the Apache Software Foundation (ASF) under one or more
- contributor license agreements.  See the NOTICE file distributed with
- this work for additional information regarding copyright ownership.
- The ASF licenses this file to You under the Apache License, Version 2.0
- (the "License"); you may not use this file except in compliance with
- the License.  You may obtain a copy of the License at
-
-   http://www.apache.org/licenses/LICENSE-2.0
-
- Unless required by applicable law or agreed to in writing, software
- distributed under the License is distributed on an "AS IS" BASIS,
- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
- See the License for the specific language governing permissions and
- limitations under the License.
-->

# AURON

[![TPC-DS](https://github.com/kwai/blaze/actions/workflows/tpcds.yml/badge.svg?branch=master)](https://github.com/kwai/blaze/actions/workflows/tpcds.yml)
[![master-ce7-builds](https://github.com/kwai/blaze/actions/workflows/build-ce7-releases.yml/badge.svg?branch=master)](https://github.com/kwai/blaze/actions/workflows/build-ce7-releases.yml)

<p align="center"><img src="./dev/auron-logo.png" /></p>

The Auron accelerator for big data engine (e.g., Spark, Flink) leverages native vectorized execution to accelerate query processing. It combines
the power of the [Apache DataFusion](https://arrow.apache.org/datafusion/) library and the scale of the distributed
computing framework.

Auron takes a fully optimized physical plan from distributed computing framework, mapping it into DataFusion's execution plan, and performs native
plan computation.

The key capabilities of Auron include:

- **Native execution**:  Implemented in Rust, eliminating JVM overhead and enabling predictable performance.
- **Vectorized computation**: Built on Apache Arrow's columnar format, fully leveraging SIMD instructions for batch processing.
- **Pluggable architecture:**: Seamlessly integrates with Apache Spark while designed for future extensibility to other engines.
- **Production-hardened optimizations:** Multi-level memory management, compacted shuffle formats, and adaptive execution strategies developed through large-scale deployment.

Based on the inherent well-defined extensibility of DataFusion, Auron can be easily extended to support:

- Various object stores.
- Operators.
- Simple and Aggregate functions.
- File formats.

We encourage you to [extend DataFusion](https://github.com/apache/arrow-datafusion) capability directly and add the
supports in Auron with simple modifications in plan-serde and extension translation.

## Build from source

To build Auron, please follow the steps below:

1. Install Rust

The native execution lib is written in Rust. So you're required to install Rust (nightly) first for
compilation. We recommend you to use [rustup](https://rustup.rs/).

2. Install JDK

Auron has been well tested on jdk8/11/17.

3. Check out the source code.

4. Build the project.

use `./auron-build.sh` for building the project. execute `./auron-build.sh --help` for help.

After the build is finished, a fat Jar package that contains all the dependencies will be generated in the `target`
directory.

## Build with docker

You can use the following command to build a centos-7 compatible release:
```shell
SHIM=spark-3.3 MODE=release JAVA_VERSION=8 SCALA_VERSION=2.12 ./release-docker.sh
```

## Run Spark Job with Auron Accelerator

This section describes how to submit and configure a Spark Job with Auron support.

1. Move the Auron JAR to the Spark client classpath (normally spark-xx.xx.xx/jars/).

2. Add the following configs to spark configuration in `spark-xx.xx.xx/conf/spark-default.conf`:

```properties
spark.auron.enable true
spark.sql.extensions org.apache.spark.sql.auron.AuronSparkSessionExtension
spark.shuffle.manager org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager
spark.memory.offHeap.enabled false

# suggested executor memory configuration
spark.executor.memory 4g
spark.executor.memoryOverhead 4096
```

3. submit a query with spark-sql, or other tools like spark-thriftserver:
```shell
spark-sql -f tpcds/q01.sql
```

## Performance

TPC-DS 1TB Benchmark Results:

![tpcds-benchmark-echarts.png](./benchmark-results/tpcds-benchmark-echarts.png)

For methodology and additional results, please refer to [benchmark documentation](https://auron.apache.org/documents/benchmarks.html).

We also encourage you to benchmark Auron and share the results with us. 🤗

## Community

### Subscribe Mailing Lists

Mail List is the most recognized form of communication in the Apache community.
Contact us through the following mailing list.

| Name                                                       | Scope                           |                                                          |                                                               | 
|:-----------------------------------------------------------|:--------------------------------|:---------------------------------------------------------|:--------------------------------------------------------------|
| [dev@auron.apache.org](mailto:dev@auron.apache.org)  | Development-related discussions | [Subscribe](mailto:dev-subscribe@auron.apache.org)    | [Unsubscribe](mailto:dev-unsubscribe@auron.apache.org)     |


### Report Issues or Submit Pull Request

If you meet any questions, connect us and fix it by submitting a 🔗[Pull Request](https://github.com/apache/auron/pulls).

## License

Auron is licensed under the Apache 2.0 License. A copy of the license
[can be found here.](LICENSE.txt)
