# blaze-v4.0.0

## New features
* supports spark3.0/3.1/3.2/3.3/3.4/3.5.
* supports integrating with Apache Celeborn.
* supports native ORC input format.
* supports bloom filter join introduced in spark 3.5.
* supports forceShuffledHashJoin for running tpch/tpcds benchmarks.
* new supported native expression/functions: year, month, day, md5.

## Bug fixes
* add missing UDTF.terminate() invokes.
* fix NPE while executing some native spark physical plans.

## Performance
* use custom implemented hash table for faster joining, supporting SIMD, bulk searching, memory prefetching, etc.
* improve shuffle write performance.
* reuse FSDataInputStream for same input file.
