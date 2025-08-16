# TPC-H 1TB Benchmark

### Versions
- Auron version: [4.0.0](https://github.com/blaze-init/auron/tree/v4.0.0)
- Vanilla spark version: spark-3.5.1

### Environment
Hadoop 2.10.2 cluster mode running on 7 nodes, See [Kwai server conf](./kwai1-hardware-conf.md).
java version: 1.8.0_102.

### Configuration

Common configurations:
```conf
spark.master yarn
spark.shuffle.service.enabled true
spark.shuffle.service.port 7337

spark.driver.memory 20g
spark.driver.memoryOverhead 4096

spark.executor.instances 10000
spark.dynamicallocation.maxExecutors 10000

spark.io.compression.codec lz4
spark.sql.parquet.compression.codec zstd

# enabled in spark 3.5 by default
spark.sql.optimizer.runtime.bloomFilter.enabled true

# enable HashJoin for small tables, which is faster both in spark and auron
# note: SortMergeJoin is still used for joining big tables with this configuration enabled
spark.sql.join.preferSortMergeJoin false
```

Configurations for Vanillia spark:
```conf
spark.executor.memory 4g
spark.executor.memoryOverhead 2048
spark.executor.cores 5
```

Configurations for auron:

note: this configuration is widely used in production environment of Kuaishou.inc, without any tricky optimizations only for benchmark. (for example, you can set `spark.auron.forceShuffledHashJoin true` to force using HashJoin instead of SortMergeJoin and get much faster benchmark result, but this is unacceptable in production environment)

```conf
spark.executor.memory 3g
spark.executor.memoryOverhead 3072
spark.auron.enable true
spark.sql.extensions org.apache.spark.sql.auron.AuronSparkSessionExtension
spark.shuffle.manager org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager
```

### Benchmark result:
Auron shows 2.12x query time speed-up comparing with Spark 3.5, with the same CPU/memory resources.

![tpch-auron400-spark351.png](tpch-auron400-spark351.png)

|     | Spark    | Auron   | Speedup |
| --- | -------- | ------- | ------- |
| q01 | 40.473   | 19.834  | 2.04    |
| q02 | 20.527   | 11.639  | 1.76    |
| q03 | 69.091   | 31.199  | 2.21    |
| q04 | 59.58    | 16.585  | 3.59    |
| q05 | 100.958  | 52.267  | 1.93    |
| q06 | 26.713   | 7.928   | 3.37    |
| q07 | 64.729   | 28.175  | 2.30    |
| q08 | 64.465   | 35.043  | 1.84    |
| q09 | 103.011  | 53.203  | 1.94    |
| q10 | 46.543   | 21.805  | 2.13    |
| q11 | 16.458   | 8.561   | 1.92    |
| q12 | 26.626   | 13.784  | 1.93    |
| q13 | 53.072   | 15.445  | 3.44    |
| q14 | 31.561   | 9.279   | 3.40    |
| q15 | 59.57    | 19.212  | 3.10    |
| q16 | 14.533   | 5.944   | 2.44    |
| q17 | 141.243  | 54.49   | 2.59    |
| q18 | 129.022  | 79.808  | 1.62    |
| q19 | 19.561   | 10.149  | 1.93    |
| q20 | 42.451   | 15.934  | 2.66    |
| q21 | 177.553  | 107.276 | 1.66    |
| q22 | 17.429   | 8.244   | 2.11    |
|     |          |         |         |
| sum | 1325.169 | 625.804 | 2.12    |
