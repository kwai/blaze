Run TPC-DS benchmark with Spark/Blaze
===

# 1. Generate TPC-DS dataset

Compile datagen tool (derived from [maropu/spark-tpcds-datagen](https://github.com/maropu/spark-tpcds-datagen)).
```bash
cd tpcds/datagen
mvn package -DskipTests
```

Generate 1TB dataset with spark.
```bash
# use correct SPARK_HOME and output data location
# --use-double-for-decimal and --use-string-for-char are optional, see dsdgen usage

SPARK_HOME=$HOME/software/spark ./bin/dsdgen \
    --output-location /user/hive/data/tpcds-1000 \
    --scale-factor 1000 \
    --format parquet \
    --overwrite \
    --use-double-for-decimal \
    --use-string-for-char
```

# 2. Run benchmark

Compile benchmark tool (derived from [databricks/spark-sql-perf](https://github.com/databricks/spark-sql-perf)).
```bash
cd tpcds/benchmark-runner
mvn package -DskipTests
```

Edit your `$SPARK_HOME/conf/spark-default.conf` to enable/disable Blaze (see the following conf), then launch benchmark runner.
If benchmarking with Blaze, ensure that the Blaze jar package is correctly built and moved into `$SPARK_HOME/jars`. ([How to build Blaze?](https://github.com/kwai/blaze/#build-from-source))
```bash
# use correct SPARK_HOME and data location
SPARK_HOME=$HOME/software/spark ./bin/run \
    --data-location /user/hive/data/tpcds-1000 \
    --format parquet \
    --output-dir ./benchmark-result
```

Monitor benchmark status:
```bash
tail -f ./benchmark-result/YYYYMMDDHHmm/log
```

Summarize query times of all cases:
```bash
./bin/stat ./benchmark-result/YYYYMMDDHHmm
```

# Additional: `spark-default.conf` used in our benchmark

```bash
spark.master yarn
spark.yarn.stagingDir.list hdfs://blaze-test/home/spark/user/

spark.eventLog.enabled true
spark.eventLog.dir hdfs://blaze-test/home/spark-eventlog
spark.history.fs.logDirectory hdfs://blaze-test/home/spark-eventlog

spark.externalBlockStore.url.list hdfs://blaze-test/home/platform
spark.driver.extraJavaOptions -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/media/disk1/spark/ -Djava.io.tmpdir=/media/disk1/tmp -Dlog4j2.formatMsgNoLookups=true
spark.local.dir /media/disk1/spark/localdir

spark.shuffle.service.enabled true
spark.shuffle.service.port 7337

spark.driver.memory 20g
spark.driver.memoryOverhead 4096

spark.executor.instances 10000
spark.dynamicallocation.maxExecutors 10000
spark.executor.cores 5

spark.io.compression.codec zstd
spark.sql.parquet.compression.codec zstd

# benchmark without blaze
#spark.executor.memory 6g
#spark.executor.memoryOverhead 2048

# benchmark with blaze
spark.executor.memory 4g
spark.executor.memoryOverhead 4096
spark.blaze.enable true
spark.sql.extensions org.apache.spark.sql.blaze.BlazeSparkSessionExtension
spark.shuffle.manager org.apache.spark.sql.execution.blaze.shuffle.BlazeShuffleManager
spark.memory.offHeap.enabled false

# spark3.3+ disable char/varchar padding
#spark.sql.readSideCharPadding false

# enable shuffled hash join
#spark.sql.join.preferSortMergeJoin false
```
