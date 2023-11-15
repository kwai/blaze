# blaze-v2.0.7

## Features
* Supports native BroadcastNestedLoopJoinExec.
* Supports multithread UDF evaluation.
* Supports spark.files.ignoreCorruptFiles.
* Supports input batch statistics.

## Performance
* Improves get_json_object() performance by reducing duplicated json parsing.
* Improves parquet reading performance by skipping utf-8 validation.
* Supports cached expression evaluator in native AggExec.
* Supports column pruning during native evaluation.
* Prefer native sort even if child is non-native.

## Bugfix
* Fix missing outputPartitioning in NativeParquetExec.
* Fix missing native converting checks in parquet scan.
* Fix inconsistency: implement spark-compatible float to int casting.
* Avoid closing hadoop fs for reusing in cache.
