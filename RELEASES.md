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

## Dependency updates
16ee61f9abc85f18ccac799afb24b05fe0177612 Bump hashbrown from 0.14.0 to 0.14.2 (#306)
aed3a312b7714f51c076e032fff600c6a56ecf2a Bump lz4_flex from 0.10.0 to 0.11.1 (#299)
c45b6fc8489e025c266ef88014d601125b0249f4 Bump base64 from 0.21.4 to 0.21.5 (#300)
a0e435bea4e196cc0d66ffbf08512b3ada13d756 Bump postcard from 1.0.7 to 1.0.8 (#301)
6a1437643e5fa382f87bd42278303cb80905fe18 Bump async-trait from 0.1.73 to 0.1.74 (#303)
9cf8c3953e0fff335e58915a997412e122ac9ea2 Bump hashbrown from 0.13.2 to 0.14.0 (#304)

## Source code changes
d05d3c5dc1984516ca346efac004f81f8bc50ec5 fix get_json_value.evaluate_with_value() error
ff0f4360b86da03073a6d8c5d6a286522b659fb5 fix missing outputPartitioning in NativeParquetExec
56b571001679252b76a2db3803b81d38d1924743 supports input batch statistics (#286)
1fb9020016e0bb5e3d753449f2f7ec27c0eb014f supports spark.files.ignoreCorruptFiles (#285)
3440789f94d9f6665b802736f9448c837a27a14c supports native broadcast nested loop join (#282)
1ccc18bfa165988e989d5950165e098497d36600 fix incorrect input array evaluation (#283)
bdd4733bc529eeab05c9294d4955f43d487a9d89 evaluate aggregate exprs with cached evaluator (#281)
e72d7f9ebdbb094f6b56f7b09ab66a368f579e28 split get_json_object into parse_json+get_parsed_json_object for better expr caching (#280)
9ff196766835e56d5b4fc08fd85de9ca8c9f1261 prefer native sort even if child is non-native
78f8b89f1b569ca7cb535ad611dc5830a5dd0cce supports multithread UDF execution
92ad6ec717eaee094d5fb5d956ef9cf90fd50d86 remove unused imports
1987482b60daf2848f511928e2c57214b993c599 fix: avoid closing hadoop fs for reusing in cache (#273)
225372db605ef2e4d72b2a50fc45e8965ad0945a fix missing native converting checks in parquet scan
97aa5369e22218470b67843dffc20be54c6ae419 chore: remove the unused package for the scalafix plugin (#271)
27d0270eabc2ef5da532f1d4d2c1ac2ef73555c2 implement spark-compatible float to int casting (#265)
d4d2736e7f66e7fe6b118174a3616c1751f5f962 supports column pruning during native evaluation (#262)
fb502899bd120470552e9770c6f1edbf6bcfa29e remove unused val in BlazeSparkSessionExtension (#261)
