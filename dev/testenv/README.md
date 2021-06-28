testenv
===

Setup an environment for testing spark-blaze-extension

### step-1: download and install hadoop/hive/spark

run `_software/download_and_install.sh`

### step-2: initialize and configure environment

run `_bin/init-env.sh`

### step-3: startup environment

run `_bin/run.sh start`

### step-4: try to execute a query with spark-sql

run `source _bin/env.sh`

run `spark-sql -f _workspace/init_bigint_1m.q`

### step-5: try to execute a query with spark-sql + blaze extension

edit ./spark-sql-blazePlugin and set the correct path spark-blaze-extension and blaze-rs.

run `./spark-sql-blazePlugin -f _workspace/shuffle.q` for testing.
