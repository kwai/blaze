testenv
===

Setup an environment for testing spark-auron-extension

### step-1: download and install hadoop/hive/spark

run `_software/download_and_install.sh`

### step-2: initialize and configure environment

run `_bin/init-env.sh`

### step-3: startup environment

run `_bin/run.sh start`

### step-4: try to execute a query with spark-sql

run `source _bin/env.sh`

run `spark-sql -f _workspace/init_bigint_1m.q`

### step-5: try to execute a query with spark-sql + auron extension

edit ./spark-sql-auronPlugin and set the correct path spark-auron-extension and auron-rs.

run `./spark-sql-auronPlugin -f _workspace/shuffle.q` for testing.
