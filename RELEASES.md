# blaze-v2.0.9.1

## Features
* Supports failing-back nondeterministic expressions.
* Supports "$[].xxx" jsonpath syntax in get_json_object().

## Performance
* Supports adaptive batch size in ParquetScan, improving vectorized reading performance.
* Supports directly spill to disk file when on-heap memory is full.

## Bugfix
* Fix incorrect parquet rowgroup pruning with files containing deprecated min/max values.
