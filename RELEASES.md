# blaze-v2.0.8

## Features
* Enables nested complex data types by default.
* Supports writing parquet table with dynamic partitions.
* Supports partial aggregate skipping.
* Enable first() aggregate function converting.
* Add spill metrics.
*
## Performance
* Implement batch updating/merging in aggregates.
* Use slim box for storing bytes.
* get_json_object use Cow to avoid copying.
* Reduce the probability of unexpected off-heap memory overflows.
* Introduce multiway merge sort to SortExec and SortRepartitioner.
* SortExec removes redundant columns from batch.
* Implement loser tree with inlined comparable traits.
* Use unchecked index in LoserTree to get slightly performance improvement.
* Remove BucketRepartitioner.
* Reduce number of awaits in sort-merge join.
* Pre-merge records in sorting mode if cardinality is low.
* Use gxhash as default hasher in AggExec.
* Optimize collect_set/collect_list function with SmallVec.
* Implement async ipc reader.

## Bugfix
* Fix buggy GetArrayItem/GetMapValue native converter pattern matching.
* Fix parquet pruning with NaN values.
* Fix map type conversion with incorrect nullable value.
* Fix ffi-export error in some cases.
* Fix incorrect behavior of get_index_field with incorrect number of rows.
* Fix task hanging in some cases with ffi-export.
