# blaze-v2.0.9

## Features
* Upgrades datafusion/arrow dependency version to v36/v50.
* Supports max/min aggregation with complex types.
* Supports json_tuple.
* Introduce sonic-rs for json parsing.
* Add stage id in operator metrics.
* Implements writing table with dynamic partitions (not tested in spark303/spark333).

## Performance
* Improves batch serialization format and reduce compressed size.
* Implements radix-based k-way merging used in shuffling and aggregating.
* Improves performance of on-heap spilling.
* Improves performance of SortExec.
* Improves performance of AggExec.
* Improves performance of collect_set/collect_list.

## Bugfix
* Fix concat_ws with empty batches.
* Fix spark333 RenameExec incorrect ordering expressions.
* Fix incorrect join type mapping in BroadcastNestedLoopJoin.
* Fix decimal dividing with zero.
