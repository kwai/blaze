# blaze-v4.0.1:

# New Feature

* Initial supports to ORC input file format.
* Initial supports to RSS framework and Apache Celeborn shuffle service.

# Improvement

* Optimize AggExec by supporting Implement columnar-based aggregation.
* Use custom implemented hashmap implement for aggregation.
* Supports specialized count(0).
* Optimize bloom filter by reusing same bloom filter in the same executor.
* Optimize bloom filter by supporting shrinking.
* Optimize reading parquet files by supporting parallel reading.
* Improve spill file deletion logics.

# Bug fixes

* Fix file not found for path with url encoded character.
* Fix Hashaggregate convert job throwing ScalaReflectionException.
* Fix pruning error while reading parquet files with multiple row groups.
* Fix incorrect number of tasks due to missing shuffleOrigin.
* Fix record batch creating error when hash joining with empty input.

# Other
* Upgrade datafusion/arrow dependency to v42/v53.
* Replace gxhash with foldhash for better compatibility on some hardwares.
* Other minor improvement & fixes.

# PRs
* AggExec: implement columnar accumulator states. by @richox in https://github.com/kwai/blaze/pull/646
* Bump bigdecimal from 0.4.5 to 0.4.6 by @dependabot in https://github.com/kwai/blaze/pull/638
* Bump bytes from 1.7.2 to 1.8.0 by @dependabot in https://github.com/kwai/blaze/pull/625
* Bump bytes from 1.8.0 to 1.9.0 by @dependabot in https://github.com/kwai/blaze/pull/671
* Bump object_store from 0.11.0 to 0.11.1 by @dependabot in https://github.com/kwai/blaze/pull/622
* Bump sonic-rs from 0.3.13 to 0.3.14 by @dependabot in https://github.com/kwai/blaze/pull/623
* Bump sonic-rs from 0.3.14 to 0.3.16 by @dependabot in https://github.com/kwai/blaze/pull/647
* Bump tempfile from 3.13.0 to 3.14.0 by @dependabot in https://github.com/kwai/blaze/pull/641
* Bump tokio from 1.40.0 to 1.41.0 by @dependabot in https://github.com/kwai/blaze/pull/629
* Bump tokio from 1.41.0 to 1.41.1 by @dependabot in https://github.com/kwai/blaze/pull/642
* Bump tokio from 1.41.0 to 1.41.1 by @dependabot in https://github.com/kwai/blaze/pull/676
* Bump uuid from 1.10.0 to 1.11.0 by @dependabot in https://github.com/kwai/blaze/pull/618
* Create RecordBatch with num_rows option to avoid bhj error caused by empty output_schema by @wForget in https://github.com/kwai/blaze/pull/683
* Fix build on windows by @wForget in https://github.com/kwai/blaze/pull/666
* Fix file not found for path with url encoded character by @wForget in https://github.com/kwai/blaze/pull/679
* Followup to #674, add -r for rm by @wForget in https://github.com/kwai/blaze/pull/681
* Introduce base blaze sql test suite by @wForget in https://github.com/kwai/blaze/pull/674
* [BLAZE-287][FOLLOWUP] Use JavaUtils#newConcurrentHashMap to speed up ConcurrentHashMap#computeIfAbsent by @SteNicholas in https://github.com/kwai/blaze/pull/615
* [BLAZE-573][FOLLOWUP] Bump Spark from 3.4.3 to 3.4.4 by @SteNicholas in https://github.com/kwai/blaze/pull/640
* [BLAZE-627] Make ORC and Parquet format detection more generic by @dixingxing0 in https://github.com/kwai/blaze/pull/628
* [BLAZE-664] Bump Celeborn version from 0.5.1 to 0.5.2 by @SteNicholas in https://github.com/kwai/blaze/pull/665
* [MINOR] Avoid NPE when native lib is not found by @wForget in https://github.com/kwai/blaze/pull/668
* add new blaze logo by @richox in https://github.com/kwai/blaze/pull/633
* chore: Make spotless plugin happy by @zuston in https://github.com/kwai/blaze/pull/653
* code refactoring by @richox in https://github.com/kwai/blaze/pull/658
* code refactoring by @richox in https://github.com/kwai/blaze/pull/677
* doc: update tpc-h benchmark result by @richox in https://github.com/kwai/blaze/pull/614
* fix Hashaggregate convert job throw ScalaReflectionException by @leizhang5s in https://github.com/kwai/blaze/pull/637
* fix pruning error while reading parquet files with multiple row groups by @richox in https://github.com/kwai/blaze/pull/616
* fix running error for Spark 3.2.0 and 3.2.1 by @XorSum in https://github.com/kwai/blaze/pull/602
* fix(shuffle): Progagate shuffle origin to native exchange exec to make AQE rebalance valid by @zuston in https://github.com/kwai/blaze/pull/663
* fix(spill): Delete spill file when dropping for rust FileSpill by @zuston in https://github.com/kwai/blaze/pull/660
* fix(spill): Explicitly delete spill file for FileBasedSpillBuf after release by @zuston in https://github.com/kwai/blaze/pull/654
* improve NativeOrcScan by @richox in https://github.com/kwai/blaze/pull/631
* improve memory management by @richox in https://github.com/kwai/blaze/pull/621
* improvement: Add numOfPartitions metrics for exchange exec to align with vanilla spark by @zuston in https://github.com/kwai/blaze/pull/669
* optimize bloom filter by @richox in https://github.com/kwai/blaze/pull/620
* parquet reading improvements by @richox in https://github.com/kwai/blaze/pull/650
* release version v4.0.0 by @richox in https://github.com/kwai/blaze/pull/613
* replace gxhash with foldhash by @richox in https://github.com/kwai/blaze/pull/624
* supports specialized count(0) by @richox in https://github.com/kwai/blaze/pull/619
* tpcd benchmarkrunner : add orc format support by @leizhang5s in https://github.com/kwai/blaze/pull/639
* update to datafusion-v42 by @richox in https://github.com/kwai/blaze/pull/574
* use custom implemented hashmap for aggregation by @richox in https://github.com/kwai/blaze/pull/617
