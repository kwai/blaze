# blaze-v3.0.0

## Features
* Supports using spark.io.compression.codec for shuffle/broadcast compression
* Supports date type casting
* Refactor join implementations to support existence joins and BHJ building hash map on driver side

## Performance
* Fixed performance issues when running on spark3 with default configurations
* Use cached parquet metadata
* Refactor native broadcast to avoid duplicated broadcast jobs
* Supports spark333 batch shuffle reading

## Bugfix
* Fix in_list conversion in from_proto.rs
