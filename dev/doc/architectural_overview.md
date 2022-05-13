# Operator-based native execution

Principles:

- Execution offloading at operator basis.
- Avoid Java <-> Rust interoperation if possible, especially for data transfers.
  - Shuffle using Segmented Arrow-IPC format for both JVM-stage and Native-stage.


## Shuffle File Format

- Two shuffle files for each task's shuffle write: one data file (of `.data` postfix) and one index file (of `.index` postfix).
- Task's shuffle write file of file name `shuffle_${shuffle_id}_${map_id}_0`.

### Data file

The data file is a per-partition concatenated file; each partition segment comprises 0 or more `part`s.

For each `part`, it's represented as an arrow-IPC file format, followed by a little-endian int64 denoting IPC-file length.

### Index file

One starting offsets for each partition, regardless of whether it contains any data. One extra offset at the end denotes the last partition end offset.

![Segmented IPC format](./segmented-ipc-format.svg)
