# Reason

The initial reason I created this module was that all existing HDFS rust bindings are based on libhdfs,
which is a C/C++ interface based on HDFS's Java client implementation through JNI.
I was expecting its poor performance as well as the clumsy kernel 
(calling back to Java which deviates the initial goal of the whole project for using system language for better performance).

Through the way of exploration, I found [libhdfs3](https://github.com/ClickHouse-Extras/libhdfs3) and 
[libhdfspp](https://github.com/apache/hadoop/tree/trunk/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfspp),
the later one is merged into the Hadoop trunk since Hadoop3.2.x. 

Besides the time struggling with libhdfs3 compilation and installation on OSX, I also tried three available Rust-Cpp interop crates:
bindgen, cxx and autocxx, made them work, and autocxx seems to be the handiest one to use if we plan to make another HDFS wrapper
over libhdfspp or libhdfs3 by our own.

Until I came across the [article](https://wesmckinney.com/blog/python-hdfs-interfaces/) written by Wes McKinney,
the benchmark results show that for python HDFS bindings, the implementation based on libhdfs is the fastest one.

> libhdfs, despite being Java and JNI-based, achieves the best throughput in this test.
> libhdfs3 performs poorly in small size reads. This may be due to some RPC latency or configuration issues that I am not aware of.
 
Therefore, I decided to not spend time on this wrapper now since the existing implementation [hdfs-rs](https://github.com/hyunsik/hdfs-rs) and
[rust-hdfs](https://github.com/frqc/rust-hdfs) are both based on libhdfs. And revisit this part when the time spending on HDFS IO is indispensable again.
The rewritten / revisit might not be the problem since the current observable HDFS lag/slow is not only the client's fault.
