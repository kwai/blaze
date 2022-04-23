#!/bin/bash

set -e
set -x

cd "$(dirname "$0")"

(mkdir -p archive && cd archive && (
    wget -c "https://mirror.iscas.ac.cn/apache/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz"
    wget -c "https://mirror.iscas.ac.cn/apache/hadoop/core/hadoop-2.10.1/hadoop-2.10.1.tar.gz"
    wget -c "https://mirror.iscas.ac.cn/apache/spark/spark-3.0.3/spark-3.0.3-bin-hadoop2.7.tgz"
))

(rm -rf hadoop && mkdir -p hadoop && cd hadoop &&
    tar -xf "../archive/hadoop-2.10.1.tar.gz" --strip-component=1)
(rm -rf hive && mkdir -p hive && cd hive &&
    tar -xf "../archive/apache-hive-2.3.9-bin.tar.gz" --strip-component=1)
(rm -rf spark && mkdir -p spark && cd spark &&
    tar -xf "../archive/spark-3.0.3-bin-hadoop2.7.tgz" --strip-component=1)
