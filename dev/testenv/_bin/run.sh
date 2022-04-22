#!/bin/bash

WORKSPACE_DIR="$(cd "$(dirname "$0")/../" && pwd)"
source "$WORKSPACE_DIR"/_bin/env.sh

export HADOOP_OPTS="-Xmx4096m"
export YARN_OPTS="-Xmx4096m"

YARN_NODEMANAGER1_OPTS="
    -Dmy.hadoop.tmp.dir=${WORKSPACE_DIR}/_data/nodemanager-data1
    -Dmy.yarn.nodemanager.localizer.address=0.0.0.0:8040
    -Dmy.yarn.nodemanager.address=0.0.0.0:8041
    -Dmy.yarn.nodemanager.webapp.address=0.0.0.0:8042
    -Dmy.mapreduce.shuffle.port=13562
    -Dmy.decommission.file=/tmp/decommission1
"
YARN_NODEMANAGER2_OPTS="
    -Dmy.hadoop.tmp.dir=${WORKSPACE_DIR}/_data/nodemanager-data2
    -Dmy.yarn.nodemanager.localizer.address=0.0.0.0:8140
    -Dmy.yarn.nodemanager.address=0.0.0.0:8141
    -Dmy.yarn.nodemanager.webapp.address=0.0.0.0:8142
    -Dmy.mapreduce.shuffle.port=13662
    -Dmy.decommission.file=/tmp/decommission2
"
YARN_NODEMANAGER3_OPTS="
    -Dmy.hadoop.tmp.dir=${WORKSPACE_DIR}/_data/nodemanager-data3
    -Dmy.yarn.nodemanager.localizer.address=0.0.0.0:8240
    -Dmy.yarn.nodemanager.address=0.0.0.0:8241
    -Dmy.yarn.nodemanager.webapp.address=0.0.0.0:8242
    -Dmy.mapreduce.shuffle.port=13762
    -Dmy.decommission.file=/tmp/decommission3
"

case "${1:-start}" in
    start)

        # start hdfs
        "$HADOOP_HOME"/sbin/hadoop-daemon.sh start namenode
        "$HADOOP_HOME"/sbin/hadoop-daemon.sh start secondarynamenode
        "$HADOOP_HOME"/sbin/hadoop-daemon.sh start datanode

        # start yarn
        "$HADOOP_HOME"/sbin/yarn-daemon.sh start resourcemanager
        YARN_NODEMANAGER_OPTS="$YARN_NODEMANAGER1_OPTS" YARN_IDENT_STRING=node1 "$HADOOP_HOME"/sbin/yarn-daemon.sh start nodemanager
        YARN_NODEMANAGER_OPTS="$YARN_NODEMANAGER2_OPTS" YARN_IDENT_STRING=node2 "$HADOOP_HOME"/sbin/yarn-daemon.sh start nodemanager
        YARN_NODEMANAGER_OPTS="$YARN_NODEMANAGER3_OPTS" YARN_IDENT_STRING=node3 "$HADOOP_HOME"/sbin/yarn-daemon.sh start nodemanager

        (
            echo 'creating necessary HDFS directories, if failed please create them manually...'
            sleep 3 # waiting namenode/datanode to start
            set -x
            hadoop/bin/hadoop fs -mkdir -p /
            hadoop/bin/hadoop fs -mkdir -p /spark-eventlog
            hadoop/bin/hadoop fs -mkdir -p /warehouse
        )

        # start history server
        "$HADOOP_HOME"/sbin/mr-jobhistory-daemon.sh start historyserver
        "$SPARK_HOME"/sbin/start-history-server.sh

        echo 'run `source _bin/env.sh` before using this environment.'
        ;;

    stop)
        # stop hdfs
        "$HADOOP_HOME"/sbin/hadoop-daemon.sh stop namenode
        "$HADOOP_HOME"/sbin/hadoop-daemon.sh stop secondarynamenode
        "$HADOOP_HOME"/sbin/hadoop-daemon.sh stop datanode

        # stop yarn
        "$HADOOP_HOME"/sbin/yarn-daemon.sh stop resourcemanager
        YARN_IDENT_STRING=node1 YARN_NODEMANAGER_OPTS="$YARN_NODEMANAGER1_OPTS" "$HADOOP_HOME"/sbin/yarn-daemon.sh stop nodemanager
        YARN_IDENT_STRING=node2 YARN_NODEMANAGER_OPTS="$YARN_NODEMANAGER2_OPTS" "$HADOOP_HOME"/sbin/yarn-daemon.sh stop nodemanager
        YARN_IDENT_STRING=node3 YARN_NODEMANAGER_OPTS="$YARN_NODEMANAGER3_OPTS" "$HADOOP_HOME"/sbin/yarn-daemon.sh stop nodemanager

        # stop history server
        "$HADOOP_HOME"/sbin/mr-jobhistory-daemon.sh stop historyserver
        "$SPARK_HOME"/sbin/stop-history-server.sh
        ;;

    restart)
        "$0" stop
        "$0" start
        ;;
esac
