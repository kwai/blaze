#!/bin/bash

if [ -f "$0" ]; then
    WORKSPACE_DIR="$(cd "$(dirname "$0")/../" && pwd)"
else
    WORKSPACE_DIR="$PWD"
fi

export HADOOP_USER_NAME="root"

export HADOOP_HOME="$WORKSPACE_DIR"/hadoop/
export HIVE_HOME="$WORKSPACE_DIR"/hive/
export SPARK_HOME="$WORKSPACE_DIR"/spark/

export HADOOP_CONF_DIR="$HADOOP_HOME"/etc/hadoop/
export YARN_CONF_DIR="$HADOOP_CONF_DIR"
export HIVE_CONF_DIR="$HIVE_HOME"/conf/
export SPARK_CONF_DIR="$SPARK_HOME"/conf/
export SPARK_DIST_CLASSPATH="$("$WORKSPACE_DIR"/hadoop/bin/hadoop classpath)"
