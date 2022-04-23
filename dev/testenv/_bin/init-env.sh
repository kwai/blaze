#!/bin/bash

set -e
set -x

WORKSPACE_DIR="$(cd "$(dirname "$0")/../" && pwd)"
source "$WORKSPACE_DIR"/_bin/env.sh

# stop all running services
"$WORKSPACE_DIR"/_bin/run.sh stop

# reset logs
rm -rf "$WORKSPACE_DIR"/{hadoop,hive,spark}/logs
rm -rf "$WORKSPACE_DIR"/_logs
mkdir -p "$WORKSPACE_DIR"/_logs/{hadoop,hive,spark}
ln -sf "$WORKSPACE_DIR"/_logs/hadoop "$WORKSPACE_DIR"/hadoop/logs
ln -sf "$WORKSPACE_DIR"/_logs/hive   "$WORKSPACE_DIR"/hive/logs
ln -sf "$WORKSPACE_DIR"/_logs/spark  "$WORKSPACE_DIR"/spark/logs

# reset conf
rm -rf "$WORKSPACE_DIR"/hadoop/etc/hadoop
ln -sf "$WORKSPACE_DIR"/_conf "$WORKSPACE_DIR"/hadoop/etc/hadoop
rm -rf "$WORKSPACE_DIR"/hive/conf
ln -sf "$WORKSPACE_DIR"/_conf "$WORKSPACE_DIR"/hive/conf
rm -rf "$WORKSPACE_DIR"/spark/conf
ln -sf "$WORKSPACE_DIR"/_conf "$WORKSPACE_DIR"/spark/conf

# reset data
rm -rf "$WORKSPACE_DIR"/_data/*
mkdir "$WORKSPACE_DIR"/_data/dfs-name

# format namenode
"$WORKSPACE_DIR"/hadoop/bin/hadoop namenode -format

# init hive metastore
"$WORKSPACE_DIR"/hive/bin/schematool -dbType derby -initSchema
