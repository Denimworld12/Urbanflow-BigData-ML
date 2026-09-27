#!/usr/bin/env bash
# One-time HDFS setup every real cluster does after the first format, run
# INSIDE the namenode container as the HDFS superuser (`make hadoop-up`).
# Idempotent: safe on every start.
set -euo pipefail
# Shared scratch space: world-writable with the sticky bit, like Unix /tmp.
# YARN, MapReduce staging and Hive all write under it as different users.
hdfs dfs -mkdir -p /tmp/hive /tmp/hadoop-yarn/staging /tmp/logs /user/hive/warehouse /user/hadoop /urbanflow
hdfs dfs -chmod 1777 /tmp /tmp/hive /tmp/hadoop-yarn /tmp/hadoop-yarn/staging /tmp/logs
hdfs dfs -chown -R hive /user/hive
hdfs dfs -chown hadoop /user/hadoop
hdfs dfs -chmod 777 /urbanflow
echo "  HDFS initialised: /tmp (1777), /user/hive/warehouse, /urbanflow"
