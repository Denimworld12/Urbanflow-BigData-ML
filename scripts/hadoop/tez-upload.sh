#!/usr/bin/env bash
# Puts the Tez runtime jars into HDFS (/apps/tez) so YARN can ship them to
# every Tez container. Runs INSIDE the hiveserver2 container (which has Tez
# at /opt/tez) from `make hadoop-up`. Skips the upload when already there.
set -euo pipefail
if hdfs dfs -test -e /apps/tez/tez-api-*.jar 2>/dev/null; then
  echo "  Tez already in HDFS: /apps/tez"; exit 0
fi
hdfs dfs -mkdir -p /apps/tez
# The Hadoop jars bundled with Tez are left out: tez.use.cluster.hadoop-libs
# makes containers use the cluster's own Hadoop version instead.
hdfs dfs -put -f /opt/tez/*.jar $(ls /opt/tez/lib/*.jar | grep -v '/hadoop-') /apps/tez/
echo "  Tez uploaded to HDFS: /apps/tez ($(hdfs dfs -ls /apps/tez | grep -c '\.jar') jars)"
