#!/usr/bin/env bash
# Runs HiveQL files through beeline inside the hiveserver2 container and
# strips the JVM's logging noise. Used by the hive-* make targets.
#   scripts/hadoop/hive-run.sh 03_reproduce_gold.sql [04_analytics.sql ...]
set -uo pipefail
cd "$(dirname "$0")/../.." || exit
for f in "$@"; do
  printf '\n######## hive/queries/%s\n' "$f"
  docker compose -p urbanflow-hadoop -f docker-compose.hadoop.yml exec -T hiveserver2 \
    beeline -u jdbc:hive2://localhost:10000/ -n hive --silent=true --outputformat=table \
    -f "/opt/urbanflow/hive/$f" 2>&1 \
    | grep -vE 'SLF4J|^WARNING: |log4j|NativeCodeLoader|\.beeline|^No such file or directory$' | cat -s
  rc=${PIPESTATUS[0]}   # beeline's exit code, not grep's
  [ "$rc" -eq 0 ] || { echo "beeline failed on hive/queries/$f (exit $rc)" >&2; exit "$rc"; }
done
