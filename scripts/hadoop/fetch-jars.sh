#!/usr/bin/env bash
# Downloads the two jars the Hive containers need that the apache/hive image
# does not ship: the PostgreSQL JDBC driver (metastore -> Postgres) and the
# Prometheus JMX exporter agent (metrics). Checksums are pinned; re-running
# skips files that are already present and correct.
set -euo pipefail
mkdir -p "$(dirname "$0")/../../hadoop/lib"
cd "$(dirname "$0")/../../hadoop/lib"

fetch() {  # fetch <file> <url> <sha256>
  if [ -f "$1" ] && echo "$3  $1" | shasum -a 256 -c - >/dev/null 2>&1; then
    return
  fi
  echo "  get  $2"
  curl -fsSL -o "$1.part" "$2"
  echo "$3  $1.part" | shasum -a 256 -c - >/dev/null || { echo "checksum mismatch for $1" >&2; rm -f "$1.part"; exit 1; }
  mv "$1.part" "$1"
}

M=https://repo1.maven.org/maven2
fetch postgresql.jar \
  $M/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
  188976721ead8e8627eb6d8389d500dccc0c9bebd885268a3047180274a6031e
fetch jmx_prometheus_javaagent.jar \
  $M/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar \
  7d61f737fd661610ccc14aea79764faa1ea94a340cbc8f0029b3d2edea3d80c1
echo "  jars ok: $(ls | tr "\n" " ")"
