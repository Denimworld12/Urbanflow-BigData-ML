#!/usr/bin/env bash
# Copies UrbanFlow's local medallion layers into HDFS. Runs INSIDE the
# namenode container (`make hadoop-load`), where ./data is mounted read-only
# at /urbanflow-data.
#
#   data/raw/<dataset>/          -> /urbanflow/bronze/<dataset>/
#   data/raw/taxi_zone_lookup.csv-> /urbanflow/bronze/zones/
#   data/curated/<dataset>/      -> /urbanflow/silver/<dataset>/   (year=/month= partitions kept)
#   data/gold/<table>/           -> /urbanflow/gold/<dataset>/<table>/
#   $TIER2_GOLD/<table>/         -> /urbanflow/gold/fhvhv/<table>/ (optional: the real Tier 2 gold)
#
# Every step replaces its HDFS target, so re-running is safe.
set -euo pipefail

SRC=/urbanflow-data
DATASET="${DATASET:-yellow}"
TIER2_GOLD_IN=/tmp/tier2_gold     # the Makefile copies TIER2_GOLD here with `docker compose cp`

say() { printf '\n==> %s\n' "$*"; }

replace() {  # replace <local path> <hdfs target>
  hdfs dfs -rm -r -f -skipTrash "$2" >/dev/null
  hdfs dfs -mkdir -p "$(dirname "$2")"
  hdfs dfs -put "$1" "$2"
  echo "  $1 -> $2"
}

replace_all() {  # replace_all <hdfs dir> <local dir>... : one put for many table folders
  local dst="$1"; shift
  hdfs dfs -rm -r -f -skipTrash "$dst" >/dev/null
  hdfs dfs -mkdir -p "$dst"
  hdfs dfs -put "${@%/}" "$dst"/
  for t in "$@"; do t="${t%/}"; echo "  $t -> $dst/$(basename "$t")"; done
}

[ -d "$SRC/curated/$DATASET" ] || { echo "no $SRC/curated/$DATASET - run 'make synth curate gold' (or ingest) first" >&2; exit 1; }

say "HDFS layout under /urbanflow"
hdfs dfs -mkdir -p /urbanflow/bronze /urbanflow/silver /urbanflow/gold /urbanflow/staging /urbanflow/mr

say "bronze: raw monthly Parquet exactly as downloaded (immutable)"
replace "$SRC/raw/$DATASET" "/urbanflow/bronze/$DATASET"
if [ -f "$SRC/raw/taxi_zone_lookup.csv" ]; then
  hdfs dfs -mkdir -p /urbanflow/bronze/zones
  hdfs dfs -put -f "$SRC/raw/taxi_zone_lookup.csv" /urbanflow/bronze/zones/
  echo "  $SRC/raw/taxi_zone_lookup.csv -> /urbanflow/bronze/zones/"
fi

say "silver: cleaned trips, partitioned by year/month"
replace "$SRC/curated/$DATASET" "/urbanflow/silver/$DATASET"

say "gold: the answer tables Spark built from that silver layer"
replace_all "/urbanflow/gold/$DATASET" "$SRC"/gold/*/

if [ -d "$TIER2_GOLD_IN" ]; then
  say "gold (Tier 2): the real 243.5M-row FHVHV answer tables"
  replace_all /urbanflow/gold/fhvhv "$TIER2_GOLD_IN"/*/
fi

say "hdfs dfs -ls /urbanflow/*"
hdfs dfs -ls /urbanflow/bronze /urbanflow/silver /urbanflow/gold "/urbanflow/gold/$DATASET"

say "hdfs dfs -du -s -h (size per layer)"
hdfs dfs -du -s -h /urbanflow/bronze /urbanflow/silver /urbanflow/gold

say "replication factor and block size of the bronze file (hdfs dfs -stat %r %o %b %n)"
hdfs dfs -stat '%r replica(s)  block=%o bytes  size=%b bytes  %n' "/urbanflow/bronze/$DATASET/*/*.parquet"

say "hdfs fsck /urbanflow -files -blocks (summary)"
hdfs fsck /urbanflow -files -blocks 2>/dev/null | sed -n '/^Status/,/Corrupt blocks/p' | grep -E 'Total (size|files|blocks)|Default replication|Average block replication|Under-replicated|Missing blocks|Corrupt blocks|Status'
