#!/usr/bin/env bash
# Runs the Hadoop Streaming job "trips per pickup zone per hour" on YARN.
# Runs INSIDE the namenode container (`make hadoop-mr`), which has the Hadoop
# client and the mapper/reducer mounted at /opt/urbanflow/mapreduce.
set -euo pipefail

IN=/urbanflow/staging/trips_tsv          # written by hive/queries/02_export_for_mapreduce.sql
OUT=/urbanflow/mr/zone_hour_counts       # read back by Hive as table mr_zone_hour_counts
say() { printf '\n==> %s\n' "$*"; }

hdfs dfs -test -d "$IN" || { echo "missing $IN - run the Hive export first (make hadoop-mr does)" >&2; exit 1; }

say "input: the text extract in HDFS, and how it is stored in blocks"
hdfs dfs -du -s -h "$IN"
hdfs fsck "$IN" -files -blocks 2>/dev/null | grep -E '^/.* bytes|Total blocks'

# MapReduce refuses to overwrite an existing output directory, on purpose.
hdfs dfs -rm -r -f -skipTrash "$OUT" >/dev/null

say "submitting the streaming job to YARN"
# job.maps=4: ask for ~4 input splits, so 4 map tasks read the ~100 MB input
#   in parallel (without it, a file smaller than one 128 MB block gets 1-2).
# num.map.output.key.fields=3: the key is borough+zone+hour, not just field 1.
# num.reduce.output.key.fields=3: the same for lines the COMBINER prints -
#   streaming re-parses them, and with the default of 1 the key would silently
#   shrink to just the borough, and a reducer would see one zone-hour in pieces.
# reduces=2: two reducers, each owning half of the keys (hash partitioning).
mapred streaming \
  -D mapreduce.job.name="UrbanFlow: trips per pickup zone per hour" \
  -D mapreduce.job.maps=4 \
  -D stream.num.map.output.key.fields=3 \
  -D stream.num.reduce.output.key.fields=3 \
  -D mapreduce.job.reduces=2 \
  -files /opt/urbanflow/mapreduce/mapper.py,/opt/urbanflow/mapreduce/reducer.py \
  -mapper "python mapper.py" \
  -combiner "python reducer.py" \
  -reducer "python reducer.py" \
  -input "$IN" \
  -output "$OUT" 2>&1 \
  | grep --line-buffered -E 'Submitted application|Running job|map [0-9]+% reduce|completed successfully|Launched (map|reduce) tasks|Map input records|Map output records|Combine (input|output) records|Reduce input records|Reduce output records|Reduce shuffle bytes|MalformedLines|ERROR|Exception' \
  | sed -u -E 's/^.* (INFO|ERROR|WARN) +[A-Za-z]+:[0-9]+ - //'

say "output files (one per reducer)"
hdfs dfs -ls "$OUT"

say "busiest pickup zone-hours (borough, zone, hour, trips)"
hdfs dfs -cat "$OUT/part-*" | sort -t "$(printf '\t')" -k4,4nr | sed -n '1,10p'   # sed, not head: no SIGPIPE under pipefail

say "YARN's record of the job (yarn application -list -appStates FINISHED)"
yarn application -list -appStates FINISHED 2>/dev/null | grep -E 'Application-Id|UrbanFlow' | tail -3
