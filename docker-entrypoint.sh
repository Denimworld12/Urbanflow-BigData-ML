#!/usr/bin/env bash
# Runs once per container start: verify Spark, get data, build bronze->gold
# if it isn't there yet, then hand off to whatever `make` target was asked for
# (default: dash). Safe to re-run — every make target already is.
set -euo pipefail

echo "==> checking Java + Spark"
make check

# Tier 2 is the fhvhv dataset only (see config.py TIERS); tiers 0/1 report on yellow.
DATASET="${DATASET:-yellow}"
if [ "${TIER:-0}" = "2" ]; then DATASET="fhvhv"; fi

if [ ! -f "data/gold/benchmarks.json" ]; then
  if [ "${INGEST:-1}" = "1" ]; then
    echo "==> downloading real TLC data (TIER=${TIER:-0}) — set INGEST=0 to skip and use synthetic data instead"
    make ingest TIER="${TIER:-0}"
    make curate TIER="${TIER:-0}"
  else
    echo "==> INGEST=0: generating synthetic data instead (no network)"
    make synth
    make curate
  fi
  make gold DATASET="$DATASET"
  make model DATASET="$DATASET"
  make bench DATASET="$DATASET"
else
  echo "==> pipeline already ran (data/gold/benchmarks.json exists), skipping rebuild"
fi

echo "==> make ${*:-dash}"
exec make "${@:-dash}"
