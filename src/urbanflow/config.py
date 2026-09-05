"""Central configuration. Every path and dataset constant lives here.

Nothing else in the codebase should hard-code a path or a URL.
"""
from __future__ import annotations
import os
from pathlib import Path

# ---------------------------------------------------------------- paths
# Override with:  export URBANFLOW_DATA=/mnt/bigdisk/urbanflow
DATA_ROOT = Path(os.environ.get("URBANFLOW_DATA", Path(__file__).resolve().parents[2] / "data"))

RAW      = DATA_ROOT / "raw"        # bronze — never modified
CURATED  = DATA_ROOT / "curated"    # silver — cleaned, partitioned
GOLD     = DATA_ROOT / "gold"       # gold   — small answer tables
BENCH    = DATA_ROOT / "bench"      # benchmark scratch + results
MODELS   = DATA_ROOT / "models"
SPILL    = DATA_ROOT / "spill"      # spark.local.dir
MANIFEST = RAW / "manifest.csv"

for _p in (RAW, CURATED, GOLD, BENCH, MODELS, SPILL):
    _p.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------- source
CDN = "https://d37ci6vzurychx.cloudfront.net"
TRIP_URL = CDN + "/trip-data/{dataset}_tripdata_{year:04d}-{month:02d}.parquet"
ZONE_URL = CDN + "/misc/taxi_zone_lookup.csv"

DATASETS = ("yellow", "green", "fhvhv", "fhv")

# Pickup / dropoff timestamp column differs per dataset. Normalised in curate.
TS_COLS = {
    "yellow": ("tpep_pickup_datetime", "tpep_dropoff_datetime"),
    "green":  ("lpep_pickup_datetime", "lpep_dropoff_datetime"),
    "fhvhv":  ("pickup_datetime", "dropoff_datetime"),
    "fhv":    ("pickup_datetime", "dropOff_datetime"),
}

# ---------------------------------------------------------------- tiers
# Develop on TIER 0. Report from TIER 1. Benchmark once on TIER 2.
TIERS = {
    "0": {"label": "dev",   "datasets": ["yellow"],           "months": [(2025, 1)]},
    "1": {"label": "main",  "datasets": ["yellow", "green"],  "months": [(y, m) for y in (2024, 2025) for m in range(1, 13)]},
    "2": {"label": "scale", "datasets": ["fhvhv"],            "months": [(2025, m) for m in range(1, 13)]},
}

# ---------------------------------------------------------------- cleaning thresholds
MIN_TRIP_SECONDS   = 60
MAX_TRIP_SECONDS   = 6 * 3600
MAX_TRIP_MILES     = 100.0
MAX_SPEED_MPH      = 90.0
