"""Stage S1 — Bronze. Download monthly Parquet from the TLC CDN.

Rules of this stage:
  * Bronze is IMMUTABLE. Nothing here ever edits a downloaded file.
  * Every file downloaded is appended to manifest.csv with its byte size and
    row count. That manifest is your evidence of what you processed — cite it
    in the report.
  * Re-running skips files already present, so it is safe to interrupt.
"""
from __future__ import annotations
import argparse, csv, sys, time, urllib.request, urllib.error
from pathlib import Path
from .. import config


def _dest(dataset: str, year: int, month: int) -> Path:
    p = config.RAW / dataset / f"year={year}"
    p.mkdir(parents=True, exist_ok=True)
    return p / f"{dataset}_{year:04d}-{month:02d}.parquet"


def _rows(path: Path) -> int:
    try:
        import pyarrow.parquet as pq
        return pq.ParquetFile(path).metadata.num_rows
    except Exception:
        return -1


def _append_manifest(row: dict) -> None:
    new = not config.MANIFEST.exists()
    with config.MANIFEST.open("a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["dataset", "year", "month", "file", "bytes", "rows", "downloaded_at"])
        if new:
            w.writeheader()
        w.writerow(row)


def fetch(dataset: str, year: int, month: int, retries: int = 3) -> Path | None:
    dest = _dest(dataset, year, month)
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  skip  {dest.name} (already present)")
        return dest
    url = config.TRIP_URL.format(dataset=dataset, year=year, month=month)
    for attempt in range(1, retries + 1):
        try:
            print(f"  get   {url}")
            tmp = dest.with_suffix(".part")
            urllib.request.urlretrieve(url, tmp)
            tmp.rename(dest)
            n = _rows(dest)
            _append_manifest({
                "dataset": dataset, "year": year, "month": month, "file": str(dest),
                "bytes": dest.stat().st_size, "rows": n,
                "downloaded_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            })
            print(f"  ok    {dest.name}  {dest.stat().st_size/1e6:.1f} MB  {n:,} rows")
            return dest
        except urllib.error.HTTPError as e:
            if e.code == 403 or e.code == 404:
                print(f"  MISS  {dataset} {year}-{month:02d} not published yet ({e.code})")
                return None
            print(f"  retry {attempt}/{retries}: HTTP {e.code}")
        except Exception as e:                                    # noqa: BLE001
            print(f"  retry {attempt}/{retries}: {e}")
        time.sleep(2 * attempt)
    print(f"  FAIL  {dataset} {year}-{month:02d}")
    return None


def fetch_zones() -> Path:
    dest = config.RAW / "taxi_zone_lookup.csv"
    if dest.exists():
        print(f"  skip  {dest.name}")
        return dest
    urllib.request.urlretrieve(config.ZONE_URL, dest)
    print(f"  ok    {dest.name}")
    return dest


def run(tier: str) -> None:
    spec = config.TIERS[tier]
    print(f"Tier {tier} ({spec['label']}): {spec['datasets']} x {len(spec['months'])} months")
    fetch_zones()
    ok = 0
    for ds in spec["datasets"]:
        for (y, m) in spec["months"]:
            if fetch(ds, y, m):
                ok += 1
    print(f"\ndone: {ok} files present. manifest -> {config.MANIFEST}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Download TLC trip data into the bronze layer")
    ap.add_argument("--tier", default="0", choices=sorted(config.TIERS))
    a = ap.parse_args()
    run(a.tier)
    sys.exit(0)
