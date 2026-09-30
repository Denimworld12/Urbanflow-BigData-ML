"""The Hadoop Streaming mapper/reducer, run the way Hadoop runs them.

Hadoop pipes lines through the scripts: map -> sort by key -> (combine) ->
reduce. These tests do the same with subprocesses, so the job's logic can be
checked in a second without starting the cluster. Run: make test
"""
import subprocess
import sys
from collections import Counter
from pathlib import Path

MR = Path(__file__).resolve().parents[1] / "hadoop" / "mapreduce"

TRIPS = [
    ("Manhattan", "Midtown Center", "18"),
    ("Manhattan", "Midtown Center", "18"),
    ("Manhattan", "Midtown Center", "17"),
    ("Queens", "JFK Airport", "18"),
    ("Unknown", "\\N", "3"),          # Hive writes a NULL zone as \N
    ("Manhattan", "Midtown Center", "18"),
]


def _run(script: str, lines: list[str]) -> list[str]:
    out = subprocess.run([sys.executable, str(MR / script)], input="".join(lines),
                         capture_output=True, text=True, check=True).stdout
    return out.splitlines(keepends=True)


def _shuffle(lines: list[str]) -> list[str]:
    # Hadoop sorts map output by key (the first three fields) before reducing.
    return sorted(lines, key=lambda l: l.split("\t")[:3])


def _counts(lines: list[str]) -> dict:
    return {tuple(l.rstrip("\n").split("\t")[:3]): int(l.split("\t")[3]) for l in lines}


def test_mapper_emits_one_per_trip_and_skips_bad_lines():
    lines = ["\t".join(t) + "\n" for t in TRIPS] + ["not a trip\n"]
    out = _run("mapper.py", lines)
    assert len(out) == len(TRIPS)
    assert all(l.endswith("\t1\n") for l in out)


def test_map_reduce_counts_trips_per_zone_hour():
    mapped = _run("mapper.py", ["\t".join(t) + "\n" for t in TRIPS])
    reduced = _run("reducer.py", _shuffle(mapped))
    assert _counts(reduced) == dict(Counter(TRIPS))


def test_combiner_gives_the_same_answer():
    # Each "mapper" combines its own half before the shuffle; the final
    # counts must not change, which is why a sum is safe as a combiner.
    half = len(TRIPS) // 2
    partial = []
    for chunk in (TRIPS[:half], TRIPS[half:]):
        mapped = _run("mapper.py", ["\t".join(t) + "\n" for t in chunk])
        partial += _run("reducer.py", _shuffle(mapped))
    reduced = _run("reducer.py", _shuffle(partial))
    assert _counts(reduced) == dict(Counter(TRIPS))
    assert len(reduced) == len(set(TRIPS))    # each key exactly once
