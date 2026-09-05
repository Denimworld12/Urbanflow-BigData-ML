# UrbanFlow

Batch analytics over NYC TLC trip records with Apache Spark, running in
**local mode on a single laptop** — no cluster, no Hadoop, no Cassandra.
BDA semester project · three members · six weeks · 8 GB laptops.

Six stages, bronze to dashboard: ingest → curate → gold → model → benchmark →
dashboard. See [`docs/OUTCOMES.md`](docs/OUTCOMES.md) for what each stage
hands in and why it counts as "big data" despite the laptop.

## Requirements

* **Java 17, 21 or 25** — Spark 4.2 will not start on 8 or 11. Check with
  `java -version` before anything else; this is the most common blocker.
* **Python 3.10+** (tested on 3.12). If `python3 --version` on your machine is
  older, install a newer one (`brew install python@3.12`, `pyenv install
  3.12`, or the Microsoft Store on Windows) rather than fighting the old one.
* 25 GB free disk, 8 GB RAM (16 GB more comfortable).

## Quickstart — 10 minutes, no download required

```bash
make setup          # venv + pinned dependencies
make check          # verifies Java + Spark actually start
make synth           # 200k synthetic trips, TLC-shaped, with realistic dirt
make curate          # bronze -> silver
make gold             # the six answer tables
make test             # unit tests — run this after touching curate/
make dash             # dashboard at localhost:8501
```

`make synth` exists so all three of you can build and test the entire pipeline
on **day one**, before anyone has finished downloading 7 GB. The synthetic data
carries the same defects as the real files — stray 2001/2098 timestamps,
negative fares, teleporting taxis — so if your cleaning rules work here they
will work on the real thing.

## New developer? One-click with Docker

No Java, no Python, no `make` needed on your machine at all — just
[Docker](https://www.docker.com/products/docker-desktop/), on Windows, macOS
or Linux alike:

```bash
docker compose up --build
```

That single command builds the image (Java 21 + Python + pinned deps baked
in), downloads a real month of taxi data (Tier 0, ~55 MB), runs the whole
pipeline — curate → gold → model → bench — and brings the dashboard up at
[localhost:8501](http://localhost:8501). Data persists in a Docker volume, so
the second `docker compose up` skips straight to the dashboard.

No network, or want synthetic data instead: `INGEST=0 docker compose up --build`.

Run a different make target instead of the dashboard, e.g. just the tests:
`docker compose run --rm urbanflow test`.

This is the fastest way for a new teammate to see the whole thing working
before they've installed anything project-specific — use the dev container
below once you're actually developing rather than just demoing.

## Then switch to real data

```bash
make ingest TIER=0     # 1 month yellow      ~3.5M rows,  55 MB
make ingest TIER=1     # 24 months y+green  ~85M rows,  1.4 GB
make ingest TIER=2     # 12 months FHVHV   ~230M rows,  5.8 GB
make curate TIER=1
make gold && make model && make bench
```

Develop on Tier 0 so mistakes cost 20 seconds. Report from Tier 1.
Run Tier 2 once, in week 4, for the benchmark chapter.

## Running it on Windows, macOS or Linux

The pipeline itself is plain Python + Java and runs the same everywhere. The
**one thing that differs by OS is `make`** — it ships with macOS and every
Linux distro, but not with Windows.

**Recommended, same on all three: the dev container.** This repo ships
`.devcontainer/devcontainer.json` (Python 3.11 + Java 21 + `make`, all
pre-installed). Open the repo in VS Code with the
[Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
(or in a GitHub Codespace) and choose **"Reopen in Container"** — you get an
identical Linux environment regardless of host OS, and `make setup && make
check` just works. Requires Docker Desktop on Windows/macOS.

**Native, without Docker:**

| OS | What to do |
|---|---|
| macOS / Linux | Install Java + Python as above, then run the Quickstart commands directly in Terminal. |
| Windows | Use **WSL2** (Ubuntu) and run the Quickstart commands inside it — this is the only native path that gives you `make` and matches how the project is documented and tested. Plain PowerShell/cmd is not supported: the Makefile assumes a POSIX shell and `.venv/bin/python`, not `.venv\Scripts\python.exe`. |

All Spark sessions are created in one place (`src/urbanflow/session.py`),
which pins the Spark worker subprocesses to the exact Python interpreter
running the driver (`sys.executable`) — this avoids a real bug we hit during
testing where Spark silently picked up a different, incompatible system
Python off `PATH`. That fix is OS-independent by construction.

## Layout

```
src/urbanflow/
  config.py            every path and URL — nothing hard-coded elsewhere
  session.py           the ONLY place a SparkSession is created
  ingest/download.py   S1 bronze — download + manifest
  ingest/synth.py      synthetic data, so work starts before the download ends
  curate/rules.py      cleaning rules, individually measurable
  curate/clean.py      S2 silver — clean, broadcast-join zones, partition
  analyze/gold.py      S3 gold — six answer tables
  analyze/model.py     S4 duration model with a naive baseline
  analyze/benchmark.py S5 format / partitioning / join / core-scaling
  dashboard/app.py     S6 Streamlit over DuckDB — no Spark in this process
schema/curated.md      THE CONTRACT between the three of you
```

## Architecture rules that matter

* **Bronze (`data/raw/`) is immutable.** Never edit or overwrite a downloaded
  file — everything downstream must be rebuildable from it.
* **The curated schema is a contract**, frozen at the end of week 2
  (`schema/curated.md`). Changing it means telling both teammates in the same
  commit.
* **The dashboard never reads the curated layer** — only `data/gold/`, through
  DuckDB. That gap is deliberate, not a shortcut.
* **Never commit data.** `data/` is gitignored; it's reproducible from
  `data/raw/manifest.csv`.

Full gotchas (cash tips never recorded, stray 2001/2098 timestamps, why
speedup goes sub-linear) live in `CLAUDE.md`.

## Working with Claude Code

This repo ships with `CLAUDE.md` (project rules), seven slash commands and two
review subagents. See [`docs/CLAUDE-CODE.md`](docs/CLAUDE-CODE.md). Start with:

```
/status      what exists, what is next
/curate 1    run curation and interpret the quality report
/bench       run benchmarks and draft the performance section
/viva        get examined on your own code
```
