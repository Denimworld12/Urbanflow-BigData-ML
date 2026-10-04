# UrbanFlow

Batch analytics over NYC TLC trip records with Apache Spark. The core
pipeline runs in **local mode on a single laptop**, with no cluster needed.
Opt-in Hadoop-ecosystem stacks run next to it in Docker (see
[Big Data ecosystem](#big-data-ecosystem)): HDFS, YARN, MapReduce and Hive
process the same data as a Hadoop cluster, HBase + ZooKeeper serve the gold
tables by key, Kafka + Spark Structured Streaming add a real-time path, and
Prometheus + Grafana monitor all of them.
BDA semester project · three members · six weeks · 8 GB laptops.

Six stages, bronze to dashboard: ingest → curate → gold → model → benchmark →
dashboard. See [`docs/OUTCOMES.md`](docs/OUTCOMES.md) for what each stage
hands in and why it counts as "big data" despite the laptop.

Alongside the batch pipeline there is an opt-in **real-time path**: trips
replayed into Apache Kafka and aggregated live by Spark Structured Streaming
(see [Big Data ecosystem](#big-data-ecosystem)).

## Screenshots

The dashboard, running against the real 243.5M-row FHVHV gold layer:

| | |
|---|---|
| **Predict** — trip duration from a trained model, looked up not scored live | **Explore** — filter trips by area, day type, hour |
| ![Predict tab](docs/screenshots/predict.png) | ![Explore tab](docs/screenshots/explore.png) |
| **Overview** — totals across the whole curated dataset | **Geography** — real NYC map, one map many metrics |
| ![Overview tab](docs/screenshots/overview.png) | ![Geography tab](docs/screenshots/geography.png) |
| **Behaviour** — tipping patterns, card vs. cash | **Performance** — Spark scaling and format benchmarks |
| ![Behaviour tab](docs/screenshots/behaviour.png) | ![Performance tab](docs/screenshots/performance.png) |

## Fastest start: one command with Docker (any OS)

No Java, no Python, no `make` needed on your machine at all — just
[Docker](https://www.docker.com/products/docker-desktop/), on Windows, macOS
or Linux alike:

```bash
docker compose up --build
```

That single command builds the image (Java 21 + Python + pinned deps baked
in), downloads a real month of taxi data (Tier 0, ~55 MB), runs the whole
pipeline — curate → gold → model → predict-grid → bench — and brings the dashboard up at
[localhost:8501](http://localhost:8501). Data persists in a Docker volume, so
the second `docker compose up` skips straight to the dashboard.

No network, or want synthetic data instead: `INGEST=0 docker compose up --build`.

Run a different make target instead of the dashboard, e.g. just the tests:
`docker compose run --rm urbanflow test`.

This is the fastest way for a new teammate to see the whole thing working
before they've installed anything project-specific — use the dev container
below once you're actually developing rather than just demoing.

## Native setup: macOS, Linux, Windows

To run the pipeline on your own machine you need:

* **Java 17, 21 or 25.** Spark 4.2 will not start on 8 or 11.
* **Python 3.10 or newer** (tested on 3.11 and 3.12). `pyspark==4.2.0` does
  not install on 3.9: pip ends with `No matching distribution found for
  pyspark==4.2.0`, and the real reason (`Requires-Python >=3.10`) is buried
  in a long line above it. macOS and some Linux distros still ship 3.9 as
  `python3`, so check before anything else.
* `make` and `git`.
* 25 GB free disk, 8 GB RAM (16 GB more comfortable).
* Only for the [Big Data ecosystem](#big-data-ecosystem) stacks: Docker with
  the Compose plugin (`docker compose version` works, v2 or later).

`bash scripts/doctor.sh` checks Java, Python, cores, RAM and disk in one go.
`PYTHON=python3.12 bash scripts/doctor.sh` checks a specific interpreter.

`make setup` builds `.venv` from `python3` unless you name another interpreter
with `PYTHON=`. It stops with a clear message if that Python is older than
3.10, and it reuses an existing `.venv` that is already 3.10+.

### macOS

```bash
python3 --version     # need 3.10+ (Apple's /usr/bin/python3 is 3.9)
java -version         # need 17, 21 or 25
```

If either is missing or too old, install them with [Homebrew](https://brew.sh):

```bash
xcode-select --install           # make + git, only if `make --version` fails
brew install python@3.12
brew install --cask temurin@21   # Java 21; open a new terminal, then java -version
```

Then build the venv with that Python by name. Bare `python3` can still be the
old 3.9 even after the install:

```bash
make setup PYTHON=python3.12     # runs python3.12 -m venv .venv, then pip install
make check
```

`python3.12 -m venv .venv && make setup` does the same thing by hand.

For the Big Data stacks, install Docker Desktop and give it at least 8 GB of
memory (Settings → Resources) for the Hadoop stack.

### Linux

```bash
python3 --version     # need 3.10+
java -version         # need 17, 21 or 25
```

Debian and Ubuntu split `venv` into its own package. Without it,
`python -m venv` fails with "ensurepip is not available". Install the row for
your distro, then run its `make setup`:

| Distro | Install | Then |
|---|---|---|
| Ubuntu 24.04+ | `sudo apt update && sudo apt install python3.12 python3.12-venv openjdk-21-jdk make git` | `make setup PYTHON=python3.12` |
| Ubuntu 22.04 | `sudo apt update && sudo apt install python3.10-venv openjdk-21-jdk make git` | `make setup PYTHON=python3.10` |
| Debian 12 | `sudo apt update && sudo apt install python3.11-venv openjdk-17-jdk make git` | `make setup PYTHON=python3.11` |
| Fedora 43+ | `sudo dnf install python3.12 java-25-openjdk-devel make git` | `make setup PYTHON=python3.12` |

Then `make check`. On an older release whose newest Python is 3.9 or below
(Ubuntu 20.04, Debian 11), use the Docker path above or the dev container below.

For the Big Data stacks, install Docker Engine with the Compose plugin
(`docker-compose-plugin`), and run `sudo usermod -aG docker $USER` then log out
and back in, so `docker` works without `sudo`.

### Windows (WSL2)

The Makefile needs a POSIX shell and `.venv/bin/python`, so plain
PowerShell/cmd is not supported. Use WSL2. In PowerShell as Administrator:

```powershell
wsl --install         # installs WSL2 + Ubuntu; reboot when it asks
```

Open **Ubuntu** from the Start menu and clone the repo inside the Linux file
system, not under `/mnt/c/`. Spark is much slower on the Windows-mounted drive.

```bash
cd ~ && git clone https://github.com/Denimworld12/Urbanflow-BigData-ML.git
cd Urbanflow-BigData-ML
```

Then follow the [Linux](#linux) steps for your Ubuntu version (`lsb_release -r`).

For the Big Data stacks, install Docker Desktop for Windows and turn on
Settings → Resources → WSL integration for your Ubuntu distro, so `docker`
works inside WSL. WSL gets half the machine's RAM by default; to give it more
for the Hadoop stack, set `memory=12GB` under `[wsl2]` in
`%UserProfile%\.wslconfig` and run `wsl --shutdown`.

### Dev container (any OS)

This repo ships `.devcontainer/devcontainer.json` (Python 3.11 + Java 21 +
`make` + Docker, all pre-installed). Open the repo in VS Code with the
[Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
(or in a GitHub Codespace) and choose **"Reopen in Container"**. You get the
same Linux environment on every host, and `make setup && make check` just
works. Needs Docker Desktop on Windows and macOS. In a Codespace the opt-in
Hadoop/HBase/Kafka/monitoring stacks run too. Machine size and how to open
their UIs: [`docs/CODESPACES.md`](docs/CODESPACES.md).

All Spark sessions are created in one place (`src/urbanflow/session.py`),
which pins the Spark worker subprocesses to the exact Python interpreter
running the driver (`sys.executable`). This avoids a real bug we hit during
testing where Spark silently picked up a different, incompatible system
Python off `PATH`. That fix is OS-independent by construction.

## Quickstart — 10 minutes, no download required

After the [native setup](#native-setup-macos-linux-windows) for your OS:

```bash
make setup          # venv + pinned dependencies (add PYTHON=python3.12 if python3 is older than 3.10)
make check          # verifies Java + Spark actually start
make synth           # 200k synthetic trips, TLC-shaped, with realistic dirt
make curate          # bronze -> silver
make gold             # the six answer tables
make model            # trip-duration model vs a naive baseline
make predict-grid     # pre-score the model for the dashboard's Predict tab
make test             # unit tests — run this after touching curate/
make dash             # dashboard at localhost:8501
```

`make synth` exists so all three of you can build and test the entire pipeline
on **day one**, before anyone has finished downloading 7 GB. The synthetic data
carries the same defects as the real files — stray 2001/2098 timestamps,
negative fares, teleporting taxis — so if your cleaning rules work here they
will work on the real thing.

## Then switch to real data

```bash
make ingest TIER=0     # 1 month yellow      ~3.5M rows,  55 MB
make ingest TIER=1     # 24 months y+green  ~85M rows,  1.4 GB
make ingest TIER=2     # 12 months FHVHV   ~230M rows,  5.8 GB
make curate TIER=1
make gold && make model && make predict-grid && make bench
```

Develop on Tier 0 so mistakes cost 20 seconds. Report from Tier 1.
Run Tier 2 once, in week 4, for the benchmark chapter.

## AI / ML

Two pieces, and they do different jobs:

* **Trip-duration model** (`make model`, `analyze/model.py`): a Spark MLlib
  gradient-boosted-trees regressor that predicts trip minutes from distance,
  pickup hour, day of week, passenger count (yellow/green only) and pickup/dropoff
  borough. It uses a time-based 80/20 split (train on earlier trips, test on
  later ones) and is scored against a naive `distance ÷ average speed`
  baseline. On the real Tier 2 run (243.5M FHVHV trips) it cut RMSE from 16.4
  to 7.1 minutes (56.8% better, R² 0.765). Metrics land in
  `data/gold/model_results.json`, the model in `data/models/duration_gbt`.
* **Predict tab** (`make predict-grid`, `analyze/predict_grid.py`): Spark scores
  the saved model once over every combination the tab offers (51,840 trips)
  into `data/gold/duration_predictions`. The dashboard then does a lookup, so
  no Spark runs in the dashboard process.
* **AI summary + chat** (`dashboard/ai.py`): sends a short list of facts
  already computed by the pipeline to Groq (`openai/gpt-oss-20b`) and shows
  the plain-English answer. It never sees the raw data. To enable it, copy
  `.env.example` to `.env` and set `GROQ_API_KEY` (both `make dash` and Docker
  read it). Without a key, the box says so and everything else still works.

How it all fits together, the metrics, and likely viva questions:
[`docs/ai-ml.md`](docs/ai-ml.md).

## Big Data ecosystem

Opt-in components that sit next to the batch pipeline. None of them is needed
for `docker compose up` or the Quickstart; each has its own compose file and
`make` targets, and none replaces anything above.

| Component | Stack | Walkthrough |
|---|---|---|
| HDFS, YARN, MapReduce, Hive (+ metastore on PostgreSQL, Tez) | `docker-compose.hadoop.yml` | [docs/hadoop/README.md](docs/hadoop/README.md) |
| HBase + ZooKeeper | `docker-compose.hbase.yml` | [docs/hadoop/hbase.md](docs/hadoop/hbase.md) |
| Kafka + Spark Structured Streaming | `docker-compose.streaming.yml` | [docs/hadoop/streaming.md](docs/hadoop/streaming.md) |
| Prometheus + Grafana monitoring | `docker-compose.monitoring.yml` | [docs/hadoop/monitoring.md](docs/hadoop/monitoring.md) |

**Before you start a stack:**

* Docker with Compose v2 must be running (see the Docker notes for your OS in
  [Native setup](#native-setup-macos-linux-windows)). All of these run in the
  dev container and in Codespaces too.
* Run `make setup` first. The Kafka producer, the Spark streaming job and the
  HBase/Hadoop data loads use the project venv and the local `data/` layers.
* Memory: the Hadoop stack wants about 8 GB for Docker by itself, HBase about
  3 GB, Kafka about 0.5 GB. On a 16 GB machine run one heavy stack at a time.
  Per-stack budget: [`docs/CODESPACES.md`](docs/CODESPACES.md#which-machine-type).
* Each stack has a `-down` target that keeps its data and a `-clean`
  (`stream-reset` for Kafka) target that deletes it. `make help` lists every
  target.

### Real-time: Kafka + Spark Structured Streaming

**What:** [Apache Kafka](https://kafka.apache.org/) is a distributed,
append-only log that producers write events to and consumers read from at
their own pace. Spark Structured Streaming runs the same DataFrame code as the
batch jobs, but incrementally, on each new slice of the log.

**Why UrbanFlow uses it:** the batch path answers "what happened last
month". The streaming path answers "what is happening in each zone right
now". A producer replays real TLC trips into Kafka as live JSON events. A
Spark job cleans them with the **same seven rules** as curate, then computes
trips, average fare and average duration per pickup zone per 5-minute
event-time window. A watermark decides how late a trip may arrive and still
count.

```bash
make setup           # once — also installs kafka-python
make ingest TIER=0   # or: make synth   (the producer replays bronze)
make stream-up       # Kafka (KRaft mode, no ZooKeeper) + kafka-exporter, topics created
make stream-run      # terminal 1: the Spark streaming job (Ctrl-C to stop)
make stream-produce  # terminal 2: replay 300k trips at 2000 events/s (RATE=, LIMIT=, SKIP=)
make stream-status   # topic layout + consumer-group lag
make stream-tail     # first windowed results from the zone-metrics topic
make stream-down     # stop (keeps data) — make stream-reset wipes Kafka + data/stream
```

Results land in `data/stream/zone_metrics/` (Parquet, final windows) and on
the `zone-metrics` Kafka topic (running updates). The dashboard's **Live**
tab reads the Parquet output. For monitoring, Kafka metrics are served at
`localhost:9308/metrics` (kafka-exporter), and Spark streaming metrics at
`localhost:4050/metrics/prometheus/` while the job runs. Needs Docker for
Kafka; the producer and the Spark job run from the project venv on the host.
The full explanation, architecture and viva notes are in
[`docs/hadoop/streaming.md`](docs/hadoop/streaming.md).

![Live tab](docs/screenshots/live.png)

### Hadoop core: HDFS, YARN, MapReduce, Hive

`docker-compose.hadoop.yml` stores UrbanFlow's bronze/silver/gold layers in
HDFS and processes them with the classic Hadoop tools. Full walkthrough, architecture diagram and monitoring endpoints:
[`docs/hadoop/README.md`](docs/hadoop/README.md).

| Component | What it is | What UrbanFlow does with it | Viva notes |
|---|---|---|---|
| **HDFS** | distributed file system: files split into 128 MB blocks, replicated across DataNodes, indexed by the NameNode | holds the data lake under `/urbanflow/{bronze,silver,gold}` | [hdfs.md](docs/hadoop/hdfs.md) |
| **YARN** | cluster resource manager: ResourceManager hands out containers, NodeManagers run them | runs the MapReduce job and every Hive query | [yarn.md](docs/hadoop/yarn.md) |
| **MapReduce** | map -> shuffle/sort -> reduce batch model | Python (Hadoop Streaming) job: trips per pickup zone per hour, output back to HDFS | [mapreduce.md](docs/hadoop/mapreduce.md) |
| **Hive** | SQL over files in HDFS; tables are schema + location in a metastore | external tables over the Parquet layers, HiveQL that reproduces the gold answers, a managed ACID ORC table | [hive.md](docs/hadoop/hive.md) |
| **Hive Metastore + PostgreSQL** | the catalogue: table/partition schemas and HDFS locations (not the data) | shared metastore backed by a real Postgres, not embedded Derby | [hive.md](docs/hadoop/hive.md) |
| **Tez** | DAG execution engine Hive 4 compiles queries into | runs Hive's queries as YARN applications | [hive.md](docs/hadoop/hive.md) |

```bash
make synth curate gold     # or the real-data path: make ingest TIER=0 && make curate && make gold
make hadoop-up             # 8 containers: NameNode, DataNode, ResourceManager, NodeManager,
                           #   JobHistory, Hive metastore + Postgres, HiveServer2 (~2 min)
make hadoop-demo           # load HDFS -> Hive tables -> MapReduce on YARN -> HiveQL results
make hadoop-down           # stop (make hadoop-clean also deletes the HDFS/metastore volumes)
```

Add the real Tier 2 gold tables (243.5M FHVHV trips, from the separate
`Urbanflow-BDA-data` repository) with `make tier2-data` before
`make hadoop-load` (a Codespace clones them on creation), or point at a copy
elsewhere with `make hadoop-load TIER2_GOLD=/path/to/Urbanflow-BDA-data/data/gold`.
UIs: NameNode http://localhost:19870, YARN http://localhost:18088,
JobHistory http://localhost:19888, HiveServer2 http://localhost:20002
(bound to `127.0.0.1` only). The stack loads the yellow dataset layout.

What it shows, measured on the Tier 0 month: Hive recomputes `daily_kpis`
(31/31 days), `demand_by_zone_hour` (10,333/10,333 groups) and `tipping`
exactly as Spark did, and the MapReduce job's 5,558 zone-hour counts match
Spark's gold table one for one (3,218,618 trips on both sides).

### Key-value serving: HBase + ZooKeeper

HBase stores the gold tables as wide-column rows with designed row keys
(e.g. `JFK Airport#weekday#17`, `2025-03-07`) for fast point gets and prefix
scans; ZooKeeper tracks the live HMaster, RegionServers and the `hbase:meta`
location.

```bash
make gold && make predict-grid   # HBase loads data/gold
make hbase-up      # ZooKeeper + HMaster + RegionServer + Thrift; waits until healthy
make hbase-load    # create namespace + tables (HBase shell), load gold (Python)
make hbase-query   # gets / scans / counts, first in the HBase shell, then from Python
make hbase-zk      # what HBase keeps in ZooKeeper
make hbase-down    # stop (make hbase-clean also deletes the HBase + ZooKeeper volumes)
```

UIs: HMaster http://localhost:16010, RegionServer http://localhost:16030.
Full explanation: [`docs/hadoop/hbase.md`](docs/hadoop/hbase.md).

### Monitoring: Prometheus + Grafana

`docker-compose.monitoring.yml` scrapes the other stacks over their Docker
networks (`urbanflow-hadoop`, `urbanflow-hbase-net`, `urbanflow-streaming`) and ships provisioned Grafana dashboards and
Prometheus alert rules. Start it before or after the stacks you want to watch:

```bash
make monitor-up       # Prometheus + Grafana + cAdvisor + json-exporter, attached to every running stack
make monitor-attach   # after starting another stack later (make monitor-up again works too)
make monitor-status   # every scrape target and whether it is up; stacks not started show as down
make monitor-down     # stop (data volumes kept)
```

Grafana http://localhost:3000 (admin / urbanflow), Prometheus
http://localhost:9090/targets, both bound to `127.0.0.1`. Port clash?
`GRAFANA_PORT=3300 PROMETHEUS_PORT=9095 make monitor-up` (pass the same to
`make monitor-status`). Setup and dashboards:
[`docs/hadoop/monitoring.md`](docs/hadoop/monitoring.md).

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
  analyze/predict_grid.py  S4b pre-scores the model for the Predict tab
  analyze/benchmark.py S5 format / partitioning / join / core-scaling
  dashboard/app.py     S6 Streamlit over DuckDB — no Spark in this process
  dashboard/ai.py      the Groq call behind the AI summary + chat
schema/curated.md      THE CONTRACT between the three of you
monitoring/            Prometheus scrape config + alerts, Grafana provisioning + dashboards
scripts/build_dashboards.py   generates the Grafana dashboard JSON

docker-compose.hadoop.yml   opt-in HDFS + YARN + MapReduce + Hive stack
hadoop/conf/                core/hdfs/yarn/mapred-site.xml, shared by every container
hadoop/mapreduce/           mapper.py + reducer.py (Hadoop Streaming job)
hive/conf/                  hive-site.xml, tez-site.xml
hive/queries/               HiveQL: tables, MapReduce export, analytics
scripts/hadoop/             load / init / job scripts run inside the containers
docs/hadoop/                how each component works and what to say about it
```

## Architecture rules that matter

* **Bronze (`data/raw/`) is immutable.** Never edit or overwrite a downloaded
  file — everything downstream must be rebuildable from it.
* **The curated schema is a contract**, frozen at the end of week 2
  (`schema/curated.md`). Changing it means telling both teammates in the same
  commit.
* **The dashboard never reads the curated layer** — only `data/gold/` (plus
  the Live tab's `data/stream/`), through DuckDB. That gap is deliberate, not a
  shortcut.
* **Never commit data.** `data/` is gitignored; it's reproducible from
  `data/raw/manifest.csv`.

Full gotchas (cash tips never recorded, stray 2001/2098 timestamps, why
speedup goes sub-linear) live in `docs/OUTCOMES.md`.
