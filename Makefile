# UrbanFlow — one command per pipeline stage.
# Every target is safe to re-run.
PY := .venv/bin/python
TIER ?= 0
DATASET ?= yellow
# real-time path (docker-compose.streaming.yml)
STREAM_COMPOSE := docker compose -p urbanflow-streaming -f docker-compose.streaming.yml
KAFKA_CLI := $(STREAM_COMPOSE) exec -T kafka /opt/kafka/bin
RATE ?= 2000
LIMIT ?= 300000
SKIP ?= 0

.PHONY: help setup check synth ingest curate gold model predict-grid bench dash all clean-data test \
        hbase-up hbase-load hbase-query hbase-zk hbase-shell hbase-down hbase-clean \
        hadoop-up hadoop-status hadoop-load hive-tables hadoop-mr hive-query hadoop-demo hadoop-down hadoop-clean

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[1m%-15s\033[0m %s\n", $$1, $$2}'

setup:        ## create venv and install pinned dependencies
	python3 -m venv .venv
	$(PY) -m pip install --upgrade pip setuptools wheel
	$(PY) -m pip install -r requirements.txt
	@echo "\nNow run: make check"

check:        ## verify Java + Spark start correctly on this machine
	@java -version 2>&1 | grep -iE '(openjdk|java) version' | head -1
	@PYTHONPATH=src $(PY) -c "from urbanflow.session import get_spark, describe; s=get_spark('check'); print(describe(s)); s.stop()"

synth:        ## generate synthetic TLC-shaped data (no network needed)
	PYTHONPATH=src $(PY) -m urbanflow.ingest.synth --rows 200000

ingest:       ## download real data for TIER=0|1|2
	PYTHONPATH=src $(PY) -m urbanflow.ingest.download --tier $(TIER)

curate:       ## bronze -> silver: clean, enrich, partition
	PYTHONPATH=src $(PY) -m urbanflow.curate.clean --tier $(TIER)

gold:         ## silver -> gold: the six answer tables
	PYTHONPATH=src $(PY) -m urbanflow.analyze.gold --dataset $(DATASET)

model:        ## train the trip-duration model against a naive baseline
	PYTHONPATH=src $(PY) -m urbanflow.analyze.model --dataset $(DATASET)

predict-grid: ## score the trained model over a representative trip grid, for the dashboard's Predict tab
	PYTHONPATH=src $(PY) -m urbanflow.analyze.predict_grid --dataset $(DATASET)

bench:        ## run the benchmark suite (format, partitioning, join, cores)
	PYTHONPATH=src $(PY) -m urbanflow.analyze.benchmark --dataset $(DATASET)

dash:         ## launch the dashboard
	PYTHONPATH=src .venv/bin/streamlit run src/urbanflow/dashboard/app.py

test:         ## run unit tests (no network, no big data)
	PYTHONPATH=src .venv/bin/pytest -q tests/

all: synth curate gold model predict-grid bench  ## full pipeline on synthetic data

clean-data:   ## delete derived layers, keep bronze
	rm -rf data/curated data/gold data/bench data/models data/spill

# ---------------------------------------------------------------- HBase + ZooKeeper (opt-in)
# Separate compose project; never touches the default `docker compose up` stack.
# Walkthrough: docs/hadoop/hbase.md
HBASE := docker compose -f docker-compose.hbase.yml -p urbanflow-hbase
HBASE_CLIENT := $(HBASE) run --rm -T hbase-client
HBASE_GOLD := $(or $(URBANFLOW_DATA),data)/gold

JMX_AGENT := docker/hbase/jmx-exporter/jmx_prometheus_javaagent.jar
JMX_AGENT_URL := https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar
JMX_AGENT_SHA256 := 7d61f737fd661610ccc14aea79764faa1ea94a340cbc8f0029b3d2edea3d80c1

$(JMX_AGENT):  # Prometheus JMX exporter java agent (~3 MB), mounted into every HBase/ZK JVM
	curl -fsSL -o $@.tmp $(JMX_AGENT_URL)
	echo "$(JMX_AGENT_SHA256)  $@.tmp" | shasum -a 256 -c -
	mv $@.tmp $@

hbase-up: $(JMX_AGENT)  ## start ZooKeeper + HBase master, regionserver, Thrift gateway
	$(HBASE) up -d --wait
	@echo "\nHBase master UI  http://localhost:16010   regionserver UI  http://localhost:16030"
	@echo "Next: make hbase-load"

hbase-load:   ## create the HBase tables and load data/gold into them
	@test -d $(HBASE_GOLD)/demand_by_zone_hour || { echo "$(HBASE_GOLD) is empty: run make gold && make predict-grid first"; exit 1; }
	$(HBASE_CLIENT) hbase shell -n /app/scripts/hbase/create_tables.rb
	$(HBASE_CLIENT) python3 -m urbanflow.hbase.load

hbase-query:  ## point gets, prefix/range scans and counts, from the HBase shell and Python
	$(HBASE_CLIENT) hbase shell -n /app/scripts/hbase/demo_queries.rb
	$(HBASE_CLIENT) python3 -m urbanflow.hbase.query demo

hbase-zk:     ## show what HBase keeps in ZooKeeper (master, regionservers, meta location)
	$(HBASE_CLIENT) bash /app/scripts/hbase/zk_inspect.sh

hbase-shell:  ## interactive HBase shell
	$(HBASE) run --rm hbase-client hbase shell

hbase-down:   ## stop the HBase stack (data volume kept)
	$(HBASE) down

hbase-clean:  ## stop the HBase stack and delete its HBase + ZooKeeper volumes
	$(HBASE) down -v

# ---------------------------------------------------------------- Hadoop ecosystem (opt-in)
# HDFS + YARN + MapReduce + Hive in Docker, beside (not instead of) the Spark
# pipeline above. Needs the local data/ layers first (make synth curate gold,
# or the ingest path). Walkthrough: docs/hadoop/README.md
HADOOP := docker compose -p urbanflow-hadoop -f docker-compose.hadoop.yml
TIER2_GOLD ?=

hadoop-up:     ## start HDFS, YARN, MapReduce history, Hive metastore (Postgres) + HiveServer2
	scripts/hadoop/fetch-jars.sh
	$(HADOOP) up -d --wait
	$(HADOOP) exec -T namenode bash /opt/urbanflow/scripts/hdfs-init.sh
	$(HADOOP) exec -T hiveserver2 bash /opt/urbanflow/scripts/tez-upload.sh
	@echo "\n  NameNode UI        http://localhost:$${UF_NAMENODE_UI_PORT:-19870}"
	@echo "  YARN ResourceMgr   http://localhost:$${UF_RESOURCEMANAGER_UI_PORT:-18088}"
	@echo "  MR JobHistory      http://localhost:$${UF_HISTORY_UI_PORT:-19888}"
	@echo "  HiveServer2 UI     http://localhost:$${UF_HIVE_UI_PORT:-20002}"
	@echo "  Next: make hadoop-demo  (or hadoop-load, hive-tables, hadoop-mr, hive-query)"

hadoop-status: ## show cluster health: containers, HDFS capacity, YARN nodes, recent jobs
	$(HADOOP) ps --format 'table {{.Service}}\t{{.Status}}'
	$(HADOOP) exec -T namenode bash -c 'hdfs dfsadmin -report 2>/dev/null | sed -n "1,6p;/^Live datanodes/p"; yarn node -list 2>/dev/null | tail -n +2; yarn application -list -appStates ALL 2>/dev/null | tail -n +2 | tail -6'

hadoop-load:   ## copy the yellow data/ bronze, silver, gold into HDFS under /urbanflow (TIER2_GOLD=path adds the real Tier 2 gold)
	@if [ -n "$(TIER2_GOLD)" ]; then \
	  echo "==> copying Tier 2 gold from $(TIER2_GOLD)"; \
	  $(HADOOP) exec -T namenode rm -rf /tmp/tier2_gold && \
	  $(HADOOP) cp "$(TIER2_GOLD)" namenode:/tmp/tier2_gold; fi
	$(HADOOP) exec -T namenode bash /opt/urbanflow/scripts/hdfs-load.sh

hive-tables:   ## declare the Hive tables over the HDFS data (hive/queries/01_create_tables.sql)
	scripts/hadoop/hive-run.sh 01_create_tables.sql

hadoop-mr:     ## Hive writes a text extract, then a Python MapReduce job counts trips per zone per hour on YARN
	scripts/hadoop/hive-run.sh 02_export_for_mapreduce.sql
	$(HADOOP) exec -T namenode bash /opt/urbanflow/scripts/mr-zone-hour.sh

hive-query:    ## run the HiveQL analytics: reproduce gold, compare with MapReduce, Tier 2 answers
	scripts/hadoop/hive-run.sh 03_reproduce_gold.sql 04_analytics.sql 05_managed_partitioned.sql

hadoop-demo: hadoop-load hive-tables hadoop-mr hive-query  ## the whole walkthrough, after hadoop-up

hadoop-down:   ## stop the Hadoop stack, keep HDFS + metastore data in their volumes
	$(HADOOP) down

hadoop-clean:  ## stop the Hadoop stack AND delete its volumes (HDFS contents, Hive metastore)
	$(HADOOP) down -v
