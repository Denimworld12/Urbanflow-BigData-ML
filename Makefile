# UrbanFlow — one command per pipeline stage.
# Every target is safe to re-run.
PY := .venv/bin/python
# interpreter `make setup` builds the venv from; override when the default
# python3 is too old, e.g. make setup PYTHON=python3.12
PYTHON ?= python3
TIER ?= 0
DATASET ?= yellow
# real-time path (docker-compose.streaming.yml)
STREAM_COMPOSE := docker compose -p urbanflow-streaming -f docker-compose.streaming.yml
KAFKA_CLI := $(STREAM_COMPOSE) exec -T kafka /opt/kafka/bin
RATE ?= 2000
LIMIT ?= 300000
SKIP ?= 0

.PHONY: help setup check synth ingest curate gold model predict-grid bench dash all tier2-data clean-data test \
        stream-up stream-produce stream-run stream-status stream-tail stream-down stream-reset \
        monitor-up monitor-attach monitor-down monitor-status monitor-reload monitor-check monitor-dashboards \
        hbase-up hbase-load hbase-query hbase-zk hbase-shell hbase-down hbase-clean \
        hadoop-up hadoop-status hadoop-load hive-tables hadoop-mr hive-query hadoop-demo hadoop-down hadoop-clean

help:
	@grep -E '^[a-z0-9-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[1m%-20s\033[0m %s\n", $$1, $$2}'

# pyspark 4.2 needs Python 3.10+; on an older one pip fails with a confusing
# "no matching distribution" error, so stop with a clear message first.
PY_VERSION_OK := import sys; sys.exit(sys.version_info < (3, 10))

setup:        ## create venv (or reuse a healthy one) and install pinned dependencies; PYTHON=python3.12 picks the interpreter
	@if [ -x $(PY) ]; then \
	  $(PY) -c '$(PY_VERSION_OK)' 2>/dev/null || { \
	    echo ".venv uses $$($(PY) --version 2>&1), too old for pyspark 4.2 (needs Python 3.10+)."; \
	    echo "Delete it and rebuild with a newer Python: rm -rf .venv && make setup PYTHON=python3.12"; exit 1; }; \
	  echo "Reusing .venv ($$($(PY) --version 2>&1))"; \
	else \
	  $(PYTHON) -c '$(PY_VERSION_OK)' 2>/dev/null || { \
	    echo "$(PYTHON) is $$($(PYTHON) --version 2>/dev/null || echo not installed); UrbanFlow needs Python 3.10+ (pyspark 4.2)."; \
	    echo "Install a newer one (README, Native setup) and run e.g.: make setup PYTHON=python3.12"; exit 1; }; \
	  echo "$(PYTHON) -m venv .venv"; $(PYTHON) -m venv .venv; \
	fi
	$(PY) -m pip install --upgrade pip setuptools wheel
	$(PY) -m pip install -r requirements.txt
	@echo "\nNow run: make check"

check:        ## verify Java + Spark start correctly on this machine
	@test -x $(PY) || { echo "no .venv yet: run make setup first"; exit 1; }
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

tier2-data:   ## clone the real Tier 2 gold + model (Urbanflow-BDA-data, ~340 KB) into external/
	scripts/fetch-tier2-data.sh

clean-data:   ## delete derived layers, keep bronze
	rm -rf data/curated data/gold data/bench data/models data/spill

# ---------------------------------------------------------------- real-time path (opt-in)
# Kafka runs in Docker; the producer and the Spark job run from the venv.
# Walkthrough: docs/hadoop/streaming.md
stream-up:    ## start Kafka (KRaft) + kafka-exporter and create the topics
	$(STREAM_COMPOSE) up -d --wait kafka kafka-exporter
	$(KAFKA_CLI)/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic trips --partitions 3 --replication-factor 1
	$(KAFKA_CLI)/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic zone-metrics --partitions 3 --replication-factor 1
	@echo "\nKafka on localhost:$${KAFKA_HOST_PORT:-9092}, Prometheus metrics on localhost:$${KAFKA_EXPORTER_PORT:-9308}/metrics.\nNext, in two terminals: make stream-run   and   make stream-produce"

stream-produce: ## replay bronze trips into Kafka (RATE=events/s, LIMIT=trips, SKIP=trips already sent)
	PYTHONPATH=src $(PY) -u -m urbanflow.stream.producer --rate $(RATE) --limit $(LIMIT) --skip $(SKIP)

stream-run:   ## Spark Structured Streaming: 5-min windowed metrics per zone -> data/stream + Kafka (STREAM_ARGS=...)
	PYTHONPATH=src $(PY) -u -m urbanflow.stream.job $(STREAM_ARGS)

stream-status: ## describe the topics and consumer-group lag
	$(KAFKA_CLI)/kafka-topics.sh --bootstrap-server kafka:29092 --describe --exclude-internal
	$(KAFKA_CLI)/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --all-groups

stream-tail:  ## print the first 5 windowed results from the zone-metrics topic
	$(KAFKA_CLI)/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic zone-metrics --from-beginning --max-messages 5 --property print.key=true

stream-down:  ## stop the streaming stack (Kafka data and data/stream kept)
	$(STREAM_COMPOSE) down

stream-reset: ## stop it AND delete Kafka's volume, checkpoints and data/stream
	$(STREAM_COMPOSE) down -v
	rm -rf $(or $(URBANFLOW_DATA),data)/stream

# ---------------------------------------------------------------- monitoring (opt-in)
# Prometheus + Grafana + cAdvisor + json-exporter; watches the Hadoop, HBase
# and streaming stacks whenever they are running. Walkthrough: docs/hadoop/monitoring.md
MONITOR := docker compose -p urbanflow-monitoring -f docker-compose.monitoring.yml

monitor-up:   ## start Prometheus :9090 + Grafana :3000 (admin/urbanflow), attach to running stacks
	$(MONITOR) up -d
	@sh scripts/monitor_attach.sh
	@echo "\nGrafana    http://localhost:$${GRAFANA_PORT:-3000}  (admin / urbanflow)"
	@echo "Prometheus http://localhost:$${PROMETHEUS_PORT:-9090}/targets"

monitor-attach: ## connect monitoring to a stack started after monitor-up
	@sh scripts/monitor_attach.sh

monitor-down: ## stop the monitoring stack (keeps its data volumes)
	$(MONITOR) down

monitor-status: ## list every scrape target and whether it is up
	@curl -fsS "http://localhost:$${PROMETHEUS_PORT:-9090}/api/v1/targets?state=active" | python3 scripts/monitor_status.py

monitor-reload: ## make Prometheus re-read prometheus.yml / alerts.yml (SIGHUP)
	$(MONITOR) kill -s SIGHUP prometheus && echo reloaded

monitor-check: ## validate the Prometheus config and alert rules with promtool
	docker run --rm --entrypoint promtool -v "$(CURDIR)/monitoring/prometheus:/p:ro" \
	    prom/prometheus:v3.5.0 check config /p/prometheus.yml

monitor-dashboards: ## regenerate the Grafana dashboard JSON
	python3 scripts/build_dashboards.py

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
TIER2_GOLD ?= $(wildcard external/Urbanflow-BDA-data/data/gold)

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

hadoop-load:   ## copy the yellow data/ bronze, silver, gold into HDFS under /urbanflow (+ the real Tier 2 gold, from make tier2-data or TIER2_GOLD=path)
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
