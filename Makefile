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

.PHONY: help setup check synth ingest curate gold model bench dash all clean-data test \
        stream-up stream-produce stream-run stream-status stream-tail stream-down stream-reset

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

all: synth curate gold model bench  ## full pipeline on synthetic data

clean-data:   ## delete derived layers, keep bronze
	rm -rf data/curated data/gold data/bench data/models data/spill

# ---------------------------------------------------------------- real-time path
stream-up:    ## start Kafka (KRaft) + kafka-exporter and create the topics
	$(STREAM_COMPOSE) up -d --wait kafka kafka-exporter
	$(KAFKA_CLI)/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic trips --partitions 3 --replication-factor 1
	$(KAFKA_CLI)/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic zone-metrics --partitions 3 --replication-factor 1
	@echo "\nKafka on localhost:9092, Prometheus metrics on localhost:9308/metrics.\nNext, in two terminals: make stream-run   and   make stream-produce"

stream-produce: ## replay bronze trips into Kafka (RATE=events/s, LIMIT=trips, SKIP=trips already sent)
	PYTHONPATH=src $(PY) -u -m urbanflow.stream.producer --rate $(RATE) --limit $(LIMIT) --skip $(SKIP)

stream-run:   ## Spark Structured Streaming: 5-min windowed metrics per zone -> data/stream + Kafka
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
	rm -rf data/stream
