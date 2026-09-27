# UrbanFlow — one command per pipeline stage.
# Every target is safe to re-run.
PY := .venv/bin/python
TIER ?= 0
DATASET ?= yellow

.PHONY: help setup check synth ingest curate gold model bench dash all clean-data test \
        monitor-up monitor-attach monitor-down monitor-status monitor-reload monitor-check \
        monitor-dashboards

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[1m%-12s\033[0m %s\n", $$1, $$2}'

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

# --- Monitoring (opt-in): Prometheus + Grafana + cAdvisor + json-exporter ---
# Watches the Hadoop, HBase and streaming stacks whenever they are running.
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

monitor-reload: ## make Prometheus re-read prometheus.yml / alerts.yml
	curl -fsS -X POST "http://localhost:$${PROMETHEUS_PORT:-9090}/-/reload" && echo reloaded

monitor-check: ## validate the Prometheus config and alert rules with promtool
	docker run --rm --entrypoint promtool -v "$(CURDIR)/monitoring/prometheus:/p:ro" \
	    prom/prometheus:v3.5.0 check config /p/prometheus.yml

monitor-dashboards: ## regenerate the Grafana dashboard JSON
	python3 scripts/build_dashboards.py
