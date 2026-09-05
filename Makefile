# UrbanFlow — one command per pipeline stage.
# Every target is safe to re-run.
PY := .venv/bin/python
TIER ?= 0
DATASET ?= yellow

.PHONY: help setup check synth ingest curate gold model bench dash all clean-data test

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

bench:        ## run the benchmark suite (format, partitioning, join, cores)
	PYTHONPATH=src $(PY) -m urbanflow.analyze.benchmark --dataset $(DATASET)

dash:         ## launch the dashboard
	PYTHONPATH=src .venv/bin/streamlit run src/urbanflow/dashboard/app.py

test:         ## run unit tests (no network, no big data)
	PYTHONPATH=src .venv/bin/pytest -q tests/

all: synth curate gold model bench  ## full pipeline on synthetic data

clean-data:   ## delete derived layers, keep bronze
	rm -rf data/curated data/gold data/bench data/models data/spill
