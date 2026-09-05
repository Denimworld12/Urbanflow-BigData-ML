# What this project produces

## Artefacts you will hand in

| Artefact | Produced by | Where it lands |
|---|---|---|
| Working Spark pipeline, 6 stages | the whole build | `src/urbanflow/` |
| Bronze manifest — proof of what you processed | `make ingest` | `data/raw/manifest.csv` |
| Data quality report — rows removed per rule | `make curate` | `data/curated/quality_report.json` |
| Six analytical tables | `make gold` | `data/gold/` |
| Trained model + metrics vs a naive baseline | `make model` | `data/gold/model_results.json` |
| Benchmark results — 4 experiments | `make bench` | `data/gold/benchmarks.{json,md}` |
| Interactive dashboard, 4 tabs | `make dash` | `localhost:8501` |
| Curated schema contract | Member A | `schema/curated.md` |
| Unit tests | included | `tests/` |
| Written report | `/report` | `docs/` |

## Findings the data will actually give you

These are real, they are in the data, and they are what separates a project that
computed averages from one that found something out.

1. **Cash tips are never recorded.** `tip_amount` is always 0 for
   `payment_type = 2`. Every "average tip" figure published without that caveat
   is wrong. Quantify the bias and correct for it.
2. **The city measurably slows at rush hour.** Average speed by hour and borough
   produces a clean, intuitive curve — a congestion proxy from taxi meters.
3. **Demand is bimodal and borough-specific.** Manhattan peaks differ from outer
   boroughs; weekend patterns differ from weekdays.
4. **Airport trips are a distinct population** — longer, more expensive, with a
   different hourly profile. Worth separating in any model.
5. **Roughly 3–5% of raw rows are unusable**, including timestamps from 2001 and
   2098 in files that should only contain one month.
6. **Parquet beats CSV by roughly 5×** on the same query and the same rows.
   (Measured 5.4× on the reference synthetic run.)
7. **Broadcast join beats sort-merge** for the zone lookup — measurable, and
   visible as `BroadcastHashJoin` vs `SortMergeJoin` in `.explain()`.
8. **Speedup is sub-linear, and on small data can be negative.** More cores made
   the reference run *slower* at 190k rows, because coordination overhead
   exceeded the parallel saving. That crossover is one of the most interesting
   things you can put in the report.

## Concepts you will be able to defend in the viva

Lazy evaluation and the DAG · transformations vs actions · partitions and
shuffles · partition pruning · predicate pushdown · broadcast vs sort-merge
joins · columnar storage and why Parquet wins · splittability · driver vs
executor and what local mode really is · why speedup is sub-linear (Amdahl) ·
time-based train/test splitting and why a random split leaks · baselines · the
medallion architecture.

## Skills for a CV

PySpark (DataFrames, Spark SQL, MLlib) · designing a layered data pipeline ·
data quality engineering · reading and acting on a query plan · benchmarking
methodology · DuckDB · Streamlit · working as a three-person team against a
frozen schema contract.

## What decides the grade

Most teams that pick a taxi dataset produce charts. These four things are what
lift it, in rough order of marks-per-hour:

1. **The benchmark chapter.** Four experiments, each changing one variable, each
   explained. Almost nobody does this properly and it is the clearest evidence
   that you understand distributed processing rather than just Spark syntax.
2. **The data quality report.** A table of rows removed per rule, with the
   overlap caveat stated. It proves you looked at the data.
3. **The cash-tip finding.** One genuine insight, honestly caveated, beats ten
   charts.
4. **Honesty about what did not work.** A sub-linear speedup explained
   correctly, or a model that barely beats its baseline and says so, reads as
   competence. A suspiciously clean result reads as a result nobody checked.

## Realistic effort

| Stage | Person | Hours |
|---|---|---|
| Setup, environment, repo | all three | 4 each |
| Ingest + manifest | A | 6 |
| Cleaning + schema freeze | A | 14 |
| Gold tables | B | 12 |
| Model | B | 8 |
| Benchmarks | B | 6 |
| Dashboard | C | 16 |
| Report + slides + rehearsal | all three | 12 each |

Roughly **45–55 hours per person** across six weeks. The dashboard and the
report are consistently underestimated; the Spark code is usually easier than
expected.
