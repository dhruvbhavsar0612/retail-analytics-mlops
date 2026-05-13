# AGENTS.md

## Project Overview
Python-based data engineering portfolio project (Real-Time Retail Insights Platform). Flat layout — no build system, no `pyproject.toml`, no `setup.py`. All modules consume packages from `requirements.txt` directly.

## Python Environment
- Python 3.12+, virtual environment at `.venv` in the repo root.
- Activate: `source .venv/bin/activate` (from repo root).
- `kafka-python==2.0.2` is broken on Python 3.12. The maintained fork `kafka-python-ng` is installed instead, using the same `kafka` import namespace.
- `requirements.txt` has a duplicate `orjson==3.9.10` entry (lines 57 and 102); pip handles this safely.
- `databricks-api-client` (in requirements.txt) does not exist on PyPI; use `databricks-connect` / `databricks-sdk` instead.

## Linting, Formatting & Type-Checking
Run from repo root with venv active:
- `python -m flake8 --max-line-length=120 kafka/ scripts/ airflow/`
- `python -m black --check kafka/ scripts/ airflow/`
- `python -m mypy --ignore-missing-imports kafka/ scripts/`

The Airflow DAG (`airflow/dags/retail_insights_pipeline.py`) uses Databricks imports and Airflow template variables (`{{ var.value.* }}`), so it is excluded from mypy coverage.

## Testing
- `python -m pytest tests/ -v` — runs unit tests (clickstream producer, synthetic data, notebook syntax).
- Tests run without Kafka/AWS/Databricks — they validate data generation logic, invariants, and notebook syntax.
- CI via `.github/workflows/mlops-ci.yml` runs on push/PR to `main`.

## Architecture (what actually exists)
```
kafka/
  producers/clickstream_producer.py   — CLI entrypoint for clickstream data generation
  consumers/databricks_consumer.py    — CLI entrypoint consuming Kafka → Databricks
airflow/dags/retail_insights_pipeline.py — Single Airflow DAG
databricks/notebooks/01_clickstream_processing.py — Databricks notebook
databricks/notebooks/02_ml_purchase_propensity.py — ML training + MLflow + Redshift notebook
scripts/deploy.py                     — Deployment orchestrator (requires config/deployment.json)
terraform/
  main.tf, variables.tf
  modules/{ec2,redshift,s3,vpc}       — only 4 modules exist; main.tf references modules not in repo (kms,iam,key_pair,cloudwatch,sns,alarms)
monitoring/
  prometheus/prometheus.yml
  grafana/dashboards/retail_insights_dashboard.json
docs/demo_guide.md
tests/
  test_clickstream_producer.py        — validates event generation logic
  test_synthetic_data.py              — validates ML session data generation
  test_notebook_syntax.py             — validates notebook Python parseability
```

## Known Missing Files (referenced in code/README but absent)
- `config/deployment.json` — required by `scripts/deploy.py`
- `kafka/docker-compose.yml`, `airflow/docker-compose.yml`
- Most terraform modules referenced in `terraform/main.tf`

## Running the Data Generator Without Kafka
The clickstream producer can generate events without a Kafka broker:
```python
import sys; sys.path.insert(0, 'kafka/producers')
from clickstream_producer import RetailClickstreamProducer
producer = RetailClickstreamProducer('localhost:9092', 'retail_clickstream')
event = producer.generate_event()  # Returns a dict, no Kafka connection needed
```

## External Services
Full end-to-end operation requires AWS (S3, Redshift, EC2), Databricks, Kafka, and Airflow — none available locally. Local development is limited to linting, type-checking, and code inspection.
