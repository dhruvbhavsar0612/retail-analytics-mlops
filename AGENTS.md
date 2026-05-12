# AGENTS.md

## Cursor Cloud specific instructions

### Project Overview
This is a Python-based data engineering portfolio project (Real-Time Retail Insights Platform) that demonstrates an end-to-end real-time data pipeline. It is **not** a monorepo and has no JavaScript/TypeScript.

### Python Environment
- Use Python 3.12+ with a virtual environment at `.venv` in the workspace root.
- Activate with: `source /workspace/.venv/bin/activate`
- The `requirements.txt` pins older versions that are not all compatible with Python 3.12. The update script installs compatible versions using `>=` constraints where exact pins fail. Key note: `kafka-python==2.0.2` is broken on Python 3.12; the maintained fork `kafka-python-ng` is installed instead (same API under the `kafka` namespace).
- The `requirements.txt` has a duplicate `orjson==3.9.10` entry (lines 57 and 102); pip handles this gracefully.
- `databricks-api-client` (listed in requirements.txt) does not exist on PyPI; `databricks-connect` and `databricks-sdk` are installed instead.

### Linting & Formatting
Commands (run from workspace root with venv active):
- `python -m flake8 --max-line-length=120 kafka/ scripts/ airflow/`
- `python -m black --check kafka/ scripts/ airflow/`
- `python -m mypy --ignore-missing-imports kafka/ scripts/`

Note: The Airflow DAG file (`airflow/dags/retail_insights_pipeline.py`) uses Databricks-specific imports and Airflow template variables, so mypy cannot type-check it cleanly.

### Testing
- No test files exist in the repository currently (the `tests/` directory mentioned in README is missing).
- `python -m pytest` exits with code 5 (no tests collected), which is expected.

### Running the Data Generator (Hello World)
The clickstream producer can generate events without a Kafka broker for demonstration:
```python
import sys; sys.path.insert(0, 'kafka/producers')
from clickstream_producer import RetailClickstreamProducer
producer = RetailClickstreamProducer('localhost:9092', 'retail_clickstream')
event = producer.generate_event()  # Returns a dict, no Kafka connection needed
```

### External Services
Full end-to-end pipeline operation requires AWS (S3, Redshift, EC2), Databricks, Kafka, and Airflow — none of which are available in the Cloud Agent VM. Local development focuses on linting, type checking, data generation logic, and unit testing.

### Missing Files
Several files referenced in `README.md` do not exist in the repo:
- `kafka/docker-compose.yml` and `airflow/docker-compose.yml`
- The `tests/` directory
- `config/deployment.json` (required by `scripts/deploy.py`)

### Terraform
Terraform is **not** pre-installed in the Cloud Agent VM. The `terraform/` directory contains IaC configs but cannot be validated without installing Terraform.
