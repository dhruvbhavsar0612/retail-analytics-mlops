# Setup Manual

This manual explains how to configure the Real-Time Retail Insights Platform from a fresh clone. The project is a Python data engineering portfolio that uses AWS, Terraform, Kafka, Databricks, Airflow, Redshift, Prometheus, and Grafana.

## 1. Prerequisites

Install these tools before running the full platform:

- Python 3.8+; Python 3.12 works for local code checks, but some pinned packages in `requirements.txt` may need compatible replacements.
- AWS CLI with permissions for S3, EC2, IAM, KMS, Redshift, CloudWatch, and SNS.
- Terraform 1.0+.
- Docker and Docker Compose for local Kafka, Airflow, and monitoring services if you add or restore compose files.
- Databricks CLI and a Databricks workspace access token.

Full end-to-end execution requires live AWS and Databricks resources. Local development can still validate the data generator and Python code without those services.

## 2. Create the Python environment

From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

If Python 3.12 rejects older pins, install compatible replacements for the known problem packages:

```bash
python -m pip install kafka-python-ng databricks-connect databricks-sdk
```

## 3. Configure environment variables

Copy the example file and fill in real values:

```bash
cp .env.example .env
chmod 600 .env
```

Load it in the current shell before running commands that read environment variables:

```bash
set -a
source .env
set +a
```

Important: the current Python scripts do not automatically call `python-dotenv`. The `.env` file is a central setup checklist; shell commands, Terraform, Airflow, Databricks, and deployment JSON still need the values loaded or copied into their own config stores.

## 4. Configure deployment JSON

The deployment helper at `scripts/deploy.py` expects `config/deployment.json`.

```bash
cp config/deployment.example.json config/deployment.json
```

Edit `config/deployment.json` and replace every placeholder. The current deployment script uses `databricks_host` and `databricks_token` when starting the Kafka consumer.

## 5. Configure AWS and Terraform

Authenticate AWS:

```bash
aws configure
aws sts get-caller-identity
```

Create the Terraform state bucket before `terraform init`, because `terraform/main.tf` uses an S3 backend:

```bash
aws s3 mb "s3://${S3_TERRAFORM_STATE_BUCKET}" --region "${AWS_REGION}"
```

Plan infrastructure:

```bash
cd terraform
terraform init
terraform plan
```

`TF_VAR_*` entries in `.env` map to Terraform variables such as `redshift_password`, `environment`, and `databricks_token`.

## 6. Configure Databricks

Log in and create the workspace location used by the deployment script:

```bash
databricks configure --token --host "${DATABRICKS_HOST}"
databricks workspace mkdirs /Shared/RetailInsights
databricks fs mkdirs dbfs:/retail-insights
```

Create the secret scope and Redshift secrets used by `databricks/notebooks/01_clickstream_processing.py`:

```bash
databricks secrets create-scope "${DATABRICKS_SECRET_SCOPE}"
databricks secrets put-secret "${DATABRICKS_SECRET_SCOPE}" redshift-host --string-value "${REDSHIFT_HOST}"
databricks secrets put-secret "${DATABRICKS_SECRET_SCOPE}" redshift-database --string-value "${REDSHIFT_DATABASE}"
databricks secrets put-secret "${DATABRICKS_SECRET_SCOPE}" redshift-username --string-value "${REDSHIFT_USERNAME}"
databricks secrets put-secret "${DATABRICKS_SECRET_SCOPE}" redshift-password --string-value "${REDSHIFT_PASSWORD}"
```

Import the notebook:

```bash
databricks workspace import databricks/notebooks/01_clickstream_processing.py /Shared/RetailInsights/01_clickstream_processing --language PYTHON --overwrite
```

## 7. Configure Airflow

The DAG in `airflow/dags/retail_insights_pipeline.py` expects:

- Connections: `databricks_conn` and `kafka_health_check`.
- Variables: `clickstream_processing_job_id`, `transaction_processing_job_id`, `inventory_processing_job_id`, `kafka_data`, `redshift_cluster_id`, `redshift_database`, `redshift_username`, `redshift_password`, `redshift_host`, `aws_account_id`, `databricks_host`, and `databricks_token`.

If using Airflow environment variables, the `AIRFLOW_VAR_*` values in `.env.example` provide the matching names.

## 8. Run the Kafka data generator

With Kafka reachable at `KAFKA_BOOTSTRAP_SERVERS`, generate clickstream events:

```bash
python kafka/producers/clickstream_producer.py \
  --bootstrap-servers "${KAFKA_BOOTSTRAP_SERVERS}" \
  --topic "${KAFKA_TOPIC_CLICKSTREAM}" \
  --events-per-second "${KAFKA_EVENTS_PER_SECOND}" \
  --duration-minutes 5
```

Run the Databricks forwarding consumer:

```bash
python kafka/consumers/databricks_consumer.py \
  --bootstrap-servers "${KAFKA_BOOTSTRAP_SERVERS}" \
  --topics "${KAFKA_TOPIC_CLICKSTREAM}" "${KAFKA_TOPIC_TRANSACTIONS}" "${KAFKA_TOPIC_INVENTORY}" \
  --databricks-host "${DATABRICKS_HOST}" \
  --databricks-token "${DATABRICKS_TOKEN}"
```

## 9. Run the deployment helper

After AWS, Terraform, Kafka, Databricks, Airflow, and monitoring prerequisites are available:

```bash
python scripts/deploy.py --config config/deployment.json
```

Use `--skip-tests` if no test suite exists yet:

```bash
python scripts/deploy.py --config config/deployment.json --skip-tests
```

## 10. Current repository limitations

These are setup blockers to resolve before a complete one-command deployment:

- `kafka/docker-compose.yml`, `airflow/docker-compose.yml`, and a monitoring compose file are referenced by docs or scripts but are not present.
- `terraform/main.tf` references modules such as `kms`, `iam`, `redshift`, `ec2`, `key_pair`, `cloudwatch`, `sns`, and `alarms`; only some module directories currently exist.
- The Databricks notebook hard-codes S3 bucket names for the `dev` environment.
- No `tests/` directory exists, so `python -m pytest` reports no collected tests.
