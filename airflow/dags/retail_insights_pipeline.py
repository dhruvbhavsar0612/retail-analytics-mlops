"""
Retail Insights Data Pipeline DAG
Orchestrates the complete real-time retail analytics pipeline
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.sensors.redshift_data import RedshiftDataSensor
from airflow.providers.http.sensors.http import HttpSensor

# Default arguments
default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

# DAG definition
dag = DAG(
    "retail_insights_pipeline",
    default_args=default_args,
    description="Real-time retail analytics pipeline",
    schedule_interval="*/5 * * * *",  # Every 5 minutes
    max_active_runs=1,
    tags=["retail", "analytics", "real-time"],
)

# Task definitions
start = DummyOperator(task_id="start", dag=dag)

# Check Kafka health
check_kafka_health = HttpSensor(
    task_id="check_kafka_health",
    http_conn_id="kafka_health_check",
    endpoint="/health",
    request_params={},
    response_check=lambda response: response.status_code == 200,
    poke_interval=30,
    timeout=300,
    dag=dag,
)

# Check Databricks workspace health
check_databricks_health = HttpSensor(
    task_id="check_databricks_health",
    http_conn_id="databricks_conn",
    endpoint="/api/2.0/clusters/list",
    request_params={},
    response_check=lambda response: response.status_code == 200,
    poke_interval=60,
    timeout=300,
    dag=dag,
)

# Trigger Databricks clickstream processing job
trigger_clickstream_processing = DatabricksRunNowOperator(
    task_id="trigger_clickstream_processing",
    databricks_conn_id="databricks_conn",
    job_id="{{ var.value.clickstream_processing_job_id }}",
    notebook_params={"data": "{{ var.value.kafka_data }}", "topic": "retail_clickstream", "batch_size": "100"},
    dag=dag,
)

# Trigger Databricks transaction processing job
trigger_transaction_processing = DatabricksRunNowOperator(
    task_id="trigger_transaction_processing",
    databricks_conn_id="databricks_conn",
    job_id="{{ var.value.transaction_processing_job_id }}",
    notebook_params={"data": "{{ var.value.kafka_data }}", "topic": "retail_transactions", "batch_size": "50"},
    dag=dag,
)

# Trigger Databricks inventory processing job
trigger_inventory_processing = DatabricksRunNowOperator(
    task_id="trigger_inventory_processing",
    databricks_conn_id="databricks_conn",
    job_id="{{ var.value.inventory_processing_job_id }}",
    notebook_params={"data": "{{ var.value.kafka_data }}", "topic": "retail_inventory", "batch_size": "25"},
    dag=dag,
)


# Data quality check function
def perform_data_quality_check(**context):
    """Perform data quality checks on processed data"""
    import boto3
    from datetime import datetime, timedelta

    # Initialize S3 client
    s3_client = boto3.client("s3")

    # Check data freshness
    bucket_name = "retail-insights-processed-dev"
    prefix = f"analytics/hourly/year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}"

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if "Contents" in response:
            latest_file = max(response["Contents"], key=lambda x: x["LastModified"])
            data_age = datetime.now(latest_file["LastModified"].tzinfo) - latest_file["LastModified"]

            if data_age > timedelta(hours=1):
                raise Exception(f"Data is too old: {data_age}")

            print(f"Data quality check passed. Latest data age: {data_age}")
        else:
            raise Exception("No data found for today")

    except Exception as e:
        print(f"Data quality check failed: {e}")
        raise


# Data quality check task
data_quality_check = PythonOperator(task_id="data_quality_check", python_callable=perform_data_quality_check, dag=dag)

# Load data to Redshift
load_to_redshift = RedshiftDataOperator(
    task_id="load_to_redshift",
    cluster_identifier="{{ var.value.redshift_cluster_id }}",
    database="{{ var.value.redshift_database }}",
    db_user="{{ var.value.redshift_username }}",
    sql="""
    COPY hourly_clickstream_metrics
    FROM 's3://retail-insights-processed-dev/analytics/hourly/'
    IAM_ROLE 'arn:aws:iam::{{ var.value.aws_account_id }}:role/RedshiftS3Role'
    FORMAT AS PARQUET;
    """,
    dag=dag,
)

# Monitor Redshift load
monitor_redshift_load = RedshiftDataSensor(
    task_id="monitor_redshift_load",
    cluster_identifier="{{ var.value.redshift_cluster_id }}",
    database="{{ var.value.redshift_database }}",
    db_user="{{ var.value.redshift_username }}",
    dag=dag,
)


# Generate daily summary
def generate_daily_summary(**context):
    """Generate daily summary report"""
    import boto3

    # Initialize Redshift client
    redshift_client = boto3.client("redshift-data")

    # Query daily metrics
    query = """
    SELECT
        DATE(event_date) as report_date,
        COUNT(*) as total_events,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT session_id) as unique_sessions,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as total_purchases,
        SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) as total_add_to_cart,
        AVG(CASE WHEN event_type = 'purchase' THEN product_price ELSE NULL END) as avg_purchase_value
    FROM hourly_clickstream_metrics
    WHERE event_date >= CURRENT_DATE - 1
    GROUP BY DATE(event_date)
    """

    try:
        response = redshift_client.execute_statement(
            ClusterIdentifier="{{ var.value.redshift_cluster_id }}",
            Database="{{ var.value.redshift_database }}",
            DbUser="{{ var.value.redshift_username }}",
            Sql=query,
        )

        print(f"Daily summary generated successfully. Query ID: {response['Id']}")

    except Exception as e:
        print(f"Failed to generate daily summary: {e}")
        raise


# Daily summary task
daily_summary = PythonOperator(task_id="generate_daily_summary", python_callable=generate_daily_summary, dag=dag)


# Monitor pipeline health
def monitor_pipeline_health(**context):
    """Monitor overall pipeline health"""
    import requests
    from datetime import datetime

    health_checks = {
        "kafka": "http://kafka-broker:9092",
        "databricks": "{{ var.value.databricks_host }}/api/2.0/clusters/list",
        "redshift": "{{ var.value.redshift_host }}:5439",
        "s3": "https://s3.amazonaws.com",
    }

    health_status = {}

    for service, endpoint in health_checks.items():
        try:
            if service == "kafka":
                # Kafka health check via JMX
                response = requests.get(f"{endpoint}/health", timeout=10)
            elif service == "databricks":
                response = requests.get(
                    endpoint, headers={"Authorization": "Bearer {{ var.value.databricks_token }}"}, timeout=10
                )
            elif service == "redshift":
                # Redshift health check via connection test
                import psycopg2

                conn = psycopg2.connect(
                    host="{{ var.value.redshift_host }}",
                    port=5439,
                    database="{{ var.value.redshift_database }}",
                    user="{{ var.value.redshift_username }}",
                    password="{{ var.value.redshift_password }}",
                )
                conn.close()
                response = type("Response", (), {"status_code": 200})()
            else:
                response = requests.get(endpoint, timeout=10)

            health_status[service] = "healthy" if response.status_code == 200 else "unhealthy"

        except Exception as e:
            health_status[service] = f"unhealthy: {str(e)}"

    # Log health status
    print(f"Pipeline health status at {datetime.now()}:")
    for service, status in health_status.items():
        print(f"  {service}: {status}")

    # Raise exception if any service is unhealthy
    unhealthy_services = [s for s, status in health_status.items() if "unhealthy" in status]
    if unhealthy_services:
        raise Exception(f"Unhealthy services: {unhealthy_services}")


# Pipeline health monitoring
pipeline_health = PythonOperator(task_id="monitor_pipeline_health", python_callable=monitor_pipeline_health, dag=dag)

# End task
end = DummyOperator(task_id="end", dag=dag)

# Task dependencies
start >> [check_kafka_health, check_databricks_health]

[check_kafka_health, check_databricks_health] >> [
    trigger_clickstream_processing,
    trigger_transaction_processing,
    trigger_inventory_processing,
]

[trigger_clickstream_processing, trigger_transaction_processing, trigger_inventory_processing] >> data_quality_check

data_quality_check >> load_to_redshift >> monitor_redshift_load

monitor_redshift_load >> [daily_summary, pipeline_health]

[daily_summary, pipeline_health] >> end
