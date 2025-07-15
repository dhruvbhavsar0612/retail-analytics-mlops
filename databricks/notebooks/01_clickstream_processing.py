# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time Clickstream Processing
# MAGIC 
# MAGIC This notebook processes real-time clickstream data from Kafka and performs:
# MAGIC - Data validation and cleansing
# MAGIC - User session analysis
# MAGIC - Product performance metrics
# MAGIC - Real-time aggregations
# MAGIC - Data enrichment and storage

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# DBTITLE 1,Import Libraries and Setup
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# DBTITLE 1,Configuration Parameters
# Get parameters from Databricks job
dbutils.widgets.text("data", "", "Input Data")
dbutils.widgets.text("topic", "retail_clickstream", "Kafka Topic")
dbutils.widgets.text("batch_size", "100", "Batch Size")

# Parse parameters
input_data = dbutils.widgets.get("data")
topic = dbutils.widgets.get("topic")
batch_size = int(dbutils.widgets.get("batch_size"))

# S3 configuration
s3_raw_bucket = "retail-insights-raw-dev"
s3_processed_bucket = "retail-insights-processed-dev"
s3_curated_bucket = "retail-insights-curated-dev"

# Redshift configuration
redshift_host = dbutils.secrets.get(scope="retail-insights", key="redshift-host")
redshift_database = dbutils.secrets.get(scope="retail-insights", key="redshift-database")
redshift_username = dbutils.secrets.get(scope="retail-insights", key="redshift-username")
redshift_password = dbutils.secrets.get(scope="retail-insights", key="redshift-password")

print(f"Processing topic: {topic}")
print(f"Batch size: {batch_size}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Schema Definition

# COMMAND ----------

# DBTITLE 1,Define Clickstream Schema
clickstream_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("page_title", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("product_price", DoubleType(), True),
    StructField("product_brand", StringType(), True),
    StructField("device_info", StructType([
        StructField("type", StringType(), True),
        StructField("os", StringType(), True),
        StructField("browser", StringType(), True)
    ]), True),
    StructField("location", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("city", StringType(), True)
    ]), True),
    StructField("user_agent", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("kafka_metadata", StructType([
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("key", StringType(), True),
        StructField("timestamp", LongType(), True)
    ]), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Ingestion and Parsing

# COMMAND ----------

# DBTITLE 1,Parse Input Data
def parse_input_data(input_data_str):
    """Parse input data from Kafka consumer"""
    try:
        if not input_data_str:
            return None
        
        data = json.loads(input_data_str)
        messages = data.get("messages", [])
        
        if not messages:
            return None
        
        # Convert to DataFrame
        df = spark.createDataFrame(messages, schema=clickstream_schema)
        return df
    
    except Exception as e:
        logger.error(f"Error parsing input data: {e}")
        return None

# Parse the input data
clickstream_df = parse_input_data(input_data)

if clickstream_df is None or clickstream_df.count() == 0:
    print("No data to process")
    dbutils.notebook.exit("No data")

print(f"Processing {clickstream_df.count()} events")

# COMMAND ----------

# DBTITLE 1,Display Sample Data
display(clickstream_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Validation and Cleansing

# COMMAND ----------

# DBTITLE 1,Data Validation
def validate_clickstream_data(df):
    """Validate and cleanse clickstream data"""
    
    # Remove null user_ids and session_ids
    df_clean = df.filter(
        col("user_id").isNotNull() & 
        col("session_id").isNotNull() &
        col("event_type").isNotNull()
    )
    
    # Validate timestamp format
    df_clean = df_clean.filter(
        col("timestamp").isNotNull() &
        (length(col("timestamp")) > 0)
    )
    
    # Validate product data for product-related events
    df_clean = df_clean.filter(
        ~(
            (col("event_type").isin(["page_view", "add_to_cart", "purchase"])) &
            (col("product_id").isNull())
        )
    )
    
    # Remove duplicate events based on event_id
    df_clean = df_clean.dropDuplicates(["event_id"])
    
    return df_clean

# Apply validation
clickstream_clean = validate_clickstream_data(clickstream_df)

print(f"After validation: {clickstream_clean.count()} events")

# COMMAND ----------

# DBTITLE 1,Data Enrichment
def enrich_clickstream_data(df):
    """Enrich clickstream data with additional fields"""
    
    # Parse timestamp
    df_enriched = df.withColumn(
        "event_timestamp", 
        to_timestamp(col("timestamp"))
    ).withColumn(
        "event_date", 
        date(col("event_timestamp"))
    ).withColumn(
        "event_hour", 
        hour(col("event_timestamp"))
    )
    
    # Add processing metadata
    df_enriched = df_enriched.withColumn(
        "processed_at", 
        current_timestamp()
    ).withColumn(
        "batch_id", 
        lit(f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    )
    
    # Add derived fields
    df_enriched = df_enriched.withColumn(
        "is_mobile", 
        col("device_info.type") == "mobile"
    ).withColumn(
        "is_tablet", 
        col("device_info.type") == "tablet"
    ).withColumn(
        "is_desktop", 
        col("device_info.type") == "desktop"
    )
    
    # Add session duration (placeholder - would need session start/end events)
    df_enriched = df_enriched.withColumn(
        "session_duration_seconds", 
        lit(0)  # Placeholder
    )
    
    return df_enriched

# Apply enrichment
clickstream_enriched = enrich_clickstream_data(clickstream_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real-Time Analytics

# COMMAND ----------

# DBTITLE 1,User Session Analysis
def analyze_user_sessions(df):
    """Analyze user session patterns"""
    
    # Session-level aggregations
    session_metrics = df.groupBy("session_id", "user_id", "event_date").agg(
        count("*").alias("page_views"),
        countDistinct("product_id").alias("unique_products_viewed"),
        sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_events"),
        sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_events"),
        sum(when(col("event_type") == "search", 1).otherwise(0)).alias("search_events"),
        min("event_timestamp").alias("session_start"),
        max("event_timestamp").alias("session_end"),
        collect_list("product_category").alias("categories_viewed"),
        collect_list("device_info.type").alias("devices_used")
    )
    
    # Calculate session duration
    session_metrics = session_metrics.withColumn(
        "session_duration_seconds",
        unix_timestamp(col("session_end")) - unix_timestamp(col("session_start"))
    )
    
    # Add conversion flags
    session_metrics = session_metrics.withColumn(
        "converted_to_purchase", 
        col("purchase_events") > 0
    ).withColumn(
        "converted_to_cart", 
        col("add_to_cart_events") > 0
    )
    
    return session_metrics

# Analyze sessions
session_analysis = analyze_user_sessions(clickstream_enriched)

display(session_analysis.limit(10))

# COMMAND ----------

# DBTITLE 1,Product Performance Metrics
def analyze_product_performance(df):
    """Analyze product performance metrics"""
    
    # Product-level aggregations
    product_metrics = df.filter(col("product_id").isNotNull()).groupBy(
        "product_id", "product_name", "product_category", "product_brand", "event_date"
    ).agg(
        count("*").alias("total_views"),
        countDistinct("user_id").alias("unique_users"),
        countDistinct("session_id").alias("unique_sessions"),
        sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
        sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
        avg("product_price").alias("avg_price"),
        sum("product_price").alias("total_value")
    )
    
    # Calculate conversion rates
    product_metrics = product_metrics.withColumn(
        "view_to_cart_rate", 
        when(col("total_views") > 0, col("add_to_cart_count") / col("total_views")).otherwise(0)
    ).withColumn(
        "view_to_purchase_rate", 
        when(col("total_views") > 0, col("purchase_count") / col("total_views")).otherwise(0)
    ).withColumn(
        "cart_to_purchase_rate", 
        when(col("add_to_cart_count") > 0, col("purchase_count") / col("add_to_cart_count")).otherwise(0)
    )
    
    return product_metrics

# Analyze product performance
product_analysis = analyze_product_performance(clickstream_enriched)

display(product_analysis.limit(10))

# COMMAND ----------

# DBTITLE 1,Real-Time Aggregations
def generate_real_time_aggregations(df):
    """Generate real-time aggregations for dashboard"""
    
    # Hourly aggregations
    hourly_metrics = df.groupBy("event_date", "event_hour").agg(
        count("*").alias("total_events"),
        countDistinct("user_id").alias("unique_users"),
        countDistinct("session_id").alias("unique_sessions"),
        sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
        sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart"),
        sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
        sum(when(col("event_type") == "search", 1).otherwise(0)).alias("searches")
    )
    
    # Category performance
    category_metrics = df.filter(col("product_category").isNotNull()).groupBy(
        "product_category", "event_date"
    ).agg(
        count("*").alias("category_views"),
        countDistinct("user_id").alias("unique_users"),
        sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart"),
        sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases")
    )
    
    # Device analytics
    device_metrics = df.groupBy("event_date").agg(
        sum(when(col("is_mobile"), 1).otherwise(0)).alias("mobile_events"),
        sum(when(col("is_desktop"), 1).otherwise(0)).alias("desktop_events"),
        sum(when(col("is_tablet"), 1).otherwise(0)).alias("tablet_events")
    )
    
    return hourly_metrics, category_metrics, device_metrics

# Generate aggregations
hourly_metrics, category_metrics, device_metrics = generate_real_time_aggregations(clickstream_enriched)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Storage

# COMMAND ----------

# DBTITLE 1,Save to S3 - Raw Data
def save_to_s3_raw(df, bucket, topic):
    """Save raw clickstream data to S3"""
    
    # Add partitioning
    df_partitioned = df.withColumn("year", year(col("event_timestamp"))) \
                      .withColumn("month", month(col("event_timestamp"))) \
                      .withColumn("day", dayofmonth(col("event_timestamp"))) \
                      .withColumn("hour", hour(col("event_timestamp")))
    
    # Save to S3
    s3_path = f"s3a://{bucket}/clickstream/{topic}/year={year(current_date())}/month={month(current_date())}/day={day(current_date())}/hour={hour(current_date())}"
    
    df_partitioned.write \
        .mode("append") \
        .partitionBy("year", "month", "day", "hour") \
        .parquet(s3_path)
    
    print(f"Saved {df_partitioned.count()} records to {s3_path}")

# Save raw data
save_to_s3_raw(clickstream_enriched, s3_raw_bucket, topic)

# COMMAND ----------

# DBTITLE 1,Save to S3 - Processed Data
def save_to_s3_processed(session_df, product_df, hourly_df, category_df, device_df, bucket):
    """Save processed analytics data to S3"""
    
    # Session analytics
    session_path = f"s3a://{bucket}/analytics/sessions/year={year(current_date())}/month={month(current_date())}/day={day(current_date())}"
    session_df.write.mode("append").partitionBy("year", "month", "day").parquet(session_path)
    
    # Product analytics
    product_path = f"s3a://{bucket}/analytics/products/year={year(current_date())}/month={month(current_date())}/day={day(current_date())}"
    product_df.write.mode("append").partitionBy("year", "month", "day").parquet(product_path)
    
    # Hourly metrics
    hourly_path = f"s3a://{bucket}/analytics/hourly/year={year(current_date())}/month={month(current_date())}/day={day(current_date())}"
    hourly_df.write.mode("append").partitionBy("year", "month", "day").parquet(hourly_path)
    
    # Category metrics
    category_path = f"s3a://{bucket}/analytics/categories/year={year(current_date())}/month={month(current_date())}/day={day(current_date())}"
    category_df.write.mode("append").partitionBy("year", "month", "day").parquet(category_path)
    
    # Device metrics
    device_path = f"s3a://{bucket}/analytics/devices/year={year(current_date())}/month={month(current_date())}/day={day(current_date())}"
    device_df.write.mode("append").partitionBy("year", "month", "day").parquet(device_path)
    
    print("Saved processed analytics data to S3")

# Save processed data
save_to_s3_processed(
    session_analysis, 
    product_analysis, 
    hourly_metrics, 
    category_metrics, 
    device_metrics, 
    s3_processed_bucket
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Redshift Integration

# COMMAND ----------

# DBTITLE 1,Write to Redshift
def write_to_redshift(df, table_name, redshift_config):
    """Write DataFrame to Redshift"""
    
    # Configure Redshift connection
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:redshift://{redshift_config['host']}:5439/{redshift_config['database']}") \
        .option("dbtable", table_name) \
        .option("user", redshift_config['username']) \
        .option("password", redshift_config['password']) \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .mode("append") \
        .save()
    
    print(f"Wrote {df.count()} records to Redshift table: {table_name}")

# Redshift configuration
redshift_config = {
    "host": redshift_host,
    "database": redshift_database,
    "username": redshift_username,
    "password": redshift_password
}

# Write key metrics to Redshift
write_to_redshift(hourly_metrics, "hourly_clickstream_metrics", redshift_config)
write_to_redshift(product_analysis, "product_performance_metrics", redshift_config)
write_to_redshift(category_metrics, "category_performance_metrics", redshift_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Monitoring

# COMMAND ----------

# DBTITLE 1,Processing Summary
# Generate processing summary
summary_stats = {
    "total_events_processed": clickstream_enriched.count(),
    "unique_users": clickstream_enriched.select("user_id").distinct().count(),
    "unique_sessions": clickstream_enriched.select("session_id").distinct().count(),
    "unique_products": clickstream_enriched.filter(col("product_id").isNotNull()).select("product_id").distinct().count(),
    "event_types": clickstream_enriched.groupBy("event_type").count().collect(),
    "processing_timestamp": datetime.now().isoformat(),
    "batch_id": f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
}

print("Processing Summary:")
for key, value in summary_stats.items():
    print(f"  {key}: {value}")

# COMMAND ----------

# DBTITLE 1,Data Quality Metrics
# Calculate data quality metrics
total_events = clickstream_df.count()
valid_events = clickstream_enriched.count()
data_quality_rate = (valid_events / total_events) * 100 if total_events > 0 else 0

print(f"Data Quality Metrics:")
print(f"  Total events received: {total_events}")
print(f"  Valid events processed: {valid_events}")
print(f"  Data quality rate: {data_quality_rate:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup and Exit

# COMMAND ----------

# DBTITLE 1,Cleanup
# Clean up temporary DataFrames
clickstream_df.unpersist()
clickstream_clean.unpersist()
clickstream_enriched.unpersist()

print("Clickstream processing completed successfully!") 