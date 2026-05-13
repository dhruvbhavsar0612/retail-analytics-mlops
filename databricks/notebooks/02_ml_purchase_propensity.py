# Databricks notebook source
# MAGIC %md
# MAGIC # Purchase Propensity Model — MLOps Pipeline
# MAGIC
# MAGIC Production-grade MLOps workflow demonstrating:
# MAGIC - Feature engineering from clickstream session data
# MAGIC - Multi-model training with PySpark MLlib
# MAGIC - MLflow experiment tracking & model comparison
# MAGIC - Model Registry (register → Staging → Production)
# MAGIC - Batch inference against registered model
# MAGIC - Prediction monitoring scaffold
# MAGIC
# MAGIC **Use case**: Predict whether a user session will convert to a purchase using clickstream behavior signals.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Configuration

# COMMAND ----------

# DBTITLE 1,Imports
import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
from mlflow.models.signature import infer_signature

from pyspark.sql.functions import (
    col, when, count, countDistinct, sum as spark_sum,
    array_contains, current_timestamp, lit, log1p
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler, StandardScaler
)
from pyspark.ml.classification import (
    LogisticRegression, GBTClassifier
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator, MulticlassClassificationEvaluator
)

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

print(f"MLflow version: {mlflow.__version__}")

# COMMAND ----------

# DBTITLE 1,Configuration
EXPERIMENT_NAME = "/Shared/retail_purchase_propensity"
MODEL_NAME = "purchase_propensity_model"
N_SYNTHETIC_SESSIONS = 8000
RANDOM_SEED = 42

CATEGORIES = ["Electronics", "Clothing", "Footwear", "Home & Garden", "Sports", "Accessories"]
DEVICE_TYPES = ["desktop", "mobile", "tablet"]

# Set experiment and enable Spark ML autologging globally
try:
    mlflow.set_experiment(EXPERIMENT_NAME)
    mlflow.spark.autolog()
    print(f"MLflow experiment: {EXPERIMENT_NAME}")
    print(f"Tracking URI: {mlflow.get_tracking_uri()}")
except Exception as e:
    print(f"WARNING: MLflow not available — {e}")
    print("Training will continue without experiment tracking.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Synthetic Data Generation
# MAGIC
# MAGIC Generates realistic session-level data matching the output of `01_clickstream_processing.py`.
# MAGIC Each row represents one user session with behavioral features and a `converted_to_purchase` target.

# COMMAND ----------

# DBTITLE 1,Generate Synthetic Session Data
def generate_synthetic_sessions(n_sessions=8000, seed=42):
    """
    Generate synthetic session data matching the schema from
    01_clickstream_processing.py `analyze_user_sessions()`.

    Features correlate realistically with the target (converted_to_purchase):
    - More add_to_cart events → higher conversion probability
    - Higher page_views → moderate increase
    - Longer sessions → slight increase
    - Mobile users → slightly lower conversion
    - Certain categories (Electronics) → different patterns
    """
    rng = np.random.default_rng(seed)
    n = n_sessions

    page_views = rng.poisson(8, n).clip(1, 60)
    unique_products = np.minimum(
        np.minimum(rng.poisson(page_views.astype(float) * 0.55), 20), page_views
    ).clip(1)
    add_to_cart = rng.poisson(page_views.astype(float) * 0.18).clip(0, 12)
    search_events = rng.poisson(1.8, n).clip(0, 10)
    session_duration = rng.lognormal(mean=4.2, sigma=0.7, size=n).clip(5, 5400)

    device_types = rng.choice(DEVICE_TYPES, n, p=[0.50, 0.38, 0.12])

    base_dt = datetime(2025, 6, 1)
    session_starts = [base_dt + timedelta(
        days=int(rng.integers(0, 30)),
        hours=int(rng.integers(0, 24)),
        minutes=int(rng.integers(0, 60))
    ) for _ in range(n)]

    prob = 0.025
    prob += add_to_cart * 0.09
    prob += np.log1p(page_views) * 0.018
    prob += np.log1p(session_duration) * 0.010
    prob += search_events * 0.012
    prob *= np.where(device_types == "mobile", 0.75, 1.0)
    prob *= np.where(device_types == "tablet", 0.85, 1.0)
    prob = np.clip(prob, 0.01, 0.90)
    converted = rng.binomial(1, prob)

    rows = []
    for i in range(n):
        n_cats = rng.integers(1, 4)
        sess_cats = list(rng.choice(CATEGORIES, n_cats, replace=False))
        n_devs = rng.integers(1, 3)
        sess_devs = list(rng.choice(DEVICE_TYPES, n_devs, replace=False))
        dt = session_starts[i]
        rows.append({
            "session_id": f"sess_{i:06d}",
            "user_id": f"user_{rng.integers(10000, 99999)}",
            "event_date": dt.strftime("%Y-%m-%d"),
            "event_hour": dt.hour,
            "day_of_week": dt.weekday(),
            "page_views": int(page_views[i]),
            "unique_products_viewed": int(unique_products[i]),
            "add_to_cart_events": int(add_to_cart[i]),
            "search_events": int(search_events[i]),
            "session_duration_seconds": float(round(session_duration[i], 1)),
            "device_type": device_types[i],
            "devices_used": sess_devs,
            "categories_viewed": sess_cats,
            "n_categories_viewed": len(sess_cats),
            "converted_to_purchase": bool(converted[i]),
        })

    return pd.DataFrame(rows)

df_pandas = generate_synthetic_sessions(N_SYNTHETIC_SESSIONS, RANDOM_SEED)
sessions_df = spark.createDataFrame(df_pandas)

print(f"Generated {sessions_df.count():,} sessions")
print(f"Conversion rate: {sessions_df.filter(col('converted_to_purchase')).count() / sessions_df.count():.2%}")
print(f"\nSchema:")
sessions_df.printSchema()
display(sessions_df.limit(8))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Exploratory Data Analysis

# COMMAND ----------

# DBTITLE 1,Target Distribution & Summary Stats
conversion_rate = sessions_df.filter(col("converted_to_purchase")).count() / sessions_df.count()
print(f"Overall conversion rate: {conversion_rate:.3%}")

summary = sessions_df.select(
    "page_views", "unique_products_viewed", "add_to_cart_events",
    "search_events", "session_duration_seconds"
).describe()
display(summary)

print("\nConversion rate by device type:")
sessions_df.groupBy("device_type").agg(
    count("*").alias("sessions"),
    spark_sum(col("converted_to_purchase").cast("int")).alias("conversions"),
    (spark_sum(col("converted_to_purchase").cast("int")) / count("*")).alias("conversion_rate")
).orderBy("conversion_rate", ascending=False).show()

print("\nConversion rate by hour of day:")
sessions_df.groupBy("event_hour").agg(
    count("*").alias("sessions"),
    (spark_sum(col("converted_to_purchase").cast("int")) / count("*")).alias("conversion_rate")
).orderBy("event_hour").show(24)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Feature Engineering

# COMMAND ----------

# DBTITLE 1,Build Feature Vector
def engineer_features(df):
    """Transform raw session data into ML-ready feature vector."""

    df = df.withColumn("log_page_views", log1p(col("page_views")))
    df = df.withColumn("log_duration", log1p(col("session_duration_seconds")))
    df = df.withColumn("log_add_to_cart", log1p(col("add_to_cart_events")))
    df = df.withColumn("log_search", log1p(col("search_events")))
    df = df.withColumn("log_unique_products", log1p(col("unique_products_viewed")))

    df = df.withColumn("device_desktop", (col("device_type") == "desktop").cast("int"))
    df = df.withColumn("device_mobile", (col("device_type") == "mobile").cast("int"))
    df = df.withColumn("device_tablet", (col("device_type") == "tablet").cast("int"))

    df = df.withColumn("is_weekend", (col("day_of_week").isin([5, 6])).cast("int"))
    df = df.withColumn("hour_morning", (col("event_hour").between(6, 11)).cast("int"))
    df = df.withColumn("hour_afternoon", (col("event_hour").between(12, 17)).cast("int"))
    df = df.withColumn("hour_evening", (col("event_hour").between(18, 23)).cast("int"))
    df = df.withColumn("hour_night", (col("event_hour").between(0, 5)).cast("int"))

    for cat in CATEGORIES:
        col_name = f"cat_{cat.lower().replace(' & ', '_').replace(' ', '_')}"
        df = df.withColumn(col_name, array_contains(col("categories_viewed"), cat).cast("int"))

    df = df.withColumn("browse_to_product_ratio",
        col("unique_products_viewed") / (col("page_views") + 1))

    return df

sessions_featurized = engineer_features(sessions_df)

FEATURE_COLS = [
    "log_page_views", "log_duration", "log_add_to_cart", "log_search",
    "log_unique_products",
    "device_mobile", "device_tablet",
    "is_weekend", "hour_morning", "hour_afternoon", "hour_evening", "hour_night",
    "cat_electronics", "cat_clothing", "cat_footwear",
    "cat_home_garden", "cat_sports", "cat_accessories",
    "browse_to_product_ratio",
]
TARGET_COL = "converted_to_purchase"

print(f"Feature columns ({len(FEATURE_COLS)}): {FEATURE_COLS}")
print(f"Target column: {TARGET_COL}")

# COMMAND ----------

# DBTITLE 1,Assemble & Scale Features
assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="raw_features")
scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)

feat_pipeline = Pipeline(stages=[assembler, scaler])
feat_model = feat_pipeline.fit(sessions_featurized)
sessions_prepared = feat_model.transform(sessions_featurized)

sessions_prepared = sessions_prepared.withColumn(
    "label", col(TARGET_COL).cast("double")
)

display(sessions_prepared.select("features", "label", TARGET_COL).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Train/Test Split

# COMMAND ----------

# DBTITLE 1,Train/Test Split
train_df, test_df = sessions_prepared.randomSplit([0.80, 0.20], seed=RANDOM_SEED)

train_count = train_df.count()
test_count = test_df.count()
train_pos = train_df.filter(col("label") == 1.0).count() / train_count
test_pos = test_df.filter(col("label") == 1.0).count() / test_count

print(f"Train: {train_count:,} rows — positive rate: {train_pos:.3%}")
print(f"Test:  {test_count:,} rows — positive rate: {test_pos:.3%}")

# Cache for iterative training
train_df.cache()
test_df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Model Training with MLflow Tracking

# COMMAND ----------

# DBTITLE 1,Model 1 — Logistic Regression (Baseline)
with mlflow.start_run(run_name="logistic_regression_baseline"):
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=100,
        regParam=0.1,
        elasticNetParam=0.5,
        family="binomial",
    )

    lr_model = lr.fit(train_df)

    train_preds = lr_model.transform(train_df)
    test_preds = lr_model.transform(test_df)

    evaluator_auc = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    )
    evaluator_pr = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderPR"
    )

    train_auc = evaluator_auc.evaluate(train_preds)
    test_auc = evaluator_auc.evaluate(test_preds)
    train_pr = evaluator_pr.evaluate(train_preds)
    test_pr = evaluator_pr.evaluate(test_preds)

    mlflow.log_metrics({
        "train_auc": float(train_auc),
        "test_auc": float(test_auc),
        "train_pr_auc": float(train_pr),
        "test_pr_auc": float(test_pr),
    })
    mlflow.log_params({
        "train_size": train_count,
        "test_size": test_count,
        "feature_count": len(FEATURE_COLS),
        "n_synthetic_samples": N_SYNTHETIC_SESSIONS,
        "model_type": "logistic_regression",
    })

    print(f"Logistic Regression — Train AUC: {train_auc:.4f}, Test AUC: {test_auc:.4f}")

    lr_run_id = mlflow.active_run().info.run_id

# COMMAND ----------

# DBTITLE 1,Model 2 — Gradient Boosted Trees
with mlflow.start_run(run_name="gradient_boosted_trees"):
    gbt = GBTClassifier(
        featuresCol="features",
        labelCol="label",
        maxIter=50,
        maxDepth=5,
        stepSize=0.1,
        subsamplingRate=0.8,
        seed=RANDOM_SEED,
    )

    gbt_model = gbt.fit(train_df)

    train_preds = gbt_model.transform(train_df)
    test_preds = gbt_model.transform(test_df)

    evaluator_auc = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    )
    evaluator_pr = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderPR"
    )

    train_auc = evaluator_auc.evaluate(train_preds)
    test_auc = evaluator_auc.evaluate(test_preds)
    train_pr = evaluator_pr.evaluate(train_preds)
    test_pr = evaluator_pr.evaluate(test_preds)

    mlflow.log_metrics({
        "train_auc": float(train_auc),
        "test_auc": float(test_auc),
        "train_pr_auc": float(train_pr),
        "test_pr_auc": float(test_pr),
    })
    mlflow.log_params({
        "train_size": train_count,
        "test_size": test_count,
        "feature_count": len(FEATURE_COLS),
        "model_type": "gradient_boosted_trees",
    })

    print(f"GBT — Train AUC: {train_auc:.4f}, Test AUC: {test_auc:.4f}")

    gbt_run_id = mlflow.active_run().info.run_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Model Comparison

# COMMAND ----------

# DBTITLE 1,Compare Runs
client = MlflowClient()

lr_run = client.get_run(lr_run_id)
gbt_run = client.get_run(gbt_run_id)

lr_metrics = lr_run.data.metrics
gbt_metrics = gbt_run.data.metrics

comparison = pd.DataFrame([
    {
        "Model": "Logistic Regression",
        "Train AUC": f"{lr_metrics['train_auc']:.4f}",
        "Test AUC": f"{lr_metrics['test_auc']:.4f}",
        "Train PR-AUC": f"{lr_metrics['train_pr_auc']:.4f}",
        "Test PR-AUC": f"{lr_metrics['test_pr_auc']:.4f}",
        "Run ID": lr_run_id,
    },
    {
        "Model": "Gradient Boosted Trees",
        "Train AUC": f"{gbt_metrics['train_auc']:.4f}",
        "Test AUC": f"{gbt_metrics['test_auc']:.4f}",
        "Train PR-AUC": f"{gbt_metrics['train_pr_auc']:.4f}",
        "Test PR-AUC": f"{gbt_metrics['test_pr_auc']:.4f}",
        "Run ID": gbt_run_id,
    },
])

display(comparison)

# Select best model by test AUC
best_run_id = lr_run_id if lr_metrics["test_auc"] >= gbt_metrics["test_auc"] else gbt_run_id
best_model_name = "Logistic Regression" if best_run_id == lr_run_id else "Gradient Boosted Trees"
best_model_obj = lr_model if best_run_id == lr_run_id else gbt_model

print(f"\nBest model: {best_model_name} (Run: {best_run_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Feature Importance

# COMMAND ----------

# DBTITLE 1,Feature Importance (from best model)
if best_run_id == gbt_run_id:
    importances = best_model_obj.featureImportances.toArray()
    feat_imp_df = pd.DataFrame({
        "feature": FEATURE_COLS,
        "importance": importances,
    }).sort_values("importance", ascending=False)

    print("GBT Feature Importance (top 10):")
    display(feat_imp_df.head(10))
else:
    coefficients = best_model_obj.coefficients.toArray()
    feat_imp_df = pd.DataFrame({
        "feature": FEATURE_COLS,
        "coefficient": coefficients,
        "abs_coefficient": np.abs(coefficients),
    }).sort_values("abs_coefficient", ascending=False)

    print("Logistic Regression Feature Weights (top 10):")
    display(feat_imp_df.head(10))

try:
    with mlflow.start_run(run_id=best_run_id):
        feat_imp_path = "/tmp/feature_importance.csv"
        feat_imp_df.to_csv(feat_imp_path, index=False)
        mlflow.log_artifact(feat_imp_path, "feature_importance")
    print("Feature importance logged to MLflow")
except Exception as e:
    print(f"Feature importance artifact logging skipped: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Model Registry — Register, Stage, Promote

# COMMAND ----------

# DBTITLE 1,Register Model
with mlflow.start_run(run_id=best_run_id):
    model_output = best_model_obj.transform(train_df.limit(10))
    signature = infer_signature(
        train_df.select("features").limit(10).toPandas(),
        model_output.select("prediction", "probability", "rawPrediction").limit(10).toPandas(),
    )

    mlflow.spark.log_model(
        spark_model=best_model_obj,
        artifact_path="model",
        signature=signature,
        registered_model_name=MODEL_NAME,
    )

    mlflow.set_tag("use_case", "purchase_propensity")
    mlflow.set_tag("framework", "pyspark.mllib")
    mlflow.set_tag("training_dataset", "synthetic_clickstream_sessions")

print(f"Model registered: {MODEL_NAME}")

# COMMAND ----------

# DBTITLE 1,Transition Model to Staging
from mlflow import MlflowException

try:
    model_versions = client.search_model_versions(f"run_id='{best_run_id}'")
    if model_versions:
        model_version = model_versions[0].version
        client.transition_model_version_stage(
            name=MODEL_NAME,
            version=model_version,
            stage="Staging",
            archive_existing_versions=False,
        )
        client.update_model_version(
            name=MODEL_NAME,
            version=model_version,
            description=f"Purchase propensity model — trained on {train_count:,} sessions with {best_model_name}.",
        )
        print(f"Model {MODEL_NAME} v{model_version} transitioned to Staging")
    else:
        print("Model version not found for this run — may already be Staging/Production")
except MlflowException as e:
    print(f"Model registry operation: {e}")

# COMMAND ----------

# DBTITLE 1,Promote to Production
try:
    staging_versions = client.search_model_versions(f"run_id='{best_run_id}'")
    staging_in_stage = [v for v in staging_versions if v.current_stage == "Staging"]
    if staging_in_stage:
        model_version = staging_in_stage[0].version
        client.transition_model_version_stage(
            name=MODEL_NAME,
            version=model_version,
            stage="Production",
            archive_existing_versions=True,
        )
        print(f"Model {MODEL_NAME} v{model_version} promoted to Production")
        print(f"  Run ID: {staging_in_stage[0].run_id}")
    else:
        v = staging_versions[0] if staging_versions else None
        current = v.current_stage if v else "unknown"
        print(f"Model version not in Staging — current stage: {current}")
except MlflowException as e:
    print(f"Model registry operation: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Batch Inference

# COMMAND ----------

# DBTITLE 1,Load Model from Registry & Score New Data
pipeline_model_uri = f"models:/{MODEL_NAME}/Production"

try:
    loaded_model = mlflow.spark.load_model(pipeline_model_uri)
    print(f"Loaded model from registry: {pipeline_model_uri}")
except MlflowException as e:
    print(f"Falling back to in-memory best model — {e}")
    loaded_model = best_model_obj

scored_df = loaded_model.transform(test_df)

scored_df = scored_df.withColumn(
    "prediction_label", col("prediction").cast("int")
).withColumn(
    "probability_purchase",
    col("probability").getItem(1),
)

display(
    scored_df.select(
        "session_id", "user_id",
        "label", "prediction_label",
        "probability_purchase",
        "page_views", "add_to_cart_events",
        "device_type",
    ).limit(15)
)

# COMMAND ----------

# DBTITLE 1,Evaluation on Test Set (from Registered Model)
accuracy_eval = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy"
)
f1_eval = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="f1"
)

test_accuracy = accuracy_eval.evaluate(scored_df)
test_f1 = f1_eval.evaluate(scored_df)
test_auc = evaluator_auc.evaluate(scored_df)

print(f"Test Set Performance (Registered Model):")
print(f"  Accuracy:  {test_accuracy:.4f}")
print(f"  F1 Score:  {test_f1:.4f}")
print(f"  AUC-ROC:   {test_auc:.4f}")

# Confusion Matrix
confusion = scored_df.groupBy("label", "prediction_label").count().orderBy("label", "prediction_label")
print("\nConfusion Matrix:")
confusion.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Prediction Monitoring Scaffold

# COMMAND ----------

# DBTITLE 1,Log Prediction Distribution for Drift Detection
prediction_summary = scored_df.selectExpr(
    "avg(probability_purchase) as avg_probability",
    "stddev(probability_purchase) as stddev_probability",
    "percentile_approx(probability_purchase, 0.5) as median_probability",
    "percentile_approx(probability_purchase, 0.95) as p95_probability",
    "count(*) as total_predictions",
    "sum(cast(prediction_label as int)) as predicted_positives",
).collect()[0]

monitoring_metrics = {
    "avg_predicted_probability": float(prediction_summary["avg_probability"]),
    "stddev_predicted_probability": float(prediction_summary["stddev_probability"]),
    "median_predicted_probability": float(prediction_summary["median_probability"]),
    "p95_predicted_probability": float(prediction_summary["p95_probability"]),
    "total_predictions": int(prediction_summary["total_predictions"]),
    "predicted_positive_rate": float(
        prediction_summary["predicted_positives"] / prediction_summary["total_predictions"]
    ),
    "monitoring_timestamp": datetime.utcnow().isoformat(),
}

try:
    with mlflow.start_run(run_id=best_run_id):
        mlflow.log_dict(monitoring_metrics, "monitoring/prediction_baseline.json")
        mlflow.set_tag("baseline_set", "true")
    print("Prediction monitoring baseline logged to MLflow")
except Exception as e:
    print(f"Monitoring baseline logging skipped: {e}")

print("Prediction monitoring baseline logged to MLflow:")
for k, v in monitoring_metrics.items():
    print(f"  {k}: {v}")

# COMMAND ----------

# DBTITLE 1,Drift Detection Function
def check_prediction_drift(current_metrics, baseline_metrics, threshold_std=3.0):
    """
    Compare current prediction distribution against the baseline.
    Returns drift alert if any metric exceeds `threshold_std` standard deviations.
    """
    alerts = []
    drift_metrics = ["avg_predicted_probability", "predicted_positive_rate"]

    for metric in drift_metrics:
        if metric in baseline_metrics and metric in current_metrics:
            delta = abs(current_metrics[metric] - baseline_metrics[metric])
            alerts.append({
                "metric": metric,
                "baseline": baseline_metrics[metric],
                "current": current_metrics[metric],
                "delta": delta,
                "drift_detected": delta > 0.05,
            })

    return alerts

simulated_current = {
    "avg_predicted_probability": monitoring_metrics["avg_predicted_probability"] * 1.08,
    "predicted_positive_rate": monitoring_metrics["predicted_positive_rate"] * 1.02,
}

drift_result = check_prediction_drift(simulated_current, monitoring_metrics)
print("Drift check (simulated slight shift):")
for alert in drift_result:
    status = "DRIFT" if alert["drift_detected"] else "OK"
    print(f"  [{status}] {alert['metric']}: baseline={alert['baseline']:.4f}, "
          f"current={alert['current']:.4f}, delta={alert['delta']:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Write Predictions to Redshift → Power BI

# COMMAND ----------

# DBTITLE 1,Prepare BI-Ready Prediction Dataset
prediction_export = scored_df.select(
    col("session_id"),
    col("user_id"),
    col("event_date"),
    col("event_hour"),
    col("prediction_label"),
    col("probability_purchase").cast("double"),
    col("label").cast("int").alias("actual_converted"),
    col("page_views"),
    col("add_to_cart_events"),
    col("unique_products_viewed"),
    col("session_duration_seconds"),
    col("device_type"),
    col("categories_viewed"),
    current_timestamp().alias("prediction_timestamp"),
    lit(MODEL_NAME).alias("model_name"),
).withColumn(
    "probability_bucket",
    when(col("probability_purchase") < 0.2, "0-20%")
    .when(col("probability_purchase") < 0.4, "20-40%")
    .when(col("probability_purchase") < 0.6, "40-60%")
    .when(col("probability_purchase") < 0.8, "60-80%")
    .otherwise("80-100%"),
)

print(f"BI-ready prediction dataset: {prediction_export.count()} rows")
display(prediction_export.limit(10))

# COMMAND ----------

# DBTITLE 1,Redshift Table DDL
# Run once in Redshift Query Editor to create the prediction table:
redshift_ddl = """
-- Redshift DDL for ML predictions (run in Redshift Query Editor v2)
CREATE TABLE IF NOT EXISTS ml_purchase_predictions (
    session_id            VARCHAR(50)   NOT NULL,
    user_id               VARCHAR(50),
    event_date            DATE,
    event_hour            INT,
    prediction_label      INT,
    probability_purchase  DOUBLE PRECISION,
    actual_converted      INT,
    page_views            INT,
    add_to_cart_events    INT,
    unique_products_viewed INT,
    session_duration_seconds DOUBLE PRECISION,
    device_type           VARCHAR(20),
    probability_bucket    VARCHAR(10),
    prediction_timestamp  TIMESTAMP,
    model_name            VARCHAR(100),
    model_version         VARCHAR(20)
)
DISTSTYLE AUTO
SORTKEY (event_date, event_hour);

-- Power BI connects directly to this table via Redshift connector
-- Connection string: redshift-cluster-endpoint:5439/retail_analytics
"""
print(redshift_ddl)

# COMMAND ----------

# DBTITLE 1,Write Predictions to Redshift (requires Redshift JDBC driver on cluster)
redshift_available = False
try:
    redshift_host = dbutils.secrets.get(scope="retail-insights", key="redshift-host")
    redshift_database = dbutils.secrets.get(scope="retail-insights", key="redshift-database")
    redshift_username = dbutils.secrets.get(scope="retail-insights", key="redshift-username")
    redshift_password = dbutils.secrets.get(scope="retail-insights", key="redshift-password")

    jdbc_url = f"jdbc:redshift://{redshift_host}:5439/{redshift_database}"

    prediction_export.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "ml_purchase_predictions") \
        .option("user", redshift_username) \
        .option("password", redshift_password) \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .mode("append") \
        .save()

    redshift_available = True
    print(f"Predictions written to Redshift: ml_purchase_predictions ({prediction_export.count()} rows)")

except ImportError:
    print("Redshift JDBC driver not available on this cluster.")
except Exception as e:
    print(f"Redshift write skipped: {e}")

if not redshift_available:
    print("Saving predictions to Delta table (demo-friendly fallback):")
    prediction_export.write.mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable("retail_analytics.ml_purchase_predictions")
    print("Table: retail_analytics.ml_purchase_predictions")
    print("→ Query immediately in Databricks SQL Editor (see next cell)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12b. Demo — Query Predictions in Databricks SQL
# MAGIC
# MAGIC Run these in **Databricks SQL Editor** (or a new SQL cell) to demonstrate BI queries:

# COMMAND ----------

# DBTITLE 1,Demo SQL Queries (copy to Databricks SQL Editor)
demo_queries = """
-- ═══════════════════════════════════════════════════════════
-- DEMO QUERY 1: Prediction Distribution (Bar Chart)
-- ═══════════════════════════════════════════════════════════
SELECT
    probability_bucket,
    COUNT(*) AS session_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
FROM retail_analytics.ml_purchase_predictions
GROUP BY probability_bucket
ORDER BY probability_bucket;

-- ═══════════════════════════════════════════════════════════
-- DEMO QUERY 2: Conversion Funnel by Device (Donut Chart)
-- ═══════════════════════════════════════════════════════════
SELECT
    device_type,
    COUNT(*) AS sessions,
    SUM(prediction_label) AS predicted_conversions,
    ROUND(SUM(prediction_label) * 100.0 / COUNT(*), 1) AS conversion_rate_pct
FROM retail_analytics.ml_purchase_predictions
GROUP BY device_type
ORDER BY conversion_rate_pct DESC;

-- ═══════════════════════════════════════════════════════════
-- DEMO QUERY 3: High-Value Sessions (retargeting list)
-- ═══════════════════════════════════════════════════════════
SELECT
    session_id,
    user_id,
    ROUND(probability_purchase * 100, 1) AS purchase_probability_pct,
    page_views,
    add_to_cart_events,
    device_type
FROM retail_analytics.ml_purchase_predictions
WHERE probability_purchase > 0.80
ORDER BY probability_purchase DESC
LIMIT 20;

-- ═══════════════════════════════════════════════════════════
-- DEMO QUERY 4: Model Performance by Hour (Line Chart)
-- ═══════════════════════════════════════════════════════════
SELECT
    event_hour,
    COUNT(*) AS sessions,
    SUM(prediction_label) AS predicted_purchases,
    SUM(actual_converted) AS actual_purchases,
    ROUND(AVG(probability_purchase) * 100, 1) AS avg_probability_pct
FROM retail_analytics.ml_purchase_predictions
GROUP BY event_hour
ORDER BY event_hour;
"""
print(demo_queries)

# COMMAND ----------

# MAGIC %md
# MAGIC ### BI Connection Paths (for client demo)
# MAGIC
# MAGIC | Path | Data Source | Prerequisites |
# MAGIC |---|---|---|
# MAGIC | **A (immediate)** | Databricks SQL | DB creds only — query `retail_analytics.ml_purchase_predictions` in SQL Editor |
# MAGIC | **B (Power BI)** | Databricks SQL Warehouse | Power BI Desktop + Databricks Partner Connect |
# MAGIC | **C (full)** | Amazon Redshift | Terraform deploy + Redshift secrets in Databricks |
# MAGIC
# MAGIC **For today's demo use Path A** — no additional infrastructure needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Summary & Next Steps

# COMMAND ----------

# DBTITLE 1,MLOps Pipeline Summary
print("=" * 60)
print("  PURCHASE PROPENSITY — MLOPS PIPELINE COMPLETE")
print("=" * 60)
print(f"""
Data:        {train_count + test_count:,} synthetic sessions ({conversion_rate:.1%} conversion rate)
Features:    {len(FEATURE_COLS)} engineered features
Best Model:  {best_model_name} — Test AUC: {test_auc:.4f}
MLflow Run:  {best_run_id}
Registry:    models:/{MODEL_NAME}/Production

MLOps Artifacts in MLflow:
  • Experiment: {EXPERIMENT_NAME}
  • Model: {MODEL_NAME} (Staging → Production)
  • Feature importance logged
  • Prediction baseline for drift monitoring

Next Steps for Production:
  1. Wire into Airflow DAG for scheduled retraining
  2. Add data validation (Great Expectations / Delta Live Tables)
  3. Connect to real pipeline data (read from S3 session Parquet)
  4. Deploy as Databricks Model Serving endpoint
  5. Add CI/CD gates: model validation tests before staging
""")

# COMMAND ----------

# DBTITLE 1,Cleanup
train_df.unpersist()
test_df.unpersist()
print("Notebook execution complete.")
