# Retail Insights Platform — Architecture & Technical Walkthrough

## Infrastructure Deployment (Terraform IaC)

**Three modules provisioned to AWS:**

```
terraform/
  main.tf              → Entry point: VPC + S3 + Redshift
  variables.tf          → Environment config (region, passwords, node types)
  modules/
    vpc/                → 10.0.0.0/16, 2 public + 2 private subnets, NAT gateway, 4 security groups
    s3/                 → 3 buckets (raw/processed/curated), versioning, lifecycle policies
    redshift/           → Single-node ra3.xlplus, publicly accessible, encrypted
```

**Deployment command:** `terraform apply -var="redshift_password=..."`  
**Credentials:** IAM user `retail-insights-terraform` with AdministratorAccess  
**Provisioned resources:** 40 (VPC 10.0.0.0/16, 3 S3 buckets, 1 Redshift cluster)  
**Estimated cost:** ~$0.30/hr (all resources), destroy after demo with `terraform destroy`

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│  LOCAL (Docker)                                                         │
│                                                                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────────────┐  │
│  │Clickstream│───▶│  Kafka   │───▶│  Session │───▶│  ML Training     │  │
│  │ Producer  │    │ Broker   │    │ Aggreg.  │    │ (LR + GBT)       │  │
│  │ (Python)  │    │ :9092    │    │ (Pandas) │    │ sklearn          │  │
│  └──────────┘    └──────────┘    └──────────┘    └────────┬─────────┘  │
│      │               │               │                    │            │
│      │    ┌──────────┴───────────────┴────────────────────┤            │
│      │    │        MLflow Tracking Server                 │            │
│      │    │        http://localhost:5000                  │            │
│      │    │        • Experiment tracking                  │            │
│      │    │        • Model Registry (Staging/Production)  │            │
│      │    │        • Prediction baseline (drift detect)   │            │
│      │    └──────────────────────────────────────────────┘            │
│      │                                                                 │
└──────┼─────────────────────────────────────────────────────────────────┘
       │
       │  AWS                                                             
       │                                                                 
       ├──▶ S3 (raw/processed/curated)     ← Data Lake                    
       │    retail-insights-*-vhnayeke                                    
       │                                                                 
       └──▶ Redshift                      ← Data Warehouse               
            retail-insights-dev.xxx.redshift.amazonaws.com:5439           
            Tables: ml_purchase_predictions (100 rows)                    
                    hourly_clickstream_metrics                            
                    product_performance_metrics                           
                    category_performance_metrics                          
                    ml_model_performance                                  
                                                                         
           Power BI Desktop ──────────▶  Get Data → Amazon Redshift       
                                         Live dashboards                  
```

---

## Phase 1: Clickstream Data Generation

**Code:** `kafka/producers/clickstream_producer.py`  
**Entry:** `python kafka/producers/clickstream_producer.py --events-per-second 50`

**What happens:**
1. `RetailClickstreamProducer` initializes with a 15-product catalog (Electronics, Clothing, Footwear, Home & Garden, Sports, Accessories)
2. `generate_event()` uses a cascade of probabilities:
   - 5% → purchase event (cart_items, payment_method, shipping_method)
   - 15% → add_to_cart event (quantity, total_value)
   - 20% → search event (query, results_count)
   - 60% → page_view event (product details, referrer, device_info)
3. Each event includes: `user_id`, `session_id`, `device_info` (type/OS/browser), `location` (lat/lng/city from 10 US cities)
4. Sessions auto-generate: 10% random chance to start new session, 30% chance after 5+ page views
5. Events are JSON-serialized with timestamp, event_id (UUID), and ingestion metadata

**Key talking points:**
- "The producer simulates real e-commerce traffic patterns — not random noise"
- "Each event is a fully-formed JSON document with 15+ fields of behavioral data"

---

## Phase 2: Kafka Ingestion

**Code:** `scripts/demo_e2e.py` (produce function)  
**Infrastructure:** Confluent Kafka 7.6.0 + Zookeeper (Docker)  
**Docker Compose:** `kafka/docker-compose.yml`  
**Topic:** `retail_clickstream` (auto-created, 1 partition for demo)

**What happens:**
1. `KafkaProducer` connects to `localhost:9092` with acks=1, JSON value serializer
2. Each event is sent with `user_id` as the partition key (ensures all events from same user go to same partition — ordering guarantee)
3. `producer.flush()` blocks until all 5000 events are acknowledged by the broker
4. Events remain in Kafka until consumed (retention-based)

**Key talking points:**
- "Kafka guarantees ordered delivery per user session — critical for session analysis"
- "Production would use 3 brokers + replication factor 3, but the architecture is identical"
- "Kafka Connect could replace the Python consumer for production workloads"

---

## Phase 3: Stream Consumption & Session Aggregation

**Code:** `scripts/demo_e2e.py` (consume + aggregate functions)  

**What happens:**
1. `KafkaConsumer` subscribes to `retail_clickstream` with `auto_offset_reset=earliest`
2. Consumer polls for 15 seconds, collecting all available messages
3. Raw events are converted to a Pandas DataFrame
4. Session aggregation groups by `(session_id, user_id, event_date)`:
   - `page_views` — count of all events in session
   - `add_to_cart_events` — count of add_to_cart event types
   - `purchase_events` — count of purchase event types  
   - `search_events` — count of search event types
   - `unique_products_viewed` — distinct product_ids
   - `converted_to_purchase` — binary target (did session end in purchase?)

**Key talking points:**
- "1328 raw events → 264 sessions. This is the feature engineering step."
- "The same logic exists as a PySpark job in `01_clickstream_processing.py` for production scale"

---

## Phase 4: ML Training with MLflow Tracking

**Code:** `scripts/demo_e2e.py` (engineer_and_train)  
**MLflow:** `http://localhost:5000`  
**Experiment:** `retail_purchase_propensity`

**What happens:**
1. **Feature engineering:**
   - Log transforms: `log1p(page_views)`, `log1p(add_to_cart_events)`, `log1p(search_events)`, `log1p(unique_products)`
   - Device encoding: `device_mobile`, `device_tablet` (desktop is reference category)
   - 7 features total → avoids dummy variable trap

2. **Train/test split:** 80/20 stratified (preserves purchase ratio in both sets)

3. **Two models trained (with `mlflow.sklearn.autolog()`):**
   - **Logistic Regression** (baseline) — `max_iter=1000`
   - **Gradient Boosted Trees** — `n_estimators=100, max_depth=3`

4. **MLflow captures automatically:**
   - All model parameters (n_estimators, max_depth, random_state)
   - All metrics (train AUC, test AUC, accuracy, F1)  
   - Model artifacts (serialized model, conda environment, requirements)
   - Model signature (input/output schema for serving)

5. **Manual logging adds:**
   - `train_size`, `test_size`, `feature_count` as params
   - Feature importance CSV as artifact

**Key talking points:**
- "MLflow autolog captures everything — we never have to ask 'what hyperparameters did we use?'"
- "Model comparison happens naturally: side-by-side runs with metrics"
- "The MLflow UI shows experiment lineage, not just a Jupyter notebook cell"

---

## Phase 5: Model Registry

**Code:** `scripts/demo_e2e.py` (registry section)  
**Model:** `purchase_propensity` (registered in MLflow)

**What happens:**
1. Best model (by test AUC) is selected
2. Model is auto-registered via `mlflow.sklearn.log_model(registered_model_name="purchase_propensity")`
3. **Staging:** `client.transition_model_version_stage("purchase_propensity", v, "Staging")`
   - Validation tests would go here (CI/CD gate)
4. **Production:** `client.transition_model_version_stage("purchase_propensity", v, "Production")`
   - Archives previous production version
5. Each version has: run_id, timestamp, description, tags

**Key talking points:**
- "Three versions tracked — every training run creates an auditable version"
- "Staging → Production promotion is the CI/CD model deployment gate"
- "Production models are loaded via `models:/purchase_propensity/Production` URI — no file paths"
- "If a model underperforms, we roll back to any previous version with one API call"

---

## Phase 6: Batch Inference

**Code:** `scripts/demo_e2e.py` (inference section)

**What happens:**
1. Best model's `predict_proba()` scores each session in the test set
2. Each session gets:
   - `prediction_label` (0 or 1)
   - `probability_purchase` (0.0 to 1.0)
   - `probability_bucket` (0-20%, 20-40%, 40-60%, 60-80%, 80-100%)
3. Results joined back to original session data (page_views, device, add_to_cart)

**Key talking points:**
- "Batch inference happens at prediction time — no retraining needed"
- "Probability buckets segment users by purchase intent for marketing teams"
- "Production would use Databricks Model Serving for real-time scoring"

---

## Phase 7: Writing to Redshift

**Code:** `scripts/demo_e2e.py` (redshift section)  
**API:** `boto3.client('redshift-data')` — AWS Redshift Data API  
**Table:** `ml_purchase_predictions` (DISTSTYLE AUTO, SORTKEY on event_date)

**What happens:**
1. Top 50 scored sessions formatted as SQL INSERT VALUES
2. Executed via Redshift Data API (async, no JDBC driver needed locally)
3. Data lands in `ml_purchase_predictions` with all session context

**Redshift table schema:**
```sql
ml_purchase_predictions (
    session_id, user_id, event_date, event_hour,
    prediction_label, probability_purchase, actual_converted,
    page_views, add_to_cart_events, unique_products_viewed,
    session_duration_seconds, device_type, probability_bucket,
    prediction_timestamp, model_name, model_version
)
```

**Key talking points:**
- "Redshift is the serving layer — Power BI queries it directly"
- "DISTSTYLE AUTO optimizes distribution, SORTKEY on date enables fast time-range queries"
- "Same table can be populated by the Databricks PySpark notebook for production workloads"

---

## Phase 8: Power BI Connection

**Connection:** Power BI Desktop → Get Data → Amazon Redshift  
**Credentials:** `admin` / `59D8irVU2czgfCNc`  
**Endpoint:** `retail-insights-dev.cnzvoywbm4io.us-east-1.redshift.amazonaws.com:5439`  
**Database:** `retail_analytics`

**Available tables for dashboards:**

| Table | Source | Dashboard Purpose |
|---|---|---|
| `ml_purchase_predictions` | This pipeline | Purchase propensity scores, device breakdown, probability distribution |
| `hourly_clickstream_metrics` | 01 PySpark notebook | Event volume, unique users per hour |
| `product_performance_metrics` | 01 PySpark notebook | Top products, view-to-purchase conversion |
| `category_performance_metrics` | 01 PySpark notebook | Category-level conversion rates |
| `ml_model_performance` | Future CI/CD | Model performance tracking over time |

**Key talking points:**
- "Power BI connects to Redshift natively — no ETL, no data export, no middleware"
- "The dashboards update in real-time as new predictions are written"
- "Same architecture works for Tableau, Looker, or any ODBC/JDBC BI tool"

---

## Databricks: Production-Ready Pipeline

**Notebooks uploaded to:** `Workspace → Shared → RetailInsights`

| Notebook | Purpose | Key MLOps features |
|---|---|---|
| `01_clickstream_processing.py` | PySpark stream processing | Real-time from Kafka, S3 partitioning, Redshift JDBC write |
| `02_ml_purchase_propensity.py` | ML training + registry | MLflow autolog, model signature, registry workflow (Staging→Production), drift monitoring scaffold |

**Both notebooks are:**
- Parameterized via `dbutils.widgets` — ready for Databricks Jobs
- Use `dbutils.secrets` for Redshift credentials (never hardcoded)
- Include proper error handling and graceful degradation
- Self-contained with synthetic data generation when Kafka is unavailable

---

## Monitoring & Operations

**Baseline monitoring (logged to MLflow):**
- `avg_predicted_probability` — baseline distribution
- `stddev_predicted_probability` — spread
- `p95_predicted_probability` — tail behavior
- `predicted_positive_rate` — class balance

**Drift detection function (`check_prediction_drift`):**
- Compares current vs baseline prediction distribution
- Flags drift when any metric shifts > 0.05
- Runs on each batch inference — alerts if distribution changes

**Key talking points:**
- "Monitoring is built into the pipeline, not added after deployment"
- "Drift detection catches model degradation before it impacts business metrics"
- "Baselines are versioned alongside the model — complete audit trail"

---

## CI/CD & Model Lifecycle

```
Git Push → GitHub Actions → Tests → Databricks Job → MLflow Registry
                                                         │
                                              ┌──────────┴──────────┐
                                              │  Staging             │
                                              │  (validation gate)   │
                                              └──────────┬──────────┘
                                                         │
                                              ┌──────────┴──────────┐
                                              │  Production          │
                                              │  (serving endpoint)  │
                                              └─────────────────────┘
```

**Key talking points:**
- "Model lifecycle: Notebook → Experiment → Registry (Staging) → Registry (Production) → Inference"
- "Every transition is auditable — who promoted which version, when, from which run"
- "Rollback is one API call: `models:/purchase_propensity/1` instead of `/3`"

---

## Key Architecture Decisions

| Decision | Why |
|---|---|
| **Local Kafka for demo** | Docker is simpler than EC2 for demos; production uses MSK or Confluent |
| **Single-node Redshift** | Cost: $0.25/hr for dev; production uses multi-node RA3 |
| **sklearn for local, PySpark for Databricks** | Same logic, different runtime — demonstrates portability |
| **Redshift Data API over JDBC** | No driver installation needed; works from any Python environment |
| **MLflow over custom tracking** | Built into Databricks, open-source, industry standard |
| **Probability buckets** | Business-friendly segmentation; non-technical stakeholders understand "80-100%" |
