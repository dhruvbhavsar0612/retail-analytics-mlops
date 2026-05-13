-- Retail Insights Platform — Redshift DDL
-- Run: psql -h retail-insights-dev.cnzvoywbm4io.us-east-1.redshift.amazonaws.com -U admin -d retail_analytics -p 5439 -f sql/setup_redshift.sql

-- Real-time hourly clickstream metrics
CREATE TABLE IF NOT EXISTS hourly_clickstream_metrics (
    event_date          DATE,
    event_hour          INT,
    total_events        BIGINT,
    unique_users        BIGINT,
    unique_sessions     BIGINT,
    page_views          BIGINT,
    add_to_cart         BIGINT,
    purchases           BIGINT,
    searches            BIGINT,
    processed_at        TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
SORTKEY (event_date, event_hour);

-- Product performance metrics
CREATE TABLE IF NOT EXISTS product_performance_metrics (
    product_id          VARCHAR(50),
    product_name        VARCHAR(200),
    product_category    VARCHAR(100),
    product_brand       VARCHAR(100),
    event_date          DATE,
    total_views         BIGINT,
    unique_users        BIGINT,
    unique_sessions     BIGINT,
    add_to_cart_count   BIGINT,
    purchase_count      BIGINT,
    avg_price           DOUBLE PRECISION,
    total_value         DOUBLE PRECISION,
    view_to_cart_rate   DOUBLE PRECISION,
    view_to_purchase_rate DOUBLE PRECISION,
    cart_to_purchase_rate DOUBLE PRECISION,
    processed_at        TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
SORTKEY (event_date, product_category);

-- Category performance metrics
CREATE TABLE IF NOT EXISTS category_performance_metrics (
    product_category    VARCHAR(100),
    event_date          DATE,
    category_views      BIGINT,
    unique_users        BIGINT,
    add_to_cart         BIGINT,
    purchases           BIGINT,
    processed_at        TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
SORTKEY (event_date, product_category);

-- ML Model Predictions (for Power BI)
CREATE TABLE IF NOT EXISTS ml_purchase_predictions (
    session_id              VARCHAR(50) NOT NULL,
    user_id                 VARCHAR(50),
    event_date              DATE,
    event_hour              INT,
    prediction_label        INT,
    probability_purchase    DOUBLE PRECISION,
    actual_converted        INT,
    page_views              INT,
    add_to_cart_events      INT,
    unique_products_viewed  INT,
    session_duration_seconds DOUBLE PRECISION,
    device_type             VARCHAR(20),
    probability_bucket      VARCHAR(10),
    prediction_timestamp    TIMESTAMP,
    model_name              VARCHAR(100),
    model_version           VARCHAR(20)
)
DISTSTYLE AUTO
SORTKEY (event_date, event_hour);

-- Model Performance Tracking
CREATE TABLE IF NOT EXISTS ml_model_performance (
    model_name        VARCHAR(100),
    model_version     VARCHAR(20),
    test_auc          DOUBLE PRECISION,
    test_accuracy     DOUBLE PRECISION,
    test_f1           DOUBLE PRECISION,
    train_samples     BIGINT,
    test_samples      BIGINT,
    training_date     TIMESTAMP,
    registered_at     TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
SORTKEY (training_date);

-- Verify tables
SELECT 'Tables created:' AS status;
SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;
