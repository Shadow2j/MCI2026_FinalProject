-- ══════════════════════════════════════════════════════════════════════
-- ClickHouse Schema: Dustinia Delixia Groceria — CX Analytics Warehouse
-- ══════════════════════════════════════════════════════════════════════
-- Dataset: Olist Brazilian E-Commerce (adapted)
-- Focus: Customer Experience Analysis — Why review scores stagnate?
--
-- FIX LOG:
--   - ingested_date / summary_date: Date → String  (Python sends ISO string)
--   - total_orders / total_rows / counts: Int32 → UInt64
--   - Removed PARTITION BY toYYYYMM(ingested_date) — incompatible with String
--   - Removed ReplacingMergeTree on Date columns — caused silent type mismatch
--   - Aligned all types with what process_orders_spark.py actually inserts
-- ══════════════════════════════════════════════════════════════════════

-- ══════════════════════════════════════════════════════════════════════
-- 1. DATABASE DECLARATION
-- ══════════════════════════════════════════════════════════════════════
CREATE DATABASE IF NOT EXISTS orders_db;
CREATE DATABASE IF NOT EXISTS analytics;


-- ══════════════════════════════════════════════════════════════════════
-- 2. DATABASE: orders_db (Core Warehouse Layer)
-- ══════════════════════════════════════════════════════════════════════

-- 2.1 Fact Table: order_items (TRUNCATE-INSERT per pipeline run)
-- Satu baris = satu item produk dalam satu pesanan
CREATE TABLE IF NOT EXISTS orders_db.order_items
(
    order_id                      String,
    customer_id                   String,
    customer_unique_id            String,
    order_item_id                 Int32,
    product_id                    String,
    seller_id                     String,
    order_status                  String,
    order_purchase_timestamp      Nullable(String),
    order_approved_at             Nullable(String),
    order_delivered_carrier_date  Nullable(String),
    order_delivered_customer_date Nullable(String),
    order_estimated_delivery_date Nullable(String),
    customer_city                 String,
    customer_state                String,
    customer_zip_code_prefix      Int32,
    seller_city                   String,
    seller_state                  String,
    seller_zip_code_prefix        Int32,
    product_category_name         String,
    product_category_name_english String,
    product_weight_g              Float64,
    price                         Float64,
    freight_value                 Float64,
    total_item_value              Float64,
    payment_type                  String,
    payment_installments          Int32,
    payment_value                 Float64,
    review_score                  Nullable(Int32),
    review_comment_message        Nullable(String),
    delivery_delay_days           Nullable(Float64),
    shipping_duration_days        Nullable(Float64),
    approval_delay_hours          Nullable(Float64),
    is_late_delivery              Nullable(Int32),
    order_hour_of_day             Int32,
    order_day_of_week             Int32,
    ingested_date                 String          -- ISO string: 'YYYY-MM-DD'
) ENGINE = MergeTree()
ORDER BY (order_id, order_item_id);


-- 2.2 Top Products (TRUNCATE-INSERT)
CREATE TABLE IF NOT EXISTS orders_db.top_products
(
    product_category_name_english String,
    total_orders                  UInt64,
    total_revenue                 Float64,
    avg_price                     Float64,
    avg_review_score              Nullable(Float64),
    total_items                   UInt64
) ENGINE = MergeTree()
ORDER BY total_revenue;


-- 2.3 Category Summary (TRUNCATE-INSERT)
CREATE TABLE IF NOT EXISTS orders_db.category_summary
(
    product_category_name_english String,
    total_orders                  UInt64,
    total_revenue                 Float64,
    total_items                   UInt64,
    avg_review_score              Nullable(Float64),
    avg_price                     Float64,
    avg_freight                   Float64
) ENGINE = MergeTree()
ORDER BY total_revenue;


-- 2.4 Hourly Activity (TRUNCATE-INSERT)
CREATE TABLE IF NOT EXISTS orders_db.hourly_activity
(
    order_hour    Int32,
    total_orders  UInt64,
    total_items   UInt64,
    total_revenue Float64
) ENGINE = MergeTree()
ORDER BY order_hour;


-- 2.5 Daily Summary (TRUNCATE-INSERT — latest run snapshot)
CREATE TABLE IF NOT EXISTS orders_db.daily_summary
(
    summary_date              String,          -- ISO string: 'YYYY-MM-DD'
    total_orders              UInt64,
    total_revenue             Float64,
    avg_basket_size           Float64,
    avg_review_score          Nullable(Float64),
    total_customers           UInt64,
    on_time_delivery_pct      Float64,
    late_delivery_count       UInt64,
    avg_delivery_delay_days   Nullable(Float64),
    ingested_date             String           -- ISO string: 'YYYY-MM-DD'
) ENGINE = MergeTree()
ORDER BY summary_date;


-- 2.6 Data Quality Report (TRUNCATE-INSERT per run)
CREATE TABLE IF NOT EXISTS orders_db.data_quality_report
(
    ingested_date String,                      -- ISO string: 'YYYY-MM-DD'
    column_name   String,
    total_rows    UInt64,
    missing_count UInt64,
    missing_pct   Float64
) ENGINE = MergeTree()
ORDER BY (ingested_date, column_name);


-- 2.7 History Category Trend (APPEND — time-series)
CREATE TABLE IF NOT EXISTS orders_db.history_category_trend
(
    product_category_name_english String,
    total_orders                  UInt64,
    total_revenue                 Float64,
    avg_review_score              Nullable(Float64),
    batch_time                    DateTime,    -- Python datetime object
    ingested_date                 String       -- ISO string: 'YYYY-MM-DD'
) ENGINE = MergeTree()
ORDER BY (ingested_date, product_category_name_english);


-- ══════════════════════════════════════════════════════════════════════
-- 3. DATABASE: analytics (Analytical Layer — CX Focus)
-- ══════════════════════════════════════════════════════════════════════

-- 3.1 Review Analysis (⭐ KEY CX TABLE)
CREATE TABLE IF NOT EXISTS analytics.review_analysis
(
    product_category_name_english String,
    total_reviews                 UInt64,
    avg_review_score              Nullable(Float64),
    positive_review_pct           Float64,
    negative_review_pct           Float64,
    neutral_review_pct            Float64,
    avg_delivery_delay_days       Nullable(Float64),
    late_delivery_pct             Float64,
    avg_freight_value             Float64,
    avg_price                     Float64
) ENGINE = MergeTree()
ORDER BY product_category_name_english;


-- 3.2 Delivery Performance (⭐ KEY CX TABLE)
CREATE TABLE IF NOT EXISTS analytics.delivery_performance
(
    customer_state         String,
    total_orders           UInt64,
    avg_delivery_days      Nullable(Float64),
    avg_delay_days         Nullable(Float64),
    on_time_pct            Float64,
    late_delivery_pct      Float64,
    avg_review_when_late   Nullable(Float64),
    avg_review_when_ontime Nullable(Float64),
    avg_freight_value      Float64
) ENGINE = MergeTree()
ORDER BY customer_state;


-- 3.3 Seller Performance (⭐ KEY CX TABLE)
CREATE TABLE IF NOT EXISTS analytics.seller_performance
(
    seller_id            String,
    seller_city          String,
    seller_state         String,
    total_orders         UInt64,
    total_revenue        Float64,
    avg_review_score     Nullable(Float64),
    avg_delivery_days    Nullable(Float64),
    late_delivery_pct    Float64,
    total_products_sold  UInt64
) ENGINE = MergeTree()
ORDER BY (seller_state, seller_id);


-- 3.4 Customer Loyalty RFM Segmentation
CREATE TABLE IF NOT EXISTS analytics.customer_loyalty_rfm
(
    customer_unique_id String,
    total_orders       UInt64,
    total_spend        Float64,
    avg_review_score   Nullable(Float64),
    first_order_date   Nullable(String),
    last_order_date    Nullable(String),
    recency_days       Nullable(Int32),
    frequency          UInt64,
    monetary           Float64,
    loyalty_tier       String,
    churn_risk         String,
    kmeans_cluster     String
) ENGINE = MergeTree()
ORDER BY customer_unique_id;


-- 3.5 Geographic Analysis
CREATE TABLE IF NOT EXISTS analytics.geographic_analysis
(
    customer_state     String,
    customer_city      String,
    total_orders       UInt64,
    total_revenue      Float64,
    avg_review_score   Nullable(Float64),
    avg_delivery_days  Nullable(Float64),
    avg_freight_value  Float64,
    total_customers    UInt64,
    late_delivery_pct  Float64
) ENGINE = MergeTree()
ORDER BY (customer_state, customer_city);


-- 3.6 Payment Analysis
CREATE TABLE IF NOT EXISTS analytics.payment_analysis
(
    payment_type       String,
    total_orders       UInt64,
    total_revenue      Float64,
    avg_payment_value  Float64,
    avg_installments   Float64,
    avg_review_score   Nullable(Float64)
) ENGINE = MergeTree()
ORDER BY payment_type;


-- 3.7 Sales Forecasting
CREATE TABLE IF NOT EXISTS analytics.sales_forecasting
(
    product_category_name_english String,
    current_total_orders          UInt64,
    current_total_revenue         Float64,
    avg_order_value               Float64,
    growth_rate                   Float64,
    forecasted_orders             Float64,
    forecasted_revenue            Float64,
    risk_level                    String
) ENGINE = MergeTree()
ORDER BY product_category_name_english;


-- 3.8 Products Performance
CREATE TABLE IF NOT EXISTS analytics.products_performance
(
    product_id                    String,
    product_category_name_english String,
    total_orders                  UInt64,
    total_revenue                 Float64,
    avg_price                     Float64,
    avg_freight                   Float64,
    avg_review_score              Nullable(Float64),
    total_unique_buyers           UInt64
) ENGINE = MergeTree()
ORDER BY (product_category_name_english, product_id);


-- 3.9 Hourly Capacity
CREATE TABLE IF NOT EXISTS analytics.hourly_capacity
(
    order_hour                 Int32,
    total_orders               UInt64,
    total_revenue              Float64,
    avg_items_per_order        Float64,
    capacity_utilization_pct   Float64,
    peak_label                 String
) ENGINE = MergeTree()
ORDER BY order_hour;


-- 3.10 History Category Trend (APPEND — Speed Layer)
CREATE TABLE IF NOT EXISTS analytics.history_category_trend
(
    product_category_name_english String,
    total_orders                  UInt64,
    total_revenue                 Float64,
    avg_review_score              Nullable(Float64),
    batch_time                    DateTime,    -- Python datetime object
    ingested_date                 String       -- ISO string: 'YYYY-MM-DD'
) ENGINE = MergeTree()
ORDER BY (ingested_date, product_category_name_english);


-- 3.11 Feature Importances (Random Forest)
CREATE TABLE IF NOT EXISTS analytics.feature_importances
(
    feature_name       String,
    importance_pct     Float64
) ENGINE = MergeTree()
ORDER BY importance_pct;


-- 3.12 Top Bad Review Words (NLP Sentiment)
CREATE TABLE IF NOT EXISTS analytics.top_bad_review_words
(
    word               String,
    frequency          UInt64
) ENGINE = MergeTree()
ORDER BY frequency;


-- ══════════════════════════════════════════════════════════════════════
-- 4. CX DEEP-DIVE TABLES (AGG 18-22) — Why review scores stagnate?
-- ══════════════════════════════════════════════════════════════════════

-- 4.1 Monthly Review Trend — Decompose review evolution month-over-month
CREATE TABLE IF NOT EXISTS analytics.monthly_review_trend
(
    year_month              String,           -- 'YYYY-MM'
    total_reviews           UInt64,
    avg_review_score        Nullable(Float64),
    positive_review_pct     Float64,          -- score >= 4
    neutral_review_pct      Float64,          -- score == 3
    negative_review_pct     Float64,          -- score <= 2
    late_delivery_pct       Float64,
    avg_delivery_delay_days Nullable(Float64),
    avg_freight_value       Float64,
    total_orders            UInt64
) ENGINE = MergeTree()
ORDER BY year_month;


-- 4.2 Review Root Cause Matrix — Compounding factor impact
CREATE TABLE IF NOT EXISTS analytics.review_root_cause_matrix
(
    is_late               String,             -- Yes/No
    is_high_freight       String,             -- Yes/No (freight > 30)
    is_slow_approval      String,             -- Yes/No (approval > 24h)
    total_orders          UInt64,
    avg_review_score      Nullable(Float64),
    negative_review_pct   Float64,
    positive_review_pct   Float64,
    factor_count          Int32               -- 0-3 active factors
) ENGINE = MergeTree()
ORDER BY (is_late, is_high_freight, is_slow_approval);


-- 4.3 Seller × State Review — Cross-analysis of seller origin vs customer destination
CREATE TABLE IF NOT EXISTS analytics.seller_state_review
(
    seller_state          String,
    customer_state        String,
    total_orders          UInt64,
    avg_review_score      Nullable(Float64),
    avg_shipping_days     Nullable(Float64),
    late_delivery_pct     Float64,
    is_same_state         String              -- 'Same State' / 'Different State'
) ENGINE = MergeTree()
ORDER BY (seller_state, customer_state);


-- 4.4 Monthly Delivery Accuracy — Delivery promise trend over time
CREATE TABLE IF NOT EXISTS analytics.monthly_delivery_accuracy
(
    year_month              String,           -- 'YYYY-MM'
    total_delivered          UInt64,
    on_time_pct             Float64,
    late_pct                Float64,
    avg_delay_days          Nullable(Float64),
    avg_shipping_days       Nullable(Float64),
    avg_review_when_late    Nullable(Float64),
    avg_review_when_ontime  Nullable(Float64),
    review_gap              Nullable(Float64)  -- ontime_review - late_review
) ENGINE = MergeTree()
ORDER BY year_month;


-- 4.5 Review Score Shift — Distribution change over time (polarization detection)
CREATE TABLE IF NOT EXISTS analytics.review_score_shift
(
    year_month       String,                  -- 'YYYY-MM'
    review_score     Int32,                   -- 1-5
    total_count      UInt64,
    month_total      UInt64,
    score_pct        Float64                  -- % of total for that month
) ENGINE = MergeTree()
ORDER BY (year_month, review_score);


-- ══════════════════════════════════════════════════════════════════════
-- 5. SIMULATION TABLES (AGG 23-25) — What-If / Counterfactual Analysis
--    "Jika solusi CX diterapkan, review score akan seperti apa?"
-- ══════════════════════════════════════════════════════════════════════

-- 5.1 Simulation Scenarios — Ringkasan projected metrics per skenario
--     5 skenario: S1 (late delivery), S2 (approval), S3 (freight),
--                 S4 (bad sellers), S5 (combined best case)
CREATE TABLE IF NOT EXISTS analytics.simulation_scenarios
(
    scenario_id                 String,           -- 'S1'..'S5'
    scenario_name               String,           -- Human-readable label
    description                 String,           -- Penjelasan skenario
    solution_recommendation     String,           -- Rekomendasi aksi
    total_orders                UInt64,           -- Total orders disimulasikan
    affected_orders             UInt64,           -- Orders yang berubah kondisinya
    affected_pct                Float64,          -- % orders terdampak
    baseline_avg_review         Float64,          -- Review sebelum solusi
    baseline_positive_pct       Float64,          -- % positive sebelum solusi
    baseline_negative_pct       Float64,          -- % negative sebelum solusi
    baseline_neutral_pct        Float64,          -- % neutral sebelum solusi
    projected_avg_review        Float64,          -- Review setelah solusi (projected)
    projected_positive_pct      Float64,          -- % positive setelah solusi
    projected_negative_pct      Float64,          -- % negative setelah solusi
    projected_neutral_pct       Float64,          -- % neutral setelah solusi
    review_delta                Float64,          -- projected - baseline
    positive_delta              Float64,          -- delta positive pct
    negative_delta              Float64,          -- delta negative pct (negatif = membaik)
    review_improvement_pct      Float64,          -- % improvement dari baseline
    ci_lower_95                 Float64,          -- 95% CI lower bound
    ci_upper_95                 Float64,          -- 95% CI upper bound
    additional_orders_est       UInt64,           -- Estimasi order tambahan
    revenue_impact_est_brl      Float64,          -- Estimasi revenue impact (BRL)
    model_type                  String            -- 'RandomForest' or 'RuleBased'
) ENGINE = MergeTree()
ORDER BY scenario_id;


-- 5.2 Simulation Feature Impact — Feature importance dari model ML
--     Menunjukkan faktor mana yang paling berpengaruh pada review score
CREATE TABLE IF NOT EXISTS analytics.simulation_feature_impact
(
    feature_name    String,           -- Nama fitur internal
    feature_label   String,           -- Label human-readable
    importance_pct  Float64,          -- % kontribusi terhadap model
    model_r2        Float64           -- R² score model (kualitas prediksi)
) ENGINE = MergeTree()
ORDER BY importance_pct;
