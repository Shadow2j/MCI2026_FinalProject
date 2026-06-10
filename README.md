<div align="center">

#  Dustinia Delixia Groceria — CX Analytics Pipeline

### Customer Experience Analytics — Final Project Lab MCI 2026

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/Apache_Spark-3.5.1-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.9.1-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![ClickHouse](https://img.shields.io/badge/ClickHouse-latest-FFCC01?style=flat-square&logo=clickhouse&logoColor=black)
![Metabase](https://img.shields.io/badge/Metabase-latest-509EE3?style=flat-square&logo=metabase&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)

> **Persona 2: Customer Experience Analyst**  
> **Mission:** Analyze why review scores tend to stagnate and are difficult to improve.

</div>

---

##  Table of Contents

1. [Team](#team)
2. [Architecture](#architecture)
3. [Dataset & Schema](#dataset--schema)
4. [Data Cleaning Pipeline](#data-cleaning-pipeline)
5. [Spark Processing & Aggregation](#spark-processing--aggregation)
6. [ClickHouse Data Warehouse](#clickhouse-data-warehouse)
7. [Docker Setup](#docker-setup)
8. [Running the Pipeline](#running-the-pipeline)
9. [Metabase Dashboard — 8 Tabs, 58 Queries](#metabase-dashboard)
10. [Key Insights & Recommendations](#key-insights--recommendations)
11. [References](#references)

---

## Team

<table align="center">
  <tr>
    <td align="center" width="400">
      <b>Raymond Julius Pardosi</b><br>
      <code>5025241268</code>
    </td>
  </tr>
</table>

**Final Project — Oprec Lab MCI 2026**

---

## Architecture

This pipeline implements a **Batch Processing Architecture** to analyze Customer Experience from an e-commerce dataset.

### Data Flow Diagram

```
┌───────────────────────────────────────────────────────────────────────────┐
│                    DUSTINIA CX ANALYTICS PIPELINE                         │
└───────────────────────────────────────────────────────────────────────────┘

  11 CSV Files (Olist E-Commerce Dataset)
           │
           ▼  fetch_orders.py — JOIN & Denormalize
  ╔══════════════════════════════════════╗
  ║   INGESTION LAYER                    ║   
  ║   Read 11 CSV files                  ║
  ║   JOIN (6 tables)                    ║
  ║   Feature Engineering (7 computed)   ║
  ║   Save .parquet v1.0                 ║
  ╚══════════╤═══════════════════════════╝
             │  /opt/airflow/data_lake/orders/*.parquet
             ▼
  ╔══════════════════════════════════════╗
  ║   PROCESSING LAYER (PySpark)         ║   process_orders_spark.py
  ║   Data Cleaning (2-stage)            ║   → Data Quality Audit
  ║   24 Analytical Aggregations         ║   → CX-Focused Metrics
  ║   Caching (MEMORY_AND_DISK)          ║   → Auto-cleanup Parquet
  ╚══════════╤═══════════════════════════╝
             │
             ▼
  ╔═════════════════════════════════════════════════════════════════╗
  ║                    SERVING LAYER — ClickHouse                   ║
  ║                                                                 ║
  ║  Database: orders_db             Database: analytics            ║
  ║  ┌─────────────────────────┐     ┌────────────────────────────┐ ║
  ║  │ order_items    [TRUNC]  │     │ review_analysis    [TRUNC] │ ║
  ║  │ daily_summary  [TRUNC]  │     │ delivery_performance       │ ║
  ║  │ history_trend  [TRUNC]  │     │ seller_performance         │ ║
  ║  │ top_products   [TRUNC]  │     │ customer_loyalty_rfm       │ ║
  ║  │ category_summary        │     │ geographic_analysis        │ ║
  ║  │ hourly_activity         │     │ payment_analysis           │ ║
  ║  │ data_quality_report     │     │ sales_forecasting          │ ║
  ║  └─────────────────────────┘     │ products_performance       │ ║
  ║                                  │ hourly_capacity            │ ║
  ║                                  │ history_trend    [TRUNC]   │ ║
  ║                                  └────────────────────────────┘ ║
  ╚══════════╤══════════════════════════════════════════════════════╝
             │
             ▼
   ╔═══════════════════════════════════════════════════╗
   ║   Metabase Dashboard — 8 Tabs, 58 Queries         ║
   ║   Overview · Knowledge · CX Predictive            ║
   ║   Improvements · Governance · CX Engine           ║
   ║   Deep Dive · CX Simulation (What-If Model)       ║
   ╚═══════════════════════════════════════════════════╝

  Pipeline is orchestrated by Apache Airflow (scheduled every 5 min)
```

### Flow Summary

| Step | File | Output |
|------|------|--------|
| Ingest | `fetch_orders.py` | Denormalized `.parquet` in Data Lake |
| Transform | `process_orders_spark.py` | 24 tables in ClickHouse (2 databases) |
| Orchestrate | `orders_pipeline_dag.py` | Schedule `*/5 * * * *`, retry 3x |
| **Simulate** | `simulate_cx_improvements.py` | What-If model → 3 CSV outputs |
| Visualize | Metabase | 8 tabs, 58 charts/queries |

---

## Dataset & Schema

This dataset represents e-commerce transactions from Olist (a Brazilian marketplace) that have been adapted for **Dustinia Delixia Groceria**.

### Input: 11 CSV Files

| File | Rows | Description |
|------|:----:|-------------|
| `orders.csv` | ~100K | Order information & timestamps |
| `order_items.csv` | ~113K | Items within an order (grain: 1 item per row) |
| `products.csv` | ~33K | Product catalog & specifications |
| `customers.csv` | ~99K | Customer information & location |
| `sellers.csv` | ~3K | Seller information & location |
| `order_payments.csv` | ~104K | Payment methods & values |
| `order_reviews.csv` | ~100K | Reviews & scores ( KEY DATA) |
| `category_translation.csv` | ~71 | Category name translation PT→EN |
| `geolocation.csv` | ~1M | Zip code coordinates |
| `mql.csv` | ~8K | Marketing qualified leads |
| `closed_deals.csv` | ~842 | Closed deals |

### Output: Denormalized Fact Table (36 columns)

| Column | Type | CX Relevance |
|--------|------|:------------:|
| `order_id` | String | Primary key |
| `customer_unique_id` | String | Cross-order identity |
| `order_status` | String | Delivery tracking |
| `order_purchase_timestamp` | DateTime | Temporal analysis |
| `order_delivered_customer_date` | DateTime | Actual delivery |
| `order_estimated_delivery_date` | DateTime | Promise vs reality |
| `customer_state` | String | Regional CX patterns |
| `seller_id` | String | Seller tracking |
| `price` | Float | Revenue |
| `freight_value` | Float | Shipping cost |
| `product_category_name_english` | String | Category |
| `payment_type` | String | Payment method |
| `review_score` | Int (1-5) |  **KEY METRIC** |
| `review_comment_message` | String | Sentiment source |
| `delivery_delay_days` | Float | **Computed**: positive = late |
| `shipping_duration_days` | Float | **Computed**: carrier → customer |
| `approval_delay_hours` | Float | **Computed**: purchase → approval |
| `is_late_delivery` | Int (0/1) | **Computed**: late flag |
| `order_hour_of_day` | Int (0-23) | **Computed**: purchase hour |
| `order_day_of_week` | Int (0-6) | **Computed**: purchase day |
| `total_item_value` | Float | **Computed**: price + freight |

---

## Data Cleaning Pipeline

Each batch goes through a transparent and auditable **2-stage cleaning pipeline**.

```
df_raw  ──────────────────────────────────────────────────
         │
         ▼ STAGE 1: DETECTION (Audit Before Cleaning)
         │   Count NULL + literal 'missing' per column
         │   → Store in orders_db.data_quality_report
         │   → Available in Tab 5 dashboard (Pipeline Health)
         │
         ▼ STAGE 2: IMPUTATION (Label, Do Not Drop)
         │   String NULL   → label "missing" (filterable in BI)
         │   Numeric NULL  → 0 (neutral aggregate value)
         │   review_score  → LEFT AS NULL (valid business case)
         │   delivery_delay → LEFT AS NULL (not yet delivered)
         │
         ▼ CACHE: df_clean.persist(MEMORY_AND_DISK)
         │   Prevents re-reading disk for 15 aggregations
         │
df_clean ─────────────────────────────────────────────────
```

### Imputation Strategy per Column

| Column | Strategy | Rationale |
|--------|----------|-----------|
| `product_category_name_english` | `"missing"` | Explicit label → detectable in query |
| `customer_state`, `seller_state` | `"missing"` | Filterable in BI tools |
| `order_id`, `product_id` | `0` | Neutral numeric value |
| `price`, `freight_value` | `0` | Safe for SUM/AVG operations |
| `review_score` | **Kept NULL** | Not all orders have reviews |
| `delivery_delay_days` | **Kept NULL** | Not all orders are delivered |

---

## Spark Processing & Aggregation

`process_orders_spark.py` executes **24 analytical aggregations & Machine Learning Models** from a single cached `df_clean`, focusing on Customer Experience.

### 24 Aggregations & ML Models

| # | Aggregation | Target Table | Mode | CX Focus |
|---|-------------|--------------|------|----------|
| 1 | Fact Table | `orders_db.order_items` | TRUNCATE | Raw data preservation |
| 2 | Top Products | `orders_db.top_products` | TRUNCATE | Product ranking by revenue |
| 3 | Category Summary | `orders_db.category_summary` | TRUNCATE | Category analytics |
| 4 | Hourly Activity | `orders_db.hourly_activity` | TRUNCATE | Time patterns |
| 5 | **Review Analysis** | `analytics.review_analysis` | TRUNCATE |  Review vs delivery/category |
| 6 | **Delivery Performance** | `analytics.delivery_performance` | TRUNCATE |  Late delivery impact |
| 7 | **Seller Performance** | `analytics.seller_performance` | TRUNCATE |  Seller quality ranking |
| 8 | **Customer Loyalty (RFM)** | `analytics.customer_loyalty_rfm` | TRUNCATE |  Churn risk prediction |
| 9 | Geographic Analysis | `analytics.geographic_analysis` | TRUNCATE | Regional CX patterns |
| 10 | Payment Analysis | `analytics.payment_analysis` | TRUNCATE | Payment method impact |
| 11 | Sales Forecasting | `analytics.sales_forecasting` | TRUNCATE | Demand projection |
| 12 | History Trend | `analytics/orders_db.history_category_trend` | TRUNCATE | Speed Layer data |
| 13 | Daily Summary | `orders_db.daily_summary` | TRUNCATE | Daily KPIs |
| 14 | Products Performance | `analytics.products_performance` | TRUNCATE | Product-level metrics |
| 15 | Hourly Capacity | `analytics.hourly_capacity` | TRUNCATE | Logistics capacity |
| 16 | **Random Forest ML** | `analytics.feature_importances` | TRUNCATE |  Review Score Drivers |
| 17 | **NLP Sentiment** | `analytics.top_bad_review_words` | TRUNCATE |  1-Star Review Topics |
| 18 | **Monthly Review Trend** | `analytics.monthly_review_trend` | TRUNCATE |  Monthly review decomposition |
| 19 | **Root Cause Matrix** | `analytics.review_root_cause_matrix` | TRUNCATE |  Compounding factor impact |
| 20 | **Seller × State Review** | `analytics.seller_state_review` | TRUNCATE |  Seller origin vs destination |
| 21 | **Monthly Delivery Accuracy** | `analytics.monthly_delivery_accuracy` | TRUNCATE |  Delivery promise trend |
| 22 | **Review Score Shift** | `analytics.review_score_shift` | TRUNCATE |  Distribution polarization |
| 23 | **Simulation Scenarios** | `analytics.simulation_scenarios` | TRUNCATE |  What-If projected review scores |
| 24 | **Simulation Feature Impact** | `analytics.simulation_feature_impact` | TRUNCATE |  ML feature importance |

### Key CX Metrics Computed

```python
# Review categories
positive = review_score >= 4
neutral  = review_score == 3
negative = review_score <= 2

# Delivery metrics
delivery_delay = delivered_date - estimated_date  # positive = late
is_late = delivery_delay > 0

# RFM Loyalty Tiers
Gold   = total_orders >= 5
Silver = total_orders 3-4
Bronze = total_orders < 3

# Churn Risk
High   = recency > 180 days
Medium = recency 90-180 days
Low    = recency < 90 days
```

---

## ClickHouse Data Warehouse

Pipeline output is stored in **2 databases** with a total of **24 tables**:

### Database: `orders_db` (Core Warehouse)

| Table | Mode | Engine | Description |
|-------|------|--------|-------------|
| `order_items` | TRUNCATE | ReplacingMergeTree | Denormalized fact table |
| `top_products` | TRUNCATE | MergeTree | Product rankings |
| `category_summary` | TRUNCATE | MergeTree | Category metrics |
| `hourly_activity` | TRUNCATE | MergeTree | Hourly distribution |
| `daily_summary` | TRUNCATE | ReplacingMergeTree | Daily KPIs |
| `data_quality_report` | DELETE-INSERT | MergeTree | Missing value audit |
| `history_category_trend` | TRUNCATE | MergeTree | NRT trend |

### Database: `analytics` (Analytical Layer)

| Table | Mode | Engine | CX Focus |
|-------|------|--------|----------|
| `review_analysis` | TRUNCATE | MergeTree |  Review patterns |
| `delivery_performance` | TRUNCATE | MergeTree |  Delivery impact |
| `seller_performance` | TRUNCATE | MergeTree |  Seller quality |
| `customer_loyalty_rfm` | TRUNCATE | MergeTree |  Churn risk |
| `geographic_analysis` | TRUNCATE | MergeTree | Regional patterns |
| `payment_analysis` | TRUNCATE | MergeTree | Payment impact |
| `sales_forecasting` | TRUNCATE | MergeTree | Demand projection |
| `products_performance` | TRUNCATE | MergeTree | Product metrics |
| `hourly_capacity` | TRUNCATE | MergeTree | Logistics planning |
| `history_category_trend` | TRUNCATE | MergeTree | Speed Layer |
| `feature_importances` | TRUNCATE | MergeTree |  RF Feature Weights |
| `top_bad_review_words` | TRUNCATE | MergeTree |  NLP Top Words |
| `monthly_review_trend` | TRUNCATE | MergeTree |  Monthly review decomposition |
| `review_root_cause_matrix` | TRUNCATE | MergeTree |  Compounding factor analysis |
| `seller_state_review` | TRUNCATE | MergeTree |  Seller-customer state analysis |
| `monthly_delivery_accuracy` | TRUNCATE | MergeTree |  Delivery accuracy trend |
| `review_score_shift` | TRUNCATE | MergeTree |  Review score distribution shift |
| `simulation_scenarios` | TRUNCATE | MergeTree |  What-If scenario projections |
| `simulation_feature_impact` | TRUNCATE | MergeTree |  ML feature importance |

---

## Docker Setup

### Prerequisites

- Docker Desktop installed and running
- Minimum 8GB RAM available for Docker
- Ports 8080, 3000, 8123, 9000 available

### Services

| Service | Image | Port | Purpose |
|---------|-------|:----:|---------|
| PostgreSQL | postgres:13 | - | Airflow metadata |
| Airflow Init | Custom | - | DB migration & admin user |
| Airflow Webserver | Custom | 8080 | Airflow Web UI |
| Airflow Scheduler | Custom | - | DAG scheduling |
| ClickHouse | clickhouse-server | 8123, 9000 | Data warehouse |
| Metabase | metabase | 3000 | Dashboard BI |

### Quick Start

```bash
# 1. Clone repository
git clone <repository-url>
cd DustiniaDelixia_Groceria

# 2. Ensure all 11 CSV files are inside the `dataset/` folder

# 3. Build & start all services
docker compose up --build -d

# 4. Wait ~2 minutes for initialization

# 5. Access services
#    Airflow:    http://localhost:8080 (admin/admin)
#    Metabase:   http://localhost:3000
#    ClickHouse: http://localhost:8123
```

---

## Running the Pipeline

### Method 1: Automatic (via Airflow Scheduler)

DAG `dustinia_cx_pipeline` will run automatically every 5 minutes once enabled in the Airflow UI.

1. Open **http://localhost:8080** (login: `admin`/`admin`)
2. Find the DAG `dustinia_cx_pipeline`
3. Toggle switch to **ON**
4. Pipeline will run automatically

### Method 2: Manual Trigger

```bash
# Trigger DAG via CLI
docker exec airflow-webserver airflow dags trigger dustinia_cx_pipeline

# Or trigger via Airflow Web UI:
# 1. Open DAG
# 2. Click the ▶ button (Trigger DAG)
```

### Verify Pipeline

```bash
# Check DAG loaded
docker exec airflow-webserver airflow dags list

# Check ClickHouse tables
docker exec clickhouse clickhouse-client --query "SHOW TABLES FROM orders_db"
docker exec clickhouse clickhouse-client --query "SHOW TABLES FROM analytics"

# Check data loaded
docker exec clickhouse clickhouse-client --query "SELECT count(*) FROM orders_db.order_items"
```

---

## Metabase Dashboard

### Setup Metabase Connection

1. Open **http://localhost:3000**
2. Complete initial setup wizard
3. Add ClickHouse as database:
   - **Database type:** ClickHouse
   - **Host:** clickhouse
   - **Port:** 8123
   - **Database name:** orders_db (for Tabs 1-2, 5)
   - Also add a connection to the `analytics` database

### 7 Dashboard Tabs

| Tab | Name | Queries | Focus |
|:---:|------|:-------:|-------|
| 1 | **General Overview** | 7 | KPI cards, revenue, categories |
| 2 | **Knowledge Detail** | 7 | Time patterns, delivery, geography |
| 3 | **Predictive & CX** | 7 |  Review analysis, delivery impact, churn |
| 4 | **Improvements** | 7 | Seller/category/geographic recommendations |
| 5 | **Data Governance** | 5 | Pipeline health, data quality |
| 6 | **CX Engine** | 9 | Deep analysis, root causes, CLV |
| 7 | **CX Deep Dive** | 11 |  Review stagnation root causes, polarization |
| 8 | **CX Simulation** | 8 |  What-If model: proyeksi dampak solusi |
| | **Total** | **58** | |

>  Full query documentation: [query_documentation.md](query_documentation.md)  
>  Raw SQL: [sql/query.sql](sql/query.sql)

---

## Key Insights & Recommendations

Based on the analysis of 58 dashboard queries + What-If simulation model, here are the main findings to answer the CEO's questions:

###  Why are Review Scores Stagnant?

| # | Root Cause | Impact | Evidence |
|---|-----------|--------|----------|
| 1 | **Late Delivery** |  Highest | Review score drops ~1.5 points when delivery is late |
| 2 | **High Freight Cost** |  High | Freight > 30 correlates with lower reviews |
| 3 | **Seller Inconsistency** |  High | Top 10% sellers have avg review 4.5, bottom 10% only 2.0 |
| 4 | **Geographic Disparity** |  Medium | Certain states have a 2-3x higher late delivery rate |
| 5 | **Slow Approval** |  Medium | Approval > 24 hours drops review ~0.3 points |

###  Actionable Recommendations

1.  **Improve Logistics** — Eliminate late deliveries (S1: +0.110 review points, 7.5% affected orders)
2.  **Evaluate Sellers** — Apply minimum standards for sellers, educate sellers with review < 3.0
3.  **Optimize Freight** — Review pricing strategy for heavy products, negotiate rates with carriers
4.  **Accelerate Approval** — Target approval < 6 hours, automation for standard orders
5.  **Continuous Monitoring** — Use Speed Layer (Tab 7) + Simulation Dashboard (Tab 8)

---

## Project Structure

```
DustiniaDelixia_Groceria/
├── .gitignore                    # Git ignore rules
├── Dockerfile                    # Airflow + Java + PySpark image
├── docker-compose.yml            # 6 services orchestration
├── requirements.txt              # Python dependencies
├── README.md                     # This file
├── query_documentation.md        # 58 query documentation
├── clickhouse/
│   └── schema.sql                # ClickHouse DDL (2 DB, 24 tables)
├── dags/
│   ├── orders_pipeline_dag.py    # Airflow DAG definition
│   └── scripts/
│       ├── __init__.py
│       ├── fetch_orders.py       # CSV → Parquet denormalization
│       ├── process_orders_spark.py  # PySpark → ClickHouse (24 aggregations)
│       └── simulate_cx_improvements.py  # What-If simulation model
├── sql/
│   └── query.sql                 # 58 Metabase dashboard queries
├── data_lake/                    # [Auto-created] Parquet files
│   ├── orders/
│   └── simulation/               # [Auto-created] Simulation CSV outputs
└── dataset/                    # Folder containing 11 CSV data files
```

---

## References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark (PySpark) Documentation](https://spark.apache.org/docs/latest/api/python/)
- [ClickHouse Documentation](https://clickhouse.com/docs)
- [Metabase Documentation](https://www.metabase.com/docs/)
- [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- Referensi arsitektur: [MCI2026_Task2_Kelompok14](https://github.com/Reiii0-0/MCI2026_Task2_Kelompok14)
- Referensi metodologi (TF-IDF & N-Grams): [TWS (Tinjauan Waktu Studi) by Farikh](https://medium.com/@farikh0mf/tws-4da6f3e57b1b?postPublishedType=repub)

---

<div align="center">

**Built with  for Final Project Lab MCI 2026**

*Raymond Julius Pardosi — 5025241268*

</div>
