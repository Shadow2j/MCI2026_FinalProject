"""
Task 2: Process Orders with Apache Spark → Load to ClickHouse
==============================================================
CX-focused analytics pipeline for Dustinia Delixia Groceria.
Analyzes review scores, delivery performance, seller quality,
and customer experience patterns.

Author: Raymond Julius Pardosi (5025241268)
"""

from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType
from pyspark.storagelevel import StorageLevel
from pyspark.ml.feature import VectorAssembler, StandardScaler as SparkStandardScaler, Tokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.clustering import KMeans
from pyspark.ml.regression import RandomForestRegressor
from clickhouse_driver import Client
import pandas as pd
import os
import re
import glob


# ---------------------------------------------------------------------------
# Helper: ClickHouse Schema DDL
# ---------------------------------------------------------------------------
def ensure_clickhouse_schema(client):
    """Create databases and all required tables in ClickHouse."""
    print("🏗️  Creating ClickHouse databases and tables …")

    client.execute("CREATE DATABASE IF NOT EXISTS orders_db")
    client.execute("CREATE DATABASE IF NOT EXISTS analytics")

    # ── orders_db tables ──────────────────────────────────────────────────
    client.execute("""
        CREATE TABLE IF NOT EXISTS orders_db.data_quality_report (
            ingested_date     String,
            column_name       String,
            total_rows        UInt64,
            missing_count     UInt64,
            missing_pct       Float64
        ) ENGINE = MergeTree()
        ORDER BY (ingested_date, column_name)
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS orders_db.order_items (
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
            ingested_date                 String
        ) ENGINE = MergeTree()
        ORDER BY (order_id, order_item_id)
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS orders_db.top_products (
            product_category_name_english String,
            total_orders                  UInt64,
            total_revenue                 Float64,
            avg_price                     Float64,
            avg_review_score              Nullable(Float64),
            total_items                   UInt64
        ) ENGINE = MergeTree()
        ORDER BY total_revenue
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS orders_db.category_summary (
            product_category_name_english String,
            total_orders                  UInt64,
            total_revenue                 Float64,
            total_items                   UInt64,
            avg_review_score              Nullable(Float64),
            avg_price                     Float64,
            avg_freight                   Float64
        ) ENGINE = MergeTree()
        ORDER BY total_revenue
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS orders_db.hourly_activity (
            order_hour    Int32,
            total_orders  UInt64,
            total_items   UInt64,
            total_revenue Float64
        ) ENGINE = MergeTree()
        ORDER BY order_hour
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS orders_db.history_category_trend (
            product_category_name_english String,
            total_orders                  UInt64,
            total_revenue                 Float64,
            avg_review_score              Nullable(Float64),
            batch_time                    DateTime,
            ingested_date                 String
        ) ENGINE = MergeTree()
        ORDER BY (ingested_date, product_category_name_english)
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS orders_db.daily_summary (
            summary_date              String,
            total_orders              UInt64,
            total_revenue             Float64,
            avg_basket_size           Float64,
            avg_review_score          Nullable(Float64),
            total_customers           UInt64,
            on_time_delivery_pct      Float64,
            late_delivery_count       UInt64,
            avg_delivery_delay_days   Nullable(Float64),
            ingested_date             String
        ) ENGINE = MergeTree()
        ORDER BY summary_date
    """)

    # ── analytics tables ──────────────────────────────────────────────────
    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.review_analysis (
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
        ORDER BY product_category_name_english
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.delivery_performance (
            customer_state        String,
            total_orders          UInt64,
            avg_delivery_days     Nullable(Float64),
            avg_delay_days        Nullable(Float64),
            on_time_pct           Float64,
            late_delivery_pct     Float64,
            avg_review_when_late  Nullable(Float64),
            avg_review_when_ontime Nullable(Float64),
            avg_freight_value     Float64
        ) ENGINE = MergeTree()
        ORDER BY customer_state
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.seller_performance (
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
        ORDER BY (seller_state, seller_id)
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.customer_loyalty_rfm (
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
        ORDER BY customer_unique_id
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.geographic_analysis (
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
        ORDER BY (customer_state, customer_city)
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.payment_analysis (
            payment_type       String,
            total_orders       UInt64,
            total_revenue      Float64,
            avg_payment_value  Float64,
            avg_installments   Float64,
            avg_review_score   Nullable(Float64)
        ) ENGINE = MergeTree()
        ORDER BY payment_type
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.sales_forecasting (
            product_category_name_english String,
            current_total_orders          UInt64,
            current_total_revenue         Float64,
            avg_order_value               Float64,
            growth_rate                   Float64,
            forecasted_orders             Float64,
            forecasted_revenue            Float64,
            risk_level                    String
        ) ENGINE = MergeTree()
        ORDER BY product_category_name_english
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.history_category_trend (
            product_category_name_english String,
            total_orders                  UInt64,
            total_revenue                 Float64,
            avg_review_score              Nullable(Float64),
            batch_time                    DateTime,
            ingested_date                 String
        ) ENGINE = MergeTree()
        ORDER BY (ingested_date, product_category_name_english)
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.products_performance (
            product_id                    String,
            product_category_name_english String,
            total_orders                  UInt64,
            total_revenue                 Float64,
            avg_price                     Float64,
            avg_freight                   Float64,
            avg_review_score              Nullable(Float64),
            total_unique_buyers           UInt64
        ) ENGINE = MergeTree()
        ORDER BY (product_category_name_english, product_id)
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.hourly_capacity (
            order_hour                 Int32,
            total_orders               UInt64,
            total_revenue              Float64,
            avg_items_per_order        Float64,
            capacity_utilization_pct   Float64,
            peak_label                 String
        ) ENGINE = MergeTree()
        ORDER BY order_hour
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.feature_importances (
            feature_name       String,
            importance_pct     Float64
        ) ENGINE = MergeTree()
        ORDER BY importance_pct
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.top_bad_review_words (
            word               String,
            frequency          UInt64
        ) ENGINE = MergeTree()
        ORDER BY frequency
    """)

    # ── NEW: CX Deep-Dive tables (AGG 18-22) ─────────────────────────────
    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.monthly_review_trend (
            year_month              String,
            total_reviews           UInt64,
            avg_review_score        Nullable(Float64),
            positive_review_pct     Float64,
            neutral_review_pct      Float64,
            negative_review_pct     Float64,
            late_delivery_pct       Float64,
            avg_delivery_delay_days Nullable(Float64),
            avg_freight_value       Float64,
            total_orders            UInt64
        ) ENGINE = MergeTree()
        ORDER BY year_month
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.review_root_cause_matrix (
            is_late               String,
            is_high_freight       String,
            is_slow_approval      String,
            total_orders          UInt64,
            avg_review_score      Nullable(Float64),
            negative_review_pct   Float64,
            positive_review_pct   Float64,
            factor_count          Int32
        ) ENGINE = MergeTree()
        ORDER BY (is_late, is_high_freight, is_slow_approval)
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.seller_state_review (
            seller_state          String,
            customer_state        String,
            total_orders          UInt64,
            avg_review_score      Nullable(Float64),
            avg_shipping_days     Nullable(Float64),
            late_delivery_pct     Float64,
            is_same_state         String
        ) ENGINE = MergeTree()
        ORDER BY (seller_state, customer_state)
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.monthly_delivery_accuracy (
            year_month              String,
            total_delivered          UInt64,
            on_time_pct             Float64,
            late_pct                Float64,
            avg_delay_days          Nullable(Float64),
            avg_shipping_days       Nullable(Float64),
            avg_review_when_late    Nullable(Float64),
            avg_review_when_ontime  Nullable(Float64),
            review_gap              Nullable(Float64)
        ) ENGINE = MergeTree()
        ORDER BY year_month
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.review_score_shift (
            year_month       String,
            review_score     Int32,
            total_count      UInt64,
            month_total      UInt64,
            score_pct        Float64
        ) ENGINE = MergeTree()
        ORDER BY (year_month, review_score)
    """)

    # ── NEW: Simulation tables (AGG 23-24) ───────────────────────────────
    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.simulation_scenarios (
            scenario_id                 String,
            scenario_name               String,
            description                 String,
            solution_recommendation     String,
            total_orders                UInt64,
            affected_orders             UInt64,
            affected_pct                Float64,
            baseline_avg_review         Float64,
            baseline_positive_pct       Float64,
            baseline_negative_pct       Float64,
            baseline_neutral_pct        Float64,
            projected_avg_review        Float64,
            projected_positive_pct      Float64,
            projected_negative_pct      Float64,
            projected_neutral_pct       Float64,
            review_delta                Float64,
            positive_delta              Float64,
            negative_delta              Float64,
            review_improvement_pct      Float64,
            ci_lower_95                 Float64,
            ci_upper_95                 Float64,
            additional_orders_est       UInt64,
            revenue_impact_est_brl      Float64,
            model_type                  String
        ) ENGINE = MergeTree()
        ORDER BY scenario_id
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS analytics.simulation_feature_impact (
            feature_name    String,
            feature_label   String,
            importance_pct  Float64,
            model_r2        Float64
        ) ENGINE = MergeTree()
        ORDER BY importance_pct
    """)

    print("✅  All databases and tables created successfully!")


# ---------------------------------------------------------------------------
# Helper: Write pandas DataFrame to ClickHouse
# ---------------------------------------------------------------------------
def write_to_clickhouse(client, df_pandas, database, table, mode="truncate"):
    """Write a pandas DataFrame to a ClickHouse table.

    Args:
        client:    clickhouse_driver.Client instance.
        df_pandas: pandas DataFrame to insert.
        database:  target database name.
        table:     target table name.
        mode:      'truncate' — wipe then insert; 'append' — insert only.
    """
    fqn = f"{database}.{table}"

    if df_pandas.empty:
        print(f"  ⚠️  No data to write to {fqn} — skipping.")
        return

    if mode == "truncate":
        client.execute(f"TRUNCATE TABLE {fqn}")

    integer_columns_by_table = {
        "orders_db.order_items": [
            "order_item_id",
            "customer_zip_code_prefix",
            "seller_zip_code_prefix",
            "payment_installments",
            "review_score",
            "is_late_delivery",
            "order_hour_of_day",
            "order_day_of_week",
        ],
        "analytics.customer_loyalty_rfm": ["recency_days"],
        "analytics.review_root_cause_matrix": ["factor_count"],
        "analytics.review_score_shift": ["review_score"],
    }

    # Replace pandas NaN/NaT with None so clickhouse_driver sends NULLs.
    # Cast through object first; otherwise NaN can survive in numeric columns.
    df_pandas = df_pandas.astype(object).where(pd.notnull(df_pandas), None)

    columns = df_pandas.columns.tolist()
    col_str = ", ".join(columns)
    data = df_pandas.values.tolist()

    int_cols_set = set(integer_columns_by_table.get(fqn, []))

    # FIX: Tambahkan `not c.startswith('is_')` untuk menghindari false-positive.
    # Contoh: kolom `is_high_freight` mengandung kata 'freight' sehingga masuk
    # float_cols_set, lalu kode mencoba float('Yes') → ValueError.
    # Kolom yang diawali 'is_' selalu berupa flag boolean (0/1) atau string (Yes/No),
    # bukan float murni — ditangani terpisah sebagai int atau dibiarkan apa adanya.
    float_cols_set = set(
        c for c in columns
        if not c.startswith("is_")           # ← exclude flag columns
        and (
            "avg"       in c
            or "pct"    in c
            or "revenue" in c
            or "price"  in c
            or "freight" in c
            or "value"  in c
            or "size"   in c
            or "monetary" in c
            or "growth" in c
            or c in ["forecasted_orders", "forecasted_revenue",
                     "importance_pct", "model_r2",
                     "review_delta", "positive_delta", "negative_delta",
                     "review_improvement_pct", "ci_lower_95", "ci_upper_95",
                     "revenue_impact_est_brl", "affected_pct",
                     "polarization_index", "score_pct"]
        )
    )

    int_col_indices   = [i for i, c in enumerate(columns) if c in int_cols_set]
    float_col_indices = [i for i, c in enumerate(columns) if c in float_cols_set]

    for row in data:
        for i in int_col_indices:
            if row[i] is not None:
                row[i] = int(row[i])
        for i in float_col_indices:
            # Guard: jangan konversi jika nilainya sudah berupa string
            # (defensive terhadap kolom yang namanya kebetulan match heuristik)
            if row[i] is not None and not isinstance(row[i], str):
                row[i] = float(row[i])

    # Batch insert for performance (10 000 rows per batch)
    batch_size = 10_000
    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        client.execute(f"INSERT INTO {fqn} ({col_str}) VALUES", batch, types_check=True)

    print(f"  💾  Wrote {len(data):,} rows → {fqn} (mode={mode})")


# ---------------------------------------------------------------------------
# Main analytics pipeline
# ---------------------------------------------------------------------------
def run_spark_analytics():
    """Execute the full Spark analytics pipeline and load to ClickHouse."""

    print("=" * 70)
    print("🚀  Dustinia Delixia Groceria — Spark Analytics Pipeline")
    print(f"📅  Run started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # ── 1. Spark session ─────────────────────────────────────────────────
    spark = (
        SparkSession.builder
        .appName("DustiniaDelixia_OrdersAnalytics")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("✅  SparkSession created (driver 2 g)")

    try:
        # ── 2. Read parquet ──────────────────────────────────────────────
        parquet_dir = "/opt/airflow/data_lake/orders"
        parquet_files = glob.glob(os.path.join(parquet_dir, "*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in {parquet_dir}")

        parquet_path = max(parquet_files, key=os.path.getmtime)
        print(f"\n📂  Reading parquet from {parquet_path} …")
        df_raw = spark.read.parquet(parquet_path)
        total_raw = df_raw.count()
        print(f"  📊  Raw rows: {total_raw:,}")

        # ── 3. ClickHouse connection ─────────────────────────────────────
        ch_client = Client(host="clickhouse", port=9000, user="airflow", password="airflow")
        ensure_clickhouse_schema(ch_client)

        # ==================================================================
        # STAGE 1 — Data Quality Detection
        # ==================================================================
        print("\n🔍  Stage 1: Data‑quality detection …")

        quality_columns = [
            "order_id", "customer_id", "product_id", "seller_id",
            "product_category_name_english", "customer_state",
            "seller_state", "review_score", "price", "order_status",
        ]

        quality_rows = []
        for col_name in quality_columns:
            null_cnt = df_raw.filter(F.col(col_name).isNull()).count()
            missing_cnt = df_raw.filter(
                F.col(col_name).cast("string") == "missing"
            ).count()
            quality_rows.append({
                "ingested_date": date.today().isoformat(),  # FIX: must be String not date object
                "column_name":  col_name,
                "total_rows":   total_raw,
                "missing_count": missing_cnt + null_cnt,
                "missing_pct":  round((missing_cnt + null_cnt) / max(total_raw, 1) * 100, 2),
            })

        df_quality_pd = pd.DataFrame(quality_rows)
        write_to_clickhouse(ch_client, df_quality_pd,
                            "orders_db", "data_quality_report", mode="truncate")
        print("  ✅  Data quality report saved.")

        # ==================================================================
        # STAGE 2 — Imputation
        # ==================================================================
        print("\n🧹  Stage 2: Data imputation …")

        string_cols = [
            "order_id", "customer_id", "customer_unique_id",
            "product_id", "seller_id", "order_status",
            "customer_city", "customer_state",
            "seller_city", "seller_state",
            "product_category_name", "product_category_name_english",
            "payment_type", "review_comment_message",
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date",
            "order_estimated_delivery_date", "ingested_date",
        ]

        integer_cols = [
            "order_item_id", "customer_zip_code_prefix", "seller_zip_code_prefix",
            "payment_installments", "is_late_delivery",
            "order_hour_of_day", "order_day_of_week",
        ]

        float_cols = [
            "product_weight_g", "price", "freight_value",
            "total_item_value", "payment_value",
        ]

        # Columns to leave as‑is
        leave_null_cols = {"review_score", "delivery_delay_days",
                           "shipping_duration_days", "approval_delay_hours"}

        df_clean = df_raw
        for c in string_cols:
            if c in df_clean.columns and c not in leave_null_cols:
                df_clean = df_clean.withColumn(
                    c, F.when(F.col(c).isNull(), F.lit("missing")).otherwise(F.col(c))
                )

        for c in integer_cols:
            if c in df_clean.columns and c not in leave_null_cols:
                df_clean = df_clean.withColumn(
                    c,
                    F.when(F.col(c).isNull(), F.lit(0))
                    .otherwise(F.col(c))
                    .cast("int"),
                )

        for c in float_cols:
            if c in df_clean.columns and c not in leave_null_cols:
                df_clean = df_clean.withColumn(
                    c,
                    F.when(F.col(c).isNull(), F.lit(0.0))
                    .otherwise(F.col(c))
                    .cast("double"),
                )

        nullable_int_cols = ["review_score"]
        nullable_float_cols = [
            "delivery_delay_days", "shipping_duration_days", "approval_delay_hours",
        ]

        for c in nullable_int_cols:
            if c in df_clean.columns:
                df_clean = df_clean.withColumn(c, F.col(c).cast("int"))

        for c in nullable_float_cols:
            if c in df_clean.columns:
                df_clean = df_clean.withColumn(c, F.col(c).cast("double"))

        df_clean = df_clean.persist(StorageLevel.MEMORY_AND_DISK)
        clean_count = df_clean.count()
        print(f"  ✅  Cleaned rows: {clean_count:,} (cached)")

        today_str = date.today().isoformat()

        # ==================================================================
        # AGGREGATIONS
        # ==================================================================

        # ── AGG 1: order_items (TRUNCATE) ─────────────────────────────────
        # FIX: was 'append' which caused duplicate rows every 5-minute run
        print("\n📦  AGG 1/24 — order_items (fact table) …")
        df_items_pd = df_clean.toPandas()
        write_to_clickhouse(ch_client, df_items_pd,
                            "orders_db", "order_items", mode="truncate")

        # ── AGG 2: top_products (TRUNCATE) ───────────────────────────────
        print("📦  AGG 2/24 — top_products …")
        df_top = (
            df_clean
            .groupBy("product_category_name_english")
            .agg(
                F.countDistinct("order_id").alias("total_orders"),
                F.sum("price").alias("total_revenue"),
                F.avg("price").alias("avg_price"),
                F.avg("review_score").alias("avg_review_score"),
                F.count("*").alias("total_items"),
            )
        )
        write_to_clickhouse(ch_client, df_top.toPandas(),
                            "orders_db", "top_products", mode="truncate")

        # ── AGG 3: category_summary (TRUNCATE) ──────────────────────────
        print("📦  AGG 3/24 — category_summary …")
        df_cat = (
            df_clean
            .groupBy("product_category_name_english")
            .agg(
                F.countDistinct("order_id").alias("total_orders"),
                F.sum("price").alias("total_revenue"),
                F.count("*").alias("total_items"),
                F.avg("review_score").alias("avg_review_score"),
                F.avg("price").alias("avg_price"),
                F.avg("freight_value").alias("avg_freight"),
            )
        )
        write_to_clickhouse(ch_client, df_cat.toPandas(),
                            "orders_db", "category_summary", mode="truncate")

        # ── AGG 4: hourly_activity (TRUNCATE) ───────────────────────────
        print("📦  AGG 4/22 — hourly_activity …")
        df_hour = (
            df_clean
            .groupBy(F.col("order_hour_of_day").alias("order_hour"))
            .agg(
                F.countDistinct("order_id").alias("total_orders"),
                F.count("*").alias("total_items"),
                F.sum("price").alias("total_revenue"),
            )
            .orderBy("order_hour")
        )
        write_to_clickhouse(ch_client, df_hour.toPandas(),
                            "orders_db", "hourly_activity", mode="truncate")

        # ── AGG 5: review_analysis (TRUNCATE) ───────────────────────────
        print("📦  AGG 5/22 — review_analysis …")
        df_reviews = df_clean.filter(F.col("review_score").isNotNull())
        df_rev = (
            df_reviews
            .groupBy("product_category_name_english")
            .agg(
                F.count("*").alias("total_reviews"),
                F.avg("review_score").alias("avg_review_score"),
                F.round(
                    F.sum(F.when(F.col("review_score") >= 4, 1).otherwise(0))
                    / F.count("*") * 100, 2
                ).alias("positive_review_pct"),
                F.round(
                    F.sum(F.when(F.col("review_score") <= 2, 1).otherwise(0))
                    / F.count("*") * 100, 2
                ).alias("negative_review_pct"),
                F.round(
                    F.sum(F.when(F.col("review_score") == 3, 1).otherwise(0))
                    / F.count("*") * 100, 2
                ).alias("neutral_review_pct"),
                F.avg("delivery_delay_days").alias("avg_delivery_delay_days"),
                F.round(
                    F.sum(F.when(F.col("is_late_delivery") == 1, 1).otherwise(0))
                    / F.count("*") * 100, 2
                ).alias("late_delivery_pct"),
                F.avg("freight_value").alias("avg_freight_value"),
                F.avg("price").alias("avg_price"),
            )
        )
        write_to_clickhouse(ch_client, df_rev.toPandas(),
                            "analytics", "review_analysis", mode="truncate")

        # ── AGG 6: delivery_performance (TRUNCATE) ──────────────────────
        print("📦  AGG 6/22 — delivery_performance …")
        df_del = (
            df_clean
            .groupBy("customer_state")
            .agg(
                F.countDistinct("order_id").alias("total_orders"),
                F.avg("shipping_duration_days").alias("avg_delivery_days"),
                F.avg("delivery_delay_days").alias("avg_delay_days"),
                F.round(
                    F.sum(F.when(
                        (F.col("is_late_delivery") == 0) | F.col("is_late_delivery").isNull(), 1
                    ).otherwise(0)) / F.count("*") * 100, 2
                ).alias("on_time_pct"),
                F.round(
                    F.sum(F.when(F.col("is_late_delivery") == 1, 1).otherwise(0))
                    / F.count("*") * 100, 2
                ).alias("late_delivery_pct"),
                F.avg(
                    F.when(F.col("is_late_delivery") == 1, F.col("review_score"))
                ).alias("avg_review_when_late"),
                F.avg(
                    F.when(F.col("is_late_delivery") == 0, F.col("review_score"))
                ).alias("avg_review_when_ontime"),
                F.avg("freight_value").alias("avg_freight_value"),
            )
        )
        write_to_clickhouse(ch_client, df_del.toPandas(),
                            "analytics", "delivery_performance", mode="truncate")

        # ── AGG 7: seller_performance (TRUNCATE) ────────────────────────
        print("📦  AGG 7/22 — seller_performance …")
        df_sell = (
            df_clean
            .groupBy("seller_id", "seller_city", "seller_state")
            .agg(
                F.countDistinct("order_id").alias("total_orders"),
                F.sum("price").alias("total_revenue"),
                F.avg("review_score").alias("avg_review_score"),
                F.avg("shipping_duration_days").alias("avg_delivery_days"),
                F.round(
                    F.sum(F.when(F.col("is_late_delivery") == 1, 1).otherwise(0))
                    / F.count("*") * 100, 2
                ).alias("late_delivery_pct"),
                F.count("*").alias("total_products_sold"),
            )
        )
        write_to_clickhouse(ch_client, df_sell.toPandas(),
                            "analytics", "seller_performance", mode="truncate")

        # ── AGG 8: customer_loyalty_rfm (TRUNCATE) ──────────────────────
        print("📦  AGG 8/24 — customer_loyalty_rfm …")

        # FIX: Gunakan max date dalam dataset sebagai referensi recency,
        #      bukan current_date(). Dataset Olist adalah data historis
        #      2016-2018 — jika pakai current_date() (2026), semua pelanggan
        #      akan memiliki recency ~3000 hari → semua masuk "High" churn risk
        #      tanpa segmentasi yang bermakna.
        max_order_row = (
            df_clean
            .agg(F.max(F.to_date(F.col("order_purchase_timestamp"))).alias("max_date"))
            .collect()[0]
        )
        max_order_date_str = str(max_order_row["max_date"])   # format: 'YYYY-MM-DD'
        print(f"  📅  Dataset max order date (reference for recency): {max_order_date_str}")

        df_rfm = (
            df_clean
            .groupBy("customer_unique_id")
            .agg(
                F.countDistinct("order_id").alias("total_orders"),
                F.sum("payment_value").alias("total_spend"),
                F.avg("review_score").alias("avg_review_score"),
                F.min("order_purchase_timestamp").alias("first_order_date"),
                F.max("order_purchase_timestamp").alias("last_order_date"),
            )
            .withColumn(
                "recency_days",
                # Hitung selisih dari max date dataset, bukan current_date()
                F.datediff(
                    F.lit(max_order_date_str).cast("date"),
                    F.to_date(F.col("last_order_date"))
                ),
            )
            .withColumn("frequency", F.col("total_orders"))
            .withColumn("monetary", F.col("total_spend"))
            .withColumn(
                "loyalty_tier",
                F.when(F.col("total_orders") >= 3, F.lit("Gold"))
                .when(F.col("total_orders") >= 2, F.lit("Silver"))
                .otherwise(F.lit("Bronze")),
            )
            .withColumn(
                "churn_risk",
                # High  : tidak order > 2 bulan (Groceria churn risk)
                # Medium: tidak order 1-2 bulan
                # Low   : masih aktif berbelanja dalam 1 bulan terakhir
                F.when(F.col("recency_days") > 60, F.lit("High"))
                .when(F.col("recency_days") > 30,  F.lit("Medium"))
                .otherwise(F.lit("Low")),
            )
        )

        # Apply K-Means
        df_rfm_ml = df_rfm.na.fill({"recency_days": 0, "frequency": 0, "monetary": 0.0})
        assembler = VectorAssembler(inputCols=["recency_days", "frequency", "monetary"], outputCol="rfm_features")
        rfm_features_df = assembler.transform(df_rfm_ml)
        scaler = SparkStandardScaler(inputCol="rfm_features", outputCol="scaledFeatures", withStd=True, withMean=True)
        scalerModel = scaler.fit(rfm_features_df)
        rfm_scaled = scalerModel.transform(rfm_features_df)
        
        kmeans = KMeans(featuresCol="scaledFeatures", k=4, seed=42)
        model = kmeans.fit(rfm_scaled)
        df_rfm_clustered = model.transform(rfm_scaled)
        
        df_rfm_clustered = df_rfm_clustered.withColumn(
            "kmeans_cluster",
            F.when(F.col("prediction") == 0, F.lit("Cluster 0"))
            .when(F.col("prediction") == 1, F.lit("Cluster 1"))
            .when(F.col("prediction") == 2, F.lit("Cluster 2"))
            .otherwise(F.lit("Cluster 3"))
        ).drop("rfm_features", "scaledFeatures", "prediction")

        write_to_clickhouse(ch_client, df_rfm_clustered.toPandas(),
                            "analytics", "customer_loyalty_rfm", mode="truncate")

        # ── AGG 9: geographic_analysis (TRUNCATE) ───────────────────────
        print("📦  AGG 9/22 — geographic_analysis …")
        df_geo = (
            df_clean
            .groupBy("customer_state", "customer_city")
            .agg(
                F.countDistinct("order_id").alias("total_orders"),
                F.sum("price").alias("total_revenue"),
                F.avg("review_score").alias("avg_review_score"),
                F.avg("shipping_duration_days").alias("avg_delivery_days"),
                F.avg("freight_value").alias("avg_freight_value"),
                F.countDistinct("customer_unique_id").alias("total_customers"),
                F.round(
                    F.sum(F.when(F.col("is_late_delivery") == 1, 1).otherwise(0))
                    / F.count("*") * 100, 2
                ).alias("late_delivery_pct"),
            )
        )
        write_to_clickhouse(ch_client, df_geo.toPandas(),
                            "analytics", "geographic_analysis", mode="truncate")

        # ── AGG 10: payment_analysis (TRUNCATE) ─────────────────────────
        print("📦  AGG 10/22 — payment_analysis …")
        df_pay = (
            df_clean
            .groupBy("payment_type")
            .agg(
                F.countDistinct("order_id").alias("total_orders"),
                F.sum("payment_value").alias("total_revenue"),
                F.avg("payment_value").alias("avg_payment_value"),
                F.avg("payment_installments").alias("avg_installments"),
                F.avg("review_score").alias("avg_review_score"),
            )
        )
        write_to_clickhouse(ch_client, df_pay.toPandas(),
                            "analytics", "payment_analysis", mode="truncate")

        # ── AGG 11: sales_forecasting (TRUNCATE) ────────────────────────
        print("📦  AGG 11/22 — sales_forecasting …")

        # Identify repeat customers per category
        df_cust_cat = (
            df_clean
            .groupBy("product_category_name_english", "customer_unique_id")
            .agg(F.countDistinct("order_id").alias("cust_orders"))
        )
        df_repeat = (
            df_cust_cat
            .groupBy("product_category_name_english")
            .agg(
                F.count("*").alias("total_customers"),
                F.sum(
                    F.when(F.col("cust_orders") > 1, 1).otherwise(0)
                ).alias("repeat_customers"),
            )
            .withColumn(
                "growth_rate",
                F.round(F.col("repeat_customers") / F.col("total_customers"), 4),
            )
        )

        df_cat_base = (
            df_clean
            .groupBy("product_category_name_english")
            .agg(
                F.countDistinct("order_id").alias("current_total_orders"),
                F.sum("price").alias("current_total_revenue"),
                F.avg("price").alias("avg_order_value"),
            )
        )

        df_forecast = (
            df_cat_base
            .join(df_repeat, on="product_category_name_english", how="left")
            .withColumn("growth_rate",
                        F.coalesce(F.col("growth_rate"), F.lit(0.0)))
            .withColumn("forecasted_orders",
                        F.round(F.col("current_total_orders") * (1 + F.col("growth_rate")), 2))
            .withColumn("forecasted_revenue",
                        F.round(F.col("current_total_revenue") * (1 + F.col("growth_rate")), 2))
            .withColumn(
                "risk_level",
                F.when(F.col("growth_rate") < 0, F.lit("High Risk"))
                .when(F.col("growth_rate") <= 0.3, F.lit("Medium"))
                .otherwise(F.lit("Low Risk")),
            )
            .select(
                "product_category_name_english",
                "current_total_orders",
                "current_total_revenue",
                "avg_order_value",
                "growth_rate",
                "forecasted_orders",
                "forecasted_revenue",
                "risk_level",
            )
        )
        write_to_clickhouse(ch_client, df_forecast.toPandas(),
                            "analytics", "sales_forecasting", mode="truncate")

        # ── AGG 12: history_category_trend (APPEND × 2) ────────────────
        print("📦  AGG 12/22 — history_category_trend …")
        df_trend = (
            df_clean
            .groupBy("product_category_name_english")
            .agg(
                F.countDistinct("order_id").alias("total_orders"),
                F.sum("price").alias("total_revenue"),
                F.avg("review_score").alias("avg_review_score"),
            )
            .withColumn("batch_time", F.current_timestamp())
            .withColumn("ingested_date", F.current_date().cast("string"))
        )
        trend_pd = df_trend.toPandas()
        # FIX: cast batch_time Timestamp → Python datetime so clickhouse_driver can serialize it
        if "batch_time" in trend_pd.columns:
            trend_pd["batch_time"] = pd.to_datetime(trend_pd["batch_time"]).dt.to_pydatetime()
        write_to_clickhouse(ch_client, trend_pd,
                            "analytics", "history_category_trend", mode="truncate")
        write_to_clickhouse(ch_client, trend_pd,
                            "orders_db", "history_category_trend", mode="truncate")

        # ── AGG 13: daily_summary (TRUNCATE) ────────────────────────────
        # FIX: was 'append' which caused unbounded growth; one summary row per run
        print("📦  AGG 13/22 — daily_summary …")

        df_daily = (
            df_clean
            .agg(
                F.countDistinct("order_id").alias("total_orders"),
                F.sum("price").alias("total_revenue"),
                (F.count("*") / F.countDistinct("order_id")).alias("avg_basket_size"),
                F.avg("review_score").alias("avg_review_score"),
                F.countDistinct("customer_unique_id").alias("total_customers"),
                F.round(
                    F.sum(F.when(F.col("is_late_delivery") == 0, 1).otherwise(0))
                    / F.count("*") * 100, 2
                ).alias("on_time_delivery_pct"),
                F.sum(
                    F.when(F.col("is_late_delivery") == 1, 1).otherwise(0)
                ).alias("late_delivery_count"),
                F.avg("delivery_delay_days").alias("avg_delivery_delay_days"),
            )
            .withColumn("summary_date", F.current_date().cast("string"))
            .withColumn("ingested_date", F.current_date().cast("string"))
            .select(
                "summary_date", "total_orders", "total_revenue",
                "avg_basket_size", "avg_review_score", "total_customers",
                "on_time_delivery_pct", "late_delivery_count",
                "avg_delivery_delay_days", "ingested_date",
            )
        )
        write_to_clickhouse(ch_client, df_daily.toPandas(),
                            "orders_db", "daily_summary", mode="truncate")

        # ── AGG 14: products_performance (TRUNCATE) ─────────────────────
        print("📦  AGG 14/22 — products_performance …")
        df_prod = (
            df_clean
            .groupBy("product_id", "product_category_name_english")
            .agg(
                F.countDistinct("order_id").alias("total_orders"),
                F.sum("price").alias("total_revenue"),
                F.avg("price").alias("avg_price"),
                F.avg("freight_value").alias("avg_freight"),
                F.avg("review_score").alias("avg_review_score"),
                F.countDistinct("customer_unique_id").alias("total_unique_buyers"),
            )
        )
        write_to_clickhouse(ch_client, df_prod.toPandas(),
                            "analytics", "products_performance", mode="truncate")

        # ── AGG 15: hourly_capacity (TRUNCATE) ──────────────────────────
        print("📦  AGG 15/22 — hourly_capacity …")
        df_hcap_base = (
            df_clean
            .groupBy(F.col("order_hour_of_day").alias("order_hour"))
            .agg(
                F.countDistinct("order_id").alias("total_orders"),
                F.sum("price").alias("total_revenue"),
                (F.count("*") / F.countDistinct("order_id")).alias("avg_items_per_order"),
            )
            .orderBy("order_hour")
        )

        max_orders = df_hcap_base.agg(
            F.max("total_orders")).first()[0] or 1

        df_hcap = (
            df_hcap_base
            .withColumn(
                "capacity_utilization_pct",
                F.round(F.col("total_orders") / F.lit(max_orders) * 100, 2),
            )
            .withColumn(
                "peak_label",
                F.when(F.col("capacity_utilization_pct") > 80, F.lit("Peak"))
                .when(F.col("capacity_utilization_pct") >= 50, F.lit("Normal"))
                .otherwise(F.lit("Off-Peak")),
            )
        )
        write_to_clickhouse(ch_client, df_hcap.toPandas(),
                            "analytics", "hourly_capacity", mode="truncate")

        # ── AGG 16: feature_importances (Random Forest) ─────────────────
        print("📦  AGG 16/22 — feature_importances (Random Forest MLlib) …")
        df_rf = df_clean.filter(F.col("review_score").isNotNull() & F.col("delivery_delay_days").isNotNull() & F.col("shipping_duration_days").isNotNull() & F.col("freight_value").isNotNull() & F.col("price").isNotNull() & F.col("product_weight_g").isNotNull())
        if df_rf.count() > 0:
            assembler_rf = VectorAssembler(inputCols=["delivery_delay_days", "shipping_duration_days", "freight_value", "price", "product_weight_g"], outputCol="features")
            df_rf_features = assembler_rf.transform(df_rf)
            rf_algo = RandomForestRegressor(featuresCol="features", labelCol="review_score", numTrees=10, maxDepth=5, seed=42)
            rf_model = rf_algo.fit(df_rf_features)
            importances = rf_model.featureImportances.toArray()
            feature_names = ["delivery_delay_days", "shipping_duration_days", "freight_value", "price", "product_weight_g"]
            df_importances = pd.DataFrame({"feature_name": feature_names, "importance_pct": [float(i) * 100 for i in importances]})
            write_to_clickhouse(ch_client, df_importances, "analytics", "feature_importances", mode="truncate")
        else:
            print("  ⚠️  Not enough data for Random Forest")

        # ── AGG 17: top_bad_review_words (NLP Sentiment) ────────────────
        print("📦  AGG 17/22 — top_bad_review_words (NLP) …")
        df_nlp = df_clean.filter((F.col("review_score") == 1) & F.col("review_comment_message").isNotNull())
        if df_nlp.count() > 0:
            df_nlp = df_nlp.withColumn("review_lower", F.lower(F.col("review_comment_message")))
            df_nlp = df_nlp.withColumn("words", F.split(F.col("review_lower"), "[^a-záéíóúâêôãõç]"))
            pt_stopwords = ["de", "a", "o", "que", "e", "do", "da", "em", "um", "para", "é", "com", "não", "uma", "os", "no", "se", "na", "por", "mais", "as", "dos", "como", "mas", "foi", "ao", "ele", "das", "tem", "à", "seu", "sua", "ou", "ser", "quando", "muito", "há", "nos", "já", "está", "eu", "também", "só", "pelo", "pela", "até", "isso", "ela", "entre", "era", "depois", "sem", "mesmo", "aos", "ter", "seus", "quem", "nas", "me", "esse", "eles", "estão", "você", "tinha", "foram", "essa", "num", "nem", "suas", "meu", "às", "minha", "têm", "numa", "pelos", "elas", "havia", "seja", "qual", "será", "nós", "tenho", "lhe", "deles", "essas", "esses", "pelas", "este", "fosse", "dele", "tu", "te", "vocês", "vos", "lhes", "meus", "minhas", "teu", "tua", "teus", "tuas", "nosso", "nossa", "nossos", "nossas", "dela", "delas", "esta", "estes", "estas", "aquele", "aquela", "aqueles", "aquelas", "isto", "aquilo", "estou", "estamos", "estão", "estive", "esteve", "estivemos", "estiveram", "estava", "estávamos", "estavam", "estivera", "estivéramos", "esteja", "estejamos", "estejam", "estivesse", "estivéssemos", "estivessem", "estiver", "estivermos", "estiverem", "hei", "há", "havemos", "hão", "houve", "houvemos", "houveram", "houvera", "houvéramos", "haja", "hajamos", "hajam", "houvesse", "houvéssemos", "houvessem", "houver", "houvermos", "houverem", "houverei", "houverá", "houveremos", "houverão", "houveria", "houveríamos", "houveriam", "sou", "somos", "são", "era", "éramos", "eram", "fui", "foi", "fomos", "foram", "fora", "fôramos", "seja", "sejamos", "sejam", "fosse", "fôssemos", "fossem", "for", "formos", "forem", "serei", "será", "seremos", "serão", "seria", "seríamos", "seriam", "tenho", "tem", "temos", "tém", "tinha", "tínhamos", "tinham", "tive", "teve", "tivemos", "tiveram", "tivera", "tivéramos", "tenha", "tenhamos", "tenham", "tivesse", "tivéssemos", "tivessem", "tiver", "tivermos", "tiverem", "terei", "terá", "teremos", "terão", "teria", "teríamos", "teriam", "produto", "comprei"]
            remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=pt_stopwords)
            df_nlp = remover.transform(df_nlp)
            df_words = df_nlp.select(F.explode(F.col("filtered_words")).alias("word"))
            df_words = df_words.filter(F.length(F.col("word")) > 2)
            df_top_words = df_words.groupBy("word").agg(F.count("*").alias("frequency")).orderBy(F.desc("frequency")).limit(20)
            write_to_clickhouse(ch_client, df_top_words.toPandas(), "analytics", "top_bad_review_words", mode="truncate")
        else:
            print("  ⚠️  Not enough 1-star reviews for NLP")

        # ══════════════════════════════════════════════════════════════
        # CX DEEP-DIVE AGGREGATIONS (AGG 18-22)
        # Answer: "Why are review scores stagnant?"
        # ══════════════════════════════════════════════════════════════

        # ── AGG 18: monthly_review_trend ─────────────────────────────
        print("📦  AGG 18/22 — monthly_review_trend …")
        df_with_month = df_clean.filter(
            F.col("review_score").isNotNull()
            & F.col("order_purchase_timestamp").isNotNull()
            & (F.col("order_purchase_timestamp") != "NaT")
        ).withColumn(
            "year_month",
            F.date_format(
                F.to_date(F.substring(F.col("order_purchase_timestamp"), 1, 10)),
                "yyyy-MM"
            ),
        )

        df_monthly_review = (
            df_with_month
            .groupBy("year_month")
            .agg(
                F.count("*").alias("total_reviews"),
                F.avg("review_score").alias("avg_review_score"),
                F.round(
                    F.sum(F.when(F.col("review_score") >= 4, 1).otherwise(0))
                    / F.count("*") * 100, 2
                ).alias("positive_review_pct"),
                F.round(
                    F.sum(F.when(F.col("review_score") == 3, 1).otherwise(0))
                    / F.count("*") * 100, 2
                ).alias("neutral_review_pct"),
                F.round(
                    F.sum(F.when(F.col("review_score") <= 2, 1).otherwise(0))
                    / F.count("*") * 100, 2
                ).alias("negative_review_pct"),
                F.round(
                    F.sum(F.when(F.col("is_late_delivery") == 1, 1).otherwise(0))
                    / F.count("*") * 100, 2
                ).alias("late_delivery_pct"),
                F.avg("delivery_delay_days").alias("avg_delivery_delay_days"),
                F.avg("freight_value").alias("avg_freight_value"),
                F.countDistinct("order_id").alias("total_orders"),
            )
            .filter(F.col("year_month").isNotNull())
            .orderBy("year_month")
        )
        write_to_clickhouse(ch_client, df_monthly_review.toPandas(),
                            "analytics", "monthly_review_trend", mode="truncate")

        # ── AGG 19: review_root_cause_matrix ─────────────────────────
        print("📦  AGG 19/22 — review_root_cause_matrix …")
        df_factors = (
            df_clean
            .filter(
                F.col("review_score").isNotNull()
                & F.col("is_late_delivery").isNotNull()
            )
            .withColumn(
                "is_late",
                F.when(F.col("is_late_delivery") == 1, F.lit("Yes")).otherwise(F.lit("No")),
            )
            .withColumn(
                "is_high_freight",
                F.when(F.col("freight_value") > 30, F.lit("Yes")).otherwise(F.lit("No")),
            )
            .withColumn(
                "is_slow_approval",
                F.when(F.col("approval_delay_hours") > 24, F.lit("Yes")).otherwise(F.lit("No")),
            )
            .withColumn(
                "factor_count",
                (F.when(F.col("is_late_delivery") == 1, 1).otherwise(0)
                 + F.when(F.col("freight_value") > 30, 1).otherwise(0)
                 + F.when(F.col("approval_delay_hours") > 24, 1).otherwise(0)),
            )
        )

        df_root_cause = (
            df_factors
            .groupBy("is_late", "is_high_freight", "is_slow_approval")
            .agg(
                F.countDistinct("order_id").alias("total_orders"),
                F.avg("review_score").alias("avg_review_score"),
                F.round(
                    F.sum(F.when(F.col("review_score") <= 2, 1).otherwise(0))
                    / F.count("*") * 100, 2
                ).alias("negative_review_pct"),
                F.round(
                    F.sum(F.when(F.col("review_score") >= 4, 1).otherwise(0))
                    / F.count("*") * 100, 2
                ).alias("positive_review_pct"),
                F.first("factor_count").alias("factor_count"),
            )
        )
        write_to_clickhouse(ch_client, df_root_cause.toPandas(),
                            "analytics", "review_root_cause_matrix", mode="truncate")

        # ── AGG 20: seller_state_review ──────────────────────────────
        print("📦  AGG 20/22 — seller_state_review …")
        df_seller_state = (
            df_clean
            .filter(
                (F.col("seller_state") != "missing")
                & (F.col("customer_state") != "missing")
                & F.col("review_score").isNotNull()
            )
            .groupBy("seller_state", "customer_state")
            .agg(
                F.countDistinct("order_id").alias("total_orders"),
                F.avg("review_score").alias("avg_review_score"),
                F.avg("shipping_duration_days").alias("avg_shipping_days"),
                F.round(
                    F.sum(F.when(F.col("is_late_delivery") == 1, 1).otherwise(0))
                    / F.count("*") * 100, 2
                ).alias("late_delivery_pct"),
            )
            .withColumn(
                "is_same_state",
                F.when(F.col("seller_state") == F.col("customer_state"),
                       F.lit("Same State")).otherwise(F.lit("Different State")),
            )
        )
        write_to_clickhouse(ch_client, df_seller_state.toPandas(),
                            "analytics", "seller_state_review", mode="truncate")

        # ── AGG 21: monthly_delivery_accuracy ────────────────────────
        print("📦  AGG 21/22 — monthly_delivery_accuracy …")
        df_del_month = (
            df_clean
            .filter(
                F.col("is_late_delivery").isNotNull()
                & F.col("order_purchase_timestamp").isNotNull()
                & (F.col("order_purchase_timestamp") != "NaT")
            )
            .withColumn(
                "year_month",
                F.date_format(
                    F.to_date(F.substring(F.col("order_purchase_timestamp"), 1, 10)),
                    "yyyy-MM"
                ),
            )
        )

        df_monthly_del = (
            df_del_month
            .groupBy("year_month")
            .agg(
                F.count("*").alias("total_delivered"),
                F.round(
                    F.sum(F.when(F.col("is_late_delivery") == 0, 1).otherwise(0))
                    / F.count("*") * 100, 2
                ).alias("on_time_pct"),
                F.round(
                    F.sum(F.when(F.col("is_late_delivery") == 1, 1).otherwise(0))
                    / F.count("*") * 100, 2
                ).alias("late_pct"),
                F.avg("delivery_delay_days").alias("avg_delay_days"),
                F.avg("shipping_duration_days").alias("avg_shipping_days"),
                F.avg(
                    F.when(F.col("is_late_delivery") == 1, F.col("review_score"))
                ).alias("avg_review_when_late"),
                F.avg(
                    F.when(F.col("is_late_delivery") == 0, F.col("review_score"))
                ).alias("avg_review_when_ontime"),
            )
            .withColumn(
                "review_gap",
                F.col("avg_review_when_ontime") - F.col("avg_review_when_late"),
            )
            .filter(F.col("year_month").isNotNull())
            .orderBy("year_month")
        )
        write_to_clickhouse(ch_client, df_monthly_del.toPandas(),
                            "analytics", "monthly_delivery_accuracy", mode="truncate")

        # ── AGG 22: review_score_shift ───────────────────────────────
        print("📦  AGG 22/22 — review_score_shift …")
        df_shift_base = (
            df_clean
            .filter(
                F.col("review_score").isNotNull()
                & F.col("order_purchase_timestamp").isNotNull()
                & (F.col("order_purchase_timestamp") != "NaT")
            )
            .withColumn(
                "year_month",
                F.date_format(
                    F.to_date(F.substring(F.col("order_purchase_timestamp"), 1, 10)),
                    "yyyy-MM"
                ),
            )
            .filter(F.col("year_month").isNotNull())
        )

        # Count per (year_month, review_score)
        df_score_counts = (
            df_shift_base
            .groupBy("year_month", "review_score")
            .agg(F.count("*").alias("total_count"))
        )

        # Total per month for percentage
        w_month = Window.partitionBy("year_month")
        df_score_shift = (
            df_score_counts
            .withColumn("month_total", F.sum("total_count").over(w_month))
            .withColumn(
                "score_pct",
                F.round(F.col("total_count") / F.col("month_total") * 100, 2),
            )
            .orderBy("year_month", "review_score")
        )
        write_to_clickhouse(ch_client, df_score_shift.toPandas(),
                            "analytics", "review_score_shift", mode="truncate")

        # ── AGG 23: simulation_scenarios ────────────────────────────────
        print("📦  AGG 23/25 — simulation_scenarios (from CSV) …")
        sim_scenarios_path = "/opt/airflow/data_lake/simulation/simulation_scenarios.csv"
        if os.path.exists(sim_scenarios_path):
            df_sim_scenarios = pd.read_csv(sim_scenarios_path, encoding="utf-8")
            # Cast types for ClickHouse
            uint64_cols = ["total_orders", "affected_orders", "additional_orders_est"]
            for col in uint64_cols:
                if col in df_sim_scenarios.columns:
                    df_sim_scenarios[col] = df_sim_scenarios[col].fillna(0).astype(int)
            write_to_clickhouse(ch_client, df_sim_scenarios,
                                "analytics", "simulation_scenarios", mode="truncate")
        else:
            print(f"  ⚠️  Simulation CSV not found: {sim_scenarios_path}")
            print("     Run dags/scripts/simulate_cx_improvements.py first.")

        # ── AGG 24: simulation_feature_impact ───────────────────────────
        print("📦  AGG 24/25 — simulation_feature_impact (from CSV) …")
        sim_feat_path = "/opt/airflow/data_lake/simulation/simulation_feature_impact.csv"
        if os.path.exists(sim_feat_path):
            df_sim_feat = pd.read_csv(sim_feat_path, encoding="utf-8")
            write_to_clickhouse(ch_client, df_sim_feat,
                                "analytics", "simulation_feature_impact", mode="truncate")
        else:
            print(f"  ⚠️  Feature impact CSV not found: {sim_feat_path}")

        # ── AGG 25: TF-IDF Review Keyword Analysis (Advanced) ───────────
        print("\n📦  AGG 25/25 — tfidf_review_keywords (Advanced TF-IDF + Bigrams) …")

        # Step 1: Filter reviews with actual Portuguese comments
        df_reviews = (
            df_clean
            .filter(
                F.col("review_score").isNotNull()
                & F.col("review_comment_message").isNotNull()
                & (F.col("review_comment_message") != "missing")
                & (F.col("review_comment_message") != "")
                & (F.length(F.col("review_comment_message")) > 5)
            )
            .select(
                F.monotonically_increasing_id().alias("doc_id"),
                F.col("review_score"),
                F.col("review_comment_message"),
            )
            .withColumn(
                "review_bucket",
                F.when(F.col("review_score") <= 2, "negative")
                 .when(F.col("review_score") >= 4, "positive")
                 .otherwise("neutral"),
            )
        )

        total_review_docs = df_reviews.count()
        print(f"  📄  Total review documents with comments: {total_review_docs:,}")

        if total_review_docs < 100:
            print("  ⚠️  Too few review comments to run TF-IDF. Skipping AGG 25.")
        else:
            # Step 2: Tokenize + Bigrams UDF
            # (No NLTK — pure Python regex + list slicing, as per paper methodology)
            def tokenize_bigrams_pt(text):
                """Tokenize Portuguese text + generate bigrams. No external dict."""
                if not text:
                    return []
                # Keep Portuguese letters (including accented), lowercase
                clean = re.sub(r"[^a-z\u00e0-\u00fc\s]", " ", text.lower())
                tokens = [w for w in clean.split() if len(w) > 2]
                # Bigrams injection (from paper Section 6)
                grams = tokens[:]
                if len(tokens) > 1:
                    grams.extend([
                        f"{tokens[i]}_{tokens[i+1]}" for i in range(len(tokens) - 1)
                    ])
                return grams

            tokenize_udf = F.udf(tokenize_bigrams_pt, ArrayType(StringType()))

            # Step 3: Apply tokenization + persist (OOM Rescue from paper)
            df_tokenized = (
                df_reviews
                .withColumn("tokens", tokenize_udf(F.col("review_comment_message")))
                .filter(F.size(F.col("tokens")) > 0)
            )
            df_tokenized.persist(StorageLevel.MEMORY_AND_DISK)
            doc_count = df_tokenized.count()

            # Step 4: Explode tokens
            df_exploded = df_tokenized.select(
                "doc_id", "review_bucket",
                F.explode("tokens").alias("word")
            )

            # Step 5: Document Frequency (DF) per word
            df_df = (
                df_exploded
                .select("doc_id", "word").distinct()
                .groupBy("word")
                .count()
                .withColumnRenamed("count", "doc_freq")
            )

            # Step 6: IDF = log(N / DF) — Dynamic thresholding (paper Section 4)
            # IDF >= 2.8 removes common words; doc_freq >= 10 removes typos
            df_idf = (
                df_df
                .withColumn("idf", F.log(F.lit(float(doc_count)) / F.col("doc_freq")))
                .filter((F.col("idf") >= 2.8) & (F.col("doc_freq") >= 10))
            )
            valid_vocab = df_idf.count()
            print(f"  🔬  Valid vocabulary (IDF≥2.8, DF≥10): {valid_vocab:,} terms")

            # Step 7: TF per (review_bucket, word)
            df_tf = (
                df_exploded
                .groupBy("review_bucket", "word")
                .agg(F.count("*").alias("tf"))
            )

            # Step 8: Broadcast Join TF × IDF (paper Section 6)
            df_tfidf = df_tf.join(F.broadcast(df_idf), on="word", how="inner")

            # Step 9: Sublinear TF Scaling: TF_sub = 1 + log(TF)  (paper Section 6)
            df_tfidf = df_tfidf.withColumn(
                "tfidf_score", (1.0 + F.log(F.col("tf"))) * F.col("idf")
            )

            # Step 10: Log-Odds Ratio (Laplace smoothed) — negative vs positive
            df_neg = (
                df_exploded.filter(F.col("review_bucket") == "negative")
                .groupBy("word").agg(F.count("*").alias("freq_negative"))
            )
            df_pos = (
                df_exploded.filter(F.col("review_bucket") == "positive")
                .groupBy("word").agg(F.count("*").alias("freq_positive"))
            )
            total_neg = float(
                df_exploded.filter(F.col("review_bucket") == "negative").count()
            )
            total_pos = float(
                df_exploded.filter(F.col("review_bucket") == "positive").count()
            )

            df_log_odds = (
                df_neg.join(df_pos, on="word", how="outer")
                .fillna({"freq_negative": 0, "freq_positive": 0})
                .withColumn(
                    "log_odds",
                    F.log(
                        (F.col("freq_negative") + 0.5)
                        / (F.lit(total_neg) - F.col("freq_negative") + 0.5)
                    )
                    - F.log(
                        (F.col("freq_positive") + 0.5)
                        / (F.lit(total_pos) - F.col("freq_positive") + 0.5)
                    )
                )
            )

            # Step 11: Join everything + round
            df_final = (
                df_tfidf
                .join(
                    df_log_odds.select(
                        "word", "freq_negative", "freq_positive", "log_odds"
                    ),
                    on="word", how="left",
                )
                .fillna({"freq_negative": 0, "freq_positive": 0, "log_odds": 0.0})
                .select(
                    "review_bucket",
                    F.col("word").alias("keyword"),
                    F.round("tfidf_score", 4).alias("tfidf_score"),
                    F.round("log_odds", 4).alias("log_odds"),
                    F.col("freq_negative").cast("long"),
                    F.col("freq_positive").cast("long"),
                    F.col("doc_freq").alias("total_docs").cast("long"),
                )
            )

            # Step 12: Top 30 per bucket (paper: Top 3 per game → we use Top 30)
            w_bucket = Window.partitionBy("review_bucket").orderBy(
                F.col("tfidf_score").desc()
            )
            df_top = (
                df_final
                .withColumn("rank", F.row_number().over(w_bucket))
                .filter(F.col("rank") <= 30)
                .drop("rank")
            )

            df_top_pd = df_top.toPandas()
            for col in ["freq_negative", "freq_positive", "total_docs"]:
                df_top_pd[col] = df_top_pd[col].fillna(0).astype(int)

            write_to_clickhouse(
                ch_client, df_top_pd,
                "analytics", "tfidf_review_keywords", mode="truncate"
            )

            df_tokenized.unpersist()
            print(f"  ✅  TF-IDF done. {valid_vocab:,} valid vocab → top 30 per bucket saved.")

        # ==================================================================
        # POST-PROCESSING
        # ==================================================================

        # ── Cleanup old parquet files ────────────────────────────────────
        print("\n🗑️  Cleaning up processed parquet files …")
        parquet_dir = "/opt/airflow/data_lake/orders"
        parquet_files = glob.glob(os.path.join(parquet_dir, "*.parquet"))
        removed = 0
        for pf in parquet_files:
            try:
                os.remove(pf)
                removed += 1
            except OSError as exc:
                print(f"  ⚠️  Could not remove {pf}: {exc}")
        print(f"  🗑️  Removed {removed} parquet file(s).")

        # ── Unpersist ────────────────────────────────────────────────────
        df_clean.unpersist()
        print("🧹  DataFrame cache released.")

        # ── Summary ──────────────────────────────────────────────────────
        print("\n" + "=" * 70)
        print("🎉  Pipeline completed successfully!")
        print(f"📊  Total raw rows processed   : {total_raw:,}")
        print(f"📊  Total clean rows loaded     : {clean_count:,}")
        print(f"📊  Parquet files cleaned up     : {removed}")
        print(f"📅  Ingested date               : {today_str}")
        print(f"⏱️  Finished at                 : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)

    except Exception as exc:
        print(f"\n❌  Pipeline FAILED: {exc}")
        raise
    finally:
        spark.stop()
        print("🛑  SparkSession stopped.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run_spark_analytics()
