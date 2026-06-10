"""
Apache Airflow DAG: Dustinia CX Analytics Pipeline
====================================================
Pipeline batch processing untuk menganalisis Customer Experience
dari dataset Olist Brazilian E-Commerce (Dustinia Delixia Groceria).

Flow:
  CSV Files → [Fetch & Denormalize] → Data Lake (.parquet)
            → [Spark Processing]   → ClickHouse (13+ tables)
            → Metabase Dashboard   → 6 tabs, 39+ queries

Dataset: Olist Brazilian E-Commerce (11 CSV files)
Schedule: Setiap 5 menit (*/5 * * * *)
Owner: Raymond Julius Pardosi (5025241268)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ══════════════════════════════════════════════════════════════
# Default Arguments
# ══════════════════════════════════════════════════════════════
default_args = {
    "owner": "raymond_pardosi",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# ══════════════════════════════════════════════════════════════
# DAG Definition
# ══════════════════════════════════════════════════════════════
with DAG(
    dag_id="dustinia_cx_pipeline",
    default_args=default_args,
    description="Customer Experience Analytics Pipeline — Dustinia Delixia Groceria",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=["mci", "cx-analytics", "dustinia", "olist"],
    doc_md="""
    ## Dustinia CX Analytics Pipeline
    
    Pipeline end-to-end untuk analisis Customer Experience:
    
    1. **fetch_orders** — Membaca CSV, JOIN, denormalisasi → Parquet
    2. **process_orders_spark** — PySpark aggregation → ClickHouse
    
    Menjawab pertanyaan CEO: *"Mengapa review score stagnan?"*
    """,
) as dag:

    # ── Task 1: Fetch & Denormalize CSV → Parquet ─────────────
    fetch_orders = BashOperator(
        task_id="fetch_orders",
        bash_command="python /opt/airflow/dags/scripts/fetch_orders.py",
        doc_md="Membaca 11 CSV, melakukan JOIN denormalisasi, simpan sebagai Parquet.",
    )

    # ── Task 2: Spark Analytics → ClickHouse ──────────────────
    process_orders_spark = BashOperator(
        task_id="process_orders_spark",
        bash_command="python /opt/airflow/dags/scripts/process_orders_spark.py",
        doc_md="PySpark: data cleaning + 13 aggregations → ClickHouse (2 DB, 13+ tables).",
    )

    # ── Pipeline Flow ─────────────────────────────────────────
    fetch_orders >> process_orders_spark
