"""
Task 1: Fetch Orders — CSV → Denormalized Parquet in Data Lake
================================================================
Membaca 11 CSV files dari dataset Olist Brazilian E-Commerce,
melakukan JOIN dan denormalisasi menjadi satu fact table,
dan menyimpan sebagai Parquet (v1.0) ke Data Lake.

Schema Input (11 CSV):
  orders, order_items, products, customers, sellers,
  order_payments, order_reviews, category_translation,
  geolocation, mql, closed_deals

Schema Output (1 Parquet — 27 columns):
  Denormalized fact table siap diproses oleh PySpark

Author: Raymond Julius Pardosi (5025241268)
"""

import pandas as pd
import os
from datetime import datetime


def fetch_orders():
    """
    Main function: Baca CSV → JOIN → Denormalisasi → Parquet
    """
    # ── Path Configuration ────────────────────────────────────
    DATA_SOURCE = "/opt/airflow/data_source"
    DATA_LAKE = "/opt/airflow/data_lake/orders"

    os.makedirs(DATA_LAKE, exist_ok=True)

    print("=" * 70)
    print("🔄 DUSTINIA CX PIPELINE — Task 1: Fetch & Denormalize")
    print("=" * 70)

    # ══════════════════════════════════════════════════════════
    # 1. BACA SEMUA CSV FILES
    # ══════════════════════════════════════════════════════════
    print("\n📖 Membaca CSV files dari Data Source...")

    orders = pd.read_csv(
        f"{DATA_SOURCE}/orders.csv",
        parse_dates=[
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    )
    print(f"   ✅ orders.csv: {len(orders):,} baris")

    order_items = pd.read_csv(f"{DATA_SOURCE}/order_items.csv")
    print(f"   ✅ order_items.csv: {len(order_items):,} baris")

    products = pd.read_csv(f"{DATA_SOURCE}/products.csv")
    print(f"   ✅ products.csv: {len(products):,} baris")

    customers = pd.read_csv(f"{DATA_SOURCE}/customers.csv")
    print(f"   ✅ customers.csv: {len(customers):,} baris")

    sellers = pd.read_csv(f"{DATA_SOURCE}/sellers.csv")
    print(f"   ✅ sellers.csv: {len(sellers):,} baris")

    payments = pd.read_csv(f"{DATA_SOURCE}/order_payments.csv")
    print(f"   ✅ order_payments.csv: {len(payments):,} baris")

    reviews = pd.read_csv(f"{DATA_SOURCE}/order_reviews.csv")
    print(f"   ✅ order_reviews.csv: {len(reviews):,} baris")

    categories = pd.read_csv(f"{DATA_SOURCE}/category_translation.csv")
    print(f"   ✅ category_translation.csv: {len(categories):,} baris")

    # ══════════════════════════════════════════════════════════
    # 2. PRE-PROCESSING: Aggregate & Deduplicate
    # ══════════════════════════════════════════════════════════
    print("\n🔧 Pre-processing sebelum JOIN...")

    # 2a. Aggregate payments per order (satu order bisa punya multiple payments)
    payments_agg = (
        payments.groupby("order_id")
        .agg(
            payment_type=("payment_type", "first"),
            payment_installments=("payment_installments", "max"),
            payment_value=("payment_value", "sum"),
        )
        .reset_index()
    )
    print(f"   📊 Payments aggregated: {len(payments_agg):,} orders")

    # 2b. Deduplicate reviews — ambil review terbaru per order
    reviews_sorted = reviews.sort_values("review_creation_date", ascending=True)
    reviews_dedup = reviews_sorted.drop_duplicates(subset="order_id", keep="last")
    print(f"   📊 Reviews deduplicated: {len(reviews_dedup):,} (dari {len(reviews):,})")

    # 2c. Enrich products with English category names
    products_enriched = products.merge(
        categories, on="product_category_name", how="left"
    )
    print(f"   📊 Products enriched with category translation")

    # ══════════════════════════════════════════════════════════
    # 3. DENORMALISASI: JOIN semua tabel
    # ══════════════════════════════════════════════════════════
    print("\n🔗 Melakukan denormalisasi (JOIN)...")

    # Mulai dari order_items sebagai basis (grain: 1 baris = 1 item dalam 1 order)
    df = order_items.copy()

    # JOIN 1: orders (informasi pesanan & timestamp)
    df = df.merge(orders, on="order_id", how="left")
    print(f"   ✅ JOIN orders: {len(df):,} baris")

    # JOIN 2: customers (informasi pelanggan & lokasi)
    df = df.merge(
        customers[
            [
                "customer_id",
                "customer_unique_id",
                "customer_zip_code_prefix",
                "customer_city",
                "customer_state",
            ]
        ],
        on="customer_id",
        how="left",
    )
    print(f"   ✅ JOIN customers: {len(df):,} baris")

    # JOIN 3: products (kategori & spesifikasi produk)
    df = df.merge(
        products_enriched[
            [
                "product_id",
                "product_category_name",
                "product_category_name_english",
                "product_weight_g",
                "product_length_cm",
                "product_height_cm",
                "product_width_cm",
            ]
        ],
        on="product_id",
        how="left",
    )
    print(f"   ✅ JOIN products: {len(df):,} baris")

    # JOIN 4: sellers (lokasi penjual)
    df = df.merge(
        sellers[["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"]],
        on="seller_id",
        how="left",
    )
    print(f"   ✅ JOIN sellers: {len(df):,} baris")

    # JOIN 5: payments (metode & nilai pembayaran)
    df = df.merge(payments_agg, on="order_id", how="left")
    print(f"   ✅ JOIN payments: {len(df):,} baris")

    # JOIN 6: reviews (skor & komentar ulasan — KEY CX METRIC)
    df = df.merge(
        reviews_dedup[["order_id", "review_score", "review_comment_message"]],
        on="order_id",
        how="left",
    )
    print(f"   ✅ JOIN reviews: {len(df):,} baris")

    # ══════════════════════════════════════════════════════════
    # 4. FEATURE ENGINEERING: Computed Columns
    # ══════════════════════════════════════════════════════════
    print("\n⚙️  Feature Engineering...")

    # 4a. Tambah ingested_date (partition key)
    df["ingested_date"] = datetime.now().strftime("%Y-%m-%d")

    # 4b. Delivery delay (hari) — positive = terlambat, negative = lebih cepat
    df["delivery_delay_days"] = (
        pd.to_datetime(df["order_delivered_customer_date"])
        - pd.to_datetime(df["order_estimated_delivery_date"])
    ).dt.total_seconds() / 86400.0

    # 4c. Shipping duration (hari) — dari carrier ke customer
    df["shipping_duration_days"] = (
        pd.to_datetime(df["order_delivered_customer_date"])
        - pd.to_datetime(df["order_delivered_carrier_date"])
    ).dt.total_seconds() / 86400.0

    # 4d. Approval delay (jam) — dari pembelian ke approval
    df["approval_delay_hours"] = (
        pd.to_datetime(df["order_approved_at"])
        - pd.to_datetime(df["order_purchase_timestamp"])
    ).dt.total_seconds() / 3600.0

    # 4e. Is late delivery flag
    df["is_late_delivery"] = (df["delivery_delay_days"] > 0).astype(int)

    # 4f. Purchase hour & day of week
    purchase_ts = pd.to_datetime(df["order_purchase_timestamp"])
    df["order_hour_of_day"] = purchase_ts.dt.hour
    df["order_day_of_week"] = purchase_ts.dt.dayofweek  # 0=Mon, 6=Sun

    # 4g. Total item value (price + freight)
    df["total_item_value"] = df["price"] + df["freight_value"]

    print(f"   ✅ {len(df.columns)} kolom siap, {len(df):,} baris total")

    # ══════════════════════════════════════════════════════════
    # 5. SELECT & REORDER COLUMNS
    # ══════════════════════════════════════════════════════════
    output_columns = [
        # Order identifiers
        "order_id",
        "customer_id",
        "customer_unique_id",
        "order_item_id",
        "product_id",
        "seller_id",
        # Order status & timestamps
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        # Location
        "customer_city",
        "customer_state",
        "customer_zip_code_prefix",
        "seller_city",
        "seller_state",
        "seller_zip_code_prefix",
        # Product
        "product_category_name",
        "product_category_name_english",
        "product_weight_g",
        # Financial
        "price",
        "freight_value",
        "total_item_value",
        "payment_type",
        "payment_installments",
        "payment_value",
        # CX Metrics (⭐ KEY)
        "review_score",
        "review_comment_message",
        # Computed features
        "delivery_delay_days",
        "shipping_duration_days",
        "approval_delay_hours",
        "is_late_delivery",
        "order_hour_of_day",
        "order_day_of_week",
        # Pipeline metadata
        "ingested_date",
    ]

    # Filter hanya kolom yang ada (guard against missing columns)
    existing_cols = [c for c in output_columns if c in df.columns]
    df_output = df[existing_cols].copy()  # .copy() to avoid SettingWithCopyWarning

    # ══════════════════════════════════════════════════════════
    # 6. SAVE PARQUET (v1.0 — cross-version compatibility)
    # ══════════════════════════════════════════════════════════
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"{DATA_LAKE}/orders_{timestamp}.parquet"

    # Convert datetime columns to string for Parquet v1.0 compatibility
    datetime_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col in datetime_cols:
        if col in df_output.columns:
            df_output[col] = df_output[col].astype(str)

    df_output.to_parquet(output_path, index=False, engine="pyarrow", version="1.0")

    print("\n" + "=" * 70)
    print(f"✅ SELESAI! Disimpan ke: {output_path}")
    print(f"   📊 Total baris: {len(df_output):,}")
    print(f"   📊 Total kolom: {len(df_output.columns)}")
    print(f"   📊 Ukuran file: {os.path.getsize(output_path) / 1024 / 1024:.1f} MB")
    print("=" * 70)


if __name__ == "__main__":
    fetch_orders()
