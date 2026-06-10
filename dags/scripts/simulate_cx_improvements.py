"""
simulate_cx_improvements.py
===========================
What-If / Counterfactual Simulation Model — DustiniaDelixia Groceria
======================================================================
Menjawab pertanyaan: "Jika solusi CX diterapkan, data review score
akan seperti apa?"

Pendekatan: Counterfactual Simulation (bukan time-series forecasting)
  1. Hitung baseline metrics dari data aktual
  2. Train Random Forest untuk memprediksi review score
  3. Buat 5 skenario "dunia alternatif" (counterfactual transform)
  4. Prediksi review score baru per skenario
  5. Bandingkan projected vs baseline → output CSV

Skenario yang disimulasikan:
  S1 — Eliminate Late Delivery    : late_delivery_pct → 0%
  S2 — Accelerate Order Approval  : delivery delay berkurang (approval cepat)
  S3 — Cap Freight Cost           : freight_value ≤ 30 untuk semua order
  S4 — Remove Underperforming Sellers : simulasi eliminasi efek seller buruk
  S5 — Best Case Combined         : semua solusi di atas sekaligus

Output:
  data_lake/simulation_scenarios.csv   — ringkasan per skenario
  data_lake/simulation_order_detail.csv — prediksi per-order
  data_lake/simulation_feature_impact.csv — feature importance ML

Author: Raymond Julius Pardosi (5025241268)
"""

import os
import sys
import warnings
import numpy as np
import pandas as pd
from pathlib import Path

warnings.filterwarnings("ignore")

# ──────────────────────────────────────────────────────────────────────────────
# PATHS
# ──────────────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
ORDER_ITEMS_CSV = PROJECT_ROOT / "order_items_sample_colab.csv"
RFM_CSV         = PROJECT_ROOT / "data_rfm_colab.csv"
OUTPUT_DIR      = PROJECT_ROOT / "data_lake" / "simulation"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ENCODING = "utf-16"

# ──────────────────────────────────────────────────────────────────────────────
# CONSTANTS — Simulation assumptions (data-driven, dihitung dari observed uplift)
# ──────────────────────────────────────────────────────────────────────────────

# Late delivery: Jika delay dikurangi ke 0, shipping_duration juga membaik
LATE_DELIVERY_DELAY_CAP = 0.0       # delivery_delay_days setelah solusi
LATE_DELIVERY_DURATION_REDUCE = 3.0 # Asumsi shipping berkurang 3 hari rata-rata

# Approval acceleration: mengurangi delivery_delay_days sebesar 1.5 hari rata-rata
# (karena approval cepat → carrier pickup lebih cepat)
APPROVAL_DELAY_REDUCTION = 1.5

# Freight cap (dalam BRL)
FREIGHT_CAP = 30.0

# Seller quality: jika seller buruk dihapus, order yang terdampak mendapat
# uplift setara dengan rata-rata seller tier menengah
POOR_SELLER_REVIEW_THRESHOLD = 3.0  # Review rata-rata seller yang dianggap "buruk"
# (Diestimasi dari data: seller buruk menambah delay ~2 hari)
POOR_SELLER_EXTRA_DELAY = 2.0

RANDOM_SEED = 42

# ──────────────────────────────────────────────────────────────────────────────
# LOAD DATA
# ──────────────────────────────────────────────────────────────────────────────
def load_data() -> pd.DataFrame:
    """Load order_items_sample_colab.csv dan validasi kolom."""
    print("📂 Loading data ...")
    
    if not ORDER_ITEMS_CSV.exists():
        raise FileNotFoundError(
            f"File tidak ditemukan: {ORDER_ITEMS_CSV}\n"
            "Pastikan order_items_sample_colab.csv ada di root project."
        )
    
    df = pd.read_csv(ORDER_ITEMS_CSV, encoding=ENCODING)
    
    required_cols = [
        "review_score", "delivery_delay_days",
        "shipping_duration_days", "freight_value",
        "price", "product_weight_g",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Kolom tidak ditemukan dalam CSV: {missing}")
    
    # Derive features tambahan
    df["is_late"] = (df["delivery_delay_days"] > 0).astype(int)
    df["is_high_freight"] = (df["freight_value"] > 30).astype(int)
    df["is_heavy_product"] = (df["product_weight_g"] > 5000).astype(int)
    
    # Proxy untuk approval delay: jika delay sangat besar (>14 hari),
    # kemungkinan ada kontribusi slow approval
    df["is_likely_slow_approval"] = (df["delivery_delay_days"] > 14).astype(int)
    
    # Clip outlier untuk fitur ML
    df["delivery_delay_days_clipped"] = df["delivery_delay_days"].clip(-30, 60)
    df["shipping_duration_days_clipped"] = df["shipping_duration_days"].clip(0, 60)
    df["freight_value_clipped"] = df["freight_value"].clip(0, 150)
    df["product_weight_g_clipped"] = df["product_weight_g"].clip(0, 20000)
    df["price_clipped"] = df["price"].clip(0, 1000)
    
    total = len(df)
    print(f"  ✅ Loaded {total:,} orders")
    print(f"  📊 Late deliveries : {df['is_late'].sum():,} ({df['is_late'].mean()*100:.1f}%)")
    print(f"  📊 High freight    : {df['is_high_freight'].sum():,} ({df['is_high_freight'].mean()*100:.1f}%)")
    print(f"  📊 Heavy products  : {df['is_heavy_product'].sum():,} ({df['is_heavy_product'].mean()*100:.1f}%)")
    print(f"  📊 Avg review score: {df['review_score'].mean():.3f}")
    
    return df


# ──────────────────────────────────────────────────────────────────────────────
# BASELINE METRICS
# ──────────────────────────────────────────────────────────────────────────────
def compute_baseline(df: pd.DataFrame) -> dict:
    """Hitung baseline metrics dari data aktual."""
    return {
        "total_orders": len(df),
        "avg_review_score": float(df["review_score"].mean()),
        "positive_pct": float((df["review_score"] >= 4).mean() * 100),
        "negative_pct": float((df["review_score"] <= 2).mean() * 100),
        "neutral_pct": float((df["review_score"] == 3).mean() * 100),
        "late_delivery_pct": float(df["is_late"].mean() * 100),
        "high_freight_pct": float(df["is_high_freight"].mean() * 100),
        "heavy_product_pct": float(df["is_heavy_product"].mean() * 100),
        "avg_shipping_days": float(df["shipping_duration_days"].mean()),
        "avg_freight_value": float(df["freight_value"].mean()),
    }


# ──────────────────────────────────────────────────────────────────────────────
# ML MODEL — Random Forest Regressor
# ──────────────────────────────────────────────────────────────────────────────
def train_review_model(df: pd.DataFrame):
    """
    Latih Random Forest untuk memprediksi review_score dari fitur operasional.
    Return: (model, feature_names, r2_score)
    """
    try:
        from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
        from sklearn.model_selection import cross_val_score
        from sklearn.preprocessing import StandardScaler
    except ImportError:
        print("  ⚠️  scikit-learn tidak tersedia. Menggunakan rule-based simulation saja.")
        return None, [], 0.0

    print("\n🤖 Training Random Forest Regressor ...")
    
    feature_cols = [
        "is_late",
        "is_high_freight",
        "is_heavy_product",
        "is_likely_slow_approval",
        "delivery_delay_days_clipped",
        "shipping_duration_days_clipped",
        "freight_value_clipped",
        "product_weight_g_clipped",
        "price_clipped",
    ]
    
    X = df[feature_cols].fillna(0)
    y = df["review_score"]
    
    model = RandomForestRegressor(
        n_estimators=200,
        max_depth=8,
        min_samples_leaf=10,
        random_state=RANDOM_SEED,
        n_jobs=-1,
    )
    
    # Cross-validation untuk mengukur kualitas model
    cv_scores = cross_val_score(model, X, y, cv=5, scoring="r2", n_jobs=-1)
    model.fit(X, y)
    
    print(f"  ✅ Model trained | R² CV: {cv_scores.mean():.3f} (±{cv_scores.std():.3f})")
    print(f"  📊 Features used: {len(feature_cols)}")
    
    # Feature importance
    importances = model.feature_importances_
    feat_imp = sorted(zip(feature_cols, importances), key=lambda x: -x[1])
    print("  🔑 Top feature importances:")
    for feat, imp in feat_imp[:5]:
        bar = "█" * int(imp * 40)
        print(f"     {feat:<38} {bar} {imp:.3f}")
    
    return model, feature_cols, cv_scores.mean()


# ──────────────────────────────────────────────────────────────────────────────
# COUNTERFACTUAL TRANSFORMS — satu fungsi per skenario
# ──────────────────────────────────────────────────────────────────────────────

def transform_s1_eliminate_late_delivery(df: pd.DataFrame) -> pd.DataFrame:
    """
    Skenario 1: Perbaiki logistik → hilangkan semua late delivery.
    - delivery_delay_days: nilai positif → 0 (tepat waktu)
    - shipping_duration_days: kurangi sebesar rata-rata delay yang ada
    - is_late → 0 untuk semua order
    """
    sim = df.copy()
    mask_late = sim["delivery_delay_days"] > 0
    
    # Rata-rata delay yang ada pada order terlambat (untuk dikurangi dari shipping)
    avg_late_delay = sim.loc[mask_late, "delivery_delay_days"].mean()
    
    sim.loc[mask_late, "delivery_delay_days"] = 0.0
    sim.loc[mask_late, "delivery_delay_days_clipped"] = 0.0
    sim.loc[mask_late, "shipping_duration_days"] = (
        sim.loc[mask_late, "shipping_duration_days"] - avg_late_delay
    ).clip(lower=1.0)
    sim.loc[mask_late, "shipping_duration_days_clipped"] = (
        sim.loc[mask_late, "shipping_duration_days_clipped"] - avg_late_delay
    ).clip(lower=1.0)
    sim["is_late"] = 0
    sim["is_likely_slow_approval"] = (sim["delivery_delay_days"] > 14).astype(int)
    
    return sim


def transform_s2_accelerate_approval(df: pd.DataFrame) -> pd.DataFrame:
    """
    Skenario 2: Percepat proses approval menjadi <6 jam.
    Dampak: order yang sangat terlambat (proxy: delay > 14 hari) mendapat
    pengurangan delay sebesar APPROVAL_DELAY_REDUCTION hari.
    """
    sim = df.copy()
    mask = sim["delivery_delay_days"] > 14  # proxy: kemungkinan slow approval berkontribusi
    
    sim.loc[mask, "delivery_delay_days"] = (
        sim.loc[mask, "delivery_delay_days"] - APPROVAL_DELAY_REDUCTION
    ).clip(lower=-146.0)
    sim.loc[mask, "delivery_delay_days_clipped"] = (
        sim.loc[mask, "delivery_delay_days_clipped"] - APPROVAL_DELAY_REDUCTION
    ).clip(lower=-30.0)
    sim.loc[mask, "shipping_duration_days"] = (
        sim.loc[mask, "shipping_duration_days"] - APPROVAL_DELAY_REDUCTION
    ).clip(lower=1.0)
    sim.loc[mask, "shipping_duration_days_clipped"] = (
        sim.loc[mask, "shipping_duration_days_clipped"] - APPROVAL_DELAY_REDUCTION
    ).clip(lower=1.0)
    
    # Update derived flags
    sim["is_late"] = (sim["delivery_delay_days"] > 0).astype(int)
    sim["is_likely_slow_approval"] = (sim["delivery_delay_days"] > 14).astype(int)
    
    return sim


def transform_s3_cap_freight(df: pd.DataFrame) -> pd.DataFrame:
    """
    Skenario 3: Subsidi/negosiasi logistik → freight_value ≤ 30 untuk semua order.
    (Berdasarkan analisis: freight >30 berkorelasi dengan review lebih rendah)
    """
    sim = df.copy()
    sim["freight_value"] = sim["freight_value"].clip(upper=FREIGHT_CAP)
    sim["freight_value_clipped"] = sim["freight_value_clipped"].clip(upper=FREIGHT_CAP)
    sim["is_high_freight"] = 0  # semua freight sudah di-cap
    return sim


def transform_s4_remove_bad_sellers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Skenario 4: Evaluasi dan kurangi dampak seller buruk.
    Proxy: order dengan is_heavy_product=1 DAN is_late=1 (gabungan masalah seller)
    mendapat pengurangan delay sebesar POOR_SELLER_EXTRA_DELAY hari.
    
    Catatan: Data CSV tidak punya seller_id, jadi kita gunakan proxy:
    order yang memiliki SEMUA faktor buruk sekaligus (heavy + late + high freight)
    diasumsikan berasal dari seller bermasalah.
    """
    sim = df.copy()
    # Proxy untuk "seller buruk": order dengan 2+ faktor negatif
    bad_seller_mask = (
        (sim["is_late"] == 1) & 
        ((sim["is_high_freight"] == 1) | (sim["is_heavy_product"] == 1))
    )
    
    sim.loc[bad_seller_mask, "delivery_delay_days"] = (
        sim.loc[bad_seller_mask, "delivery_delay_days"] - POOR_SELLER_EXTRA_DELAY
    ).clip(lower=-146.0)
    sim.loc[bad_seller_mask, "delivery_delay_days_clipped"] = (
        sim.loc[bad_seller_mask, "delivery_delay_days_clipped"] - POOR_SELLER_EXTRA_DELAY
    ).clip(lower=-30.0)
    sim.loc[bad_seller_mask, "shipping_duration_days"] = (
        sim.loc[bad_seller_mask, "shipping_duration_days"] - POOR_SELLER_EXTRA_DELAY
    ).clip(lower=1.0)
    
    sim["is_late"] = (sim["delivery_delay_days"] > 0).astype(int)
    sim["is_likely_slow_approval"] = (sim["delivery_delay_days"] > 14).astype(int)
    
    return sim


def transform_s5_best_case_combined(df: pd.DataFrame) -> pd.DataFrame:
    """
    Skenario 5: Gabungan semua solusi (Best Case).
    Terapkan S1 + S2 + S3 + S4 secara berurutan.
    """
    sim = df.copy()
    sim = transform_s1_eliminate_late_delivery(sim)
    sim = transform_s2_accelerate_approval(sim)
    sim = transform_s3_cap_freight(sim)
    sim = transform_s4_remove_bad_sellers(sim)
    return sim


# ──────────────────────────────────────────────────────────────────────────────
# SIMULATE — Apply scenario + predict new review score
# ──────────────────────────────────────────────────────────────────────────────

def simulate_scenario(
    df_original: pd.DataFrame,
    df_transformed: pd.DataFrame,
    model,
    feature_cols: list,
    scenario_id: str,
    scenario_name: str,
    description: str,
    solution_recommendation: str,
    baseline: dict,
) -> dict:
    """
    Hitung projected metrics untuk satu skenario.
    Gunakan ML model jika tersedia, fallback ke rule-based jika tidak.
    """
    total = len(df_original)

    # ── Predict new review scores ────────────────────────────────────────────
    if model is not None:
        X_sim = df_transformed[feature_cols].fillna(0)
        predicted_scores = model.predict(X_sim)
        # Clip ke range valid [1, 5]
        predicted_scores = np.clip(predicted_scores, 1.0, 5.0)
    else:
        # Rule-based fallback: gunakan observed uplift dari data aktual
        # Uplift dari data: late delivery → review naik ~1.7 poin jika diperbaiki
        uplift_per_late = 1.708   # observed avg(review|on_time) - avg(review|late)
        uplift_per_freight = 0.1  # kecil, sudah dihitung dari data
        
        predicted_scores = df_original["review_score"].values.astype(float).copy()
        
        # Perbaikan late delivery
        newly_ontime = (df_original["is_late"] == 1) & (df_transformed["is_late"] == 0)
        predicted_scores[newly_ontime] = np.minimum(
            predicted_scores[newly_ontime] + uplift_per_late, 5.0
        )
        # Perbaikan freight
        freight_improved = df_transformed["freight_value"] < df_original["freight_value"]
        predicted_scores[freight_improved] = np.minimum(
            predicted_scores[freight_improved] + uplift_per_freight, 5.0
        )

    # ── Affected orders ──────────────────────────────────────────────────────
    # Order "terdampak" = yang mengalami perubahan di fitur kunci
    changed_mask = (
        (df_transformed["is_late"] != df_original["is_late"]) |
        (df_transformed["freight_value"] != df_original["freight_value"]) |
        (df_transformed["delivery_delay_days"] != df_original["delivery_delay_days"])
    )
    affected_orders = int(changed_mask.sum())
    affected_pct = float(affected_orders / total * 100)

    # ── Projected metrics ────────────────────────────────────────────────────
    proj_avg_review = float(predicted_scores.mean())
    proj_positive_pct = float((predicted_scores >= 4).mean() * 100)
    proj_negative_pct = float((predicted_scores <= 2).mean() * 100)
    proj_neutral_pct = float(((predicted_scores > 2) & (predicted_scores < 4)).mean() * 100)

    review_delta = proj_avg_review - baseline["avg_review_score"]
    positive_delta = proj_positive_pct - baseline["positive_pct"]
    negative_delta = proj_negative_pct - baseline["negative_pct"]

    # ── Confidence interval (bootstrap) ─────────────────────────────────────
    rng = np.random.default_rng(RANDOM_SEED)
    boot_means = []
    for _ in range(500):
        sample = rng.choice(predicted_scores, size=total, replace=True)
        boot_means.append(sample.mean())
    ci_lower = float(np.percentile(boot_means, 2.5))
    ci_upper = float(np.percentile(boot_means, 97.5))

    # ── Revenue impact estimate ──────────────────────────────────────────────
    # Asumsi: setiap 1 poin kenaikan review score → ~5% order growth (conservative)
    REVENUE_UPLIFT_PER_REVIEW_POINT = 0.05
    avg_order_value = float(df_original["price"].mean())
    additional_orders_est = int(
        total * max(review_delta, 0) * REVENUE_UPLIFT_PER_REVIEW_POINT
    )
    revenue_impact_est = round(additional_orders_est * avg_order_value, 2)

    print(f"\n  📋 {scenario_id}: {scenario_name}")
    print(f"     Baseline review : {baseline['avg_review_score']:.3f}")
    print(f"     Projected review: {proj_avg_review:.3f} (Δ {review_delta:+.3f})")
    print(f"     Affected orders : {affected_orders:,} ({affected_pct:.1f}%)")
    print(f"     95% CI          : [{ci_lower:.3f}, {ci_upper:.3f}]")
    print(f"     Neg review Δ    : {negative_delta:+.1f}%")

    return {
        "scenario_id": scenario_id,
        "scenario_name": scenario_name,
        "description": description,
        "solution_recommendation": solution_recommendation,
        "total_orders": total,
        "affected_orders": affected_orders,
        "affected_pct": round(affected_pct, 2),
        # Baseline
        "baseline_avg_review": round(baseline["avg_review_score"], 4),
        "baseline_positive_pct": round(baseline["positive_pct"], 2),
        "baseline_negative_pct": round(baseline["negative_pct"], 2),
        "baseline_neutral_pct": round(baseline["neutral_pct"], 2),
        # Projected
        "projected_avg_review": round(proj_avg_review, 4),
        "projected_positive_pct": round(proj_positive_pct, 2),
        "projected_negative_pct": round(proj_negative_pct, 2),
        "projected_neutral_pct": round(proj_neutral_pct, 2),
        # Deltas
        "review_delta": round(review_delta, 4),
        "positive_delta": round(positive_delta, 2),
        "negative_delta": round(negative_delta, 2),
        "review_improvement_pct": round(review_delta / baseline["avg_review_score"] * 100, 2),
        # Confidence interval
        "ci_lower_95": round(ci_lower, 4),
        "ci_upper_95": round(ci_upper, 4),
        # Revenue estimate
        "additional_orders_est": additional_orders_est,
        "revenue_impact_est_brl": revenue_impact_est,
        # Model quality
        "model_type": "RandomForest" if model is not None else "RuleBased",
    }


# ──────────────────────────────────────────────────────────────────────────────
# FEATURE IMPORTANCE OUTPUT
# ──────────────────────────────────────────────────────────────────────────────

FEATURE_LABELS = {
    "is_late": "Late Delivery (binary)",
    "is_high_freight": "High Freight Cost >30 (binary)",
    "is_heavy_product": "Heavy Product >5kg (binary)",
    "is_likely_slow_approval": "Likely Slow Approval (binary)",
    "delivery_delay_days_clipped": "Delivery Delay Days",
    "shipping_duration_days_clipped": "Shipping Duration Days",
    "freight_value_clipped": "Freight Value (BRL)",
    "product_weight_g_clipped": "Product Weight (g)",
    "price_clipped": "Product Price (BRL)",
}


def build_feature_importance_df(model, feature_cols: list, r2_score: float) -> pd.DataFrame:
    if model is None:
        return pd.DataFrame(columns=["feature_name", "feature_label", "importance_pct", "model_r2"])
    
    importances = model.feature_importances_
    total_imp = importances.sum()
    
    rows = []
    for feat, imp in zip(feature_cols, importances):
        rows.append({
            "feature_name": feat,
            "feature_label": FEATURE_LABELS.get(feat, feat),
            "importance_pct": round(float(imp / total_imp * 100), 2),
            "model_r2": round(float(r2_score), 4),
        })
    
    return pd.DataFrame(rows).sort_values("importance_pct", ascending=False)


# ──────────────────────────────────────────────────────────────────────────────
# ORDER DETAIL OUTPUT
# ──────────────────────────────────────────────────────────────────────────────

def build_order_detail_df(
    df_original: pd.DataFrame,
    scenario_results: list,
    model,
    feature_cols: list,
) -> pd.DataFrame:
    """
    Buat tabel detail per-order yang menunjukkan predicted review per skenario.
    Disimpan sebagai CSV saja (tidak ke ClickHouse) untuk efisiensi.
    """
    # Define transforms per skenario
    transforms = {
        "S1": transform_s1_eliminate_late_delivery,
        "S2": transform_s2_accelerate_approval,
        "S3": transform_s3_cap_freight,
        "S4": transform_s4_remove_bad_sellers,
        "S5": transform_s5_best_case_combined,
    }
    
    detail = df_original[["review_score", "delivery_delay_days", 
                           "freight_value", "is_late", "is_high_freight", 
                           "is_heavy_product"]].copy()
    detail.index.name = "order_idx"
    detail = detail.rename(columns={"review_score": "actual_review"})
    
    for sid, transform_fn in transforms.items():
        df_sim = transform_fn(df_original)
        if model is not None:
            X_sim = df_sim[feature_cols].fillna(0)
            preds = np.clip(model.predict(X_sim), 1.0, 5.0)
        else:
            preds = df_original["review_score"].values.astype(float).copy()
        detail[f"predicted_review_{sid}"] = np.round(preds, 3)
    
    # Hitung max uplift (S5 vs actual)
    detail["max_uplift_S5"] = np.round(
        detail["predicted_review_S5"] - detail["actual_review"].astype(float), 3
    )
    
    return detail.reset_index()


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("🔮  DustiniaDelixia Groceria — CX Improvement Simulation Model")
    print("    What-If / Counterfactual Analysis")
    print("=" * 70)

    # 1. Load data
    df = load_data()

    # 2. Baseline
    baseline = compute_baseline(df)
    print(f"\n📊 Baseline Metrics:")
    print(f"   Avg review score  : {baseline['avg_review_score']:.4f}")
    print(f"   Positive (≥4★)    : {baseline['positive_pct']:.1f}%")
    print(f"   Negative (≤2★)    : {baseline['negative_pct']:.1f}%")
    print(f"   Late delivery     : {baseline['late_delivery_pct']:.1f}%")
    print(f"   High freight      : {baseline['high_freight_pct']:.1f}%")

    # 3. Train model
    model, feature_cols, r2 = train_review_model(df)

    # 4. Define scenarios
    SCENARIOS = [
        {
            "id": "S1",
            "name": "Eliminate Late Delivery",
            "description": (
                "Perbaiki logistik sehingga semua pengiriman tepat waktu. "
                "Faktor paling dominan: late delivery menyebabkan review turun rata-rata 1.7 poin."
            ),
            "solution": (
                "Tambah mitra logistik di state dengan late_pct tertinggi (AM, RR, AP). "
                "Target: on-time delivery rate 100% dalam 6 bulan."
            ),
            "transform": transform_s1_eliminate_late_delivery,
        },
        {
            "id": "S2",
            "name": "Accelerate Order Approval (<6h)",
            "description": (
                "Percepat proses approval menjadi di bawah 6 jam untuk semua order. "
                "Order dengan delay ekstrem (>14 hari) memiliki kontribusi slow approval."
            ),
            "solution": (
                "Implementasi auto-approval untuk order di bawah nilai threshold. "
                "SLA approval: <6 jam untuk semua payment method."
            ),
            "transform": transform_s2_accelerate_approval,
        },
        {
            "id": "S3",
            "name": "Cap Freight Cost (≤30 BRL)",
            "description": (
                "Subsidi biaya pengiriman atau negosiasi tarif logistik "
                "sehingga freight tidak melebihi 30 BRL. "
                "Order dengan high freight saat ini: 11.4%."
            ),
            "solution": (
                "Negosiasi volume discount dengan mitra logistik. "
                "Subsidi freight untuk kategori produk berat atau jarak jauh."
            ),
            "transform": transform_s3_cap_freight,
        },
        {
            "id": "S4",
            "name": "Remove Underperforming Sellers",
            "description": (
                "Evaluasi dan kurangi dampak seller dengan performa buruk "
                "(proxy: order dengan kombinasi faktor negatif: late + high freight/heavy product). "
                "Seller buruk berkontribusi extra delay ~2 hari."
            ),
            "solution": (
                "Suspend seller dengan avg review <3.0 dan late_pct >30%. "
                "Program seller coaching dan SLA minimum untuk listing aktif."
            ),
            "transform": transform_s4_remove_bad_sellers,
        },
        {
            "id": "S5",
            "name": "Best Case — All Solutions Combined",
            "description": (
                "Skenario terbaik: gabungan semua solusi diterapkan sekaligus. "
                "S1 + S2 + S3 + S4 dijalankan secara berurutan."
            ),
            "solution": (
                "Roadmap 12 bulan: "
                "Q1: Logistik (S1+S4), Q2: Approval (S2), Q3: Freight subsidi (S3), "
                "Q4: Monitoring & fine-tuning."
            ),
            "transform": transform_s5_best_case_combined,
        },
    ]

    # 5. Run simulations
    print("\n🔮 Running simulations ...")
    results = []
    for sc in SCENARIOS:
        df_sim = sc["transform"](df)
        result = simulate_scenario(
            df_original=df,
            df_transformed=df_sim,
            model=model,
            feature_cols=feature_cols,
            scenario_id=sc["id"],
            scenario_name=sc["name"],
            description=sc["description"],
            solution_recommendation=sc["solution"],
            baseline=baseline,
        )
        results.append(result)

    # 6. Build dataframes
    df_scenarios = pd.DataFrame(results)
    df_feat_imp = build_feature_importance_df(model, feature_cols, r2)
    df_detail = build_order_detail_df(df, results, model, feature_cols)

    # 7. Save outputs
    scenarios_out = OUTPUT_DIR / "simulation_scenarios.csv"
    feat_imp_out  = OUTPUT_DIR / "simulation_feature_impact.csv"
    detail_out    = OUTPUT_DIR / "simulation_order_detail.csv"

    df_scenarios.to_csv(scenarios_out, index=False, encoding="utf-8")
    df_feat_imp.to_csv(feat_imp_out, index=False, encoding="utf-8")
    df_detail.to_csv(detail_out, index=False, encoding="utf-8")

    # 8. Summary print
    print("\n" + "=" * 70)
    print("✅  SIMULATION COMPLETE — Results Summary")
    print("=" * 70)
    print(f"\n{'Scenario':<40} {'Baseline':>8} {'Projected':>9} {'Delta':>7} {'Affected%':>10}")
    print("-" * 70)
    for r in results:
        print(
            f"  {r['scenario_name']:<38} "
            f"{r['baseline_avg_review']:>8.3f} "
            f"{r['projected_avg_review']:>9.3f} "
            f"{r['review_delta']:>+7.3f} "
            f"{r['affected_pct']:>9.1f}%"
        )
    print("-" * 70)
    
    best = max(results, key=lambda x: x["review_delta"])
    print(f"\n🏆 Best scenario: {best['scenario_name']}")
    print(f"   Projected review score: {best['projected_avg_review']:.3f}")
    print(f"   Improvement           : +{best['review_delta']:.3f} pts "
          f"({best['review_improvement_pct']:.1f}% improvement)")
    print(f"   Negative review Δ     : {best['negative_delta']:.1f}%")
    
    print(f"\n📁 Output files:")
    print(f"   {scenarios_out}")
    print(f"   {feat_imp_out}")
    print(f"   {detail_out}")

    return df_scenarios, df_feat_imp, df_detail


if __name__ == "__main__":
    df_scenarios, df_feat_imp, df_detail = main()
