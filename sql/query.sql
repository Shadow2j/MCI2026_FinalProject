-- ══════════════════════════════════════════════════════════════════════════
-- Dustinia Delixia Groceria — Dashboard Queries
-- Customer Experience Analytics — 8 Tabs, 50 Queries
-- ══════════════════════════════════════════════════════════════════════════
-- Author: Raymond Julius Pardosi (5025241268)
-- Database: ClickHouse (orders_db + analytics)
-- Visualization: Metabase Dashboard
-- ══════════════════════════════════════════════════════════════════════════


-- ═════════════════════════════════════════════════════════════════════════
-- TAB 1 — GENERAL OVERVIEW (KPI Cards & High-Level Metrics)
-- ═════════════════════════════════════════════════════════════════════════

-- 1.1 Executive KPI Summary (7 cards)
SELECT
    count(DISTINCT order_id)                                  AS total_orders,
    count(DISTINCT customer_unique_id)                        AS total_customers,
    count(DISTINCT product_id)                                AS total_products,
    round(sum(price), 2)                                      AS total_revenue,
    round(count(*) / count(DISTINCT order_id), 1)             AS avg_basket_size,
    round(avg(review_score), 2)                               AS avg_review_score,
    round(countIf(is_late_delivery = 0) * 100.0 / countIf(is_late_delivery IS NOT NULL), 1)
                                                              AS on_time_delivery_pct
FROM orders_db.order_items;

-- 1.2 Top 10 Best-Selling Categories (by revenue)
SELECT
    product_category_name_english AS category,
    total_orders,
    round(total_revenue, 2) AS revenue,
    round(avg_review_score, 2) AS avg_review
FROM orders_db.top_products
WHERE product_category_name_english != 'missing'
ORDER BY total_revenue DESC
LIMIT 10;

-- 1.3 Revenue Distribution per Category (Pie chart)
SELECT
    product_category_name_english AS category,
    round(total_revenue, 2) AS revenue
FROM orders_db.category_summary
WHERE product_category_name_english != 'missing'
ORDER BY revenue DESC
LIMIT 15;

-- 1.4 Overall Review Score Distribution (Bar chart)
SELECT
    review_score,
    count(*) AS total
FROM orders_db.order_items
WHERE review_score IS NOT NULL
GROUP BY review_score
ORDER BY review_score;

-- 1.5 Order Status Breakdown (Pie chart)
SELECT
    order_status,
    count(DISTINCT order_id) AS total_orders
FROM orders_db.order_items
GROUP BY order_status
ORDER BY total_orders DESC;

-- 1.6 Monthly Revenue Trend (Line chart)
SELECT
    formatDateTime(toDate(substring(order_purchase_timestamp, 1, 10)), '%Y-%m') AS month,
    count(DISTINCT order_id) AS total_orders,
    round(sum(price), 2) AS revenue,
    round(avg(review_score), 2) AS avg_review
FROM orders_db.order_items
WHERE order_purchase_timestamp IS NOT NULL AND order_purchase_timestamp != 'NaT'
GROUP BY month
ORDER BY month;

-- 1.7 Top vs Bottom 3 Categories (Comparative table)
SELECT
    product_category_name_english AS category,
    total_orders,
    round(total_revenue, 2) AS revenue,
    round(avg_review_score, 2) AS avg_review,
    'Top Performer' AS label
FROM orders_db.category_summary
WHERE product_category_name_english != 'missing'
ORDER BY total_revenue DESC
LIMIT 3

UNION ALL

SELECT
    product_category_name_english AS category,
    total_orders,
    round(total_revenue, 2) AS revenue,
    round(avg_review_score, 2) AS avg_review,
    'Bottom Performer' AS label
FROM orders_db.category_summary
WHERE total_orders >= 10
  AND product_category_name_english != 'missing'
ORDER BY total_revenue ASC
LIMIT 3;


-- ═════════════════════════════════════════════════════════════════════════
-- TAB 2 — KNOWLEDGE DETAIL (Behavioral Patterns & Drill-Down)
-- ═════════════════════════════════════════════════════════════════════════

-- 2.1 Order Distribution per Day of Week (Bar chart)
SELECT
    order_day_of_week,
    CASE order_day_of_week
        WHEN 0 THEN 'Senin'
        WHEN 1 THEN 'Selasa'
        WHEN 2 THEN 'Rabu'
        WHEN 3 THEN 'Kamis'
        WHEN 4 THEN 'Jumat'
        WHEN 5 THEN 'Sabtu'
        WHEN 6 THEN 'Minggu'
    END AS hari,
    count(DISTINCT order_id) AS total_orders
FROM orders_db.order_items
WHERE order_day_of_week IS NOT NULL
GROUP BY order_day_of_week
ORDER BY order_day_of_week;

-- 2.2 Hourly Order Activity (Bar chart)
SELECT
    order_hour AS jam,
    total_orders,
    total_items,
    round(total_revenue, 2) AS revenue
FROM orders_db.hourly_activity
ORDER BY order_hour;

-- 2.3 Heatmap: Hour × Day of Week (Pivot table)
SELECT
    order_hour_of_day AS jam,
    countIf(order_day_of_week = 0) AS Senin,
    countIf(order_day_of_week = 1) AS Selasa,
    countIf(order_day_of_week = 2) AS Rabu,
    countIf(order_day_of_week = 3) AS Kamis,
    countIf(order_day_of_week = 4) AS Jumat,
    countIf(order_day_of_week = 5) AS Sabtu,
    countIf(order_day_of_week = 6) AS Minggu
FROM orders_db.order_items
WHERE order_hour_of_day IS NOT NULL
GROUP BY jam
ORDER BY jam;

-- 2.4 Category Detail: Items, Revenue, Review (Table)
SELECT
    product_category_name_english AS category,
    total_items,
    round(total_revenue, 2) AS revenue,
    round(avg_review_score, 2) AS avg_review,
    round(avg_price, 2) AS avg_price,
    round(avg_freight, 2) AS avg_freight
FROM orders_db.category_summary
WHERE product_category_name_english != 'missing'
ORDER BY total_revenue DESC;

-- 2.5 Payment Method Distribution (Pie chart)
SELECT
    payment_type,
    total_orders,
    round(total_revenue, 2) AS revenue,
    round(avg_review_score, 2) AS avg_review
FROM analytics.payment_analysis
WHERE payment_type != 'missing'
ORDER BY total_orders DESC;

-- 2.6 Delivery Duration Distribution (Histogram)
SELECT
    CASE
        WHEN shipping_duration_days IS NULL THEN 'Belum Terkirim'
        WHEN shipping_duration_days <= 3 THEN '0-3 hari'
        WHEN shipping_duration_days <= 7 THEN '4-7 hari'
        WHEN shipping_duration_days <= 14 THEN '8-14 hari'
        WHEN shipping_duration_days <= 21 THEN '15-21 hari'
        ELSE '22+ hari'
    END AS delivery_bucket,
    count(DISTINCT order_id) AS total_orders,
    round(avg(review_score), 2) AS avg_review
FROM orders_db.order_items
GROUP BY delivery_bucket
ORDER BY total_orders DESC;

-- 2.7 Customer State Distribution (Map/Bar chart)
SELECT
    customer_state,
    total_orders,
    round(total_revenue, 2) AS revenue,
    round(avg_review_score, 2) AS avg_review,
    total_customers
FROM analytics.geographic_analysis
WHERE customer_state != 'missing'
GROUP BY customer_state, total_orders, total_revenue, avg_review_score, total_customers
ORDER BY total_orders DESC
LIMIT 15;


-- ═════════════════════════════════════════════════════════════════════════
-- TAB 3 — PREDICTIVE & CX ANALYSIS (⭐ Why Review Scores Stagnate?)
-- ═════════════════════════════════════════════════════════════════════════

-- 3.1 ⭐ Review Score vs Delivery Delay Correlation (Scatter/Bar)
SELECT
    CASE
        WHEN delivery_delay_days IS NULL THEN 'N/A'
        WHEN delivery_delay_days <= -7 THEN '7+ hari lebih cepat'
        WHEN delivery_delay_days <= -1 THEN '1-7 hari lebih cepat'
        WHEN delivery_delay_days <= 0 THEN 'Tepat waktu'
        WHEN delivery_delay_days <= 7 THEN '1-7 hari terlambat'
        WHEN delivery_delay_days <= 14 THEN '8-14 hari terlambat'
        ELSE '14+ hari terlambat'
    END AS delivery_category,
    count(DISTINCT order_id) AS total_orders,
    round(avg(review_score), 2) AS avg_review_score,
    round(avg(delivery_delay_days), 1) AS avg_delay
FROM orders_db.order_items
WHERE review_score IS NOT NULL
GROUP BY delivery_category
ORDER BY avg_delay;

-- 3.2 ⭐ Late Delivery Impact on Review Score (Key insight)
SELECT
    CASE WHEN is_late_delivery = 1 THEN 'Terlambat' ELSE 'Tepat Waktu' END AS status,
    count(DISTINCT order_id) AS total_orders,
    round(avg(review_score), 2) AS avg_review_score,
    round(countIf(review_score <= 2) * 100.0 / countIf(review_score IS NOT NULL), 1) AS negative_pct,
    round(countIf(review_score >= 4) * 100.0 / countIf(review_score IS NOT NULL), 1) AS positive_pct
FROM orders_db.order_items
WHERE is_late_delivery IS NOT NULL AND review_score IS NOT NULL
GROUP BY status;

-- 3.3 ⭐ Review Analysis per Category (Which categories drag scores down?)
SELECT
    product_category_name_english AS category,
    total_reviews,
    round(avg_review_score, 2) AS avg_review,
    round(negative_review_pct, 1) AS negative_pct,
    round(positive_review_pct, 1) AS positive_pct,
    round(late_delivery_pct, 1) AS late_delivery_pct
FROM analytics.review_analysis
WHERE product_category_name_english != 'missing'
ORDER BY avg_review_score ASC
LIMIT 15;

-- 3.4 ⭐ Churn Risk Segmentation (RFM Matrix)
SELECT
    loyalty_tier,
    churn_risk,
    count(*) AS total_customers,
    round(avg(total_spend), 2) AS avg_spend,
    round(avg(avg_review_score), 2) AS avg_review,
    round(avg(recency_days), 0) AS avg_recency_days
FROM analytics.customer_loyalty_rfm
GROUP BY loyalty_tier, churn_risk
ORDER BY
    CASE loyalty_tier WHEN 'Gold' THEN 1 WHEN 'Silver' THEN 2 ELSE 3 END,
    CASE churn_risk WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END;

-- 3.5 ⭐ Delivery Performance per State (Where are CX problems?)
SELECT
    customer_state AS state,
    total_orders,
    round(avg_delivery_days, 1) AS avg_delivery,
    round(late_delivery_pct, 1) AS late_pct,
    round(avg_review_when_late, 2) AS review_when_late,
    round(avg_review_when_ontime, 2) AS review_when_ontime,
    round(avg_review_when_ontime - avg_review_when_late, 2) AS review_gap
FROM analytics.delivery_performance
ORDER BY late_delivery_pct DESC;

-- 3.6 Sales Forecasting per Category (Demand projection)
SELECT
    product_category_name_english AS category,
    current_total_orders,
    round(current_total_revenue, 2) AS current_revenue,
    round(forecasted_orders, 0) AS forecast_orders,
    round(forecasted_revenue, 2) AS forecast_revenue,
    round(growth_rate * 100, 1) AS growth_pct,
    risk_level
FROM analytics.sales_forecasting
WHERE product_category_name_english != 'missing'
ORDER BY current_total_revenue DESC
LIMIT 20;

-- 3.7 Approval Delay Impact on Review
SELECT
    CASE
        WHEN approval_delay_hours <= 1  THEN '< 1 jam'
        WHEN approval_delay_hours <= 6  THEN '1-6 jam'
        WHEN approval_delay_hours <= 24 THEN '6-24 jam'
        WHEN approval_delay_hours <= 48 THEN '1-2 hari'
        ELSE '2+ hari'
    END AS approval_bucket,
    count(DISTINCT order_id) AS total_orders,
    round(avg(review_score), 2) AS avg_review
FROM orders_db.order_items
WHERE review_score IS NOT NULL
  AND approval_delay_hours IS NOT NULL
GROUP BY approval_bucket
ORDER BY
    CASE approval_bucket
        WHEN '< 1 jam'  THEN 1
        WHEN '1-6 jam'  THEN 2
        WHEN '6-24 jam' THEN 3
        WHEN '1-2 hari' THEN 4
        ELSE 5
    END


-- ═════════════════════════════════════════════════════════════════════════
-- TAB 4 — IMPROVEMENTS & RECOMMENDATIONS
-- ═════════════════════════════════════════════════════════════════════════

-- 4.1 Worst Performing Sellers (by review — need improvement)
SELECT
    seller_id,
    seller_city,
    seller_state,
    total_orders,
    round(total_revenue, 2) AS revenue,
    round(avg_review_score, 2) AS avg_review,
    round(late_delivery_pct, 1) AS late_pct,
    round(avg_delivery_days, 1) AS avg_delivery_days
FROM analytics.seller_performance
WHERE total_orders >= 10
ORDER BY avg_review_score ASC
LIMIT 20;

-- 4.2 Best Performing Sellers (benchmark)
SELECT
    seller_id,
    seller_city,
    seller_state,
    total_orders,
    round(total_revenue, 2) AS revenue,
    round(avg_review_score, 2) AS avg_review,
    round(late_delivery_pct, 1) AS late_pct
FROM analytics.seller_performance
WHERE total_orders >= 10
ORDER BY avg_review_score DESC
LIMIT 20;

-- 4.3 Categories Needing CX Improvement
SELECT
    product_category_name_english AS category,
    total_reviews,
    round(avg_review_score, 2) AS avg_review,
    round(negative_review_pct, 1) AS negative_pct,
    round(late_delivery_pct, 1) AS late_delivery_pct,
    round(avg_freight_value, 2) AS avg_freight
FROM analytics.review_analysis
WHERE total_reviews >= 20 AND product_category_name_english != 'missing'
ORDER BY negative_review_pct DESC
LIMIT 15;

-- 4.4 Geographic Problem Areas (High late delivery + low review)
SELECT
    customer_state                                      AS state,
    total_orders,
    round(
        (avg_review_when_late * late_delivery_pct / 100)
        + (avg_review_when_ontime * on_time_pct / 100)
    , 2)                                                AS avg_review,
    round(late_delivery_pct, 1)                         AS late_pct,
    round(avg_freight_value, 2)                         AS avg_freight,
    round(avg_delivery_days, 1)                         AS avg_delivery
FROM analytics.delivery_performance
WHERE total_orders >= 50
ORDER BY avg_review ASC;

-- 4.5 Payment Friction Analysis (Which payment methods cause issues?)
SELECT
    payment_type,
    total_orders,
    round(avg_review_score, 2) AS avg_review,
    round(avg_installments, 1) AS avg_installments,
    round(avg_payment_value, 2) AS avg_value
FROM analytics.payment_analysis
WHERE payment_type != 'missing'
ORDER BY avg_review_score ASC;

-- 4.6 High-Value but Low-Review Products (Priority fixes)
SELECT
    product_id,
    product_category_name_english AS category,
    total_orders,
    round(total_revenue, 2) AS revenue,
    round(avg_review_score, 2) AS avg_review,
    total_unique_buyers
FROM analytics.products_performance
WHERE total_orders >= 5
  AND avg_review_score < 3.5
  AND product_category_name_english != 'missing'
ORDER BY total_revenue DESC
LIMIT 20;

-- 4.7 Hourly Capacity Analysis (Logistics optimization)
SELECT
    order_hour,
    total_orders,
    round(total_revenue, 2) AS revenue,
    round(avg_items_per_order, 1) AS avg_items,
    round(capacity_utilization_pct, 1) AS utilization_pct,
    peak_label
FROM analytics.hourly_capacity
ORDER BY order_hour;


-- ═════════════════════════════════════════════════════════════════════════
-- TAB 5 — DATA GOVERNANCE & PIPELINE HEALTH
-- ═════════════════════════════════════════════════════════════════════════

-- 5.1 Data Quality Report — Missing Values per Column
SELECT
    column_name,
    total_rows,
    missing_count,
    round(missing_pct, 2) AS missing_pct,
    ingested_date
FROM orders_db.data_quality_report
ORDER BY ingested_date DESC, missing_pct DESC;

-- 5.2 Pipeline Freshness & Volume Summary (Number cards)
SELECT
    (SELECT max(batch_time)
     FROM analytics.history_category_trend)                        AS latest_batch,

    (SELECT count(DISTINCT product_category_name_english)
     FROM analytics.history_category_trend
     WHERE product_category_name_english != 'missing')             AS categories_tracked,

    (SELECT count(DISTINCT ingested_date)
     FROM analytics.history_category_trend)                        AS total_batch_days,

    (SELECT count(DISTINCT order_id)
     FROM orders_db.order_items)                                   AS total_orders_ingested,

    (SELECT count(DISTINCT customer_unique_id)
     FROM orders_db.order_items)                                   AS total_customers_ingested,

    (SELECT max(ingested_date)
     FROM orders_db.order_items)                                   AS last_ingest_date;

-- 5.3 NULL Impact Assessment (How missing data affects analysis)
SELECT
    column_name,
    missing_count,
    total_rows,
    round(missing_pct, 2) AS pct,
    CASE
        WHEN missing_pct > 10 THEN '⚠️ High Impact'
        WHEN missing_pct > 5 THEN '⚡ Medium Impact'
        WHEN missing_pct > 0 THEN '✅ Low Impact'
        ELSE '✅ Complete'
    END AS impact_level
FROM orders_db.data_quality_report
ORDER BY missing_pct DESC;


-- ═════════════════════════════════════════════════════════════════════════
-- TAB 6 — CX ENGINE (Deep Customer Experience Analysis)
-- ═════════════════════════════════════════════════════════════════════════

-- 6.1 Customer Lifetime Value Distribution (CLV)
SELECT
    loyalty_tier,
    count(*) AS total_customers,
    round(avg(total_spend), 2) AS avg_clv,
    round(avg(total_orders), 1) AS avg_orders,
    round(avg(avg_review_score), 2) AS avg_review,
    round(avg(recency_days), 0) AS avg_recency
FROM analytics.customer_loyalty_rfm
GROUP BY loyalty_tier
ORDER BY avg_clv DESC;

-- 6.2 Review Comment Analysis — Orders with/without Comments
SELECT
    comment_status,
    count(DISTINCT order_id)                                   AS total_orders,
    round(avg(review_score), 2)                                AS avg_review,
    round(countIf(review_score <= 2) * 100.0 / count(*), 1)  AS negative_pct
FROM (
    SELECT
        order_id,
        review_score,
        if(
            review_comment_message = ''
            OR review_comment_message = 'missing'
            OR review_comment_message IS NULL,
            'Tanpa Komentar',
            'Dengan Komentar'
        ) AS comment_status
    FROM orders_db.order_items
    WHERE review_score IS NOT NULL
)
GROUP BY comment_status;

-- 6.3 Review Score Trend over Time (Is it truly stagnating?)
SELECT
    formatDateTime(toDate(substring(order_purchase_timestamp, 1, 10)), '%Y-%m') AS month,
    round(avg(review_score), 3) AS avg_review,
    count(DISTINCT order_id) AS total_orders,
    round(countIf(review_score >= 4) * 100.0 / countIf(review_score IS NOT NULL), 1) AS positive_pct,
    round(countIf(review_score <= 2) * 100.0 / countIf(review_score IS NOT NULL), 1) AS negative_pct
FROM orders_db.order_items
WHERE review_score IS NOT NULL AND order_purchase_timestamp IS NOT NULL AND order_purchase_timestamp != 'NaT'
GROUP BY month
HAVING total_orders >= 50  -- filter bulan dengan data terlalu sedikit (noise statistik)
ORDER BY month;



-- 6.4 ⭐ KEY INSIGHT: Root Causes of Low Review Scores
SELECT
    'Late Delivery' AS factor,
    round(avg(CASE WHEN is_late_delivery = 1 THEN review_score END), 2) AS avg_review_affected,
    round(avg(CASE WHEN is_late_delivery = 0 THEN review_score END), 2) AS avg_review_unaffected,
    round(avg(CASE WHEN is_late_delivery = 0 THEN review_score END) - avg(CASE WHEN is_late_delivery = 1 THEN review_score END), 2) AS impact_delta,
    countIf(is_late_delivery = 1) AS affected_orders
FROM orders_db.order_items
WHERE review_score IS NOT NULL

UNION ALL

SELECT
    'High Freight (>30)',
    round(avg(CASE WHEN freight_value > 30 THEN review_score END), 2),
    round(avg(CASE WHEN freight_value <= 30 THEN review_score END), 2),
    round(avg(CASE WHEN freight_value <= 30 THEN review_score END) - avg(CASE WHEN freight_value > 30 THEN review_score END), 2),
    countIf(freight_value > 30)
FROM orders_db.order_items
WHERE review_score IS NOT NULL

UNION ALL

SELECT
    'Slow Approval (>24h)',
    round(avg(CASE WHEN approval_delay_hours > 24 THEN review_score END), 2),
    round(avg(CASE WHEN approval_delay_hours <= 24 THEN review_score END), 2),
    round(avg(CASE WHEN approval_delay_hours <= 24 THEN review_score END) - avg(CASE WHEN approval_delay_hours > 24 THEN review_score END), 2),
    countIf(approval_delay_hours > 24)
FROM orders_db.order_items
WHERE review_score IS NOT NULL

UNION ALL

SELECT
    'Heavy Product (>5kg)',
    round(avg(CASE WHEN product_weight_g > 5000 THEN review_score END), 2),
    round(avg(CASE WHEN product_weight_g <= 5000 THEN review_score END), 2),
    round(avg(CASE WHEN product_weight_g <= 5000 THEN review_score END) - avg(CASE WHEN product_weight_g > 5000 THEN review_score END), 2),
    countIf(product_weight_g > 5000)
FROM orders_db.order_items
WHERE review_score IS NOT NULL;

-- 6.5 TF-IDF: Top Keywords per Review Bucket (Horizontal bar chart)
-- Advanced TF-IDF + Bigrams + Sublinear TF scaling (paper methodology)
-- Kata-kata dengan skor TF-IDF tertinggi yang EKSKLUSIF di review buruk
SELECT
    keyword,
    review_bucket,
    round(tfidf_score, 4)    AS tfidf_score,
    round(log_odds, 3)       AS log_odds,
    freq_negative,
    freq_positive,
    total_docs
FROM analytics.tfidf_review_keywords
WHERE review_bucket = 'negative'
ORDER BY tfidf_score DESC
LIMIT 20;

-- 6.6 Log-Odds: Kata Eksklusif Negatif vs Positif (Table)
-- log_odds tinggi = kata ini jauh lebih sering di review buruk vs baik
SELECT
    keyword,
    round(log_odds, 3)   AS log_odds,
    freq_negative,
    freq_positive,
    total_docs
FROM analytics.tfidf_review_keywords
WHERE review_bucket = 'negative'
  AND freq_negative >= 10
  AND total_docs >= 10
ORDER BY log_odds DESC
LIMIT 20;



-- ═════════════════════════════════════════════════════════════════════════
-- TAB 7 — CX DEEP DIVE: Review Stagnation Analysis
-- ⭐ Answers: "Why are review scores stagnant and hard to improve?"
-- ═════════════════════════════════════════════════════════════════════════

-- 7.1 ⭐ Monthly Review Score Evolution (Line chart — dual axis)
-- Shows avg review score + negative percentage trend over time
SELECT
    year_month,
    round(avg_review_score, 3) AS avg_review,
    total_reviews,
    total_orders,
    round(positive_review_pct, 1) AS positive_pct,
    round(negative_review_pct, 1) AS negative_pct,
    round(neutral_review_pct, 1) AS neutral_pct
FROM analytics.monthly_review_trend
WHERE total_orders >= 50
ORDER BY year_month;

-- 7.2 ⭐ Monthly Positive vs Negative Trend (Stacked area chart)
-- Visualizes the shift between positive, neutral, and negative reviews
SELECT
    year_month,
    round(positive_review_pct, 1) AS positive_pct,
    round(neutral_review_pct, 1) AS neutral_pct,
    round(negative_review_pct, 1) AS negative_pct
FROM analytics.monthly_review_trend
WHERE total_orders >= 50
ORDER BY year_month;

-- 7.3 ⭐ Late Delivery Rate vs Review Score Trend (Dual-axis line)
-- Overlays late_delivery_pct on avg_review to show month-by-month correlation
SELECT
    year_month,
    round(avg_review_score, 2) AS avg_review,
    round(late_delivery_pct, 1) AS late_delivery_pct,
    round(avg_delivery_delay_days, 1) AS avg_delay_days,
    round(avg_freight_value, 2) AS avg_freight
FROM analytics.monthly_review_trend
WHERE total_orders >= 50
ORDER BY year_month;

-- 7.4 ⭐⭐ Compounding Factor Impact Matrix (Heatmap / pivot table)
-- Shows how multiple negative factors compound to destroy reviews
-- Key insight: 0 factors → high review; 3 factors → very low review
SELECT
    is_late AS late_delivery,
    is_high_freight AS high_freight,
    is_slow_approval AS slow_approval,
    total_orders,
    round(avg_review_score, 2) AS avg_review,
    round(negative_review_pct, 1) AS negative_pct,
    round(positive_review_pct, 1) AS positive_pct,
    factor_count AS num_factors
FROM analytics.review_root_cause_matrix
ORDER BY avg_review_score ASC;

-- 7.5 ⭐⭐ Single vs Multi-Factor Impact Summary (Bar chart)
-- Compare avg review when 0, 1, 2, or 3 negative factors are present
SELECT
    CASE factor_count
        WHEN 0 THEN '0 factors (baseline)'
        WHEN 1 THEN '1 factor'
        WHEN 2 THEN '2 factors'
        WHEN 3 THEN '3 factors (worst)'
    END AS factor_group,
    factor_count,
    sum(total_orders) AS total_orders_sum,
    round(sum(avg_review_score * total_orders) / sum(total_orders), 2) AS weighted_avg_review,
    round(sum(negative_review_pct * total_orders) / sum(total_orders), 1) AS weighted_negative_pct
FROM analytics.review_root_cause_matrix
GROUP BY factor_count
ORDER BY factor_count;

-- 7.6 ⭐ Seller Origin vs Customer Destination (Top problematic routes)
-- Shows which seller→customer state routes have worst CX
SELECT
    seller_state,
    customer_state,
    is_same_state,
    total_orders,
    round(avg_review_score, 2) AS avg_review,
    round(avg_shipping_days, 1) AS avg_shipping_days,
    round(late_delivery_pct, 1) AS late_pct
FROM analytics.seller_state_review
WHERE total_orders >= 20
ORDER BY avg_review_score ASC
LIMIT 30;

-- 7.7 ⭐⭐ Review Score Distribution Shift Over Time (100% stacked bar)
-- Shows proportion of each score (1-5) changing month by month
-- Key for detecting polarization: if 5★ AND 1★ both grow → avg stagnates
SELECT
    year_month,
    review_score,
    total_count,
    month_total,
    round(score_pct, 1) AS score_pct
FROM analytics.review_score_shift
WHERE month_total >= 50
ORDER BY year_month, review_score;

-- 7.8 ⭐⭐⭐ Review Polarization Index Over Time (Line chart)
-- Tracks (1★ + 5★) / total — if rising, reviews are polarizing
-- Polarization = average looks stable but extremes are growing
SELECT
    year_month,
    sum(CASE WHEN review_score IN (1, 5) THEN total_count ELSE 0 END) AS extreme_count,
    sum(total_count) AS total_count_sum,
    round(
        sum(CASE WHEN review_score IN (1, 5) THEN total_count ELSE 0 END) * 100.0
        / sum(total_count), 1
    ) AS polarization_index,
    round(
        sum(CASE WHEN review_score = 5 THEN total_count ELSE 0 END) * 100.0
        / sum(total_count), 1
    ) AS five_star_pct,
    round(
        sum(CASE WHEN review_score = 1 THEN total_count ELSE 0 END) * 100.0
        / sum(total_count), 1
    ) AS one_star_pct
FROM analytics.review_score_shift
WHERE month_total >= 50
GROUP BY year_month
ORDER BY year_month;


-- ═════════════════════════════════════════════════════════════════════════
-- TAB 8 — CX SIMULATION & PROJECTIONS
-- ⭐ "Jika solusi diterapkan, review score akan seperti apa?"
-- Model: Random Forest Counterfactual Simulation
-- ═════════════════════════════════════════════════════════════════════════

-- 8.1 ⭐⭐⭐ Scenario Comparison Summary (Bar chart — before vs after per scenario)
-- Tampilan utama: baseline vs projected avg review untuk setiap skenario
SELECT
    scenario_id,
    scenario_name,
    round(baseline_avg_review, 3) AS baseline_review,
    round(projected_avg_review, 3) AS projected_review,
    round(review_delta, 3) AS review_delta,
    round(review_improvement_pct, 1) AS improvement_pct,
    affected_orders,
    round(affected_pct, 1) AS affected_pct,
    round(ci_lower_95, 3) AS ci_lower,
    round(ci_upper_95, 3) AS ci_upper,
    model_type
FROM analytics.simulation_scenarios
ORDER BY scenario_id;

-- 8.2 ⭐⭐ Orders Affected per Scenario (Stacked bar: affected vs not affected)
-- Menunjukkan berapa banyak order yang berubah kondisi di setiap skenario
SELECT
    scenario_id,
    scenario_name,
    total_orders,
    affected_orders,
    (total_orders - affected_orders) AS unaffected_orders,
    round(affected_pct, 1) AS affected_pct,
    round(100 - affected_pct, 1) AS unaffected_pct
FROM analytics.simulation_scenarios
ORDER BY affected_orders DESC;

-- 8.3 ⭐⭐ Revenue Impact Projection (KPI cards per scenario)
-- Estimasi dampak finansial dari peningkatan review score
SELECT
    scenario_id,
    scenario_name,
    additional_orders_est,
    round(revenue_impact_est_brl, 2) AS revenue_impact_brl,
    round(review_delta, 3) AS review_delta,
    round(affected_pct, 1) AS affected_pct,
    CASE
        WHEN revenue_impact_est_brl > 10000 THEN 'High ROI'
        WHEN revenue_impact_est_brl > 5000 THEN 'Medium ROI'
        ELSE 'Low ROI'
    END AS roi_level
FROM analytics.simulation_scenarios
WHERE review_delta > 0
ORDER BY revenue_impact_est_brl DESC;

-- 8.4 ⭐ Negative Review Reduction per Scenario (Bar chart)
-- Penurunan % review negatif setelah solusi diterapkan
SELECT
    scenario_id,
    scenario_name,
    round(baseline_negative_pct, 1) AS baseline_negative_pct,
    round(projected_negative_pct, 1) AS projected_negative_pct,
    round(negative_delta, 1) AS negative_reduction,
    round(baseline_positive_pct, 1) AS baseline_positive_pct,
    round(projected_positive_pct, 1) AS projected_positive_pct,
    round(positive_delta, 1) AS positive_gain
FROM analytics.simulation_scenarios
ORDER BY negative_reduction ASC;

-- 8.5 ⭐⭐ ML Feature Importance (Horizontal bar chart)
-- Faktor apa yang paling berpengaruh terhadap review score dalam model ML?
SELECT
    feature_label,
    round(importance_pct, 2) AS importance_pct,
    round(model_r2, 3) AS model_r2,
    CASE
        WHEN importance_pct >= 50 THEN 'Critical Driver'
        WHEN importance_pct >= 20 THEN 'Major Driver'
        WHEN importance_pct >= 10 THEN 'Moderate Driver'
        ELSE 'Minor Driver'
    END AS driver_level
FROM analytics.simulation_feature_impact
ORDER BY importance_pct DESC;