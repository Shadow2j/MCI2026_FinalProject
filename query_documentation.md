#  Dokumentasi Query — Dustinia Delixia Groceria CX Dashboard

> **Database:** ClickHouse (`orders_db` + `analytics`)  
> **Pipeline:** CSV → PySpark → ClickHouse → Metabase  
> **Dataset:** Olist Brazilian E-Commerce (adapted)  
> **Focus:** Customer Experience — Mengapa review score stagnan?  
> **Author:** Raymond Julius Pardosi (5025241268)

---

##  Struktur Dashboard

| Tab | Nama | Fokus | Jumlah Query |
|-----|------|-------|:------------:|
| 1 | **General Overview** | KPI ringkasan bisnis, revenue, kategori | 7 |
| 2 | **Knowledge Detail** | Pola waktu, distribusi, perilaku pelanggan | 7 |
| 3 | **Predictive & CX** |  Analisis review score, delivery, churn | 7 |
| 4 | **Improvements** | Rekomendasi perbaikan seller, kategori, logistik | 7 |
| 5 | **Data Governance** | Kesehatan pipeline, data quality, freshness | 3 |
| 6 | **CX Engine** | Deep analysis: CLV, sentiment, root cause | 9 |
| 7 | **CX Deep Dive** | Review stagnation root causes, polarization | 11 |
| 8 | **CX Simulation** |  What-If model: proyeksi dampak solusi | 8 |
| | | **Total** | **56** |

---

##  TAB 1 — General Overview

###  Q1.1 — Executive KPI Summary
```sql
SELECT
    count(DISTINCT order_id)             AS total_orders,
    count(DISTINCT customer_unique_id)   AS total_customers,
    count(DISTINCT product_id)           AS total_products,
    round(sum(price), 2)                 AS total_revenue,
    round(count(*) / count(DISTINCT order_id), 1) AS avg_basket_size,
    round(avg(review_score), 2)          AS avg_review_score,
    round(countIf(is_late_delivery = 0) * 100.0 / countIf(is_late_delivery IS NOT NULL), 1) AS on_time_delivery_pct
FROM orders_db.order_items
```
**Fungsi:** 7 KPI cards di baris teratas dashboard — memberikan gambaran C-level secara instan.  
**Visualisasi:** Number cards (tidak memiliki axis)  
**Kolom Ditampilkan:** `total_orders`, `total_customers`, `total_products`, `total_revenue`, `avg_basket_size`, `avg_review_score`, `on_time_delivery_pct`

---

###  Q1.2 — Top 10 Best-Selling Categories
**Fungsi:** Identifikasi 10 kategori produk tertinggi berdasarkan revenue untuk strategi stok.  
**Visualisasi:** Horizontal bar chart  
**X Axis:** `revenue` (nilai revenue dalam BRL, numerik)  
**Y Axis:** `category` (nama kategori produk, kategorikal)  
**Tooltip:** `total_orders`, `avg_review`  
**Urutan:** Descending by revenue (kategori terlaris di atas)

---

###  Q1.3 — Revenue Distribution per Category
**Fungsi:** Proporsi revenue per kategori — mana yang mendominasi.  
**Visualisasi:** Pie / Donut chart  
**Dimensi (Label):** `category` (nama kategori)  
**Nilai (Ukuran Segmen):** `revenue` (BRL)  
**Warna:** Tiap segmen mewakili satu kategori  
**Catatan:** Filter `!= 'missing'` digunakan karena NULL di-impute sebagai string `'missing'` di PySpark

---

###  Q1.4 — Overall Review Score Distribution
**Fungsi:** Distribusi skor ulasan 1–5 — seberapa banyak yang puas vs tidak puas.  
**Visualisasi:** Bar chart (5 bars)  
**X Axis:** `review_score` (nilai 1, 2, 3, 4, 5 — diskrit/kategorikal)  
**Y Axis:** `total` (jumlah order, numerik)  
**Warna:** Gradient merah (1) → hijau (5)

---

###  Q1.5 — Order Status Breakdown
**Fungsi:** Proporsi status pesanan (delivered, shipped, canceled, dll.).  
**Visualisasi:** Pie chart  
**Dimensi (Label):** `order_status`  
**Nilai (Ukuran Segmen):** `total_orders`  
**Warna:** Tiap status memiliki warna berbeda

---

###  Q1.6 — Monthly Revenue Trend
**Fungsi:** Tren bulanan revenue dan review score — melihat stagnasi.  
**Visualisasi:** Dual-axis line chart  
**X Axis:** `month` (format YYYYMM, kronologis)  
**Y Axis (Kiri):** `revenue` (total revenue BRL, numerik)  
**Y Axis (Kanan):** `avg_review` (rata-rata review score 1–5, numerik)  
**Series:** 2 garis — Revenue (area/line biru) & Avg Review (line oranye)  
**Tooltip:** `total_orders`, `revenue`, `avg_review`

---

###  Q1.7 — Top vs Bottom 3 Categories
**Fungsi:** Kontras antara kategori terbaik dan terburuk untuk strategi merchandising.  
**Visualisasi:** Comparative table  
**Kolom:** `category`, `total_orders`, `revenue`, `avg_review`, `label`  
**Warna Baris:** Hijau untuk Top Performer, Merah untuk Bottom Performer  
**Urutan:** Top 3 (revenue tertinggi) diikuti Bottom 3 (revenue terendah, min 10 orders)

---

##  TAB 2 — Knowledge Detail

###  Q2.1 — Order Distribution per Day of Week
**Fungsi:** Hari apa pelanggan paling banyak memesan?  
**Visualisasi:** Bar chart (7 bars)  
**X Axis:** `hari` (Senin–Minggu, kategorikal terurut)  
**Y Axis:** `total_orders` (jumlah pesanan unik, numerik)  
**Warna:** Single color atau gradient intensitas

---

###  Q2.2 — Hourly Order Activity
**Fungsi:** Jam sibuk dan jam sepi — untuk perencanaan kapasitas.  
**Visualisasi:** Area chart  
**X Axis:** `jam` (0–23, numerik/diskrit)  
**Y Axis:** `total_orders` (jumlah pesanan, numerik)  
**Series tambahan:** `revenue` (opsional, secondary axis)  
**Tooltip:** `total_orders`, `total_items`, `revenue`

---

###  Q2.3 — Heatmap: Hour × Day of Week
**Fungsi:** Peta intensitas pesanan per jam per hari — identifikasi peak time.  
**Visualisasi:** Heatmap / Pivot table  
**X Axis (Kolom):** Hari (Senin–Minggu)  
**Y Axis (Baris):** `jam` (0–23)  
**Nilai Sel:** Jumlah order (count)  
**Warna:** Gradient putih → biru tua (semakin gelap = semakin ramai)

---

###  Q2.4 — Category Detail: Items, Revenue, Review
**Fungsi:** Tabel detail per kategori — revenue, review, harga rata-rata.  
**Visualisasi:** Table with sorting  
**Kolom:** `category`, `total_items`, `revenue`, `avg_review`, `avg_price`, `avg_freight`  
**Urutan Default:** Descending by `revenue`  
**Conditional Formatting:** `avg_review` < 3.5 → merah, ≥ 4.0 → hijau

---

###  Q2.5 — Payment Method Distribution
**Fungsi:** Metode pembayaran mana yang paling populer dan bagaimana review-nya.  
**Visualisasi:** Pie chart + table  
**Pie — Dimensi:** `payment_type`  
**Pie — Nilai:** `total_orders`  
**Table — Kolom:** `payment_type`, `total_orders`, `revenue`, `avg_review`  
**Tooltip:** `avg_review` (untuk melihat kepuasan per metode bayar)

---

###  Q2.6 — Delivery Duration Distribution
**Fungsi:** Distribusi lama pengiriman dan korelasi dengan review score.  
**Visualisasi:** Bar chart with review overlay  
**X Axis:** `delivery_bucket` (0–3 hari, 4–7 hari, 8–14 hari, 15–21 hari, 22+ hari — kategorikal terurut)  
**Y Axis (Kiri):** `total_orders` (jumlah pesanan, numerik)  
**Y Axis (Kanan):** `avg_review` (rata-rata review score, numerik)  
**Series:** Bar = total_orders, Line = avg_review  
**Insight:** Semakin lama pengiriman → avg_review turun signifikan

---

###  Q2.7 — Customer State Distribution
**Fungsi:** Distribusi pelanggan per negara bagian — mana yang terbesar.  
**Visualisasi:** Bar chart / Map  
**X Axis:** `customer_state` (kode state Brazil, kategorikal)  
**Y Axis:** `total_orders` (jumlah pesanan, numerik)  
**Warna:** Gradient berdasarkan volume pesanan  
**Tooltip:** `revenue`, `avg_review`, `total_customers`

---

##  TAB 3 — Predictive & CX Analysis ( KEY TAB)

###  Q3.1 — Review Score vs Delivery Delay Correlation
```sql
SELECT
    CASE
        WHEN delivery_delay_days <= -7 THEN '7+ hari lebih cepat'
        WHEN delivery_delay_days <= -1 THEN '1-7 hari lebih cepat'
        WHEN delivery_delay_days <= 0 THEN 'Tepat waktu'
        WHEN delivery_delay_days <= 7 THEN '1-7 hari terlambat'
        ELSE '14+ hari terlambat'
    END AS delivery_category,
    count(DISTINCT order_id) AS total_orders,
    round(avg(review_score), 2) AS avg_review_score
FROM orders_db.order_items
WHERE review_score IS NOT NULL
GROUP BY delivery_category
```
**Fungsi:**  KORELASI KUNCI — membuktikan bahwa keterlambatan pengiriman secara langsung menurunkan review score. Ini adalah ROOT CAUSE utama.  
**Visualisasi:** Bar chart with color gradient  
**X Axis:** `delivery_category` (5 bucket keterlambatan, terurut dari cepat → sangat terlambat)  
**Y Axis:** `avg_review_score` (rata-rata review 1–5, numerik)  
**Warna:** Gradient hijau (cepat) → merah (sangat terlambat)  
**Tooltip:** `total_orders`, `avg_review_score`  
**Insight:** Pengiriman terlambat >7 hari menghasilkan review score rata-rata 1.5–2.0, sementara pengiriman tepat waktu menghasilkan 4.0+

---

###  Q3.2 — Late Delivery Impact on Review Score
**Fungsi:** Perbandingan langsung review score antara pesanan tepat waktu vs terlambat.  
**Visualisasi:** Comparative bar chart  
**X Axis:** `status` (2 kategori: "Tepat Waktu" vs "Terlambat")  
**Y Axis:** `avg_review_score` (numerik, skala 1–5)  
**Series tambahan:** `negative_pct` & `positive_pct` (bisa ditampilkan sebagai label di atas bar)  
**Warna:** Hijau = Tepat Waktu, Merah = Terlambat  
**Tooltip:** `total_orders`, `avg_review_score`, `negative_pct`, `positive_pct`  
**Insight:** Gap review score antara tepat waktu vs terlambat bisa mencapai 1.5–2.0 poin

---

###  Q3.3 — Review Analysis per Category
**Fungsi:** Kategori mana yang paling banyak review negatif? Apa korelasinya dengan late delivery?  
**Visualisasi:** Table sorted by avg_review ascending  
**Kolom:** `category`, `total_reviews`, `avg_review`, `negative_pct`, `positive_pct`, `late_delivery_pct`  
**Urutan:** Ascending by `avg_review` (kategori terburuk di atas)  
**Conditional Formatting:** `avg_review` < 3.0 → merah, `negative_pct` > 20% → merah

---

###  Q3.4 — Churn Risk Segmentation (RFM Matrix)
**Fungsi:** Segmentasi pelanggan berdasarkan RFM — identifikasi pelanggan berisiko churn.  
**Visualisasi:** RFM Matrix heatmap / pivot table  
**X Axis (Kolom):** `churn_risk` (Low, Medium, High)  
**Y Axis (Baris):** `loyalty_tier` (Bronze, Silver, Gold)  
**Nilai Sel:** `total_customers` (jumlah pelanggan per kombinasi)  
**Warna Sel:** Gradient — merah jika High Risk + Bronze (paling kritis)  
**Tooltip:** `avg_spend`, `avg_review`, `avg_recency_days`

---

###  Q3.5 — Delivery Performance per State
**Fungsi:** State mana yang paling bermasalah delivery-nya? Apa dampaknya ke review?  
**Visualisasi:** Table with conditional formatting  
**Kolom:** `state`, `total_orders`, `avg_delivery`, `late_pct`, `review_when_late`, `review_when_ontime`, `review_gap`  
**Urutan:** Descending by `late_pct` (state terburuk di atas)  
**Conditional Formatting:** `late_pct` > 15% → merah, `review_gap` > 1.5 → oranye

---

###  Q3.6 — Sales Forecasting per Category
**Fungsi:** Proyeksi demand per kategori berdasarkan growth rate historis.  
**Visualisasi:** Table with risk level badges  
**Kolom:** `category`, `current_total_orders`, `current_revenue`, `forecast_orders`, `forecast_revenue`, `growth_pct`, `risk_level`  
**Urutan:** Descending by `current_revenue`  
**Badge Warna:** risk_level: High Risk = merah, Medium = kuning, Low = hijau

---

###  Q3.7 — Approval Delay Impact on Review
**Fungsi:** Apakah waktu approval mempengaruhi kepuasan pelanggan?  
**Visualisasi:** Bar chart  
**X Axis:** `approval_bucket` (< 1 jam, 1–6 jam, 6–24 jam, 1–2 hari, 2+ hari — kategorikal terurut)  
**Y Axis:** `avg_review` (rata-rata review score, numerik)  
**Tooltip:** `total_orders`, `avg_review`  
**Warna:** Gradient hijau (cepat) → merah (sangat lambat)  
**Insight:** Approval > 24 jam menunjukkan penurunan review yang konsisten

---

##  TAB 4 — Improvements & Recommendations

###  Q4.1 — Worst Performing Sellers
**Fungsi:** 20 seller dengan review terburuk — target prioritas perbaikan.  
**Visualisasi:** Table (sortable)  
**Kolom:** `seller_id`, `seller_city`, `seller_state`, `total_orders`, `revenue`, `avg_review`, `late_pct`, `avg_delivery_days`  
**Urutan:** Ascending by `avg_review_score` (seller terburuk di atas)  
**Filter:** `total_orders >= 10` (menghindari seller dengan sampel terlalu kecil)  
**Conditional Formatting:** `avg_review` < 2.5 → merah, `late_pct` > 30% → merah  
**Rekomendasi:** Evaluasi seller dengan avg_review < 3.0, terutama yang late delivery tinggi

---

###  Q4.2 — Best Performing Sellers
**Fungsi:** Benchmark — apa yang dilakukan seller terbaik?  
**Visualisasi:** Table (sortable)  
**Kolom:** `seller_id`, `seller_city`, `seller_state`, `total_orders`, `revenue`, `avg_review`, `late_pct`  
**Urutan:** Descending by `avg_review_score`  
**Filter:** `total_orders >= 10`  
**Conditional Formatting:** `avg_review` >= 4.5 → hijau  
**Rekomendasi:** Pelajari pola seller dengan review 4.5+ untuk direplikasi

---

###  Q4.3 — Categories Needing CX Improvement
**Fungsi:** Kategori dengan negative review tertinggi — perlu intervention.  
**Visualisasi:** Bar chart (horizontal)  
**X Axis:** `negative_pct` (persentase review negatif, numerik)  
**Y Axis:** `category` (nama kategori, kategorikal)  
**Tooltip:** `total_reviews`, `avg_review`, `late_delivery_pct`, `avg_freight`  
**Urutan:** Descending by `negative_review_pct`  
**Warna:** Merah (negatif paling tinggi) → kuning  
**Rekomendasi:** Evaluasi apakah masalah di produk, shipping, atau seller

---

###  Q4.4 — Geographic Problem Areas
**Fungsi:** State dengan late delivery tertinggi — masalah logistik.  
**Visualisasi:** Table with conditional formatting  
**Kolom:** `state`, `total_orders`, `avg_review`, `late_pct`, `avg_freight`, `avg_delivery`  
**Urutan:** Ascending by `avg_review_score` (state terburuk di atas)  
**Filter:** `total_orders >= 50`  
**Conditional Formatting:** `late_pct` > 20% → merah, `avg_review` < 3.5 → merah  
**Rekomendasi:** Pertimbangkan warehouse tambahan atau partner logistik baru

---

###  Q4.5 — Payment Friction Analysis
**Fungsi:** Metode pembayaran mana yang berkorelasi dengan review rendah?  
**Visualisasi:** Grouped bar chart  
**X Axis:** `payment_type` (credit_card, boleto, voucher, debit_card — kategorikal)  
**Y Axis (Kiri):** `total_orders` (numerik)  
**Y Axis (Kanan):** `avg_review` (numerik, skala 1–5)  
**Tooltip:** `avg_installments`, `avg_value`, `avg_review`  
**Rekomendasi:** Evaluasi UX checkout per payment method

---

###  Q4.6 — High-Value but Low-Review Products
**Fungsi:** Produk revenue tinggi tapi review rendah — paling urgent untuk diperbaiki.  
**Visualisasi:** Scatter plot / Table  
**Kolom:** `product_id`, `category`, `total_orders`, `revenue`, `avg_review`, `total_unique_buyers`  
**Filter:** `total_orders >= 5` AND `avg_review_score < 3.5`  
**Urutan:** Descending by `revenue`  
**Scatter — X Axis:** `revenue`  
**Scatter — Y Axis:** `avg_review`  
**Bubble Size:** `total_orders`  
**Rekomendasi:** Prioritas QA/QC untuk produk ini

---

###  Q4.7 — Hourly Capacity Analysis
**Fungsi:** Kapan jam sibuk? Apakah kapasitas cukup?  
**Visualisasi:** Bar chart with line overlay  
**X Axis:** `order_hour` (0–23, numerik/diskrit)  
**Y Axis (Kiri):** `total_orders` (jumlah pesanan, numerik)  
**Y Axis (Kanan):** `utilization_pct` (% utilisasi kapasitas, numerik)  
**Warna Bar:** Merah = Peak, Kuning = Normal, Hijau = Low  
**Tooltip:** `avg_items`, `utilization_pct`, `peak_label`  
**Rekomendasi:** Scale up customer service dan logistics pada peak hours

---

##  TAB 5 — Data Governance & Pipeline Health

### Q5.1 — Data Quality Report: Missing Values
**Fungsi:** Audit transparansi — kolom mana yang banyak missing data?  
**Visualisasi:** Bar chart + table  
**X Axis:** `column_name` (nama kolom, kategorikal)  
**Y Axis:** `missing_pct` (persentase data hilang, numerik 0–100%)  
**Warna:** Merah = > 10%, Kuning = 5–10%, Hijau = < 5%  
**Kolom Table:** `column_name`, `total_rows`, `missing_count`, `missing_pct`, `ingested_date`

---

### Q5.2 — Pipeline Freshness & Volume Summary
**Fungsi:** Satu baris ringkasan lengkap — kapan batch terakhir, berapa kategori dipantau, berapa total order & pelanggan yang sudah diingest.  
**Visualisasi:** Number cards (6 KPI)  
**Kolom Ditampilkan:** `latest_batch`, `categories_tracked`, `total_batch_days`, `total_orders_ingested`, `total_customers_ingested`, `last_ingest_date`  
**Catatan:** Q5.2 dan Q5.3 (Data Volume) digabung menjadi satu query ini untuk efisiensi.

---

### Q5.3 — NULL Impact Assessment
**Fungsi:** Seberapa besar dampak missing data terhadap akurasi analisis?  
**Visualisasi:** Table with badge  
**Kolom:** `column_name`, `missing_count`, `total_rows`, `pct`, `impact_level`  
**Badge Warna:**  High Impact = merah,  Medium = kuning,  Low/Complete = hijau  
**Urutan:** Descending by `missing_pct`

---

##  TAB 6 — CX Engine (Deep Analysis)

###  Q6.1 — Customer Lifetime Value Distribution
**Fungsi:** CLV per loyalty tier — siapa pelanggan paling bernilai?  
**Visualisasi:** Grouped bar chart  
**X Axis:** `loyalty_tier` (Bronze, Silver, Gold — kategorikal)  
**Y Axis:** `avg_clv` (rata-rata total belanja per pelanggan, BRL)  
**Series:** `avg_orders`, `avg_review`, `avg_recency` (sebagai metric card atau tooltip)  
**Warna:** Bronze = coklat, Silver = abu, Gold = emas  
**Tooltip:** `total_customers`, `avg_clv`, `avg_orders`, `avg_review`

---

###  Q6.2 — Review Comment Analysis
**Fungsi:** Apakah pelanggan yang menulis komentar cenderung memberi review lebih rendah?  
**Visualisasi:** Comparative bar chart  
**X Axis:** `comment_status` (2 kategori: "Dengan Komentar" vs "Tanpa Komentar")  
**Y Axis:** `avg_review` (rata-rata review score, numerik)  
**Tooltip:** `total_orders`, `avg_review`, `negative_pct`  
**Insight:** Order dengan komentar cenderung memiliki review lebih rendah (pelanggan kecewa lebih termotivasi menulis)

---

###  Q6.3 — Review Score Trend over Time
**Fungsi:**  Tren review score bulanan — apakah benar stagnan?  
**Visualisasi:** Multi-line chart  
**X Axis:** `month` (YYYY-MM, kronologis)  
**Y Axis (Kiri):** `avg_review` (rata-rata review score, numerik, skala 1–5)  
**Y Axis (Kanan):** `positive_pct` & `negative_pct` (persentase, numerik 0–100%)  
**Series:** 3 garis — avg_review (biru), positive_pct (hijau), negative_pct (merah)  
**Tooltip:** `total_orders`, `avg_review`, `positive_pct`, `negative_pct`

---

###  Q6.4 — ROOT CAUSES of Low Review Scores
```sql
SELECT
    'Late Delivery' AS factor,
    round(avg(CASE WHEN is_late_delivery = 1 THEN review_score END), 2) AS avg_review_affected,
    round(avg(CASE WHEN is_late_delivery = 0 THEN review_score END), 2) AS avg_review_unaffected,
    round(...) AS impact_delta,
    countIf(is_late_delivery = 1) AS affected_orders
FROM orders_db.order_items
```
**Fungsi:**  QUERY PALING PENTING — membandingkan 4 faktor utama penyebab review rendah:  
1. **Late Delivery** — faktor paling dominan  
2. **High Freight** — biaya kirim mahal menurunkan expectation  
3. **Slow Approval** — delay approval membuat pelanggan frustrasi  
4. **Heavy Product** — produk berat lebih lama dikirim  

**Visualisasi:** Grouped bar chart (2 bar per faktor)  
**X Axis:** `factor` (4 faktor: Late Delivery, High Freight, Slow Approval, Heavy Product)  
**Y Axis:** `avg_review` (numerik, skala 1–5)  
**Series:** 2 bar per faktor — `avg_review_affected` (merah) vs `avg_review_unaffected` (hijau)  
**Anotasi:** `impact_delta` ditampilkan sebagai label di antara dua bar  
**Tooltip:** `affected_orders`, `avg_review_affected`, `avg_review_unaffected`, `impact_delta`  
**Insight untuk CEO:** Review score stagnan bukan karena satu faktor, melainkan kombinasi delivery issues yang mempengaruhi ~15–25% pesanan

---

###  Q6.5 — TF-IDF: Top Keywords per Review Bucket
**Function:** Identify phrases that most exclusively appear in negative reviews using TF-IDF scores and N-Grams (Paper Methodology).  
**Visualization:** Horizontal Bar Chart  
**X Axis:** `tfidf_score` (Word weighting score)  
**Y Axis:** `keyword` (Phrase / N-Gram)  
**Filter:** `review_bucket = 'negative'`  
**Insight:** Discovers specific root causes such as 'no response', 'item not received', etc.

---

###  Q6.6 — Log-Odds: Negative vs Positive Exclusive Words
**Function:** Identifies which words are statistically used much more frequently in bad reviews compared to good reviews.  
**Visualization:** Table with conditional formatting  
**Columns:** `keyword`, `log_odds`, `freq_negative`, `freq_positive`, `total_docs`  
**Sort:** `log_odds` DESC (most indicative of negative reviews at the top)  
**Insight:** Validates that the biggest problems lie in customer service and shipping complaints.

---

##  TAB 7 — CX Deep Dive: Review Stagnation Analysis (11 Queries)

Tab khusus untuk menjawab pertanyaan CEO: "Mengapa review score sulit naik dan cenderung stagnan?"

###  Q7.1 — Monthly Review Score Evolution
- **Function**: Track avg review score, positive/negative/neutral percentage month-over-month
- **Data Source**: `analytics.monthly_review_trend`
- **Visualization**: Line chart (dual axis)
- **X Axis**: `year_month` (format 'YYYY-MM', kronologis)
- **Y Axis (Kiri)**: `avg_review` (rata-rata review score, skala 1–5)
- **Y Axis (Kanan)**: `negative_pct` (% review negatif, skala 0–100%)
- **Series**: avg_review (biru), negative_pct (merah)
- **Tooltip**: `total_reviews`, `total_orders`, `positive_pct`, `neutral_pct`, `negative_pct`
- **Insight**: Shows the exact trajectory of review scores — is it truly stagnant, declining, or fluctuating?

---

###  Q7.2 — Monthly Positive vs Negative Trend
- **Function**: Visualize shift between positive, neutral, and negative review proportions over time
- **Data Source**: `analytics.monthly_review_trend`
- **Visualization**: 100% Stacked area chart
- **X Axis**: `year_month` (kronologis)
- **Y Axis**: Persentase kumulatif (0–100%)
- **Series**: `positive_pct` (hijau), `neutral_pct` (abu), `negative_pct` (merah)
- **Tooltip**: `positive_pct`, `neutral_pct`, `negative_pct` per bulan
- **Insight**: Even if average is stable, the composition may be shifting — growing negatives offset by growing positives

---

###  Q7.3 — Late Delivery Rate vs Review Score Trend
- **Function**: Overlay late_delivery_pct on avg_review score month-by-month
- **Data Source**: `analytics.monthly_review_trend`
- **Visualization**: Dual-axis line chart
- **X Axis**: `year_month` (kronologis)
- **Y Axis (Kiri)**: `avg_review` (skala 1–5)
- **Y Axis (Kanan)**: `late_delivery_pct` (skala 0–100%)
- **Series**: avg_review (biru solid), late_delivery_pct (merah putus-putus)
- **Tooltip**: `avg_delay_days`, `avg_freight`, `avg_review`, `late_delivery_pct`
- **Insight**: Shows temporal correlation — when late deliveries spike, do reviews drop proportionally?

---

###  Q7.4 — Compounding Factor Impact Matrix
- **Function**: Cross-tabulate 3 binary factors (late delivery × high freight × slow approval) with avg review score
- **Data Source**: `analytics.review_root_cause_matrix`
- **Visualization**: Heatmap / Pivot table
- **Baris (Y)**: Kombinasi faktor (8 kombinasi: 0-faktor hingga 3-faktor)
- **Kolom (X)**: `is_late`, `is_high_freight`, `is_slow_approval` (Yes/No)
- **Nilai Sel**: `avg_review_score`
- **Warna Sel**: Merah (review rendah) → Hijau (review tinggi)
- **Tooltip**: `total_orders`, `avg_review_score`, `negative_pct`, `positive_pct`
- **Insight**: Reveals compounding effects — e.g., late delivery alone drops review to ~2.5, but late + high freight drops to ~1.8

---

###  Q7.5 — Single vs Multi-Factor Impact Summary
- **Function**: Compare weighted avg review when 0, 1, 2, or 3 negative factors are present
- **Data Source**: `analytics.review_root_cause_matrix`
- **Visualization**: Bar chart
- **X Axis**: `factor_group` (0 factors, 1 factor, 2 factors, 3 factors — kategorikal terurut)
- **Y Axis**: `weighted_avg_review` (rata-rata review tertimbang, numerik)
- **Warna**: Gradient hijau (0 faktor) → merah (3 faktor)
- **Tooltip**: `total_orders`, `weighted_avg_review`, `weighted_negative_pct`
- **Insight**: Quantifies the incremental damage of each additional negative factor

---

###  Q7.6 — Seller Origin vs Customer Destination
- **Function**: Show worst-performing seller→customer state routes by review score
- **Data Source**: `analytics.seller_state_review`
- **Visualization**: Table with conditional formatting
- **Kolom**: `seller_state`, `customer_state`, `is_same_state`, `total_orders`, `avg_review`, `avg_shipping_days`, `late_pct`
- **Filter**: `total_orders >= 20`
- **Urutan**: Ascending by `avg_review_score` (rute terburuk di atas)
- **Conditional Formatting**: `avg_review` < 3.0 → merah, `late_pct` > 20% → merah
- **Insight**: Identifies if poor CX comes from specific shipping corridors

---

###  Q7.7 — Review Score Distribution Shift
- **Function**: Show proportion of each review score (1–5) changing month-by-month
- **Data Source**: `analytics.review_score_shift`
- **Visualization**: 100% Stacked bar chart
- **X Axis**: `year_month` (kronologis)
- **Y Axis**: `score_pct` (PENTING: Hanya masukkan `score_pct` ke Y-Axis, hapus kolom lain)
- **Series Breakout (Metabase)**: `review_score` (PENTING: Klik "Add series breakout" di bawah X-Axis, JANGAN tumpuk di Y-Axis)
- **Display Setting**: Buka tab Display -> ubah opsi Stacking menjadi "100%"
- **Tooltip**: `review_score`, `total_count`, `score_pct` per bulan
- **Insight**: Detects polarization — if both 5 and 1 are growing while 3 shrinks, the average looks stable but customer experience is actually diverging

---

###  Q7.8 — Review Polarization Index
- **Function**: Track (1 + 5) / total percentage over time as a polarization metric
- **Data Source**: `analytics.review_score_shift`
- **Visualization**: Line chart
- **X Axis**: `year_month` (kronologis)
- **Y Axis**: `polarization_index` (persentase, 0–100%)
- **Series**: polarization_index (ungu solid), five_star_pct (hijau putus), one_star_pct (merah putus)
- **Tooltip**: `extreme_count`, `total_count`, `polarization_index`, `five_star_pct`, `one_star_pct`
- **Insight**: Rising polarization index = average review looks stable but extremes are growing, suggesting fundamentally different customer segments having very different experiences

---

##  TAB 8 — CX Simulation & Projections: What-If Model (8 Queries)

Tab khusus untuk menjawab pertanyaan: **"Jika solusi CX diterapkan, data review score akan seperti apa?"**

> **Model**: Random Forest Counterfactual Simulation  
> **Data Source**: `analytics.simulation_scenarios` + `analytics.simulation_feature_impact`  
> **Baseline**: 20.000 orders historis dari `order_items_sample_colab.csv`

###  Q8.1 — Scenario Comparison Summary 
- **Function**: Tampilan utama — baseline vs projected avg review untuk 5 skenario simulasi
- **Data Source**: `analytics.simulation_scenarios`
- **Visualization**: Grouped bar chart (2 bar per skenario)
- **X Axis**: `scenario_name` (5 skenario: S1–S5, kategorikal)
- **Y Axis**: `avg_review` (numerik, skala 1–5)
- **Series**: `baseline_review` (abu/putih) vs `projected_review` (biru/hijau)
- **Error Bar**: `ci_lower` – `ci_upper` (95% confidence interval)
- **Tooltip**: `review_delta`, `improvement_pct`, `affected_orders`, `affected_pct`, `model_type`
- **Insight**: Skenario mana yang paling berdampak terhadap peningkatan review score?

---

###  Q8.2 — Projected Review Score per Scenario 
- **Function**: Ranking skenario berdasarkan projected avg review, dengan 95% confidence interval
- **Data Source**: `analytics.simulation_scenarios`
- **Visualization**: Horizontal bar chart dengan reference line baseline
- **X Axis**: `projected_avg_review` (numerik, skala 1–5)
- **Y Axis**: `scenario_name` (kategorikal, diurutkan descending by projected review)
- **Reference Line**: `baseline_avg_review` = 4.078 (garis vertikal)
- **Warna**: Gradient berdasarkan `impact_level` (High Impact = hijau, Low = abu)
- **Error Bar Horizontal**: `ci_lower_95` – `ci_upper_95`
- **Tooltip**: `delta`, `impact_level`, `ci_lower_95`, `ci_upper_95`
- **Insight**: Proyeksi review score + uncertainty range jika solusi diimplementasikan

---

###  Q8.3 — Orders Affected per Scenario 
- **Function**: Berapa banyak order yang kondisinya berubah di setiap skenario?
- **Data Source**: `analytics.simulation_scenarios`
- **Visualization**: Stacked bar chart
- **X Axis**: `scenario_name` (5 skenario, kategorikal)
- **Y Axis**: `affected_orders` dan `unaffected_orders` (PENTING: JANGAN masukkan `total_orders` ke dalam Y-Axis agar tinggi bar tidak terjumlah ganda)
- **Display Setting**: Buka tab Display -> ubah opsi Stacking menjadi "Stack"
- **Label**: `affected_pct` ditampilkan di dalam segmen affected
- **Tooltip**: `affected_orders`, `unaffected_orders`, `affected_pct`, `unaffected_pct`
- **Insight**: S5 (Combined) mempengaruhi 17.7% orders — scope terbesar

---

###  Q8.4 — ML Feature Importance 
- **Function**: Faktor mana yang paling berpengaruh terhadap review score dalam model Random Forest?
- **Data Source**: `analytics.simulation_feature_impact`
- **Visualization**: Horizontal bar chart (sorted descending)
- **X Axis**: `importance_pct` (persentase kontribusi, numerik 0–100%)
- **Y Axis**: `feature_label` (nama fitur human-readable, kategorikal)
- **Warna**: Gradient berdasarkan `driver_level` (Critical = merah tua, Major = oranye, Moderate = kuning, Minor = abu)
- **Anotasi**: `importance_pct` sebagai label di ujung bar
- **Tooltip**: `model_r2`, `driver_level`
- **Insight**: `delivery_delay_days` berkontribusi **75.4%** dari total feature importance — konfirmasi bahwa keterlambatan adalah ROOT CAUSE utama

---

###  Q8.5 — Negative Review Reduction per Scenario 
- **Function**: Berapa persen review negatif yang berkurang setelah setiap solusi?
- **Data Source**: `analytics.simulation_scenarios`
- **Visualization**: Grouped bar chart (before vs after, 2 metrik)
- **X Axis**: `scenario_name` (5 skenario, kategorikal)
- **Y Axis**: `baseline_negative_pct`, `projected_negative_pct`, `baseline_positive_pct`, `projected_positive_pct` 
  *(PENTING: HANYA 4 kolom ini. JANGAN masukkan `negative_reduction` atau `positive_gain` ke Y-Axis agar bar tidak berdesakan)*
- **Display Setting**: Pastikan tipe chart adalah "Bar" (berdampingan). Ubah warnanya:
  - `baseline_negative_pct`: Merah Muda
  - `projected_negative_pct`: Merah Tua
  - `baseline_positive_pct`: Hijau Muda
  - `projected_positive_pct`: Hijau Tua
- **Tooltip**: Fokus pada 4 nilai baris untuk perbandingan Before vs After
- **Insight**: S1 dan S5 berhasil menurunkan negative review terbesar

---

###  Q8.6 — Revenue Impact Projection 
- **Function**: Estimasi dampak finansial dari peningkatan review score per skenario
- **Data Source**: `analytics.simulation_scenarios` (filter `review_delta > 0`)
- **Visualization**: Bar chart dengan badge
- **X Axis**: `scenario_name` (skenario yang berdampak positif, kategorikal)
- **Y Axis**: `revenue_impact_brl` (estimasi revenue tambahan dalam BRL, numerik)
- **Series Breakout (Metabase)**: `roi_level` (PENTING: Masukkan ke "Add series breakout" di bawah X-axis agar bar diberi warna sesuai tingkat ROI)
- **Tooltip**: `additional_orders_est`, `review_delta`, `affected_pct`
- **Catatan**: Asumsi +1 poin review → +5% order growth × avg order value
- **Insight**: Skenario dengan review_delta paling besar memberikan revenue impact terbesar

---

###  Q8.7 — Solution Recommendation Summary 
- **Function**: Tabel ringkasan CEO-level: skenario, before/after, confidence interval, aksi rekomendasi
- **Data Source**: `analytics.simulation_scenarios`
- **Visualization**: Table with conditional formatting
- **Kolom**: `scenario_id`, `scenario_name`, `before`, `after`, `delta`, `pct_orders_affected`, `recommendation_preview`, `ci_lower`, `ci_upper`
- **Urutan**: `scenario_id` ASC (S1 → S5)
- **Conditional Formatting**: `delta` > 0.05 → hijau bold, `delta` < 0 → merah
- **Insight**: One-page summary untuk presentasi ke CEO/management

---

###  Q8.8 — Priority Matrix: Impact vs Scope 
- **Function**: Kuadran prioritas solusi berdasarkan dampak (review_delta) vs cakupan (affected_pct)
- **Data Source**: `analytics.simulation_scenarios`
- **Visualization**: Scatter / Bubble chart
- **X Axis**: `scope_pct` (% orders terdampak, numerik — proxy "effort/scope")
- **Y Axis**: `impact_score` (review_delta, numerik — ukuran dampak)
- **Bubble Size**: `est_revenue_impact_brl` (semakin besar revenue impact = bubble lebih besar)
- **Warna**: Berdasarkan `priority_quadrant` — Quick Win (hijau), Strategic (biru), Incremental (kuning), Low Priority (abu)
- **Label**: `scenario_name` di setiap titik
- **Quadrant Lines**: Garis vertikal di x=10% dan garis horizontal di y=0.05
- **Tooltip**: `projected_review`, `impact_score`, `scope_pct`, `priority_quadrant`, `est_revenue_impact_brl`
- **Insight**: S1 masuk kuadran "Quick Win" — dampak tinggi dengan scope sempit (hanya 7.5% orders)

---

##  Kesimpulan & Rekomendasi untuk CEO

Berdasarkan 58 query di atas + model simulasi What-If, analisis menunjukkan bahwa **review score stagnan** disebabkan oleh:

1. **Late Delivery** — Faktor #1 penurun review score (feature importance: 75.4%, impact delta: ~1.7 poin)
2. **Biaya Freight Tinggi** — Korelasi negatif dengan kepuasan (feature importance: 6.7%)
3. **Seller Performance Tidak Merata** — Seller tertentu secara konsisten buruk
4. **Masalah Geografis** — State tertentu memiliki delivery time jauh lebih lama
5. **Approval Delay** — Waktu approval >24 jam menurunkan review

**Proyeksi Simulasi (Model RF, R²=0.147 pada data 20K orders):**

| Skenario | Baseline | Projected | Delta | Affected Orders |
|----------|----------|-----------|-------|----------------|
| S1 — Hilangkan Late Delivery | 4.078 | **4.187** | **+0.110** | 7.5% |
| S2 — Percepat Approval | 4.078 | 4.078 | +0.001 | 1.6% |
| S3 — Cap Freight ≤30 | 4.078 | 4.077 | -0.001 | 11.4% |
| S4 — Remove Bad Sellers | 4.078 | 4.082 | +0.004 | 1.7% |
| S5 — Best Case Combined | 4.078 | **4.186** | **+0.108** | 17.7% |

**Rekomendasi Prioritas (berdasarkan Priority Matrix Q8.8):**
1.  **[Quick Win]** Perbaiki logistik → eliminasi late delivery (S1: dampak +0.110 poin)
2.  **[Strategic]** Evaluasi seller buruk + perbaiki approval flow (S4+S2)
3.  **[Incremental]** Optimalkan freight cost via subsidi/negosiasi (S3)
4.  Monitor trend per batch melalui Speed Layer dashboard Tab 7
