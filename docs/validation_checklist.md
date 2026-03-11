# Model Reporting Framework — v1 Validation Checklist

Use this checklist when validating a new score month run before promoting output tables.
Each check can be run as a manual SQL query or wrapped into an automated assertion script.

---

## 1. Row Count Checks

### 1.1 Layer1 — Account-level output

| Check | Query | Expected |
|---|---|---|
| Layer1 row count matches source | `SELECT COUNT(*) FROM lfs_layer1_account WHERE score_month = '...'` vs source | Equal |
| No duplicate account keys | `SELECT creditaccountid, COUNT(*) FROM lfs_layer1_account GROUP BY 1 HAVING COUNT(*) > 1` | 0 rows |
| All channels present | `SELECT DISTINCT channel FROM lfs_layer1_account` | {digital, directmail} |
| vintage_month populated for all rows | `SELECT COUNT(*) FROM lfs_layer1_account WHERE vintage_month IS NULL` | 0 |

### 1.2 Layer2 — Aggregated tables

| Check | Query | Expected |
|---|---|---|
| `overall_summary` account_count sum matches Layer1 | `SELECT SUM(account_count) FROM lfs_layer2_overall_summary WHERE vintage_month = '...'` vs `SELECT COUNT(*) FROM lfs_layer1_account WHERE vintage_month = '...'` | Equal |
| `score_distribution` has one row per (vintage_month, channel, decile) | No duplicate (vintage_month, channel, lfs_decile_dyn) | 0 duplicates |
| `by_source` account_count sum = Layer1 total | Same as overall_summary | Equal |
| `by_line` account_count sum = Layer1 total | Same as overall_summary | Equal |
| All 4 driver tables produced with > 0 rows | Check row count > 0 for each driver table | > 0 |

### 1.3 Layer3 — Monitoring tables

| Check | Query | Expected |
|---|---|---|
| `score_drift` has one row per (vintage_month, channel) | No duplicate (vintage_month, channel) | 0 duplicates |
| `feature_psi` has N_features rows per (vintage_month, channel) | `SELECT COUNT(*) / COUNT(DISTINCT vintage_month || channel) FROM lfs_layer3_feature_psi` | = 11 (LFS feature count) |
| `data_quality` has one row per (vintage_month, channel) | No duplicate (vintage_month, channel) | 0 duplicates |
| `feature_quality` has N_features rows per (vintage_month, channel) | Same as feature_psi | = 11 |
| `feature_score_relationship` same as feature_quality | Count per (vintage_month, channel) | = count of numeric features |
| `population_mix` has rows for each segment × segment_value | Check segment_types are {source, line, receivable_bucket, saleamount_bucket} | All 4 present |

---

## 2. Aggregation Reconciliation Checks

These verify that aggregated numbers in Layer2 and Layer3 are consistent with Layer1.

### 2.1 Score distribution reconciliation

```sql
-- Layer2 average score should match direct computation from Layer1.
SELECT
    l2.vintage_month,
    l2.channel,
    l2.avg_lfs_score    AS l2_avg,
    l1_agg.avg_l1_score AS l1_avg,
    ABS(l2.avg_lfs_score - l1_agg.avg_l1_score) AS diff
FROM lfs_layer2_overall_summary l2
JOIN (
    SELECT vintage_month, channel, AVG(lfs_score) AS avg_l1_score
    FROM lfs_layer1_account
    WHERE score_month = '<score_month>'
    GROUP BY vintage_month, channel
) l1_agg USING (vintage_month, channel);
```

**Expected**: `diff` < 0.0001 for all rows.

### 2.2 Decile distribution completeness

```sql
-- Each (vintage_month, channel) should have exactly 10 decile bins.
SELECT vintage_month, channel, COUNT(DISTINCT lfs_decile_dyn) AS n_bins
FROM lfs_layer2_score_distribution
WHERE score_month = '<score_month>'
GROUP BY vintage_month, channel;
```

**Expected**: `n_bins = 10` for every row.

### 2.3 Layer3 mean score cross-check

```sql
-- score_drift mean should match Layer2 overall_summary avg.
SELECT
    sd.vintage_month,
    sd.channel,
    sd.mean_lfs_score   AS l3_mean,
    os.avg_lfs_score    AS l2_mean,
    ABS(sd.mean_lfs_score - os.avg_lfs_score) AS diff
FROM lfs_layer3_score_drift sd
JOIN lfs_layer2_overall_summary os USING (vintage_month, channel);
```

**Expected**: `diff` < 0.001.

---

## 3. Percentage Sum Checks

All derived percentage columns must sum to 1.0 (or 100%) within their partition.

### 3.1 Layer2 — pct_of_channel_vintage_accounts

```sql
-- Must sum to 1.0 per (vintage_month, channel) across all rows (e.g. decile groups).
SELECT vintage_month, channel,
       SUM(pct_of_channel_vintage_accounts) AS total_pct
FROM lfs_layer2_score_distribution
WHERE score_month = '<score_month>'
GROUP BY vintage_month, channel;
```

**Expected**: `total_pct = 1.0` ± 1e-9 for every group.

### 3.2 Layer3 — pct_of_channel_accounts (population_mix)

```sql
-- Must sum to 1.0 per (vintage_month, channel, segment_type).
SELECT vintage_month, channel, segment_type,
       SUM(pct_of_channel_accounts) AS total_pct
FROM lfs_layer3_population_mix
WHERE score_month = '<score_month>'
GROUP BY vintage_month, channel, segment_type;
```

**Expected**: `total_pct = 1.0` ± 1e-9 for every group.

### 3.3 Rate columns bounded between 0 and 1

```sql
-- All rate columns must be in [0, 1].
SELECT COUNT(*) AS violations
FROM lfs_layer3_data_quality
WHERE missing_score_rate NOT BETWEEN 0.0 AND 1.0
   OR outlier_score_rate NOT BETWEEN 0.0 AND 1.0;
```

**Expected**: 0.

```sql
SELECT COUNT(*) AS violations
FROM lfs_layer3_feature_quality
WHERE missing_rate NOT BETWEEN 0.0 AND 1.0
   OR outlier_rate NOT BETWEEN 0.0 AND 1.0;
```

**Expected**: 0.

---

## 4. Baseline Self-Comparison Checks

When the current month is within the baseline window, or when running the pipeline
against baseline data as "current", certain metrics should equal their identity values.

### 4.1 PSI self-comparison (baseline vs baseline)

```sql
-- When current_df == baseline_df, PSI should be near 0.
SELECT vintage_month, channel, score_psi
FROM lfs_layer3_score_drift
WHERE score_month = '<baseline_month>';
```

**Expected**: `score_psi < 0.05` for all rows (no meaningful drift when compared to itself).

### 4.2 KS self-comparison

```sql
SELECT vintage_month, channel, score_ks
FROM lfs_layer3_score_drift
WHERE score_month = '<baseline_month>';
```

**Expected**: `score_ks < 0.05`.

### 4.3 Feature PSI self-comparison

```sql
SELECT feature_name, psi
FROM lfs_layer3_feature_psi
WHERE score_month = '<baseline_month>';
```

**Expected**: `psi < 0.05` for all features.

### 4.4 Mean and std stability in baseline month

```sql
-- baseline_mean ≈ current_mean, baseline_std ≈ current_std when running on baseline data.
SELECT feature_name,
       ABS(baseline_mean - current_mean) AS mean_shift,
       ABS(baseline_std  - current_std)  AS std_shift
FROM lfs_layer3_feature_psi
WHERE score_month = '<baseline_month>';
```

**Expected**: `mean_shift < 0.001` and `std_shift < 0.001`.

---

## 5. Channel-Specific Checks

Verify that both channels are consistently represented and that channel-level
numbers are not cross-contaminated.

### 5.1 Both channels present in every Layer2 table

```sql
SELECT DISTINCT channel FROM lfs_layer2_overall_summary WHERE score_month = '<score_month>';
```

**Expected**: rows for both `digital` and `directmail`.

### 5.2 Channel account counts sum correctly

```sql
-- Per-channel count from Layer2 should match Layer1 per-channel count.
SELECT l2.channel,
       l2.account_count        AS l2_count,
       l1.account_count        AS l1_count,
       l2.account_count - l1.account_count AS diff
FROM lfs_layer2_overall_summary l2
JOIN (
    SELECT channel, COUNT(*) AS account_count
    FROM lfs_layer1_account
    WHERE vintage_month = '<score_month>'
    GROUP BY channel
) l1 USING (channel)
WHERE l2.vintage_month = '<score_month>';
```

**Expected**: `diff = 0` for all channels.

### 5.3 No channel bleed in Layer3 metrics

```sql
-- Each (vintage_month, channel) row in score_drift should only contain
-- data for that channel.  Verify by checking that per-channel account
-- totals from population_mix match Layer2 account_count.
SELECT
    pm.vintage_month,
    pm.channel,
    SUM(pm.account_count) AS pm_total,
    os.account_count      AS l2_total
FROM lfs_layer3_population_mix pm
JOIN lfs_layer2_overall_summary os USING (vintage_month, channel)
WHERE pm.segment_type = 'source'
GROUP BY pm.vintage_month, pm.channel, os.account_count;
```

**Expected**: `pm_total = l2_total` for all rows.

### 5.4 Channel-specific PSI reasonableness

```sql
-- Flag any channel where PSI > 0.2 (high instability threshold).
SELECT vintage_month, channel, score_psi,
       CASE WHEN score_psi > 0.2 THEN 'HIGH' WHEN score_psi > 0.1 THEN 'MEDIUM' ELSE 'OK' END AS psi_flag
FROM lfs_layer3_score_drift
WHERE score_month = '<score_month>'
ORDER BY score_psi DESC;
```

**Expected**: `psi_flag = 'OK'` for routine months; investigate `MEDIUM` and escalate `HIGH`.

---

## 6. Data Quality Gate (Go / No-Go)

The following are hard-stop conditions. If any fail, **do not promote** the output tables.

| # | Condition | Threshold |
|---|---|---|
| DQ-1 | Layer1 row count vs source count | Δ = 0 |
| DQ-2 | Any column in Layer1 has > 5% unexpected nulls | < 5% |
| DQ-3 | `missing_score_rate` in data_quality | < 1% |
| DQ-4 | `score_psi` in score_drift | < 0.25 |
| DQ-5 | `pct_of_channel_vintage_accounts` sums to 1.0 per partition | Δ < 1e-9 |
| DQ-6 | All channels present in Layer2 and Layer3 | = {digital, directmail} |
| DQ-7 | No duplicate keys in Layer1 | 0 duplicates |

---

*Generated for model_reporting framework v1 — update thresholds as model matures.*
