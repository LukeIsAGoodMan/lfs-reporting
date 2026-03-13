# Loss Forecast Suite — Reporting Run 2025-05

> **Score month**: 2025-05 &nbsp;|&nbsp; **Model version**: v1.0 &nbsp;|&nbsp; **Generated**: 2026-03-13 21:08 UTC

---

## A. Run Metadata

| Property | Value |
|---|---|
| Model | lfs — Loss Forecast Suite |
| Score month | 2025-05 |
| Model version | v1.0 |
| Baseline window | 2024-08 → 2025-02 |
| Channels | digital, directmail |
| Features monitored | 11 |
| Generated | 2026-03-13 21:08 UTC |
| Layer 1 accounts (current) | 1,200 |

---

## B. Executive Summary

### Score Health

| Channel | Avg Score | Std | PSI vs Baseline | KS vs Baseline | % Above Baseline P90 | % Above Baseline P95 |
|---|---|---|---|---|---|---|
| digital | 0.4205 | 0.2115 | 0.1201 [WARN] | 0.1274 [WARN] | 23.0% [WARN] | 18.0% [WARN] |
| directmail | 0.4035 | 0.2081 | 0.4461 **[ALERT]** | 0.2603 [WARN] | 30.5% [WARN] | 22.5% [WARN] |

### Top Drifting Features (by PSI)

| Rank | Feature | Max PSI (across channels) | Baseline Mean | Current Mean | Mean Shift |
|---|---|---|---|---|---|
| 1 | feature_03 | 0.8483 **[ALERT]** | 0.3023 | 0.4346 | +0.1323 |
| 2 | feature_01 | 0.8375 **[ALERT]** | 0.3461 | 0.4526 | +0.1065 |
| 3 | feature_10 | 0.7268 **[ALERT]** | 0.4938 | 0.5727 | +0.0789 |
| 4 | feature_04 | 0.2539 **[ALERT]** | 0.3961 | 0.4569 | +0.0608 |
| 5 | feature_02 | 0.1084 [WARN] | 0.4650 | 0.4750 | +0.0100 |

---

## C. Business Summary

### Overall Volume & Score (2025-05)

| Channel | Accounts | Avg Score | Avg Receivable | Avg Sale Amount |
|---|---|---|---|---|
| digital | 200 | 0.4205 | 7283.25 | 2844.92 |
| directmail | 200 | 0.4035 | 5841.38 | 2445.50 |

### Score Band Distribution (2025-05)

Decile bins 1–3 = Low · 4–7 = Medium · 8–10 = High

| Channel | Low (D1–D3) | Medium (D4–D7) | High (D8–D10) |
|---|---|---|---|
| digital | 30.0% | 40.0% | 30.0% |
| directmail | 30.0% | 40.0% | 30.0% |

### Score by Acquisition Source (2025-05)

| Channel | Source | Accounts | Avg Score |
|---|---|---|---|
| digital | organic | 83 | 0.4245 |
| digital | paid | 69 | 0.4123 |
| digital | referral | 48 | 0.4252 |
| directmail | organic | 74 | 0.3913 |
| directmail | paid | 67 | 0.3988 |
| directmail | referral | 59 | 0.4244 |

### Score by Product Line (2025-05)

| Channel | Line | Accounts | Avg Score |
|---|---|---|---|
| digital | business | 82 | 0.4395 |
| digital | personal | 118 | 0.4072 |
| directmail | business | 38 | 0.3759 |
| directmail | personal | 162 | 0.4100 |

---

## D. Monitoring Summary

### D1. Score Drift (2025-05)

| Channel | Mean | Std | PSI | KS | P50 | P90 | P95 | Top-10% Mean | % Above P90 Base | % Above P95 Base |
|---|---|---|---|---|---|---|---|---|---|---|
| digital | 0.4205 | 0.2115 | 0.1201 [WARN] | 0.1274 [WARN] | 0.3887 | 0.6949 | 0.7870 | 0.7949 | 23.0% [WARN] | 18.0% [WARN] |
| directmail | 0.4035 | 0.2081 | 0.4461 **[ALERT]** | 0.2603 [WARN] | 0.4038 | 0.6429 | 0.6977 | 0.7510 | 30.5% [WARN] | 22.5% [WARN] |

### D2. Feature PSI — Top 5 per Channel (2025-05)

| Channel | Feature | PSI | Base Mean | Curr Mean | Mean Shift | Base Std | Curr Std |
|---|---|---|---|---|---|---|---|
| digital | feature_03 | 0.7228 **[ALERT]** | 0.2983 | 0.4273 | +0.1291 | 0.1487 | 0.1511 |
| digital | feature_10 | 0.7117 **[ALERT]** | 0.4965 | 0.5595 | +0.0630 | 0.2329 | 0.2242 |
| digital | feature_01 | 0.4405 **[ALERT]** | 0.3804 | 0.4592 | +0.0789 | 0.1115 | 0.1343 |
| digital | feature_04 | 0.2027 [WARN] | 0.4195 | 0.4746 | +0.0550 | 0.1250 | 0.1423 |
| digital | feature_02 | 0.1084 [WARN] | 0.4650 | 0.4750 | +0.0100 | 0.1503 | 0.1680 |
| directmail | feature_03 | 0.8483 **[ALERT]** | 0.3023 | 0.4346 | +0.1323 | 0.1446 | 0.1518 |
| directmail | feature_01 | 0.8375 **[ALERT]** | 0.3461 | 0.4526 | +0.1065 | 0.1025 | 0.1308 |
| directmail | feature_10 | 0.7268 **[ALERT]** | 0.4938 | 0.5727 | +0.0789 | 0.2331 | 0.2238 |
| directmail | feature_04 | 0.2539 **[ALERT]** | 0.3961 | 0.4569 | +0.0608 | 0.1239 | 0.1312 |
| directmail | feature_11 | 0.0705 | 0.4986 | 0.4720 | -0.0266 | 0.2325 | 0.2256 |

### D3. Feature–Score Correlation Drift — Top 5 (2025-05)

| Channel | Feature | Baseline Corr | Current Corr | Change |
|---|---|---|---|---|
| digital | feature_04 | 0.4939 | 0.6841 | +0.1902 |
| digital | feature_11 | 0.0250 | -0.1362 | -0.1612 |
| directmail | feature_02 | 0.2998 | 0.4544 | +0.1546 |
| directmail | feature_04 | 0.4790 | 0.6005 | +0.1215 |
| directmail | feature_01 | 0.7900 | 0.9089 | +0.1189 |

### D4. Population Mix — Source Segment (2025-05)

| Channel | Source | Accounts | % of Channel |
|---|---|---|---|
| digital | organic | 83 | 41.5% |
| digital | paid | 69 | 34.5% |
| digital | referral | 48 | 24.0% |
| directmail | organic | 74 | 37.0% |
| directmail | paid | 67 | 33.5% |
| directmail | referral | 59 | 29.5% |

### D5. Data Quality (2025-05)

| Channel | Missing Score Rate | Outlier Score Rate |
|---|---|---|
| digital | 0.000% | 0.000% |
| directmail | 0.000% | 0.000% |

---

## E. Flags and Observations

- [WARN]  digital: score PSI = 0.1201 — exceeds warning threshold 0.1
- [WARN]  digital: score KS = 0.1274 — exceeds threshold 0.1
- [WARN]  digital: 23.0% of accounts above baseline P90 (threshold 15.0%)
- [WARN]  digital: 18.0% of accounts above baseline P95 (threshold 15.0%)
- [ALERT] directmail: score PSI = 0.4461 — exceeds alert threshold 0.25
- [WARN]  directmail: score KS = 0.2603 — exceeds threshold 0.1
- [WARN]  directmail: 30.5% of accounts above baseline P90 (threshold 15.0%)
- [WARN]  directmail: 22.5% of accounts above baseline P95 (threshold 15.0%)
- [ALERT] Feature feature_01 (digital): PSI = 0.4405
- [WARN]  Feature feature_02 (digital): PSI = 0.1084
- [ALERT] Feature feature_03 (digital): PSI = 0.7228
- [WARN]  Feature feature_04 (digital): PSI = 0.2027
- [ALERT] Feature feature_10 (digital): PSI = 0.7117
- [INFO]  Null segment values replaced with 'Unknown' in: receivable_bucket, saleamount_bucket

---

*Report generated by the model_reporting framework · lfs · v1.0*