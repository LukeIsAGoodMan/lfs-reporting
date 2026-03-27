"""Mock data generators for v2 monitoring.

Builds a full-history score snapshot mart and performance mart from
the raw baseline_df + current_df produced by ``notebooks/mock_data.py``.

The score mart spans **all** months (2024-08 through 2025-05) so the
v2 runner can populate:

- baseline window (2024-08 – 2025-02)
- current cohort (2025-05)
- M3 cohort (2025-02)
- M6 cohort (2024-11)
- M9 cohort (2024-08)

The performance mart is keyed on ``(creditaccountid, score_month)`` and
contains sigmoid-derived binary outcomes for every account in the score
mart.
"""
from __future__ import annotations

from framework.utils import get_logger

logger = get_logger(__name__)


# ── Score Snapshot Mart ───────────────────────────────────────────────

def build_mock_score_mart(
    spark,
    baseline_df,
    current_df,
    model_version: str = "v1.0",
):
    """Build the v2 score snapshot mart from **both** baseline and current.

    The raw DataFrames come straight from ``generate_lfs_mock_data()``
    (schema: ``month``, ``lfs_score``, ``creditaccountid``, features, …).
    Neither needs to have been enriched by Layer 1.

    Steps:

    1. Union baseline + current into one DataFrame.
    2. Rename ``month`` → ``score_month``, ``lfs_score`` → ``score``.
    3. Add ``score_decile_static`` via ``ntile(10)`` per channel.
    4. Add v2-required columns: ``scorecard_id``,
       ``is_excluded_from_monitoring``, ``exclusion_reason``,
       ``model_version``, ``created_at``.
    5. Mark ~2 % of rows as excluded (for test coverage of the
       monitoring filter).

    Args:
        spark: Active SparkSession.
        baseline_df: Raw baseline-period DataFrame (months 2024-08 – 2025-02).
        current_df:  Raw current-period DataFrame  (months 2025-03 – 2025-05).
        model_version: Version string stamped on every row.

    Returns:
        A single DataFrame with every account from both periods.
    """
    from pyspark.sql import Window, functions as F

    # 1. Union all rows
    full = baseline_df.unionByName(current_df)

    # 2. Canonical column names
    df = (
        full
        .withColumnRenamed("month", "score_month")
        .withColumn("score", F.col("lfs_score"))
    )

    # 3. Static decile bins computed over the *full* dataset per channel
    #    so the baseline and current share the same bin edges.
    w = Window.partitionBy("channel").orderBy("score")
    df = df.withColumn("score_decile_static", F.ntile(10).over(w))

    # 4. V2-required columns
    df = (
        df
        .withColumn("scorecard_id", F.lit("SC001"))
        .withColumn("exclusion_reason", F.lit(None).cast("string"))
        .withColumn("model_version", F.lit(model_version))
        .withColumn("created_at", F.current_timestamp())
    )

    # 5. ~2 % excluded (deterministic: hash-based so it's reproducible)
    df = df.withColumn(
        "is_excluded_from_monitoring",
        (F.abs(F.hash("creditaccountid", "score_month")) % 50 == 0).cast("int"),
    )

    n = df.count()
    months = sorted([
        r["score_month"]
        for r in df.select("score_month").distinct().collect()
    ])
    n_excluded = df.filter(F.col("is_excluded_from_monitoring") == 1).count()

    print(f"Score mart: {n:,} rows across {len(months)} months")
    print(f"  Months: {months}")
    print(f"  Excluded from monitoring: {n_excluded:,} ({n_excluded / n:.1%})")
    logger.info(
        "Mock score mart: %d rows, %d months, %d excluded",
        n, len(months), n_excluded,
    )
    return df


# ── Performance Mart ──────────────────────────────────────────────────

def build_mock_perf_mart(
    spark,
    score_mart,
    seed: int = 42,
):
    """Generate v2 performance mart from the **full** score mart.

    Uses a sigmoid function of ``score`` to derive realistic bad-rate
    probabilities::

        P(bad | score) = sigmoid(score × 5 − 2.5)
        Score 0.30 → ~6 %  bad rate
        Score 0.50 → ~29 % bad rate
        Score 0.70 → ~67 % bad rate

    All maturity flags are set to 1 for the demo.  The M12 cohort will
    be skipped naturally by the runner because the aligned score_month
    (2024-05) does not exist in the data.

    Args:
        spark: Active SparkSession.
        score_mart: Full score snapshot mart (must have creditaccountid,
            score_month, score).
        seed: Random seed for ``F.rand()``.

    Returns:
        Performance mart with columns: creditaccountid, score_month,
        edr30_m3 … co_m12, bad30_m3 … badco_m12, is_mature_m3 … m12.
    """
    from pyspark.sql import functions as F

    score_col = "score" if "score" in score_mart.columns else "lfs_score"

    df = (
        score_mart
        .select("creditaccountid", "score_month", score_col)
        .withColumn(
            "_p_bad",
            F.lit(1.0)
            / (F.lit(1.0) + F.exp(-(F.col(score_col) * F.lit(5.0) - F.lit(2.5)))),
        )
        # Realistic maturity scaling: early windows have LOWER bad rates
        # than the final 12-month charge-off rate.
        #   M3  (EDR30) ≈ 15% of CO rate — very early signal
        #   M6  (EDR60) ≈ 35% of CO rate — intermediate
        #   M9  (EDR90) ≈ 55% of CO rate — approaching target
        #   M12 (CO)    = full charge-off probability
        .withColumn("_p_edr30", F.col("_p_bad") * F.lit(0.15))
        .withColumn("_p_edr60", F.col("_p_bad") * F.lit(0.35))
        .withColumn("_p_edr90", F.col("_p_bad") * F.lit(0.55))
        .withColumn("_p_co",    F.col("_p_bad"))
        # Binary outcomes
        .withColumn("edr30_m3",  (F.rand(seed=seed)     < F.col("_p_edr30")).cast("int"))
        .withColumn("edr60_m6",  (F.rand(seed=seed + 1) < F.col("_p_edr60")).cast("int"))
        .withColumn("edr90_m9",  (F.rand(seed=seed + 2) < F.col("_p_edr90")).cast("int"))
        .withColumn("co_m12",    (F.rand(seed=seed + 3) < F.col("_p_co")).cast("int"))
        # Bad flags
        .withColumn("bad30_m3",  F.col("edr30_m3"))
        .withColumn("bad60_m6",  F.col("edr60_m6"))
        .withColumn("bad90_m9",  F.col("edr90_m9"))
        .withColumn("badco_m12", F.col("co_m12"))
        # Maturity flags — all 1 for demo
        .withColumn("is_mature_m3",  F.lit(1))
        .withColumn("is_mature_m6",  F.lit(1))
        .withColumn("is_mature_m9",  F.lit(1))
        .withColumn("is_mature_m12", F.lit(1))
        # Cleanup
        .drop("_p_bad", "_p_edr30", "_p_edr60", "_p_edr90", "_p_co", score_col)
    )

    n = df.count()
    bad_n = df.filter(F.col("bad90_m9") == 1).count()
    months = df.select("score_month").distinct().count()
    print(f"Perf mart: {n:,} rows across {months} months  (M9 bad rate: {bad_n / n:.1%})")
    logger.info("Mock perf mart: %d rows, %d months", n, months)
    return df


# ── Scenario-Based V2 Mock Data Generator ────────────────────────────

AVAILABLE_SCENARIOS = {
    "healthy": "Stable distributions, monotonic EDR, adequate samples, M12 available",
    "drift_warning": "Moderate score/feature drift in current months, valid samples",
    "misrank_demo": "Intentional EDR ordering break in 0.40-0.50 bin",
    "insufficient_bads": "Too few bads for KS — demonstrates NA_INSUFFICIENT_BADS",
    "missing_m12": "M12 cohort unavailable — demonstrates calibration skip",
}


def _month_range(start: str, end: str) -> list[str]:
    """Generate YYYY-MM strings from start to end inclusive."""
    from datetime import date  # noqa: F811

    months: list[str] = []
    y, m = int(start[:4]), int(start[5:7])
    ey, em = int(end[:4]), int(end[5:7])
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def generate_v2_mock_data(
    spark,
    scenario: str = "healthy",
    reporting_month: str = "2025-05",
    seed: int = 42,
    n_per_month: int = 200,
):
    """Self-contained mock data generator with scenario presets.

    Args:
        spark: Active SparkSession.
        scenario: One of "healthy", "drift_warning", "misrank_demo",
            "insufficient_bads", "missing_m12".
        reporting_month: Reporting month in YYYY-MM format.
        seed: Random seed for reproducibility.
        n_per_month: Accounts per (month, channel) combination.

    Returns:
        (score_mart, perf_mart) tuple of DataFrames.
    """
    import math
    import random

    if scenario not in AVAILABLE_SCENARIOS:
        raise ValueError(
            f"Unknown scenario {scenario!r}. "
            f"Choose from: {sorted(AVAILABLE_SCENARIOS)}"
        )

    rng = random.Random(seed)

    # ── Override n_per_month for insufficient_bads ────────────────────
    if scenario == "insufficient_bads":
        n_per_month = 30

    # ── Month ranges ─────────────────────────────────────────────────
    baseline_months = _month_range("2024-08", "2025-02")   # 7 months
    current_months = _month_range("2025-03", "2025-05")    # 3 months
    m12_months = _month_range("2024-05", "2024-07")        # 3 extra months

    include_m12 = scenario != "missing_m12"

    if include_m12:
        months = m12_months + baseline_months + current_months
    else:
        months = baseline_months + current_months

    channels = ["digital", "directmail"]
    sources = ["organic", "paid", "referral"]
    lines = ["standard", "premium"]

    current_set = set(current_months)

    # ── Build all rows in Python ─────────────────────────────────────
    score_rows: list[dict] = []
    perf_rows: list[dict] = []

    for month_idx, month in enumerate(months):
        is_current = month in current_set
        for ch_idx, channel in enumerate(channels):
            for acct_idx in range(n_per_month):
                credit_id = month_idx * 10000 + ch_idx * n_per_month + acct_idx

                # ── Score generation ─────────────────────────────────
                base = rng.betavariate(2, 5)
                if channel == "directmail":
                    base += 0.03

                # Drift scenario: shift scores in current months
                if scenario == "drift_warning" and is_current:
                    base += 0.05

                score = max(0.001, min(0.999, base))

                # ── Feature generation ───────────────────────────────
                features: dict[str, float] = {}
                for f_idx in range(1, 12):
                    feat_val = score * 0.4 + rng.gauss(0.3, 0.12)
                    # Drift scenario: shift features 03 and 07
                    if scenario == "drift_warning" and is_current and f_idx in (3, 7):
                        feat_val += 0.10
                    features[f"feature_{f_idx:02d}"] = round(max(0.0, feat_val), 6)

                # ── Performance outcomes (sigmoid-based) ─────────────
                p_co = 1.0 / (1.0 + math.exp(-(score * 5.0 - 2.5)))
                p_edr30 = p_co * 0.15
                p_edr60 = p_co * 0.35
                p_edr90 = p_co * 0.55

                edr30_m3 = 1 if rng.random() < p_edr30 else 0
                edr60_m6 = 1 if rng.random() < p_edr60 else 0
                edr90_m9 = 1 if rng.random() < p_edr90 else 0
                co_m12 = 1 if rng.random() < p_co else 0

                # Misrank scenario: flip EDR outcomes in 0.40-0.50
                if scenario == "misrank_demo" and 0.40 <= score < 0.50:
                    if rng.random() < 0.7:
                        edr30_m3 = 0
                        edr60_m6 = 0
                        edr90_m9 = 0
                        co_m12 = 0

                # ── Exclusion flag (~2%) ─────────────────────────────
                is_excluded = 1 if rng.random() < 0.02 else 0

                # ── Build score row ──────────────────────────────────
                source = sources[acct_idx % len(sources)]
                line = lines[acct_idx % len(lines)]

                score_row = {
                    "creditaccountid": credit_id,
                    "customerid": credit_id + 1_000_000,
                    "score_month": month,
                    "channel": channel,
                    "source": source,
                    "line": line,
                    "active": 1,
                    "saleamount": round(rng.uniform(100.0, 5000.0), 2),
                    "salecount": rng.randint(1, 10),
                    "mob_pti": round(rng.uniform(0.05, 0.60), 4),
                    "endingreceivable": round(rng.uniform(500.0, 25000.0), 2),
                    "lfs_score": round(score, 6),
                    "score": round(score, 6),
                    "scorecard_id": "SC001",
                    "is_excluded_from_monitoring": is_excluded,
                    "model_version": "v1.0",
                    **features,
                }
                score_rows.append(score_row)

                # ── Build perf row ───────────────────────────────────
                perf_row = {
                    "creditaccountid": credit_id,
                    "score_month": month,
                    "edr30_m3": edr30_m3,
                    "edr60_m6": edr60_m6,
                    "edr90_m9": edr90_m9,
                    "co_m12": co_m12,
                    "bad30_m3": edr30_m3,
                    "bad60_m6": edr60_m6,
                    "bad90_m9": edr90_m9,
                    "badco_m12": co_m12,
                    "is_mature_m3": 1,
                    "is_mature_m6": 1,
                    "is_mature_m9": 1,
                    "is_mature_m12": 1,
                }
                perf_rows.append(perf_row)

    # ── Assign score_decile_static (Python sort, no Spark Window) ────
    score_rows.sort(key=lambda r: r["lfs_score"])
    n = len(score_rows)
    for i, row in enumerate(score_rows):
        row["score_decile_static"] = min(int(i * 10 / n) + 1, 10)

    # ── Create Spark DataFrames ──────────────────────────────────────
    score_mart = spark.createDataFrame(score_rows)
    perf_mart = spark.createDataFrame(perf_rows)

    # ── Summary ──────────────────────────────────────────────────────
    print(f"V2 Mock Data [{scenario}]")
    print(f"  Score mart: {len(score_rows):,} rows, {len(months)} months")
    print(f"  Perf mart:  {len(perf_rows):,} rows")
    print(f"  Channels:   {channels}")
    print(f"  M12 available: {'yes' if include_m12 else 'no'}")

    logger.info(
        "V2 mock [%s]: score=%d rows, perf=%d rows, months=%d, m12=%s",
        scenario, len(score_rows), len(perf_rows), len(months), include_m12,
    )

    return score_mart, perf_mart
