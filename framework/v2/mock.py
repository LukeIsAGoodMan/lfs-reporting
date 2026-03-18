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
