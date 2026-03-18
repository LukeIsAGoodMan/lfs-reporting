"""Mock data generators for v2 monitoring.

Converts existing mock data (from notebooks/mock_data.py) into v2 format:
score_snapshot_mart and performance_mart.
"""
from __future__ import annotations
from framework.utils import get_logger

logger = get_logger(__name__)


def build_mock_score_mart(spark, enriched_df, model_version: str = "v1.0"):
    """Convert enriched Layer 1 output to v2 score snapshot mart.

    Adds: scorecard_id, is_excluded_from_monitoring, exclusion_reason,
    model_version, created_at.  Renames month -> score_month, lfs_score -> score.
    """
    from pyspark.sql import functions as F

    has_score_month = "score_month" in enriched_df.columns
    has_vintage_month = "vintage_month" in enriched_df.columns

    df = enriched_df

    # Map month/vintage_month -> score_month if needed
    if not has_score_month:
        if has_vintage_month:
            df = df.withColumnRenamed("vintage_month", "score_month")
        elif "month" in df.columns:
            df = df.withColumnRenamed("month", "score_month")

    # Map lfs_score -> score if needed
    if "score" not in df.columns and "lfs_score" in df.columns:
        df = df.withColumn("score", F.col("lfs_score"))

    # Add v2-required columns
    df = (
        df
        .withColumn("scorecard_id", F.lit("SC001"))
        .withColumn("is_excluded_from_monitoring", F.lit(0))
        .withColumn("exclusion_reason", F.lit(None).cast("string"))
        .withColumn("model_version", F.lit(model_version))
        .withColumn("created_at", F.current_timestamp())
    )

    # Ensure score_decile_static exists
    if "score_decile_static" not in df.columns and "lfs_decile_static" in df.columns:
        df = df.withColumn("score_decile_static", F.col("lfs_decile_static"))

    n = df.count()
    months = df.select("score_month").distinct().count()
    logger.info("Mock score mart: %d accounts across %d months", n, months)
    return df


def build_mock_perf_mart(spark, enriched_df, seed: int = 42):
    """Generate v2 performance mart with maturity-aligned outcomes.

    Uses a sigmoid function of score to generate realistic bad rates:
        P(bad | score) = sigmoid(score * 5 - 2.5)

    Generates maturity-aligned columns:
        edr30_m3, edr60_m6, edr90_m9, co_m12
        bad30_m3, bad60_m6, bad90_m9, badco_m12
        is_mature_m3, is_mature_m6, is_mature_m9, is_mature_m12

    Args:
        spark: Active SparkSession.
        enriched_df: Layer 1 enriched output (needs creditaccountid,
            vintage_month/score_month, lfs_score/score).
        seed: Random seed for reproducibility.
    """
    from pyspark.sql import functions as F

    score_col = "lfs_score" if "lfs_score" in enriched_df.columns else "score"
    month_col = "vintage_month" if "vintage_month" in enriched_df.columns else "score_month"

    # Compute P(bad) per account using sigmoid
    df = (
        enriched_df
        .select("creditaccountid", F.col(month_col).alias("score_month"), score_col)
        .withColumn(
            "_p_bad",
            F.lit(1.0) / (F.lit(1.0) + F.exp(-(F.col(score_col) * F.lit(5.0) - F.lit(2.5)))),
        )
    )

    # Generate outcomes at different maturity thresholds
    # M3: more lenient (higher rate) — captures early delinquency
    # M12: stricter — captures charge-offs
    df = (
        df
        .withColumn("_p_edr30", F.least(F.col("_p_bad") * F.lit(1.5), F.lit(0.95)))
        .withColumn("_p_edr60", F.least(F.col("_p_bad") * F.lit(1.3), F.lit(0.95)))
        .withColumn("_p_edr90", F.col("_p_bad"))
        .withColumn("_p_co", F.col("_p_bad") * F.lit(0.7))  # CO rate lower than EDR90

        # Generate binary outcomes
        .withColumn("edr30_m3",  (F.rand(seed=seed)     < F.col("_p_edr30")).cast("int"))
        .withColumn("edr60_m6",  (F.rand(seed=seed + 1) < F.col("_p_edr60")).cast("int"))
        .withColumn("edr90_m9",  (F.rand(seed=seed + 2) < F.col("_p_edr90")).cast("int"))
        .withColumn("co_m12",    (F.rand(seed=seed + 3) < F.col("_p_co")).cast("int"))

        # Bad flags (same as EDR for simplicity in demo)
        .withColumn("bad30_m3",  F.col("edr30_m3"))
        .withColumn("bad60_m6",  F.col("edr60_m6"))
        .withColumn("bad90_m9",  F.col("edr90_m9"))
        .withColumn("badco_m12", F.col("co_m12"))

        # Maturity flags: all mature for demo (would be based on calendar logic in prod)
        .withColumn("is_mature_m3",  F.lit(1))
        .withColumn("is_mature_m6",  F.lit(1))
        .withColumn("is_mature_m9",  F.lit(1))
        .withColumn("is_mature_m12", F.lit(1))

        # Cleanup
        .drop("_p_bad", "_p_edr30", "_p_edr60", "_p_edr90", "_p_co", score_col)
    )

    n = df.count()
    bad_n = df.filter(F.col("bad90_m9") == 1).count()
    print(f"Mock perf mart: {n:,} rows (M9 bad rate: {bad_n / n:.1%})")
    logger.info("Mock perf mart: %d rows", n)
    return df
