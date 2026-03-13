"""Layer 3: Model monitoring tables.

Compares current-period data against the baseline window to produce
six monitoring outputs:

- **score_drift** — score distribution statistics, PSI, KS, quantile
  cutoff drift, and baseline-threshold exceedance rates per
  (vintage_month, channel).
- **feature_psi** — PSI, mean shift, and std shift per feature per
  (vintage_month, channel, feature_name).
- **data_quality** — missing and outlier rates for the score column
  per (vintage_month, channel).
- **feature_quality** — missing and outlier rates per feature per
  (vintage_month, channel, feature_name).  Outlier bounds are derived
  from the baseline p1/p99 per feature × channel, not hard-coded.
- **feature_score_relationship** — Pearson correlation between each
  feature and the score, compared against the baseline correlation.
- **population_mix** — account distribution across configured
  segments per (vintage_month, channel, segment_type, segment_value).

All table names come from the model config.  Channel is kept as a
column — tables are NOT physically split by channel.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from framework.config import ModelConfig
from framework.metrics import (
    baseline_outlier_bounds,
    dynamic_cutoffs,
    ks_statistic,
    missing_rate,
    outlier_rate,
    pearson_corr,
    psi,
)
from framework.utils import get_logger

logger = get_logger(__name__)


def build_layer3(
    current_df: DataFrame,
    baseline_df: DataFrame,
    config: ModelConfig,
    actuals_df: DataFrame | None = None,
) -> dict[str, DataFrame]:
    """Build all Layer 3 monitoring tables.

    Args:
        current_df: Enriched Layer 1 output for the current score_month.
        baseline_df: Raw source data for the baseline window (pre-
            enrichment — only needs the score and feature columns).
        config: Model configuration.
        actuals_df: Optional actuals DataFrame with columns
            (creditaccountid, vintage_month, is_bad, edr30, edr60, edr90).
            Required to produce the performance and calibration tables.

    Returns:
        ``{output_table_name: DataFrame}``
    """
    l3_cfg = config.layer3
    score_col = l3_cfg["score_col"]
    channel_col = l3_cfg.get("channel_col", config.source.get("channel_col", "channel"))
    channels = config.source.get("channels", [])
    tables_cfg = l3_cfg["tables"]

    spark = current_df.sparkSession

    results: dict[str, DataFrame] = {}

    # ── score_drift ──────────────────────────────────────────────────
    if "score_drift" in tables_cfg:
        cfg = tables_cfg["score_drift"]
        output_table = cfg["output_table"]
        logger.info("Building %s", output_table)
        df = _build_score_drift(
            current_df, baseline_df, spark,
            score_col=score_col,
            channel_col=channel_col,
            channels=channels,
            psi_n_bins=cfg.get("psi_n_bins", 10),
            quantiles=cfg.get("quantiles", [0.50, 0.80, 0.90, 0.95]),
            top_pct_means=cfg.get("top_pct_means", [0.10, 0.05]),
            baseline_threshold_quantiles=cfg.get(
                "baseline_threshold_quantiles", [0.90, 0.95]
            ),
        )
        results[output_table] = df

    # ── feature_psi ──────────────────────────────────────────────────
    if "feature_psi" in tables_cfg:
        cfg = tables_cfg["feature_psi"]
        output_table = cfg["output_table"]
        logger.info("Building %s", output_table)
        df = _build_feature_psi(
            current_df, baseline_df, spark,
            feature_cols=cfg["columns"],
            channel_col=channel_col,
            channels=channels,
            n_bins=cfg.get("n_bins", 10),
        )
        results[output_table] = df

    # ── data_quality ─────────────────────────────────────────────────
    if "data_quality" in tables_cfg:
        cfg = tables_cfg["data_quality"]
        output_table = cfg["output_table"]
        logger.info("Building %s", output_table)
        bounds = cfg.get("score_outlier_bounds", {"lower": 0.0, "upper": 1.0})
        df = _build_data_quality(
            current_df, spark,
            score_col=score_col,
            channel_col=channel_col,
            channels=channels,
            outlier_lower=bounds["lower"],
            outlier_upper=bounds["upper"],
        )
        results[output_table] = df

    # ── feature_quality ──────────────────────────────────────────────
    if "feature_quality" in tables_cfg:
        cfg = tables_cfg["feature_quality"]
        output_table = cfg["output_table"]
        logger.info("Building %s", output_table)
        outlier_qs = cfg.get("outlier_quantiles", [0.01, 0.99])
        df = _build_feature_quality(
            current_df, baseline_df, spark,
            feature_cols=cfg["columns"],
            channel_col=channel_col,
            channels=channels,
            outlier_lower_q=outlier_qs[0],
            outlier_upper_q=outlier_qs[1],
        )
        results[output_table] = df

    # ── feature_score_relationship ───────────────────────────────────
    if "feature_score_relationship" in tables_cfg:
        cfg = tables_cfg["feature_score_relationship"]
        output_table = cfg["output_table"]
        logger.info("Building %s", output_table)
        df = _build_feature_score_relationship(
            current_df, baseline_df, spark,
            score_col=score_col,
            feature_cols=cfg["columns"],
            channel_col=channel_col,
            channels=channels,
        )
        results[output_table] = df

    # ── population_mix ───────────────────────────────────────────────
    if "population_mix" in tables_cfg:
        cfg = tables_cfg["population_mix"]
        output_table = cfg["output_table"]
        logger.info("Building %s", output_table)
        df = _build_population_mix(
            current_df, spark,
            segments=cfg["segments"],
            channel_col=channel_col,
            channels=channels,
        )
        results[output_table] = df

    # ── performance ──────────────────────────────────────────────────
    if "performance" in tables_cfg and actuals_df is not None:
        cfg = tables_cfg["performance"]
        output_table = cfg["output_table"]
        logger.info("Building %s", output_table)
        df = _build_performance(
            current_df, actuals_df, spark,
            score_col=score_col,
            channel_col=channel_col,
            channels=channels,
        )
        results[output_table] = df

    # ── calibration ──────────────────────────────────────────────────
    if "calibration" in tables_cfg and actuals_df is not None:
        cfg = tables_cfg["calibration"]
        output_table = cfg["output_table"]
        logger.info("Building %s", output_table)
        df = _build_calibration(
            current_df, actuals_df, spark,
            score_col=score_col,
            channel_col=channel_col,
            channels=channels,
            score_bin_col=cfg.get("score_bin_col", "lfs_decile_static"),
        )
        results[output_table] = df

    logger.info("Layer 3 complete — %d tables produced", len(results))
    return results


# ── Private builders ─────────────────────────────────────────────────

def _build_score_drift(
    current_df: DataFrame,
    baseline_df: DataFrame,
    spark: SparkSession,
    *,
    score_col: str,
    channel_col: str,
    channels: list[str],
    psi_n_bins: int,
    quantiles: list[float],
    top_pct_means: list[float],
    baseline_threshold_quantiles: list[float],
) -> DataFrame:
    """Score drift table: one row per (vintage_month, channel).

    Includes mean, std, quantile cutoffs, top-pct means, PSI, KS,
    and pct_accounts_above_pXX_baseline columns.
    """
    vintage_months = [
        row["vintage_month"]
        for row in current_df.select("vintage_month").distinct().collect()
    ]

    # Pre-compute baseline thresholds per channel (small lookup).
    baseline_thresholds: dict[str, dict[str, float]] = {}
    for ch in channels:
        base_slice = baseline_df.filter(F.col(channel_col) == ch)
        base_clean = base_slice.filter(F.col(score_col).isNotNull())
        if base_clean.count() > 0:
            vals = base_clean.stat.approxQuantile(
                score_col, baseline_threshold_quantiles, 0.01
            )
            baseline_thresholds[ch] = {
                f"p{int(q * 100)}": v
                for q, v in zip(baseline_threshold_quantiles, vals)
            }
        else:
            baseline_thresholds[ch] = {
                f"p{int(q * 100)}": 0.0 for q in baseline_threshold_quantiles
            }

    rows: list[dict] = []
    for vm in vintage_months:
        for ch in channels:
            cur_slice = current_df.filter(
                (F.col("vintage_month") == vm) & (F.col(channel_col) == ch)
            )
            base_slice = baseline_df.filter(F.col(channel_col) == ch)

            # Basic stats.
            stats_row = cur_slice.agg(
                F.avg(score_col).alias("mean"),
                F.stddev(score_col).alias("std"),
            ).collect()[0]

            # Cutoffs.
            cuts = dynamic_cutoffs(cur_slice, score_col, quantiles, top_pct_means)

            # PSI and KS vs baseline.
            score_psi = psi(base_slice, cur_slice, score_col, psi_n_bins)
            score_ks = ks_statistic(base_slice, cur_slice, score_col)

            row: dict = {
                "vintage_month": vm,
                "channel": ch,
                "mean_lfs_score": _to_float(stats_row["mean"]),
                "std_lfs_score": _to_float(stats_row["std"]),
                "score_psi": score_psi,
                "score_ks": score_ks,
            }

            # Quantile columns.
            for q in quantiles:
                key = f"q{int(q * 100)}"
                row[f"{key}_score"] = cuts.get(key, 0.0)

            # Top-pct mean columns.
            for pct in top_pct_means:
                key = f"mean_top{int(pct * 100)}pct"
                row[f"{key}_score"] = cuts.get(key, 0.0)

            # Baseline threshold exceedance rates.
            cur_clean = cur_slice.filter(F.col(score_col).isNotNull())
            cur_count = cur_clean.count()
            ch_thresholds = baseline_thresholds.get(ch, {})
            for q in baseline_threshold_quantiles:
                pkey = f"p{int(q * 100)}"
                threshold_val = ch_thresholds.get(pkey, 0.0)
                if cur_count > 0:
                    above_count = cur_clean.filter(
                        F.col(score_col) > threshold_val
                    ).count()
                    row[f"pct_accounts_above_{pkey}_baseline"] = above_count / cur_count
                else:
                    row[f"pct_accounts_above_{pkey}_baseline"] = 0.0

            rows.append(row)

    if not rows:
        return _empty_score_drift_df(
            spark, quantiles, top_pct_means, baseline_threshold_quantiles,
        )

    return spark.createDataFrame(rows)


def _build_feature_psi(
    current_df: DataFrame,
    baseline_df: DataFrame,
    spark: SparkSession,
    *,
    feature_cols: list[str],
    channel_col: str,
    channels: list[str],
    n_bins: int,
) -> DataFrame:
    """Feature PSI table: one row per (vintage_month, channel, feature_name).

    Columns: vintage_month, channel, feature_name, psi,
    baseline_mean, current_mean, baseline_std, current_std.
    """
    vintage_months = [
        row["vintage_month"]
        for row in current_df.select("vintage_month").distinct().collect()
    ]

    rows: list[dict] = []
    for vm in vintage_months:
        for ch in channels:
            cur_slice = current_df.filter(
                (F.col("vintage_month") == vm) & (F.col(channel_col) == ch)
            )
            base_slice = baseline_df.filter(F.col(channel_col) == ch)

            # Compute mean + std for all features in one pass each.
            base_stats_row = base_slice.agg(
                *[F.avg(c).alias(f"{c}_mean") for c in feature_cols],
                *[F.stddev(c).alias(f"{c}_std") for c in feature_cols],
            ).collect()[0]
            cur_stats_row = cur_slice.agg(
                *[F.avg(c).alias(f"{c}_mean") for c in feature_cols],
                *[F.stddev(c).alias(f"{c}_std") for c in feature_cols],
            ).collect()[0]

            for feat in feature_cols:
                feat_psi = psi(base_slice, cur_slice, feat, n_bins)
                rows.append({
                    "vintage_month": vm,
                    "channel": ch,
                    "feature_name": feat,
                    "psi": feat_psi,
                    "baseline_mean": _to_float(base_stats_row[f"{feat}_mean"]),
                    "current_mean": _to_float(cur_stats_row[f"{feat}_mean"]),
                    "baseline_std": _to_float(base_stats_row[f"{feat}_std"]),
                    "current_std": _to_float(cur_stats_row[f"{feat}_std"]),
                })

    if not rows:
        schema = StructType([
            StructField("vintage_month", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("feature_name", StringType(), True),
            StructField("psi", DoubleType(), True),
            StructField("baseline_mean", DoubleType(), True),
            StructField("current_mean", DoubleType(), True),
            StructField("baseline_std", DoubleType(), True),
            StructField("current_std", DoubleType(), True),
        ])
        return spark.createDataFrame([], schema)

    return spark.createDataFrame(rows)


def _build_data_quality(
    current_df: DataFrame,
    spark: SparkSession,
    *,
    score_col: str,
    channel_col: str,
    channels: list[str],
    outlier_lower: float,
    outlier_upper: float,
) -> DataFrame:
    """Data quality table: one row per (vintage_month, channel).

    Columns: vintage_month, channel, missing_score_rate, outlier_score_rate.

    Score outlier bounds are fixed [0, 1] (probability range).
    """
    vintage_months = [
        row["vintage_month"]
        for row in current_df.select("vintage_month").distinct().collect()
    ]

    rows: list[dict] = []
    for vm in vintage_months:
        for ch in channels:
            ch_df = current_df.filter(
                (F.col("vintage_month") == vm) & (F.col(channel_col) == ch)
            )

            mr = missing_rate(ch_df, [score_col])
            orl = outlier_rate(ch_df, score_col, outlier_lower, outlier_upper)

            rows.append({
                "vintage_month": vm,
                "channel": ch,
                "missing_score_rate": mr[0]["missing_rate"] if mr else 0.0,
                "outlier_score_rate": orl["outlier_rate"],
            })

    if not rows:
        schema = StructType([
            StructField("vintage_month", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("missing_score_rate", DoubleType(), True),
            StructField("outlier_score_rate", DoubleType(), True),
        ])
        return spark.createDataFrame([], schema)

    return spark.createDataFrame(rows)


def _build_feature_quality(
    current_df: DataFrame,
    baseline_df: DataFrame,
    spark: SparkSession,
    *,
    feature_cols: list[str],
    channel_col: str,
    channels: list[str],
    outlier_lower_q: float,
    outlier_upper_q: float,
) -> DataFrame:
    """Feature quality table: one row per (vintage_month, channel, feature_name).

    Outlier bounds are derived from the baseline window's p1/p99 per
    feature × channel, so each feature gets its own natural range.

    Columns: vintage_month, channel, feature_name, missing_rate,
    outlier_rate, outlier_lower_bound, outlier_upper_bound.
    """
    vintage_months = [
        row["vintage_month"]
        for row in current_df.select("vintage_month").distinct().collect()
    ]

    # Pre-compute baseline bounds per (channel, feature).
    bounds_cache: dict[tuple[str, str], tuple[float, float]] = {}
    for ch in channels:
        base_ch = baseline_df.filter(F.col(channel_col) == ch)
        for feat in feature_cols:
            bounds_cache[(ch, feat)] = baseline_outlier_bounds(
                base_ch, feat, outlier_lower_q, outlier_upper_q,
            )

    rows: list[dict] = []
    for vm in vintage_months:
        for ch in channels:
            ch_df = current_df.filter(
                (F.col("vintage_month") == vm) & (F.col(channel_col) == ch)
            )

            # Missing rates for all features in one pass.
            mr_results = missing_rate(ch_df, feature_cols)
            mr_by_col = {r["column_name"]: r["missing_rate"] for r in mr_results}

            for feat in feature_cols:
                lower_b, upper_b = bounds_cache[(ch, feat)]
                orl = outlier_rate(ch_df, feat, lower_b, upper_b)
                rows.append({
                    "vintage_month": vm,
                    "channel": ch,
                    "feature_name": feat,
                    "missing_rate": mr_by_col.get(feat, 0.0),
                    "outlier_rate": orl["outlier_rate"],
                    "outlier_lower_bound": lower_b,
                    "outlier_upper_bound": upper_b,
                })

    if not rows:
        schema = StructType([
            StructField("vintage_month", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("feature_name", StringType(), True),
            StructField("missing_rate", DoubleType(), True),
            StructField("outlier_rate", DoubleType(), True),
            StructField("outlier_lower_bound", DoubleType(), True),
            StructField("outlier_upper_bound", DoubleType(), True),
        ])
        return spark.createDataFrame([], schema)

    return spark.createDataFrame(rows)


_NUMERIC_TYPES = {"byte", "short", "int", "long", "float", "double", "decimal"}


def _numeric_feature_cols(df: DataFrame, feature_cols: list[str]) -> list[str]:
    """Filter *feature_cols* to those with a numeric dtype in *df*."""
    schema_map = {f.name: f.dataType.simpleString().split("(")[0]
                  for f in df.schema.fields}
    return [c for c in feature_cols if schema_map.get(c, "") in _NUMERIC_TYPES]


def _build_feature_score_relationship(
    current_df: DataFrame,
    baseline_df: DataFrame,
    spark: SparkSession,
    *,
    score_col: str,
    feature_cols: list[str],
    channel_col: str,
    channels: list[str],
) -> DataFrame:
    """Feature–score relationship drift: one row per
    (vintage_month, channel, feature_name).

    Computes Pearson correlation between each feature and the score
    for both the baseline and the current period.  Works for both
    tree-based and linear models.

    Only numeric features are included.  Non-numeric features in the
    config list are silently skipped.

    Columns: vintage_month, channel, feature_name,
    baseline_corr, current_corr, corr_change.

    ``baseline_corr`` and ``current_corr`` are ``None`` when
    correlation cannot be computed (insufficient data or zero variance).
    ``corr_change`` is ``None`` when either side is ``None``.
    """
    # Filter to numeric features using the current df schema.
    numeric_feats = _numeric_feature_cols(current_df, feature_cols)
    skipped = set(feature_cols) - set(numeric_feats)
    if skipped:
        logger.warning(
            "Skipping non-numeric features for correlation: %s", sorted(skipped),
        )

    vintage_months = [
        row["vintage_month"]
        for row in current_df.select("vintage_month").distinct().collect()
    ]

    # Pre-compute baseline correlations per (channel, feature).
    baseline_corrs: dict[tuple[str, str], float | None] = {}
    for ch in channels:
        base_ch = baseline_df.filter(F.col(channel_col) == ch)
        for feat in numeric_feats:
            baseline_corrs[(ch, feat)] = pearson_corr(base_ch, feat, score_col)

    rows: list[dict] = []
    for vm in vintage_months:
        for ch in channels:
            cur_slice = current_df.filter(
                (F.col("vintage_month") == vm) & (F.col(channel_col) == ch)
            )
            for feat in numeric_feats:
                b_corr = baseline_corrs[(ch, feat)]
                c_corr = pearson_corr(cur_slice, feat, score_col)
                corr_change = (
                    (c_corr - b_corr)
                    if b_corr is not None and c_corr is not None
                    else None
                )
                rows.append({
                    "vintage_month": vm,
                    "channel": ch,
                    "feature_name": feat,
                    "baseline_corr": b_corr,
                    "current_corr": c_corr,
                    "corr_change": corr_change,
                })

    if not rows:
        schema = StructType([
            StructField("vintage_month", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("feature_name", StringType(), True),
            StructField("baseline_corr", DoubleType(), True),
            StructField("current_corr", DoubleType(), True),
            StructField("corr_change", DoubleType(), True),
        ])
        return spark.createDataFrame([], schema)

    return spark.createDataFrame(rows)


def _build_population_mix(
    current_df: DataFrame,
    spark: SparkSession,
    *,
    segments: list[dict],
    channel_col: str,
    channels: list[str],
) -> DataFrame:
    """Population mix table: one row per
    (vintage_month, channel, segment_type, segment_value).

    Monitors shifts in population composition across configured
    segment dimensions.

    Columns: vintage_month, channel, segment_type, segment_value,
    account_count, pct_of_channel_accounts.
    """
    vintage_months = [
        row["vintage_month"]
        for row in current_df.select("vintage_month").distinct().collect()
    ]

    rows: list[dict] = []
    for vm in vintage_months:
        for ch in channels:
            ch_df = current_df.filter(
                (F.col("vintage_month") == vm) & (F.col(channel_col) == ch)
            )
            ch_total = ch_df.count()

            for seg_spec in segments:
                seg_col = seg_spec["column"]
                seg_type = seg_spec["segment_type"]

                # Cast segment column to string for consistent output type,
                # then group and count.
                seg_df = ch_df.withColumn(
                    "_seg_val",
                    F.coalesce(F.col(seg_col).cast("string"), F.lit("Unknown")),
                )
                seg_counts = (
                    seg_df.groupBy("_seg_val")
                    .agg(F.count("*").alias("cnt"))
                    .collect()
                )

                for seg_row in seg_counts:
                    seg_val = seg_row["_seg_val"]  # already string
                    cnt = seg_row["cnt"]
                    rows.append({
                        "vintage_month": vm,
                        "channel": ch,
                        "segment_type": seg_type,
                        "segment_value": seg_val,
                        "account_count": cnt,
                        "pct_of_channel_accounts": (
                            cnt / ch_total if ch_total > 0 else 0.0
                        ),
                    })

    if not rows:
        schema = StructType([
            StructField("vintage_month", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("segment_type", StringType(), True),
            StructField("segment_value", StringType(), True),
            StructField("account_count", LongType(), True),
            StructField("pct_of_channel_accounts", DoubleType(), True),
        ])
        return spark.createDataFrame([], schema)

    return spark.createDataFrame(rows)


def _build_performance(
    current_df: DataFrame,
    actuals_df: DataFrame,
    spark: SparkSession,
    *,
    score_col: str,
    channel_col: str,
    channels: list[str],
) -> DataFrame:
    """Performance monitoring table: one row per (vintage_month, channel).

    Joins enriched Layer 1 output with an actuals DataFrame on
    (creditaccountid, vintage_month) and aggregates bad rates and EDR metrics.

    Actuals schema expected: creditaccountid, vintage_month, is_bad INT,
    edr30 INT, edr60 INT, edr90 INT.
    """
    actuals_slim = actuals_df.select(
        "creditaccountid", "vintage_month",
        "is_bad", "edr30", "edr60", "edr90",
    )
    joined = current_df.join(
        actuals_slim, on=["creditaccountid", "vintage_month"], how="left",
    )
    return (
        joined
        .groupBy("vintage_month", channel_col)
        .agg(
            F.count("*").alias("account_count"),
            F.avg(score_col).alias("avg_lfs_score"),
            F.avg("is_bad").alias("actual_bad_rate"),
            F.avg("edr30").alias("edr30"),
            F.avg("edr60").alias("edr60"),
            F.avg("edr90").alias("edr90"),
        )
        .withColumn("predicted_bad_rate", F.col("avg_lfs_score"))
        .withColumn(
            "calibration_gap",
            F.col("actual_bad_rate") - F.col("avg_lfs_score"),
        )
        .orderBy("vintage_month", channel_col)
    )


def _build_calibration(
    current_df: DataFrame,
    actuals_df: DataFrame,
    spark: SparkSession,
    *,
    score_col: str,
    channel_col: str,
    channels: list[str],
    score_bin_col: str = "lfs_decile_static",
) -> DataFrame:
    """Calibration table: one row per (vintage_month, channel, score_bin).

    Aggregates predicted rate (avg lfs_score) vs actual bad rate per static
    score decile bin.  The calibration_gap = actual_rate − predicted_rate.
    """
    actuals_slim = actuals_df.select(
        "creditaccountid", "vintage_month", "is_bad",
    )
    joined = current_df.join(
        actuals_slim, on=["creditaccountid", "vintage_month"], how="left",
    )
    return (
        joined
        .groupBy("vintage_month", channel_col, score_bin_col)
        .agg(
            F.count("*").alias("account_count"),
            F.avg(score_col).alias("predicted_rate"),
            F.avg("is_bad").alias("actual_rate"),
        )
        .withColumn(
            "calibration_gap",
            F.col("actual_rate") - F.col("predicted_rate"),
        )
        .withColumnRenamed(score_bin_col, "score_bin")
        .orderBy("vintage_month", channel_col, "score_bin")
    )


# ── Helpers ──────────────────────────────────────────────────────────

def _to_float(val) -> float:
    """Safely convert a possibly-None value to float."""
    return float(val) if val is not None else 0.0


def _empty_score_drift_df(
    spark: SparkSession,
    quantiles: list[float],
    top_pct_means: list[float],
    baseline_threshold_quantiles: list[float],
) -> DataFrame:
    """Return an empty DataFrame with the score_drift schema."""
    fields = [
        StructField("vintage_month", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("mean_lfs_score", DoubleType(), True),
        StructField("std_lfs_score", DoubleType(), True),
        StructField("score_psi", DoubleType(), True),
        StructField("score_ks", DoubleType(), True),
    ]
    for q in quantiles:
        fields.append(StructField(f"q{int(q * 100)}_score", DoubleType(), True))
    for pct in top_pct_means:
        fields.append(
            StructField(f"mean_top{int(pct * 100)}pct_score", DoubleType(), True)
        )
    for q in baseline_threshold_quantiles:
        fields.append(
            StructField(
                f"pct_accounts_above_p{int(q * 100)}_baseline", DoubleType(), True
            )
        )
    return spark.createDataFrame([], StructType(fields))
