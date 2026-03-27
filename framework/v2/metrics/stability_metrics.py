"""Stability and data quality metrics.

Contains compute_stability, compute_data_quality, and private helpers.
Logic is identical to the original metrics.py — only the module location changed.
"""
from __future__ import annotations
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from framework.metrics import psi, missing_rate, outlier_rate
from framework.v2.config import MonitoringConfig
from framework.v2.thresholds import ThresholdEngine
from framework.utils import get_logger

logger = get_logger(__name__)


def compute_stability(
    current_df: DataFrame,
    baseline_df: DataFrame,
    config: MonitoringConfig,
    thresholds: ThresholdEngine,
    scorecard_id: str | None = None,
) -> dict:
    """Compute stability metrics (PSI/CSI) for score and features.

    These do NOT require actuals — computed on current vs baseline score distributions.

    Returns:
        {
            "score_psi": float,
            "score_psi_status": str,
            "feature_psi": [{feature_name, psi, status, baseline_mean, current_mean, mean_shift}],
            "channels": {channel: {"score_psi": float, ...}},
        }
    """
    score_col = config.score_col
    channels = config.channels
    channel_col = config.channel_col
    n_bins = config.psi_n_bins

    # Overall score PSI
    overall_psi = psi(baseline_df, current_df, score_col, n_bins)
    overall_status = thresholds.evaluate("psi", overall_psi, scorecard_id)

    # Per-channel
    channel_results = {}
    for ch in channels:
        base_ch = baseline_df.filter(F.col(channel_col) == ch)
        cur_ch = current_df.filter(F.col(channel_col) == ch)
        ch_psi = psi(base_ch, cur_ch, score_col, n_bins)
        ch_status = thresholds.evaluate("psi", ch_psi, scorecard_id)

        # Basic stats
        stats = cur_ch.agg(
            F.avg(score_col).alias("mean"),
            F.stddev(score_col).alias("std"),
            F.count("*").alias("n"),
        ).collect()[0]

        channel_results[ch] = {
            "score_psi": ch_psi,
            "score_psi_status": ch_status,
            "mean_score": float(stats["mean"]) if stats["mean"] else 0.0,
            "std_score": float(stats["std"]) if stats["std"] else 0.0,
            "account_count": int(stats["n"] or 0),
        }

    # Feature PSI
    feature_results = []
    for feat in config.feature_cols:
        feat_psi_val = psi(baseline_df, current_df, feat, n_bins)
        feat_status = thresholds.evaluate("feature_psi", feat_psi_val, scorecard_id)

        base_mean = _safe_mean(baseline_df, feat)
        cur_mean = _safe_mean(current_df, feat)

        feature_results.append({
            "feature_name": feat,
            "psi": feat_psi_val,
            "status": feat_status,
            "baseline_mean": base_mean,
            "current_mean": cur_mean,
            "mean_shift": cur_mean - base_mean if base_mean is not None and cur_mean is not None else None,
        })

    return {
        "score_psi": overall_psi,
        "score_psi_status": overall_status,
        "feature_psi": sorted(feature_results, key=lambda x: -(x["psi"] or 0)),
        "channels": channel_results,
    }


def compute_data_quality(
    current_df: DataFrame,
    config: MonitoringConfig,
) -> dict:
    """Compute data quality metrics: missing rates, outlier rates."""
    score_col = config.score_col
    channel_col = config.channel_col

    results = {"overall": {}, "channels": {}}

    # Overall
    mr = missing_rate(current_df, [score_col])
    score_mr = mr[0]["missing_rate"] if mr else 0.0
    score_or = outlier_rate(current_df, score_col, 0.0, 1.0)["outlier_rate"]
    results["overall"] = {
        "score_missing_rate": score_mr,
        "score_outlier_rate": score_or,
        "total_accounts": current_df.count(),
    }

    # Per channel
    for ch in config.channels:
        ch_df = current_df.filter(F.col(channel_col) == ch)
        mr_ch = missing_rate(ch_df, [score_col])
        or_ch = outlier_rate(ch_df, score_col, 0.0, 1.0)
        results["channels"][ch] = {
            "score_missing_rate": mr_ch[0]["missing_rate"] if mr_ch else 0.0,
            "score_outlier_rate": or_ch["outlier_rate"],
            "total_accounts": ch_df.count(),
        }

    # Feature-level
    feat_dq = []
    for feat in config.feature_cols:
        feat_mr = missing_rate(current_df, [feat])
        feat_dq.append({
            "feature_name": feat,
            "missing_rate": feat_mr[0]["missing_rate"] if feat_mr else 0.0,
        })
    results["feature_quality"] = feat_dq

    return results


# ── Private helpers ───────────────────────────────────────────────────

def compute_psi_table(
    current_df: DataFrame,
    baseline_df: DataFrame,
    score_col: str,
    intervals: list[tuple[float, float]],
) -> list[dict]:
    """Full PSI table with per-bucket detail.

    Args:
        current_df: Current period DataFrame.
        baseline_df: Baseline DataFrame.
        score_col: Score column name.
        intervals: List of (lower, upper) tuples defining bucket boundaries.
            Example: [(0.0, 0.1), (0.1, 0.2), ..., (0.9, 1.0)]

    Returns:
        List of dicts, one per interval, with keys:
        - interval, min_score, max_score, baseline_count, compare_count,
          baseline_pct, compare_pct, difference, obs_to_baseline_ratio,
          woe, contribution
    """
    import math

    EPSILON = 0.0001

    baseline_clean = baseline_df.filter(F.col(score_col).isNotNull())
    current_clean = current_df.filter(F.col(score_col).isNotNull())

    baseline_total = baseline_clean.count()
    current_total = current_clean.count()

    if baseline_total == 0 or current_total == 0:
        return []

    last_idx = len(intervals) - 1
    rows = []

    for i, (lower, upper) in enumerate(intervals):
        # Last bucket: inclusive on upper bound
        if i == last_idx:
            base_count = baseline_clean.filter(
                (F.col(score_col) >= lower) & (F.col(score_col) <= upper)
            ).count()
            cur_count = current_clean.filter(
                (F.col(score_col) >= lower) & (F.col(score_col) <= upper)
            ).count()
        else:
            base_count = baseline_clean.filter(
                (F.col(score_col) >= lower) & (F.col(score_col) < upper)
            ).count()
            cur_count = current_clean.filter(
                (F.col(score_col) >= lower) & (F.col(score_col) < upper)
            ).count()

        baseline_pct = max(base_count / baseline_total, EPSILON)
        compare_pct = max(cur_count / current_total, EPSILON)

        difference = compare_pct - baseline_pct
        obs_to_baseline_ratio = compare_pct / baseline_pct
        woe = math.log(compare_pct / baseline_pct)
        contribution = (compare_pct - baseline_pct) * woe

        rows.append({
            "interval": f"{lower:.2f}-{upper:.2f}",
            "min_score": lower,
            "max_score": upper,
            "baseline_count": base_count,
            "compare_count": cur_count,
            "baseline_pct": baseline_pct,
            "compare_pct": compare_pct,
            "difference": difference,
            "obs_to_baseline_ratio": obs_to_baseline_ratio,
            "woe": woe,
            "contribution": contribution,
        })

    return sorted(rows, key=lambda r: r["min_score"])


def compute_csi_table(
    current_df: DataFrame,
    baseline_df: DataFrame,
    feature_col: str,
    n_bins: int = 10,
) -> list[dict]:
    """Full CSI table for one feature with per-bucket detail.

    Bins are derived from baseline quantiles (deterministic, not qcut).

    Returns:
        List of dicts, one per bin:
        - feature, bin_index, interval, min_value, max_value,
          baseline_count, compare_count, baseline_pct, compare_pct,
          difference, information_value
    """
    import math

    EPSILON = 0.0001

    baseline_clean = baseline_df.filter(F.col(feature_col).isNotNull())
    current_clean = current_df.filter(F.col(feature_col).isNotNull())

    baseline_total = baseline_clean.count()
    current_total = current_clean.count()

    if baseline_total == 0 or current_total == 0:
        return []

    # Compute quantile edges from baseline
    fractions = [i / n_bins for i in range(1, n_bins)]
    edges = baseline_clean.stat.approxQuantile(feature_col, fractions, 0.01)

    if not edges:
        return []

    # Deduplicate edges and build intervals
    edges = sorted(set(edges))

    # Get min/max from baseline for boundary bins
    bounds = baseline_clean.agg(
        F.min(feature_col).alias("min_val"),
        F.max(feature_col).alias("max_val"),
    ).collect()[0]
    min_val = float(bounds["min_val"])
    max_val = float(bounds["max_val"])

    # Also consider current min/max so we don't miss observations
    cur_bounds = current_clean.agg(
        F.min(feature_col).alias("min_val"),
        F.max(feature_col).alias("max_val"),
    ).collect()[0]
    global_min = min(min_val, float(cur_bounds["min_val"]))
    global_max = max(max_val, float(cur_bounds["max_val"]))

    # Build interval boundaries
    boundaries = [global_min] + edges + [global_max]

    rows = []
    last_idx = len(boundaries) - 2  # number of intervals = len(boundaries) - 1

    for idx in range(len(boundaries) - 1):
        lower = boundaries[idx]
        upper = boundaries[idx + 1]

        # Last bucket: inclusive on upper
        if idx == last_idx:
            base_count = baseline_clean.filter(
                (F.col(feature_col) >= lower) & (F.col(feature_col) <= upper)
            ).count()
            cur_count = current_clean.filter(
                (F.col(feature_col) >= lower) & (F.col(feature_col) <= upper)
            ).count()
        else:
            base_count = baseline_clean.filter(
                (F.col(feature_col) >= lower) & (F.col(feature_col) < upper)
            ).count()
            cur_count = current_clean.filter(
                (F.col(feature_col) >= lower) & (F.col(feature_col) < upper)
            ).count()

        baseline_pct = max(base_count / baseline_total, EPSILON)
        compare_pct = max(cur_count / current_total, EPSILON)

        difference = compare_pct - baseline_pct
        iv = (compare_pct - baseline_pct) * math.log(compare_pct / baseline_pct)

        rows.append({
            "feature": feature_col,
            "bin_index": idx,
            "interval": f"{lower:.4f}-{upper:.4f}",
            "min_value": lower,
            "max_value": upper,
            "baseline_count": base_count,
            "compare_count": cur_count,
            "baseline_pct": baseline_pct,
            "compare_pct": compare_pct,
            "difference": difference,
            "information_value": iv,
        })

    return sorted(rows, key=lambda r: r["bin_index"])


def _safe_mean(df: DataFrame, col: str) -> float | None:
    """Safely compute mean of a column."""
    row = df.agg(F.avg(col).alias("m")).collect()[0]
    return float(row["m"]) if row["m"] is not None else None
