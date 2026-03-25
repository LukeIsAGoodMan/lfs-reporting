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

def _safe_mean(df: DataFrame, col: str) -> float | None:
    """Safely compute mean of a column."""
    row = df.agg(F.avg(col).alias("m")).collect()[0]
    return float(row["m"]) if row["m"] is not None else None
