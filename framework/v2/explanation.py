"""Optional explanation layer for strategy attribution.

Gated by config: if explanation_config.enabled is False, all functions
return None immediately.
"""
from __future__ import annotations
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from framework.v2.config import MonitoringConfig
from framework.utils import get_logger

logger = get_logger(__name__)


def compute_explanation(
    current_df: DataFrame,
    baseline_df: DataFrame,
    config: MonitoringConfig,
    strategy_df: DataFrame | None = None,
) -> dict | None:
    """Compute explanation/attribution if enabled.

    Returns None if explanation is disabled.
    If enabled, returns:
        {
            "mix_shift": [...],
            "within_band_deterioration": [...],
            "attribution_summary": str,
        }
    """
    if not config.explanation_enabled:
        logger.debug("Explanation layer disabled — skipping")
        return None

    score_col = config.score_col

    # Mix shift analysis: how has the population shifted across score bands?
    bin_col = "score_decile_static"
    if bin_col not in current_df.columns or bin_col not in baseline_df.columns:
        logger.warning("Cannot compute mix shift: '%s' not in both DataFrames", bin_col)
        return {"mix_shift": [], "within_band_deterioration": [], "attribution_summary": "Insufficient data for attribution."}

    # Baseline band distribution
    base_total = baseline_df.count()
    base_dist = {
        int(r[bin_col]): int(r["n"]) / base_total if base_total > 0 else 0
        for r in baseline_df.groupBy(bin_col).agg(F.count("*").alias("n")).collect()
        if r[bin_col] is not None
    }

    # Current band distribution
    cur_total = current_df.count()
    cur_dist = {
        int(r[bin_col]): int(r["n"]) / cur_total if cur_total > 0 else 0
        for r in current_df.groupBy(bin_col).agg(F.count("*").alias("n")).collect()
        if r[bin_col] is not None
    }

    mix_shift = []
    for band in sorted(set(list(base_dist.keys()) + list(cur_dist.keys()))):
        base_pct = base_dist.get(band, 0.0)
        cur_pct = cur_dist.get(band, 0.0)
        mix_shift.append({
            "score_bin": band,
            "baseline_pct": base_pct,
            "current_pct": cur_pct,
            "shift": cur_pct - base_pct,
        })

    # Attribution summary
    shifts_up = sum(1 for m in mix_shift if m["shift"] > 0.02)
    shifts_down = sum(1 for m in mix_shift if m["shift"] < -0.02)
    summary = (
        f"Population mix has shifted: {shifts_up} bins grew, {shifts_down} bins shrank. "
        f"Total population change: baseline {base_total:,} → current {cur_total:,}."
    )

    return {
        "mix_shift": mix_shift,
        "within_band_deterioration": [],  # Requires strategy_df
        "attribution_summary": summary,
    }
