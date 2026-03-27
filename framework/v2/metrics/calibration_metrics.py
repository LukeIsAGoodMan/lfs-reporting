"""Calibration metrics — M12 predicted vs actual calibration.

Contains compute_calibration.
Logic is identical to the original metrics.py — only the module location changed.
"""
from __future__ import annotations
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from framework.v2.config import MonitoringConfig
from framework.v2.cohort import CohortResult, MATURITY_MAP
from framework.v2.thresholds import ThresholdEngine
from framework.v2.sample_controls import check_calibration_sample
from framework.utils import get_logger

logger = get_logger(__name__)


def compute_calibration(
    cohort: CohortResult,
    config: MonitoringConfig,
    thresholds: ThresholdEngine,
    scorecard_id: str | None = None,
) -> list[dict] | None:
    """Compute calibration: predicted vs actual by score bin.

    Returns list of dicts per (score_bin, channel) or None if no data.
    """
    if cohort.label != "M12":
        logger.warning(
            "Calibration should only be computed for M12 (1-year charge-off). "
            "Got '%s' — skipping.", cohort.label,
        )
        return None

    if not cohort.is_available:
        return None

    info = MATURITY_MAP[cohort.label]
    bad_col = info["bad_col"]
    score_col = config.score_col
    df = cohort.df

    # Use score_decile_static if available, else create bins
    bin_col = "score_decile_static"
    if bin_col not in df.columns:
        # Assign decile bins using quantile edges to avoid unpartitioned Window.
        clean = df.filter(F.col(score_col).isNotNull())
        edges = clean.stat.approxQuantile(score_col, [i / 10 for i in range(1, 10)], 0.01)
        edges = sorted(set(edges))

        if edges:
            expr = F.lit(len(edges) + 1)
            for i in range(len(edges)):
                expr = F.when(F.col(score_col) <= edges[i], F.lit(i + 1)).otherwise(expr)
            df = df.withColumn(bin_col, expr)
        else:
            df = df.withColumn(bin_col, F.lit(1))

    # Sample check
    ok, reason = check_calibration_sample(df, bin_col, config)

    rows = []
    for ch in ["all"] + config.channels:
        ch_df = df if ch == "all" else df.filter(F.col(config.channel_col) == ch)
        if ch_df.count() == 0:
            continue

        bin_stats = (
            ch_df
            .groupBy(bin_col)
            .agg(
                F.count("*").alias("account_count"),
                F.avg(score_col).alias("predicted_rate"),
                F.avg(bad_col).alias("actual_rate"),
                F.sum(bad_col).alias("bad_count"),
            )
            .collect()
        )

        for r in bin_stats:
            pred = float(r["predicted_rate"]) if r["predicted_rate"] is not None else 0.0
            actual = float(r["actual_rate"]) if r["actual_rate"] is not None else 0.0
            gap = actual - pred
            gap_status = thresholds.evaluate(
                "calibration_gap", abs(gap), scorecard_id,
            )
            rows.append({
                "maturity": cohort.label,
                "score_month": cohort.score_month,
                "channel": ch,
                "score_bin": int(r[bin_col]) if r[bin_col] is not None else 0,
                "account_count": int(r["account_count"] or 0),
                "bad_count": int(r["bad_count"] or 0),
                "predicted_rate": pred,
                "actual_rate": actual,
                "calibration_gap": gap,
                "gap_status": gap_status,
                "sample_sufficient": ok,
            })

    return sorted(rows, key=lambda x: (x["channel"], x["score_bin"]))
