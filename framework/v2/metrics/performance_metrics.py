"""Performance metrics — EDR and bad rate computation.

Contains compute_performance and private helpers.
Logic is identical to the original metrics.py — only the module location changed.
"""
from __future__ import annotations
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from framework.v2.config import MonitoringConfig
from framework.v2.cohort import CohortResult, MATURITY_MAP, EDR_DISPLAY
from framework.v2.thresholds import ThresholdEngine
from framework.utils import get_logger

logger = get_logger(__name__)


def compute_performance(
    cohorts: dict[str, CohortResult],
    config: MonitoringConfig,
    thresholds: ThresholdEngine,
    scorecard_id: str | None = None,
) -> list[dict]:
    """Compute EDR/bad rate metrics across maturity cohorts.

    Returns a list of performance rows, one per (maturity_label, channel).
    """
    rows = []
    for label, cohort in cohorts.items():
        if not cohort.is_available:
            rows.append({
                "maturity": label,
                "score_month": cohort.score_month,
                "channel": "all",
                "display_label": EDR_DISPLAY.get(label, label),
                "note": cohort.note or "Not available",
            })
            continue

        info = MATURITY_MAP[label]
        edr_col = info["edr_col"]
        bad_col = info["bad_col"]
        df = cohort.df

        # Overall
        overall = _agg_performance(df, edr_col, bad_col, config.score_col)
        overall["maturity"] = label
        overall["score_month"] = cohort.score_month
        overall["channel"] = "all"
        overall["display_label"] = EDR_DISPLAY.get(label, label)
        overall["edr_status"] = thresholds.evaluate(
            "edr_delta", float(overall.get("edr", 0) or 0), scorecard_id,
        )
        rows.append(overall)

        # Per channel
        for ch in config.channels:
            ch_df = df.filter(F.col(config.channel_col) == ch)
            if ch_df.count() == 0:
                continue
            ch_row = _agg_performance(ch_df, edr_col, bad_col, config.score_col)
            ch_row["maturity"] = label
            ch_row["score_month"] = cohort.score_month
            ch_row["channel"] = ch
            ch_row["display_label"] = EDR_DISPLAY.get(label, label)
            ch_row["edr_status"] = thresholds.evaluate(
                "edr_delta", float(ch_row.get("edr", 0) or 0), scorecard_id,
            )
            rows.append(ch_row)

    return rows


def compute_edr_rank_ordering(
    cohort: CohortResult,
    config: MonitoringConfig,
    score_intervals: list[tuple[float, float]],
) -> list[dict] | None:
    """EDR rank-ordering table by config-defined score intervals.

    One row per interval. NOT using qcut.

    Returns:
        List of dicts per interval:
        - interval, min_score, max_score, accounts_n, accounts_pct,
          observation_pct, misrank
    """
    if not cohort.is_available:
        return None

    info = MATURITY_MAP[cohort.label]
    edr_col = info["edr_col"]
    score_col = config.score_col
    df = cohort.df

    total_count = df.count()
    if total_count == 0:
        return None

    last_idx = len(score_intervals) - 1
    rows = []

    for i, (lower, upper) in enumerate(score_intervals):
        if i == last_idx:
            bucket = df.filter(
                (F.col(score_col) >= lower) & (F.col(score_col) <= upper)
            )
        else:
            bucket = df.filter(
                (F.col(score_col) >= lower) & (F.col(score_col) < upper)
            )

        stats = bucket.agg(
            F.count("*").alias("n"),
            F.avg(edr_col).alias("obs_rate"),
        ).collect()[0]

        n = int(stats["n"] or 0)
        obs_rate = float(stats["obs_rate"]) if stats["obs_rate"] is not None else 0.0

        rows.append({
            "interval": f"{lower:.2f}-{upper:.2f}",
            "min_score": lower,
            "max_score": upper,
            "accounts_n": n,
            "accounts_pct": n / total_count if total_count > 0 else 0.0,
            "observation_pct": obs_rate,
            "misrank": "NO",  # placeholder, set below
        })

    # Sort by min_score ascending
    rows.sort(key=lambda r: r["min_score"])

    # Check monotonicity: higher score should have higher observation rate
    for i in range(1, len(rows)):
        if rows[i]["observation_pct"] < rows[i - 1]["observation_pct"]:
            rows[i]["misrank"] = "YES"

    return rows


def compute_edr_capture_summary(
    cohort: CohortResult,
    config: MonitoringConfig,
    risk_threshold: float = 0.5,
) -> dict | None:
    """Risk capture summary for one EDR window.

    Returns:
        {
            "threshold": float,
            "pct_population_flagged": float,
            "pct_bad_captured": float,
        }
        or None if cohort not available.
    """
    if not cohort.is_available:
        return None

    info = MATURITY_MAP[cohort.label]
    edr_col = info["edr_col"]
    score_col = config.score_col
    df = cohort.df

    total_count = df.count()
    if total_count == 0:
        return None

    total_bads = df.filter(F.col(edr_col) == 1).count()

    high_risk = df.filter(F.col(score_col) >= risk_threshold)
    flagged_count = high_risk.count()
    flagged_bads = high_risk.filter(F.col(edr_col) == 1).count()

    pct_population_flagged = flagged_count / total_count
    pct_bad_captured = flagged_bads / total_bads if total_bads > 0 else 0.0

    return {
        "threshold": risk_threshold,
        "pct_population_flagged": pct_population_flagged,
        "pct_bad_captured": pct_bad_captured,
    }


# ── Private helpers ───────────────────────────────────────────────────

def _agg_performance(
    df: DataFrame,
    edr_col: str,
    bad_col: str,
    score_col: str,
) -> dict:
    """Aggregate EDR/bad rate metrics for a cohort slice."""
    agg_cols = [
        F.count("*").alias("account_count"),
        F.avg(score_col).alias("avg_score"),
    ]

    if edr_col in df.columns:
        agg_cols.append(F.avg(edr_col).alias("edr"))
        agg_cols.append(F.sum(edr_col).alias("edr_count"))

    if bad_col in df.columns:
        agg_cols.append(F.avg(bad_col).alias("bad_rate"))
        agg_cols.append(F.sum(bad_col).alias("bad_count"))

    row = df.agg(*agg_cols).collect()[0]

    return {
        "account_count": int(row["account_count"] or 0),
        "avg_score": float(row["avg_score"]) if row["avg_score"] is not None else 0.0,
        "edr": float(row["edr"]) if "edr" in row.asDict() and row["edr"] is not None else None,
        "edr_count": int(row["edr_count"]) if "edr_count" in row.asDict() and row["edr_count"] is not None else 0,
        "bad_rate": float(row["bad_rate"]) if "bad_rate" in row.asDict() and row["bad_rate"] is not None else None,
        "bad_count": int(row["bad_count"]) if "bad_count" in row.asDict() and row["bad_count"] is not None else 0,
    }
