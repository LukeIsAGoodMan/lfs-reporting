"""Separation metrics — KS, Gini, Odds, monotonicity.

Contains compute_separation and private helpers.
Logic is identical to the original metrics.py — only the module location changed.
"""
from __future__ import annotations
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from framework.metrics import ks_statistic
from framework.v2.config import MonitoringConfig
from framework.v2.cohort import CohortResult, MATURITY_MAP
from framework.v2.thresholds import ThresholdEngine
from framework.v2.sample_controls import check_ks_sample
from framework.utils import get_logger

logger = get_logger(__name__)


def compute_separation(
    cohort: CohortResult,
    config: MonitoringConfig,
    thresholds: ThresholdEngine,
    baseline_df: DataFrame | None = None,
    scorecard_id: str | None = None,
) -> dict | None:
    """Compute separation metrics (KS, Gini) on a mature cohort.

    Requires actuals. Uses the bad_col from the cohort's maturity definition.

    Returns:
        {"ks": float, "ks_status": str, "gini": float, ...} or None if insufficient sample.
    """
    if not cohort.is_available:
        return None

    info = MATURITY_MAP[cohort.label]
    bad_col = info["bad_col"]
    score_col = config.score_col

    df = cohort.df
    if bad_col not in df.columns:
        logger.warning("Bad column '%s' not in cohort %s", bad_col, cohort.label)
        return None

    # Sample check
    ok, reason = check_ks_sample(df, bad_col, config)
    if not ok:
        return {"ks": None, "gini": None, "note": reason, "label": cohort.label}

    # KS computation: split into goods vs bads
    goods = df.filter(F.col(bad_col) == 0)
    bads = df.filter(F.col(bad_col) == 1)

    ks_val = ks_statistic(goods, bads, score_col)

    # Gini = 2 * AUC - 1 (approximate via KS relationship: Gini ≈ KS for monotonic scores)
    # More accurate: compute AUC via trapezoidal approximation
    gini_val = _compute_gini(df, score_col, bad_col)

    # Compute odds by decile
    odds = _compute_odds(df, score_col, bad_col, config)

    # Baseline KS for comparison (if available)
    ks_drop = None
    if baseline_df is not None and bad_col in baseline_df.columns:
        base_goods = baseline_df.filter(F.col(bad_col) == 0)
        base_bads = baseline_df.filter(F.col(bad_col) == 1)
        if base_goods.count() > 0 and base_bads.count() > 0:
            baseline_ks = ks_statistic(base_goods, base_bads, score_col)
            ks_drop = ks_val - baseline_ks

    ks_status = thresholds.evaluate("psi", abs(ks_val) if ks_val else 0, scorecard_id)
    ks_drop_status = (
        thresholds.evaluate("ks_drop", ks_drop, scorecard_id, higher_is_worse=False)
        if ks_drop is not None else "N/A"
    )

    return {
        "label": cohort.label,
        "score_month": cohort.score_month,
        "ks": ks_val,
        "ks_status": ks_status,
        "gini": gini_val,
        "ks_drop": ks_drop,
        "ks_drop_status": ks_drop_status,
        "odds": odds,
        "odds_monotonic": all(r.get("is_monotonic", True) for r in odds),
        "misrank_count": sum(1 for r in odds if not r.get("is_monotonic", True)),
        "account_count": cohort.mature_count,
    }


# ── Private helpers ───────────────────────────────────────────────────

def _compute_gini(df: DataFrame, score_col: str, target_col: str) -> float | None:
    """Approximate Gini coefficient from score vs binary target.

    Uses the relationship: Gini = 2 * AUC - 1.
    AUC is approximated by the concordance rate.
    """
    total = df.count()
    if total < 10:
        return None

    bads = df.filter(F.col(target_col) == 1)
    goods = df.filter(F.col(target_col) == 0)
    n_bads = bads.count()
    n_goods = goods.count()

    if n_bads == 0 or n_goods == 0:
        return None

    # Approximate AUC using mean rank method
    from pyspark.sql import Window
    w = Window.orderBy(F.col(score_col).asc())
    ranked = df.withColumn("_rank", F.row_number().over(w))

    bad_rank_sum = (
        ranked.filter(F.col(target_col) == 1)
        .agg(F.sum("_rank").alias("s"))
        .collect()[0]["s"]
    )

    if bad_rank_sum is None:
        return None

    auc = (float(bad_rank_sum) - n_bads * (n_bads + 1) / 2) / (n_bads * n_goods)
    gini = 2 * auc - 1
    return round(gini, 6)


def _compute_odds(
    df: DataFrame,
    score_col: str,
    bad_col: str,
    config: MonitoringConfig,
) -> list[dict]:
    """Compute goods/bads odds by score decile."""
    bin_col = "score_decile_static"
    if bin_col not in df.columns:
        from pyspark.sql import Window
        w = Window.orderBy(score_col)
        df = df.withColumn(bin_col, F.ntile(10).over(w))

    rows = (
        df.groupBy(bin_col)
        .agg(
            F.count("*").alias("n"),
            F.sum(F.when(F.col(bad_col) == 1, 1).otherwise(0)).alias("bads"),
            F.sum(F.when(F.col(bad_col) == 0, 1).otherwise(0)).alias("goods"),
        )
        .collect()
    )

    odds_list = []
    for r in sorted(rows, key=lambda x: x[bin_col] or 0):
        bads = int(r["bads"] or 0)
        goods = int(r["goods"] or 0)
        odds = goods / bads if bads > 0 else float("inf")
        odds_list.append({
            "score_bin": int(r[bin_col]) if r[bin_col] is not None else 0,
            "total": int(r["n"] or 0),
            "goods": goods,
            "bads": bads,
            "odds": odds,
            "bad_rate": bads / int(r["n"]) if int(r["n"] or 0) > 0 else 0.0,
        })

    # Monotonicity check: odds should increase with score bin
    prev_odds = None
    misrank_count = 0
    for row in odds_list:
        if prev_odds is not None and row["odds"] < prev_odds:
            row["is_monotonic"] = False
            misrank_count += 1
        else:
            row["is_monotonic"] = True
        prev_odds = row["odds"]

    # Stamp summary on each row for easy access
    for row in odds_list:
        row["total_misranks"] = misrank_count

    return odds_list
