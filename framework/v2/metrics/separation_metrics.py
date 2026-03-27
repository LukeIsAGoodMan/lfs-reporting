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


def compute_ks_table(
    cohort: CohortResult,
    config: MonitoringConfig,
    n_bins: int = 20,
) -> list[dict] | None:
    """Full KS table with 20 bins (default).

    Uses collect + Python binning to avoid unpartitioned Window operations.
    Equal-frequency bins are assigned after sorting by score.

    Returns list of dicts per tier (+ TOTAL row) or None if unavailable.
    """
    if not cohort.is_available:
        return None

    info = MATURITY_MAP[cohort.label]
    bad_col = info["bad_col"]
    score_col = config.score_col
    df = cohort.df

    if bad_col not in df.columns:
        return None

    # Sample check — short-circuit early
    ok, reason = check_ks_sample(df, bad_col, config)
    if not ok:
        logger.info("KS table skipped for %s: %s", cohort.label, reason)
        return None

    # Collect (score, bad) pairs — cohort data is small enough
    pairs = df.select(score_col, bad_col).collect()
    data = sorted(
        [
            (float(r[score_col]), int(r[bad_col]))
            for r in pairs
            if r[score_col] is not None and r[bad_col] is not None
        ],
        key=lambda x: x[0],
    )

    n = len(data)
    if n == 0:
        return None

    # Assign equal-frequency tiers in Python
    bin_size = n / n_bins
    from collections import defaultdict

    tier_stats: dict[int, dict] = defaultdict(
        lambda: {"n": 0, "goods": 0, "bads": 0, "min": float("inf"), "max": float("-inf")}
    )

    for idx, (score, bad) in enumerate(data):
        tier = min(int(idx / bin_size) + 1, n_bins)
        t = tier_stats[tier]
        t["n"] += 1
        t["min"] = min(t["min"], score)
        t["max"] = max(t["max"], score)
        if bad == 1:
            t["bads"] += 1
        else:
            t["goods"] += 1

    total_goods = sum(t["goods"] for t in tier_stats.values())
    total_bads = sum(t["bads"] for t in tier_stats.values())
    overall_bad_rate = total_bads / n if n > 0 else 0.0

    cum_goods = 0
    cum_bads = 0
    results = []

    for tier_num in sorted(tier_stats.keys()):
        t = tier_stats[tier_num]
        cum_goods += t["goods"]
        cum_bads += t["bads"]

        cum_good_pct = cum_goods / total_goods if total_goods > 0 else 0.0
        cum_bad_pct = cum_bads / total_bads if total_bads > 0 else 0.0
        bad_rate = t["bads"] / t["n"] if t["n"] > 0 else 0.0

        results.append({
            "tier": str(tier_num),
            "min_score": t["min"],
            "max_score": t["max"],
            "accounts_n": t["n"],
            "accounts_pct": t["n"] / n,
            "goods_n": t["goods"],
            "goods_pct": cum_good_pct,
            "bads_n": t["bads"],
            "bads_pct": cum_bad_pct,
            "bad_rate": bad_rate,
            "ks": abs(cum_good_pct - cum_bad_pct),
            "lift": bad_rate / overall_bad_rate if overall_bad_rate > 0 else 0.0,
        })

    # TOTAL row
    max_ks = max((r["ks"] for r in results), default=0.0)
    results.append({
        "tier": "TOTAL",
        "min_score": results[0]["min_score"] if results else 0.0,
        "max_score": results[-1]["max_score"] if results else 0.0,
        "accounts_n": n,
        "accounts_pct": 1.0,
        "goods_n": total_goods,
        "goods_pct": 1.0,
        "bads_n": total_bads,
        "bads_pct": 1.0,
        "bad_rate": overall_bad_rate,
        "ks": max_ks,
        "lift": 1.0,
    })

    return results


# ── Private helpers ───────────────────────────────────────────────────

def _compute_gini(df: DataFrame, score_col: str, target_col: str) -> float | None:
    """Approximate Gini coefficient from score vs binary target.

    Uses Gini = 2 * AUC - 1, where AUC is computed via the mean rank method
    on collected (score, target) pairs.  Avoids unpartitioned Window operations.
    """
    total = df.count()
    if total < 10:
        return None

    # Collect (score, target) pairs — cohort data is small enough
    pairs = df.select(score_col, target_col).collect()
    data = [
        (float(r[score_col]), int(r[target_col]))
        for r in pairs
        if r[score_col] is not None and r[target_col] is not None
    ]

    if len(data) < 10:
        return None

    n_bads = sum(1 for _, b in data if b == 1)
    n_goods = sum(1 for _, b in data if b == 0)

    if n_bads == 0 or n_goods == 0:
        return None

    # Sort by score ascending, assign ranks (1-based)
    data.sort(key=lambda x: x[0])
    bad_rank_sum = sum(i + 1 for i, (_, b) in enumerate(data) if b == 1)

    auc = (bad_rank_sum - n_bads * (n_bads + 1) / 2) / (n_bads * n_goods)
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
        # Assign deciles in Python to avoid unpartitioned Window.
        # Collect (score, all_cols) is too heavy — instead, use approxQuantile
        # to derive 9 edges, then bucket via CASE WHEN (no Window needed).
        clean = df.filter(F.col(score_col).isNotNull())
        edges = clean.stat.approxQuantile(score_col, [i / 10 for i in range(1, 10)], 0.01)
        edges = sorted(set(edges))  # deduplicate

        if edges:
            # Build CASE expression for bin assignment
            expr = F.lit(len(edges) + 1)  # default: last bin
            for i in range(len(edges)):
                expr = F.when(F.col(score_col) <= edges[i], F.lit(i + 1)).otherwise(expr)
            df = df.withColumn(bin_col, expr)
        else:
            df = df.withColumn(bin_col, F.lit(1))

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
