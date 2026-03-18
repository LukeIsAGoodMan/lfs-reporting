"""V2 metric computations.

Wraps the core framework.metrics functions with running-month cohort
alignment, scorecard grouping, and sample size controls.
"""
from __future__ import annotations
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from framework.metrics import psi, ks_statistic, missing_rate, outlier_rate, pearson_corr
from framework.v2.config import MonitoringConfig
from framework.v2.cohort import CohortResult, MATURITY_MAP
from framework.v2.thresholds import ThresholdEngine
from framework.v2.sample_controls import check_ks_sample, check_calibration_sample
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
        "account_count": cohort.mature_count,
    }


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
            ch_row["edr_status"] = thresholds.evaluate(
                "edr_delta", float(ch_row.get("edr", 0) or 0), scorecard_id,
            )
            rows.append(ch_row)

    return rows


def compute_calibration(
    cohort: CohortResult,
    config: MonitoringConfig,
    thresholds: ThresholdEngine,
    scorecard_id: str | None = None,
) -> list[dict] | None:
    """Compute calibration: predicted vs actual by score bin.

    Returns list of dicts per (score_bin, channel) or None if no data.
    """
    if not cohort.is_available:
        return None

    info = MATURITY_MAP[cohort.label]
    bad_col = info["bad_col"]
    score_col = config.score_col
    df = cohort.df

    # Use score_decile_static if available, else create bins
    bin_col = "score_decile_static"
    if bin_col not in df.columns:
        # Create decile bins on the fly
        from pyspark.sql import Window
        w = Window.orderBy(score_col)
        df = df.withColumn(bin_col, F.ntile(10).over(w))

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

    return odds_list
