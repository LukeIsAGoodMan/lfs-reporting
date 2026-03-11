"""Reusable statistical metric functions for model monitoring.

All functions operate on PySpark DataFrames and return DataFrames
or scalars.  They are model-agnostic — column names are passed in,
not hardcoded.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from framework.utils import get_logger

logger = get_logger(__name__)


# ── PSI ──────────────────────────────────────────────────────────────

def psi(
    expected_df: DataFrame,
    actual_df: DataFrame,
    col: str,
    n_bins: int,
) -> float:
    """Population Stability Index between two distributions of *col*.

    Computes quantile bin edges from *expected_df*, buckets both
    distributions, and returns the scalar PSI value.

    Null values in *col* are excluded before computation.
    """
    expected_clean = expected_df.filter(F.col(col).isNotNull())
    actual_clean = actual_df.filter(F.col(col).isNotNull())

    # Quantile edges from expected (baseline).
    fractions = [i / n_bins for i in range(1, n_bins)]
    edges = expected_clean.stat.approxQuantile(col, fractions, 0.01)

    if not edges:
        logger.warning("PSI: no edges computed for '%s' — returning 0.0", col)
        return 0.0

    # Assign bins using the edges.
    expected_binned = _assign_psi_bin(expected_clean, col, edges)
    actual_binned = _assign_psi_bin(actual_clean, col, edges)

    expected_total = expected_binned.count()
    actual_total = actual_binned.count()

    if expected_total == 0 or actual_total == 0:
        return 0.0

    # Compute bin proportions.
    exp_counts = {
        row["_psi_bin"]: row["cnt"]
        for row in expected_binned.groupBy("_psi_bin")
        .agg(F.count("*").alias("cnt"))
        .collect()
    }
    act_counts = {
        row["_psi_bin"]: row["cnt"]
        for row in actual_binned.groupBy("_psi_bin")
        .agg(F.count("*").alias("cnt"))
        .collect()
    }

    psi_value = 0.0
    all_bins = set(exp_counts.keys()) | set(act_counts.keys())
    for b in all_bins:
        # Clamp to avoid log(0): use 0.0001 as floor.
        exp_pct = max(exp_counts.get(b, 0) / expected_total, 0.0001)
        act_pct = max(act_counts.get(b, 0) / actual_total, 0.0001)
        psi_value += (act_pct - exp_pct) * _safe_ln(act_pct / exp_pct)

    return psi_value


def _assign_psi_bin(df: DataFrame, col: str, edges: list[float]) -> DataFrame:
    """Bucket rows into integer bins based on *edges*."""
    case_expr = F.lit(len(edges))  # default: above all edges
    for i, edge in enumerate(edges):
        case_expr = F.when(F.col(col) <= edge, F.lit(i)).otherwise(case_expr)
    # Build from highest edge down so the first matching when wins.
    case_expr = F.lit(len(edges))
    for i in range(len(edges) - 1, -1, -1):
        case_expr = F.when(F.col(col) <= edges[i], F.lit(i)).otherwise(case_expr)
    return df.withColumn("_psi_bin", case_expr)


def _safe_ln(x: float) -> float:
    """Natural log with guard against non-positive values."""
    import math
    return math.log(max(x, 1e-10))


# ── KS ───────────────────────────────────────────────────────────────

def ks_statistic(
    df1: DataFrame,
    df2: DataFrame,
    col: str,
) -> float:
    """Kolmogorov-Smirnov statistic between two distributions of *col*.

    Computes the maximum absolute difference between the empirical
    CDFs of the two DataFrames on *col*.  Nulls are excluded.
    """
    df1_clean = df1.filter(F.col(col).isNotNull()).select(F.col(col).alias("_val"))
    df2_clean = df2.filter(F.col(col).isNotNull()).select(F.col(col).alias("_val"))

    n1 = df1_clean.count()
    n2 = df2_clean.count()
    if n1 == 0 or n2 == 0:
        return 0.0

    # Tag and union, then compute empirical CDF.
    tagged1 = df1_clean.withColumn("_src", F.lit(1))
    tagged2 = df2_clean.withColumn("_src", F.lit(2))
    combined = tagged1.unionAll(tagged2)

    # Sort and compute cumulative fractions.
    w = Window.orderBy("_val")
    combined = combined.withColumn(
        "_cdf1",
        F.sum(F.when(F.col("_src") == 1, 1).otherwise(0)).over(w) / F.lit(n1),
    ).withColumn(
        "_cdf2",
        F.sum(F.when(F.col("_src") == 2, 1).otherwise(0)).over(w) / F.lit(n2),
    )

    ks = combined.agg(F.max(F.abs(F.col("_cdf1") - F.col("_cdf2")))).collect()[0][0]
    return float(ks) if ks is not None else 0.0


# ── Missing rate ─────────────────────────────────────────────────────

def missing_rate(
    df: DataFrame,
    cols: list[str],
) -> list[dict[str, float]]:
    """Compute fraction of nulls/NaN for each column.

    Returns a list of dicts, one per column:
    ``{"column_name": ..., "total_count": ..., "missing_count": ..., "missing_rate": ...}``
    """
    total = df.count()
    if total == 0:
        return [
            {"column_name": c, "total_count": 0, "missing_count": 0, "missing_rate": 0.0}
            for c in cols
        ]

    # Compute all missing counts in a single pass.
    agg_exprs = [
        F.sum(F.when(F.col(c).isNull() | F.isnan(F.col(c)), 1).otherwise(0)).alias(c)
        for c in cols
    ]
    row = df.agg(*agg_exprs).collect()[0]

    results = []
    for c in cols:
        mc = int(row[c]) if row[c] is not None else 0
        results.append({
            "column_name": c,
            "total_count": total,
            "missing_count": mc,
            "missing_rate": mc / total,
        })
    return results


# ── Outlier rate ─────────────────────────────────────────────────────

def outlier_rate(
    df: DataFrame,
    col: str,
    lower: float,
    upper: float,
) -> dict[str, float]:
    """Fraction of non-null values outside [lower, upper].

    Returns dict with keys:
    ``column_name, total_count, outlier_count, outlier_rate``.
    """
    non_null = df.filter(F.col(col).isNotNull())
    total = non_null.count()
    if total == 0:
        return {"column_name": col, "total_count": 0, "outlier_count": 0, "outlier_rate": 0.0}

    outliers = non_null.filter(
        (F.col(col) < lower) | (F.col(col) > upper)
    ).count()

    return {
        "column_name": col,
        "total_count": total,
        "outlier_count": outliers,
        "outlier_rate": outliers / total,
    }


# ── Dynamic cutoffs ─────────────────────────────────────────────────

def dynamic_cutoffs(
    df: DataFrame,
    score_col: str,
    quantiles: list[float],
    top_pct_means: list[float],
) -> dict[str, float]:
    """Compute score quantile values and top-percentile means.

    Returns dict with keys like ``q50``, ``q80``, ``mean_top10pct``, etc.
    """
    clean = df.filter(F.col(score_col).isNotNull())
    total = clean.count()
    if total == 0:
        result: dict[str, float] = {}
        for q in quantiles:
            result[f"q{int(q * 100)}"] = 0.0
        for pct in top_pct_means:
            result[f"mean_top{int(pct * 100)}pct"] = 0.0
        return result

    # Quantile values.
    q_values = clean.stat.approxQuantile(score_col, quantiles, 0.01)

    result = {}
    for q, v in zip(quantiles, q_values):
        result[f"q{int(q * 100)}"] = v

    # Top-percentile means.
    for pct in top_pct_means:
        threshold_quantile = 1.0 - pct
        threshold = clean.stat.approxQuantile(score_col, [threshold_quantile], 0.01)
        if threshold:
            top_mean = (
                clean.filter(F.col(score_col) >= threshold[0])
                .agg(F.avg(score_col))
                .collect()[0][0]
            )
            result[f"mean_top{int(pct * 100)}pct"] = float(top_mean) if top_mean else 0.0
        else:
            result[f"mean_top{int(pct * 100)}pct"] = 0.0

    return result


# ── Pearson correlation ──────────────────────────────────────────────

def pearson_corr(
    df: DataFrame,
    col_a: str,
    col_b: str,
) -> float | None:
    """Pearson correlation coefficient between *col_a* and *col_b*.

    Rows where either column is null are excluded.  Returns ``None``
    when correlation cannot be computed (fewer than 2 rows, zero
    variance in either column, or non-numeric data).
    """
    clean = df.filter(F.col(col_a).isNotNull() & F.col(col_b).isNotNull())
    if clean.count() < 2:
        return None
    corr_val = clean.stat.corr(col_a, col_b)
    if corr_val is None or _is_nan(corr_val):
        return None
    return float(corr_val)


def _is_nan(val) -> bool:
    """Check if a value is NaN."""
    import math
    try:
        return math.isnan(float(val))
    except (TypeError, ValueError):
        return False


# ── Baseline-derived outlier bounds ──────────────────────────────────

def baseline_outlier_bounds(
    baseline_df: DataFrame,
    col: str,
    lower_quantile: float = 0.01,
    upper_quantile: float = 0.99,
) -> tuple[float, float]:
    """Derive outlier bounds from baseline percentiles.

    Returns ``(lower_bound, upper_bound)`` computed as
    ``approxQuantile(col, [lower_quantile, upper_quantile])``.
    Falls back to ``(0.0, 1.0)`` if the baseline is empty.
    """
    clean = baseline_df.filter(F.col(col).isNotNull())
    if clean.count() == 0:
        return (0.0, 1.0)
    bounds = clean.stat.approxQuantile(col, [lower_quantile, upper_quantile], 0.01)
    if len(bounds) < 2:
        return (0.0, 1.0)
    return (bounds[0], bounds[1])
