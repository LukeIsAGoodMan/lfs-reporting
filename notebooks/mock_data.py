"""Synthetic LFS data generator for notebook demos.

Produces two DataFrames that match the LFS source schema exactly:

- ``baseline_df`` — 7 months of data (2024-08 to 2025-02), used by
  the framework for static binning edges and Layer 3 baseline metrics.
- ``current_df``  — 3 months of data (2025-03 to 2025-05), used as the
  scored population being reported on.

The distributions are designed to produce non-trivial monitoring output:

- **Score drift**: current mean is ~0.06–0.08 higher than baseline,
  with slight tail thickening (larger std).
- **Feature drift**: ``feature_03`` has the largest shift (+0.14 mean),
  followed by ``feature_01``, ``feature_10``, and ``feature_04``.
- **Source mix shift**: "organic" proportion falls while "paid" and
  "referral" grow in the current period.
- **Channel differences**: digital scores are consistently higher than
  directmail, with a slightly different receivable profile.

No production tables are read or written.  Import this module and call
``generate_lfs_mock_data(spark)`` to get started.
"""
from __future__ import annotations

import random

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# ── Schema ────────────────────────────────────────────────────────────

LFS_SCHEMA = StructType([
    StructField("creditaccountid",  IntegerType(), False),
    StructField("customerid",       IntegerType(), False),
    StructField("month",            StringType(),  False),
    StructField("channel",          StringType(),  False),
    StructField("source",           StringType(),  True),
    StructField("line",             StringType(),  True),
    StructField("active",           IntegerType(), True),
    StructField("saleamount",       DoubleType(),  True),
    StructField("salecount",        IntegerType(), True),
    StructField("mob_pti",          IntegerType(), True),
    StructField("endingreceivable", DoubleType(),  True),
    StructField("feature_01",       DoubleType(),  True),
    StructField("feature_02",       DoubleType(),  True),
    StructField("feature_03",       DoubleType(),  True),
    StructField("feature_04",       DoubleType(),  True),
    StructField("feature_05",       DoubleType(),  True),
    StructField("feature_06",       DoubleType(),  True),
    StructField("feature_07",       DoubleType(),  True),
    StructField("feature_08",       DoubleType(),  True),
    StructField("feature_09",       DoubleType(),  True),
    StructField("feature_10",       DoubleType(),  True),
    StructField("feature_11",       DoubleType(),  True),
    StructField("lfs_score",        DoubleType(),  True),
])

# ── Period and channel parameters ─────────────────────────────────────

BASELINE_MONTHS = [
    "2024-08", "2024-09", "2024-10", "2024-11", "2024-12",
    "2025-01", "2025-02",
]
CURRENT_MONTHS = ["2025-03", "2025-04", "2025-05"]

# (score_mean, score_std, [p_organic, p_paid, p_referral])
_CHANNEL_PARAMS = {
    "baseline": {
        "digital":    (0.38, 0.17, [0.50, 0.30, 0.20]),
        "directmail": (0.32, 0.15, [0.50, 0.30, 0.20]),
    },
    "current": {
        # Mild score drift upward; source mix shifts toward paid/referral.
        "digital":    (0.44, 0.21, [0.40, 0.35, 0.25]),
        "directmail": (0.40, 0.22, [0.38, 0.37, 0.25]),
    },
}

# Feature mean offsets added to baseline for the current period.
_BASELINE_FEAT_DRIFT: dict[str, float] = {}   # zero drift in baseline
_CURRENT_FEAT_DRIFT: dict[str, float] = {
    "feature_01": 0.06,   # mild upward
    "feature_03": 0.14,   # notable drift — key signal for monitoring
    "feature_04": 0.04,   # slight drift
    "feature_10": 0.08,   # moderate drift
}


# ── Private helpers ───────────────────────────────────────────────────

def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


def _pick(options: list[str], weights: list[float]) -> str:
    r = random.random()
    cumul = 0.0
    for opt, w in zip(options, weights):
        cumul += w
        if r <= cumul:
            return opt
    return options[-1]


def _gen_rows(
    month: str,
    channel: str,
    n: int,
    score_mean: float,
    score_std: float,
    src_weights: list[float],
    feat_drift: dict[str, float],
    start_id: int,
) -> list[tuple]:
    """Generate ``n`` synthetic account rows for a single (month, channel)."""
    sources = ["organic", "paid", "referral"]
    rows = []
    for i in range(n):
        acct_id = start_id + i + 1
        cust_id  = random.randint(1, max(1, int(n * 0.75)))

        score = _clamp(random.gauss(score_mean, score_std), 0.01, 0.99)

        # feature_01: high correlation with score; mild drift between periods.
        f01 = _clamp(0.55 * score + 0.45 * random.gauss(0.38, 0.14)
                     + feat_drift.get("feature_01", 0.0))
        # feature_02: moderate correlation; stable.
        f02 = _clamp(0.30 * score + 0.70 * random.gauss(0.50, 0.20))
        # feature_03: independent of score; strongest drift.
        f03 = _clamp(random.gauss(0.30 + feat_drift.get("feature_03", 0.0), 0.15))
        # feature_04: moderate correlation; slight drift.
        f04 = _clamp(0.40 * score + 0.60 * random.gauss(
            0.45 + feat_drift.get("feature_04", 0.0), 0.18))
        # feature_05–08: weak correlation; stable.
        f05 = _clamp(0.20 * score + 0.80 * random.gauss(0.50, 0.22))
        f06 = _clamp(random.gauss(0.55, 0.20))
        f07 = _clamp(random.gauss(0.60, 0.18))
        f08 = _clamp(random.gauss(0.45, 0.20))
        # feature_09: near-uniform; stable.
        f09 = random.uniform(0.10, 0.90)
        # feature_10: near-uniform; moderate drift.
        f10 = _clamp(random.uniform(0.10, 0.90)
                     + feat_drift.get("feature_10", 0.0))
        # feature_11: near-uniform; stable.
        f11 = random.uniform(0.10, 0.90)

        src = _pick(sources, src_weights)

        # Digital skews more toward business line; directmail toward personal.
        biz_prob = 0.40 if channel == "digital" else 0.22
        ln = "business" if random.random() < biz_prob else "personal"

        active = 1 if random.random() < 0.87 else 0

        # Financial columns vary by line.
        if ln == "business":
            saleamt  = round(random.uniform(200.0, 9000.0), 2)
            recv     = round(random.uniform(800.0, 22000.0), 2)
        else:
            saleamt  = round(random.uniform(0.0, 3500.0), 2)
            recv     = round(random.uniform(0.0, 9000.0), 2)

        salecnt = random.randint(0, 45)
        mob     = random.randint(0, 58)

        rows.append((
            acct_id, cust_id, month, channel,
            src, ln, active,
            saleamt, salecnt, mob, recv,
            round(f01, 6), round(f02, 6), round(f03, 6), round(f04, 6),
            round(f05, 6), round(f06, 6), round(f07, 6), round(f08, 6),
            round(f09, 6), round(f10, 6), round(f11, 6),
            round(score, 6),
        ))
    return rows


# ── Public API ────────────────────────────────────────────────────────

def generate_lfs_mock_data(
    spark: SparkSession,
    n_baseline_per_month_channel: int = 250,
    n_current_per_month_channel: int = 200,
    seed: int = 42,
) -> dict[str, DataFrame]:
    """Generate synthetic LFS scored-account DataFrames.

    Args:
        spark: Active SparkSession.
        n_baseline_per_month_channel: Accounts per (month, channel) in the
            baseline window.  Default 250 → 3,500 baseline rows total.
        n_current_per_month_channel: Accounts per (month, channel) in the
            current window.  Default 200 → 1,200 current rows total.
        seed: Random seed for reproducibility.

    Returns:
        ``{"baseline_df": DataFrame, "current_df": DataFrame}``
    """
    random.seed(seed)
    next_id = 0

    baseline_rows: list[tuple] = []
    for month in BASELINE_MONTHS:
        for channel, (sm, ss, sw) in _CHANNEL_PARAMS["baseline"].items():
            n = n_baseline_per_month_channel
            baseline_rows.extend(
                _gen_rows(month, channel, n, sm, ss, sw, _BASELINE_FEAT_DRIFT, next_id)
            )
            next_id += n

    current_rows: list[tuple] = []
    for month in CURRENT_MONTHS:
        for channel, (sm, ss, sw) in _CHANNEL_PARAMS["current"].items():
            n = n_current_per_month_channel
            current_rows.extend(
                _gen_rows(month, channel, n, sm, ss, sw, _CURRENT_FEAT_DRIFT, next_id)
            )
            next_id += n

    baseline_df = spark.createDataFrame(baseline_rows, schema=LFS_SCHEMA)
    current_df  = spark.createDataFrame(current_rows,  schema=LFS_SCHEMA)

    n_base_months = len(BASELINE_MONTHS)
    n_curr_months = len(CURRENT_MONTHS)
    print(
        f"Generated baseline_df: "
        f"{n_baseline_per_month_channel * 2 * n_base_months:,} rows  "
        f"({n_base_months} months × 2 channels × {n_baseline_per_month_channel})"
    )
    print(
        f"Generated current_df:  "
        f"{n_current_per_month_channel * 2 * n_curr_months:,} rows  "
        f"({n_curr_months} months × 2 channels × {n_current_per_month_channel})"
    )
    return {"baseline_df": baseline_df, "current_df": current_df}
