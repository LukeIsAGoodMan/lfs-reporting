"""Data model definitions and validation for v2 monitoring."""
from __future__ import annotations
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from framework.utils import get_logger

logger = get_logger(__name__)

SCORE_MART_REQUIRED = [
    "creditaccountid",
    "score_month",
    "score",
    "channel",
]

SCORE_MART_OPTIONAL = [
    "customerid",
    "scorecard_id",
    "score_band_static",
    "score_band_dynamic",
    "score_decile_static",
    "score_decile_dynamic",
    "is_excluded_from_monitoring",
    "exclusion_reason",
    "model_version",
    "created_at",
]

PERF_MART_REQUIRED = [
    "creditaccountid",
    "score_month",
]

PERF_MART_OUTCOME_COLS = [
    "edr30_m3", "edr60_m6", "edr90_m9", "co_m12",
    "bad30_m3", "bad60_m6", "bad90_m9", "badco_m12",
]

PERF_MART_MATURITY_COLS = [
    "is_mature_m3", "is_mature_m6", "is_mature_m9", "is_mature_m12",
]


def validate_score_mart(df: DataFrame) -> list[str]:
    """Validate score snapshot mart. Returns list of warnings (empty = valid)."""
    cols = set(df.columns)
    warnings = []
    for c in SCORE_MART_REQUIRED:
        if c not in cols:
            warnings.append(f"Missing required column: {c}")
    for c in SCORE_MART_OPTIONAL:
        if c not in cols:
            logger.debug("Score mart missing optional column: %s", c)
    return warnings


def validate_perf_mart(df: DataFrame) -> list[str]:
    """Validate performance mart. Returns list of warnings."""
    cols = set(df.columns)
    warnings = []
    for c in PERF_MART_REQUIRED:
        if c not in cols:
            warnings.append(f"Missing required column: {c}")

    has_outcome = any(c in cols for c in PERF_MART_OUTCOME_COLS)
    if not has_outcome:
        warnings.append("No outcome columns found (edr*_m*, bad*_m*, co_m12)")

    has_maturity = any(c in cols for c in PERF_MART_MATURITY_COLS)
    if not has_maturity:
        warnings.append("No maturity flags found (is_mature_m*)")

    return warnings


def apply_monitoring_filter(df: DataFrame) -> DataFrame:
    """Filter out accounts excluded from monitoring.

    If is_excluded_from_monitoring column doesn't exist, returns the
    full DataFrame unchanged.
    """
    if "is_excluded_from_monitoring" in df.columns:
        before = df.count()
        result = df.filter(F.col("is_excluded_from_monitoring") == 0)
        after = result.count()
        excluded = before - after
        if excluded > 0:
            logger.info("Monitoring filter: excluded %d of %d accounts", excluded, before)
        return result
    return df


def get_available_maturities(df: DataFrame) -> list[str]:
    """Return list of maturity labels (M3, M6, M9, M12) present in perf mart."""
    cols = set(df.columns)
    available = []
    for label, col_name in [("M3", "is_mature_m3"), ("M6", "is_mature_m6"),
                             ("M9", "is_mature_m9"), ("M12", "is_mature_m12")]:
        if col_name in cols:
            available.append(label)
    return available
