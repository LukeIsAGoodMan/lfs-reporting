"""Sample size and maturity controls.

Prevents computation of unstable metrics when sample sizes are too small.
"""
from __future__ import annotations
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from framework.v2.config import MonitoringConfig
from framework.utils import get_logger

logger = get_logger(__name__)


def check_ks_sample(
    df: DataFrame,
    target_col: str,
    config: MonitoringConfig,
) -> tuple[bool, str]:
    """Check if sample is sufficient for KS/Gini computation.

    Returns:
        (is_sufficient, reason)
    """
    if df is None:
        return False, "No data available"

    total = df.count()
    min_accounts = config.sample_rules.minimum_accounts_for_ks

    if total < min_accounts:
        msg = f"Insufficient accounts: {total} < {min_accounts}"
        logger.info("KS sample check failed: %s", msg)
        return False, msg

    if target_col in df.columns:
        bads = df.filter(F.col(target_col) == 1).count()
        min_bads = config.sample_rules.minimum_bads_for_ks
        if bads < min_bads:
            msg = f"Insufficient bads: {bads} < {min_bads}"
            logger.info("KS sample check failed: %s", msg)
            return False, msg

    return True, f"OK: {total} accounts"


def check_calibration_sample(
    df: DataFrame,
    bin_col: str,
    config: MonitoringConfig,
) -> tuple[bool, str]:
    """Check if bins have enough accounts for calibration."""
    if df is None:
        return False, "No data available"

    min_per_bin = config.sample_rules.minimum_accounts_for_calibration_bin

    bin_counts = (
        df.groupBy(bin_col)
        .agg(F.count("*").alias("n"))
        .collect()
    )

    small_bins = [
        r[bin_col] for r in bin_counts
        if (r["n"] or 0) < min_per_bin
    ]

    if small_bins:
        msg = f"{len(small_bins)} bins below minimum {min_per_bin} accounts"
        logger.info("Calibration sample check: %s", msg)
        return False, msg

    return True, "OK"


def handle_missing_maturity(
    label: str,
    config: MonitoringConfig,
) -> dict:
    """Handle missing maturity data according to config.

    Returns a metadata dict about the skip/fail decision.
    """
    if config.maturity_on_missing == "skip_with_note":
        note = f"Cohort {label}: maturity data not available — skipped"
        logger.info(note)
        return {"action": "skip", "note": note}

    raise ValueError(f"Cohort {label}: maturity data required but not available")
