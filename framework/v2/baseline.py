"""Baseline computation and management.

Supports fixed_window, rolling_window, and initial_val baseline types.
Falls back to first available window if insufficient history.
"""
from __future__ import annotations
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from framework.v2.config import MonitoringConfig
from framework.v2.data_model import apply_monitoring_filter
from framework.utils import get_logger

logger = get_logger(__name__)


class BaselineResult:
    """Container for baseline computation result."""
    __slots__ = ("df", "type", "start_month", "end_month", "months", "account_count")

    def __init__(
        self,
        df: DataFrame,
        type: str,
        start_month: str,
        end_month: str,
        months: list[str],
        account_count: int,
    ):
        self.df = df
        self.type = type
        self.start_month = start_month
        self.end_month = end_month
        self.months = months
        self.account_count = account_count


def compute_baseline(
    score_mart: DataFrame,
    config: MonitoringConfig,
    reporting_month: str | None = None,
) -> BaselineResult:
    """Compute the baseline DataFrame according to config.

    Args:
        score_mart: Full score snapshot mart (all months).
        config: Monitoring configuration.
        reporting_month: Current reporting month (for rolling window).

    Returns:
        BaselineResult with the filtered baseline DataFrame and metadata.
    """
    filtered = apply_monitoring_filter(score_mart)
    bl_cfg = config.baseline

    if bl_cfg.type == "fixed_window" and bl_cfg.start_month and bl_cfg.end_month:
        return _fixed_window(filtered, bl_cfg.start_month, bl_cfg.end_month)

    if bl_cfg.type == "rolling_window" and reporting_month:
        return _rolling_window(filtered, reporting_month, bl_cfg.minimum_history_months)

    if bl_cfg.type == "initial_val":
        return _initial_val(filtered, bl_cfg.minimum_history_months)

    # Fallback: try fixed_window, then initial_val
    logger.warning("Baseline type '%s' not fully configured — using fallback", bl_cfg.type)
    return _initial_val(filtered, bl_cfg.minimum_history_months)


def _fixed_window(df: DataFrame, start: str, end: str) -> BaselineResult:
    """Filter to a fixed [start, end] month window."""
    baseline = df.filter(
        (F.col("score_month") >= start) & (F.col("score_month") <= end)
    )
    n = baseline.count()
    months = sorted([
        r["score_month"]
        for r in baseline.select("score_month").distinct().collect()
    ])

    if n == 0:
        logger.warning("Fixed-window baseline [%s, %s] is empty", start, end)
    else:
        logger.info(
            "Fixed-window baseline [%s, %s]: %d accounts across %d months",
            start, end, n, len(months),
        )

    return BaselineResult(
        df=baseline,
        type="fixed_window",
        start_month=start,
        end_month=end,
        months=months,
        account_count=n,
    )


def _rolling_window(
    df: DataFrame, reporting_month: str, window_months: int,
) -> BaselineResult:
    """Baseline = [reporting_month - window_months, reporting_month - 1]."""
    dt = datetime.strptime(reporting_month, "%Y-%m")
    end_dt = dt - relativedelta(months=1)
    start_dt = dt - relativedelta(months=window_months)
    start = start_dt.strftime("%Y-%m")
    end = end_dt.strftime("%Y-%m")
    return _fixed_window(df, start, end)


def _initial_val(df: DataFrame, min_months: int) -> BaselineResult:
    """Use the first N months of data as baseline."""
    months = sorted([
        r["score_month"]
        for r in df.select("score_month").distinct().collect()
    ])

    if len(months) < min_months:
        logger.warning(
            "Only %d months available (need %d for baseline) — using all available",
            len(months), min_months,
        )
        use_months = months
    else:
        use_months = months[:min_months]

    if not use_months:
        logger.warning("No data available for baseline")
        return BaselineResult(
            df=df.limit(0), type="initial_val", start_month="",
            end_month="", months=[], account_count=0,
        )

    start, end = use_months[0], use_months[-1]
    baseline = df.filter(
        (F.col("score_month") >= start) & (F.col("score_month") <= end)
    )
    n = baseline.count()
    logger.info(
        "Initial-val baseline [%s, %s]: %d accounts across %d months",
        start, end, n, len(use_months),
    )

    return BaselineResult(
        df=baseline, type="initial_val",
        start_month=start, end_month=end,
        months=use_months, account_count=n,
    )
