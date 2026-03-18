"""Backfill system for historical rebuild."""
from __future__ import annotations
from datetime import datetime
from dateutil.relativedelta import relativedelta
from framework.v2.config import MonitoringConfig
from framework.utils import get_logger

logger = get_logger(__name__)


def generate_backfill_months(
    config: MonitoringConfig,
    reporting_month: str,
) -> list[str]:
    """Generate the list of months to backfill.

    Produces [reporting_month - history_months, ..., reporting_month].
    """
    if not config.backfill_enabled:
        return [reporting_month]

    dt = datetime.strptime(reporting_month, "%Y-%m")
    months = []
    for i in range(config.backfill_history_months, -1, -1):
        m = dt - relativedelta(months=i)
        months.append(m.strftime("%Y-%m"))

    logger.info(
        "Backfill months: %d months from %s to %s",
        len(months), months[0], months[-1],
    )
    return months


def is_backfill_mode(config: MonitoringConfig) -> bool:
    """Check if backfill mode is enabled."""
    return config.backfill_enabled


def validate_backfill_readiness(
    score_mart_months: list[str],
    config: MonitoringConfig,
) -> tuple[bool, str]:
    """Check if there's enough data for baseline in backfill mode.

    Returns:
        (is_ready, message)
    """
    n_months = len(score_mart_months)
    required = config.backfill_minimum_baseline

    if n_months >= required:
        return True, f"Ready: {n_months} months available (need {required})"

    return False, (
        f"Insufficient history: {n_months} months available, "
        f"need {required} for baseline"
    )
