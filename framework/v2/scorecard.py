"""Scorecard grouping utilities."""
from __future__ import annotations
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from framework.v2.config import MonitoringConfig
from framework.utils import get_logger

logger = get_logger(__name__)


def get_scorecard_groups(
    df: DataFrame,
    config: MonitoringConfig,
) -> dict[str, DataFrame]:
    """Split a DataFrame into scorecard groups.

    Returns:
        {"overall": full_df, "SC001": filtered_df, ...}
        If scorecard is disabled, returns only {"overall": df}.
    """
    groups: dict[str, DataFrame] = {"overall": df}

    if not config.scorecard_enabled:
        return groups

    sc_col = config.scorecard_column
    if sc_col not in df.columns:
        logger.warning(
            "Scorecard column '%s' not in DataFrame — returning overall only",
            sc_col,
        )
        return groups

    sc_values = sorted([
        str(r[sc_col])
        for r in df.select(sc_col).distinct().collect()
        if r[sc_col] is not None
    ])

    for sc_id in sc_values:
        groups[sc_id] = df.filter(F.col(sc_col) == sc_id)
        logger.debug("Scorecard group '%s': %d rows", sc_id, groups[sc_id].count())

    logger.info("Scorecard groups: overall + %d scorecards", len(sc_values))
    return groups
