"""Performance mart builder — loads actuals from config-defined sources.

In production, the performance mart lives in a Hive/Spark table.
This module reads that table using the config's ``actual_source`` block,
validates the schema, and returns a canonical DataFrame.

If ``actual_source.enabled`` is False, callers should use mock data or
pass an explicit ``perf_mart`` to the runner.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from framework.v2.config import MonitoringConfig
from framework.v2.data_model import PERF_MART_REQUIRED, PERF_MART_OUTCOME_COLS, PERF_MART_MATURITY_COLS
from framework.utils import get_logger

logger = get_logger(__name__)


def build_perf_mart_from_source(
    spark: SparkSession,
    config: MonitoringConfig,
) -> DataFrame:
    """Load performance mart from a Hive/Spark table defined in config.

    Reads ``{actual_source.database}.{actual_source.table}``, validates
    that required columns exist, and returns the result.

    Raises:
        ValueError: If actual_source is not configured or not enabled.
        ValueError: If required columns are missing from the source table.
    """
    src = config.actual_source
    if src is None or not src.enabled:
        raise ValueError(
            "Cannot build perf mart from source: actual_source is not "
            "configured or not enabled in the monitoring config."
        )

    fq_table = f"{src.database}.{src.table}" if src.database else src.table
    logger.info("Loading performance mart from %s", fq_table)

    df = spark.table(fq_table)
    source_cols = set(df.columns)

    # Validate join keys
    missing_keys = [k for k in src.key_cols if k not in source_cols]
    if missing_keys:
        raise ValueError(
            f"Performance mart source {fq_table} is missing join key columns: {missing_keys}"
        )

    if src.score_month_col not in source_cols:
        raise ValueError(
            f"Performance mart source {fq_table} is missing score_month column: "
            f"{src.score_month_col}"
        )

    # Validate target columns from target_definition
    target_cols = set()
    target_cols.add(config.target.calibration_target)
    target_cols.update(config.target.early_read_targets.values())
    target_cols.update(config.target.separation_targets.values())

    # Also check maturity flags
    maturity_cols = set()
    for label in config.target.separation_targets:
        flag = f"is_mature_{label.lower()}"
        maturity_cols.add(flag)

    missing_targets = [c for c in target_cols if c not in source_cols]
    missing_maturity = [c for c in maturity_cols if c not in source_cols]

    if missing_targets:
        logger.warning(
            "Performance mart is missing target columns: %s — "
            "some metrics will be unavailable", missing_targets,
        )
    if missing_maturity:
        logger.warning(
            "Performance mart is missing maturity flags: %s — "
            "all accounts will be treated as mature", missing_maturity,
        )

    # Standardize score_month column name
    if src.score_month_col != "score_month":
        df = df.withColumnRenamed(src.score_month_col, "score_month")

    n = df.count()
    months = df.select("score_month").distinct().count()
    logger.info("Perf mart loaded: %d rows across %d months", n, months)
    print(f"Perf mart (from source): {n:,} rows across {months} months")

    return df
