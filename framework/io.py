"""I/O: read source tables from Trino/Hive, write output tables."""
from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession, DataFrame

from framework.config import ModelConfig
from framework.utils import get_logger, validate_columns

logger = get_logger(__name__)


def read_source_table(
    spark: SparkSession,
    config: ModelConfig,
    score_month: str | None = None,
    month_range: tuple[str, str] | None = None,
) -> DataFrame:
    """Read the model's source table, optionally filtered to a month or range.

    Args:
        score_month: Single YYYY-MM to filter on. Ignored if *month_range* is set.
        month_range: Inclusive (start, end) YYYY-MM range (e.g. for baseline).
    """
    src = config.source
    full_table = f"{src['database']}.{src['table']}"
    month_col = src["score_month_col"]

    logger.info("Reading source table %s", full_table)
    df = spark.table(full_table)

    if month_range is not None:
        start, end = month_range
        df = df.filter(
            (df[month_col] >= start) & (df[month_col] <= end)
        )
        logger.info("Filtered to month range %s – %s", start, end)
    elif score_month is not None:
        df = df.filter(df[month_col] == score_month)
        logger.info("Filtered to score_month=%s", score_month)

    expected_cols = (
        src["key_cols"]
        + [src["score_month_col"], src["score_col"], src["channel_col"]]
        + src["columns"].get("features", [])
        + src["columns"].get("business", [])
    )
    validate_columns(df, expected_cols)

    return df


def write_output(
    df: DataFrame,
    config: ModelConfig,
    table_name: str,
    score_month: str,
    model_version: str,
) -> None:
    """Write a reporting DataFrame to the output catalog.

    Args:
        table_name: Full output table name as defined in the model
            config (e.g. ``"lfs_layer1_account"``).  The final
            catalog path is ``{output.database}.{table_name}``.
    """
    out = config.output
    full_table = f"{out['database']}.{table_name}"
    fmt = out.get("format", "parquet")
    mode = out.get("mode", "overwrite")
    partition_cols = out.get("partition_cols", [])

    logger.info("Writing %s (format=%s, mode=%s)", full_table, fmt, mode)

    writer = df.write.format(fmt).mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(full_table)
