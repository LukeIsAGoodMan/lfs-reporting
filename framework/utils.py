"""Shared utilities: logging, column validation, column helpers."""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger for the given module name."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def validate_columns(df: DataFrame, required_cols: list[str]) -> None:
    """Raise ``ValueError`` if *df* is missing any of *required_cols*."""
    existing = set(df.columns)
    missing = [c for c in required_cols if c not in existing]
    if missing:
        raise ValueError(f"DataFrame is missing required columns: {missing}")


def add_literal_column(df: DataFrame, col_name: str, value) -> DataFrame:
    """Add a column with a constant literal value."""
    return df.withColumn(col_name, F.lit(value))


def add_expr_column(df: DataFrame, col_name: str, sql_expr: str) -> DataFrame:
    """Add a column using a SQL expression string."""
    return df.withColumn(col_name, F.expr(sql_expr))
