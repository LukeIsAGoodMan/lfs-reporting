"""SparkSession builder."""
from __future__ import annotations

from pyspark.sql import SparkSession


def get_or_create_spark(
    app_name: str = "model_reporting",
    conf_overrides: dict[str, str] | None = None,
) -> SparkSession:
    """Build or retrieve a SparkSession with framework defaults.

    Merges *conf_overrides* (from ``conf/framework.yaml``) on top of builder
    defaults so callers can tune shuffle partitions, AQE, etc.
    """
    builder = SparkSession.builder.appName(app_name)

    if conf_overrides:
        for key, value in conf_overrides.items():
            builder = builder.config(key, value)

    return builder.getOrCreate()
