"""Shared test fixtures for the reporting framework."""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Local SparkSession for unit tests."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("model_reporting_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
