"""Unit tests for framework.layer2 — focus on percentage / window logic."""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from framework.layer2 import _apply_extra_columns, _apply_metrics


# ── _apply_metrics ───────────────────────────────────────────────────

class TestApplyMetrics:
    def test_count_metric(self, spark: SparkSession):
        df = spark.createDataFrame(
            [("A", "2024-01"), ("A", "2024-01"), ("B", "2024-01")],
            ["channel", "vintage_month"],
        )
        metrics = [{"func": "count", "col": None, "alias": "account_count"}]
        result = _apply_metrics(df, ["channel", "vintage_month"], metrics)
        rows = {r["channel"]: r["account_count"] for r in result.collect()}
        assert rows["A"] == 2
        assert rows["B"] == 1

    def test_avg_metric(self, spark: SparkSession):
        df = spark.createDataFrame(
            [("A", 0.2), ("A", 0.8), ("B", 0.5)],
            ["channel", "score"],
        )
        metrics = [{"func": "avg", "col": "score", "alias": "avg_score"}]
        result = _apply_metrics(df, ["channel"], metrics)
        rows = {r["channel"]: r["avg_score"] for r in result.collect()}
        assert abs(rows["A"] - 0.5) < 1e-9
        assert abs(rows["B"] - 0.5) < 1e-9

    def test_multiple_metrics_in_one_pass(self, spark: SparkSession):
        df = spark.createDataFrame(
            [("A", 0.2), ("A", 0.8)],
            ["channel", "score"],
        )
        metrics = [
            {"func": "count", "col": None, "alias": "cnt"},
            {"func": "avg", "col": "score", "alias": "avg_score"},
            {"func": "min", "col": "score", "alias": "min_score"},
        ]
        result = _apply_metrics(df, ["channel"], metrics)
        row = result.collect()[0]
        assert row["cnt"] == 2
        assert abs(row["avg_score"] - 0.5) < 1e-9
        assert abs(row["min_score"] - 0.2) < 1e-9

    def test_unsupported_func_raises(self, spark: SparkSession):
        df = spark.createDataFrame([("A", 1.0)], ["ch", "val"])
        with pytest.raises(ValueError, match="Unsupported aggregation function"):
            _apply_metrics(df, ["ch"], [{"func": "median", "col": "val", "alias": "x"}])


# ── _apply_extra_columns: percentage logic ───────────────────────────

class TestApplyExtraColumns:
    def test_pct_sums_to_one_per_partition(self, spark: SparkSession):
        """pct_of_channel_vintage_accounts must sum to 1.0 per (vintage_month, channel)."""
        agg_df = spark.createDataFrame(
            [
                ("2025-01", "digital", 1, 30),
                ("2025-01", "digital", 2, 70),
                ("2025-01", "directmail", 1, 50),
                ("2025-01", "directmail", 2, 50),
            ],
            ["vintage_month", "channel", "decile", "account_count"],
        )
        extra_cols = [
            {
                "name": "pct_of_channel_vintage_accounts",
                "over": ["vintage_month", "channel"],
                "source": "account_count",
            }
        ]
        result = _apply_extra_columns(agg_df, extra_cols)

        sums = (
            result.groupBy("vintage_month", "channel")
            .agg(F.sum("pct_of_channel_vintage_accounts").alias("total_pct"))
            .collect()
        )
        for row in sums:
            assert abs(row["total_pct"] - 1.0) < 1e-9, (
                f"pct does not sum to 1.0 for "
                f"({row['vintage_month']}, {row['channel']}): {row['total_pct']}"
            )

    def test_pct_values_between_zero_and_one(self, spark: SparkSession):
        agg_df = spark.createDataFrame(
            [("2025-01", "A", 25), ("2025-01", "A", 75)],
            ["vintage_month", "channel", "account_count"],
        )
        extra_cols = [
            {
                "name": "pct",
                "over": ["vintage_month", "channel"],
                "source": "account_count",
            }
        ]
        result = _apply_extra_columns(agg_df, extra_cols)
        pcts = [r["pct"] for r in result.select("pct").collect()]
        for p in pcts:
            assert 0.0 <= p <= 1.0

    def test_missing_over_or_source_skips_column(self, spark: SparkSession):
        """A malformed extra_column spec should be silently skipped."""
        agg_df = spark.createDataFrame([("A", 10)], ["channel", "count"])
        extra_cols = [{"name": "bad_col", "source": "count"}]
        result = _apply_extra_columns(agg_df, extra_cols)
        assert "bad_col" not in result.columns

    def test_multiple_partitions_independent(self, spark: SparkSession):
        """Different (vintage_month, channel) groups each sum to 1.0 independently."""
        agg_df = spark.createDataFrame(
            [
                ("2025-01", "A", 40),
                ("2025-01", "A", 60),
                ("2025-02", "A", 10),
                ("2025-02", "A", 90),
            ],
            ["vintage_month", "channel", "account_count"],
        )
        extra_cols = [
            {
                "name": "pct",
                "over": ["vintage_month", "channel"],
                "source": "account_count",
            }
        ]
        result = _apply_extra_columns(agg_df, extra_cols)
        sums = (
            result.groupBy("vintage_month")
            .agg(F.sum("pct").alias("s"))
            .collect()
        )
        for row in sums:
            assert abs(row["s"] - 1.0) < 1e-9
