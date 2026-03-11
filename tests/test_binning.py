"""Unit tests for framework.binning."""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from framework.binning import (
    compute_bucket,
    compute_dynamic_bins,
    compute_static_bins,
    map_decile_to_band,
)


# ── compute_dynamic_bins ─────────────────────────────────────────────

class TestComputeDynamicBins:
    def test_bins_in_range(self, spark: SparkSession):
        data = [(i, float(i) / 20, "A") for i in range(1, 21)]
        df = spark.createDataFrame(data, ["id", "score", "ch"])
        result = compute_dynamic_bins(df, "score", 5, ["ch"], "bin")
        bins = {row["bin"] for row in result.select("bin").collect()}
        assert bins == {1, 2, 3, 4, 5}

    def test_row_count_preserved(self, spark: SparkSession):
        data = [(i, float(i), "A") for i in range(1, 31)]
        df = spark.createDataFrame(data, ["id", "score", "ch"])
        result = compute_dynamic_bins(df, "score", 10, ["ch"], "bin")
        assert result.count() == 30

    def test_partition_respected(self, spark: SparkSession):
        data = (
            [(i, float(i), "A") for i in range(1, 11)]
            + [(i, float(i), "B") for i in range(1, 11)]
        )
        df = spark.createDataFrame(data, ["id", "score", "ch"])
        result = compute_dynamic_bins(df, "score", 5, ["ch"], "bin")
        for ch in ("A", "B"):
            bins = {
                row["bin"]
                for row in result.filter(F.col("ch") == ch).select("bin").collect()
            }
            assert bins == {1, 2, 3, 4, 5}, f"channel {ch} missing bins"

    def test_tiebreak_determinism(self, spark: SparkSession):
        # All same score — without tiebreak, bin assignment may vary.
        # With tiebreak (id), ordering is stable across invocations.
        data = [(i, 0.5, "A") for i in range(1, 11)]
        df = spark.createDataFrame(data, ["id", "score", "ch"])
        result1 = compute_dynamic_bins(df, "score", 5, ["ch"], "bin", tiebreak_col="id")
        result2 = compute_dynamic_bins(df, "score", 5, ["ch"], "bin", tiebreak_col="id")
        rows1 = sorted(
            [(r["id"], r["bin"]) for r in result1.select("id", "bin").collect()]
        )
        rows2 = sorted(
            [(r["id"], r["bin"]) for r in result2.select("id", "bin").collect()]
        )
        assert rows1 == rows2

    def test_output_col_added(self, spark: SparkSession):
        data = [(1, 0.1, "A"), (2, 0.9, "A")]
        df = spark.createDataFrame(data, ["id", "score", "ch"])
        result = compute_dynamic_bins(df, "score", 2, ["ch"], "my_bin")
        assert "my_bin" in result.columns


# ── compute_static_bins ──────────────────────────────────────────────

class TestComputeStaticBins:
    def test_null_score_produces_null_bin(self, spark: SparkSession):
        # Baseline has real scores.
        baseline_data = [(i, float(i) / 10, "A") for i in range(1, 11)]
        baseline_df = spark.createDataFrame(baseline_data, ["id", "score", "ch"])

        # Current has one null score.
        current_data = [(99, None, "A")]
        current_df = spark.createDataFrame(current_data, ["id", "score", "ch"])

        result = compute_static_bins(
            current_df, baseline_df, "score", 5, ["ch"], "sbin"
        )
        row = result.collect()[0]
        assert row["sbin"] is None, "Null score should produce null bin"

    def test_empty_baseline_returns_null_bins(self, spark: SparkSession):
        baseline_df = spark.createDataFrame([], schema="id INT, score DOUBLE, ch STRING")
        current_data = [(1, 0.5, "A")]
        current_df = spark.createDataFrame(current_data, ["id", "score", "ch"])
        result = compute_static_bins(
            current_df, baseline_df, "score", 5, ["ch"], "sbin"
        )
        row = result.collect()[0]
        assert row["sbin"] is None, "Empty baseline should produce null bins"

    def test_bins_assigned_from_baseline_edges(self, spark: SparkSession):
        # Baseline: uniform [0.1 .. 1.0], 10 points → 5 equal bins.
        baseline_data = [(i, float(i) / 10, "A") for i in range(1, 11)]
        baseline_df = spark.createDataFrame(baseline_data, ["id", "score", "ch"])

        # Scores at extreme ends should land in bin 1 and bin 5.
        current_data = [(1, 0.05, "A"), (2, 0.99, "A")]
        current_df = spark.createDataFrame(current_data, ["id", "score", "ch"])

        result = compute_static_bins(
            current_df, baseline_df, "score", 5, ["ch"], "sbin"
        )
        bins = {r["id"]: r["sbin"] for r in result.select("id", "sbin").collect()}
        assert bins[1] == 1, "Score below all edges should be bin 1"
        assert bins[2] == 5, "Score above all edges should be bin 5"

    def test_row_count_preserved(self, spark: SparkSession):
        baseline_data = [(i, float(i) / 10, "A") for i in range(1, 11)]
        baseline_df = spark.createDataFrame(baseline_data, ["id", "score", "ch"])
        current_data = [(i, float(i) / 20, "A") for i in range(1, 21)]
        current_df = spark.createDataFrame(current_data, ["id", "score", "ch"])
        result = compute_static_bins(
            current_df, baseline_df, "score", 5, ["ch"], "sbin"
        )
        assert result.count() == 20


# ── compute_bucket ───────────────────────────────────────────────────

class TestComputeBucket:
    def test_first_bucket(self, spark: SparkSession):
        df = spark.createDataFrame([(1, 50.0)], ["id", "amount"])
        result = compute_bucket(df, "amount", [100.0, 500.0], ["Low", "Mid", "High"], "bucket")
        assert result.collect()[0]["bucket"] == "Low"

    def test_middle_bucket(self, spark: SparkSession):
        df = spark.createDataFrame([(1, 300.0)], ["id", "amount"])
        result = compute_bucket(df, "amount", [100.0, 500.0], ["Low", "Mid", "High"], "bucket")
        assert result.collect()[0]["bucket"] == "Mid"

    def test_last_bucket(self, spark: SparkSession):
        df = spark.createDataFrame([(1, 1000.0)], ["id", "amount"])
        result = compute_bucket(df, "amount", [100.0, 500.0], ["Low", "Mid", "High"], "bucket")
        assert result.collect()[0]["bucket"] == "High"

    def test_boundary_value_goes_to_lower_bucket(self, spark: SparkSession):
        # Value exactly at boundary: <= boundaries[0] → first label.
        df = spark.createDataFrame([(1, 100.0)], ["id", "amount"])
        result = compute_bucket(df, "amount", [100.0, 500.0], ["Low", "Mid", "High"], "bucket")
        assert result.collect()[0]["bucket"] == "Low"

    def test_null_value_produces_null(self, spark: SparkSession):
        df = spark.createDataFrame([(1, None)], schema="id INT, amount DOUBLE")
        result = compute_bucket(df, "amount", [100.0, 500.0], ["Low", "Mid", "High"], "bucket")
        assert result.collect()[0]["bucket"] is None

    def test_multiple_rows(self, spark: SparkSession):
        data = [(1, 50.0), (2, 200.0), (3, 600.0)]
        df = spark.createDataFrame(data, ["id", "amount"])
        result = compute_bucket(df, "amount", [100.0, 500.0], ["Low", "Mid", "High"], "bucket")
        buckets = {r["id"]: r["bucket"] for r in result.select("id", "bucket").collect()}
        assert buckets == {1: "Low", 2: "Mid", 3: "High"}


# ── map_decile_to_band ───────────────────────────────────────────────

class TestMapDecileToBand:
    BANDS = [
        {"name": "Low", "deciles": [1, 2, 3]},
        {"name": "Medium", "deciles": [4, 5, 6, 7]},
        {"name": "High", "deciles": [8, 9, 10]},
    ]

    def test_low_band(self, spark: SparkSession):
        df = spark.createDataFrame([(1, 2)], ["id", "decile"])
        result = map_decile_to_band(df, "decile", self.BANDS, "band")
        assert result.collect()[0]["band"] == "Low"

    def test_high_band(self, spark: SparkSession):
        df = spark.createDataFrame([(1, 9)], ["id", "decile"])
        result = map_decile_to_band(df, "decile", self.BANDS, "band")
        assert result.collect()[0]["band"] == "High"

    def test_null_decile_produces_null_band(self, spark: SparkSession):
        df = spark.createDataFrame([(1, None)], schema="id INT, decile INT")
        result = map_decile_to_band(df, "decile", self.BANDS, "band")
        assert result.collect()[0]["band"] is None
