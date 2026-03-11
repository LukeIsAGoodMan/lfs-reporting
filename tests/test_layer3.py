"""Unit tests for framework.layer3 — null / insufficient-data safeguards."""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from framework.layer3 import (
    _build_data_quality,
    _build_feature_quality,
    _build_feature_score_relationship,
    _build_population_mix,
    _numeric_feature_cols,
)
from framework.metrics import pearson_corr


# ── _numeric_feature_cols ────────────────────────────────────────────

class TestNumericFeatureCols:
    def test_filters_out_string_columns(self, spark: SparkSession):
        df = spark.createDataFrame(
            [(1.0, "hello", 2.0)], ["num1", "str_col", "num2"]
        )
        result = _numeric_feature_cols(df, ["num1", "str_col", "num2"])
        assert "str_col" not in result
        assert "num1" in result
        assert "num2" in result

    def test_empty_feature_list(self, spark: SparkSession):
        df = spark.createDataFrame([(1.0,)], ["score"])
        result = _numeric_feature_cols(df, [])
        assert result == []

    def test_all_numeric(self, spark: SparkSession):
        df = spark.createDataFrame([(1, 2.0, 3)], ["a", "b", "c"])
        result = _numeric_feature_cols(df, ["a", "b", "c"])
        assert sorted(result) == ["a", "b", "c"]

    def test_all_non_numeric(self, spark: SparkSession):
        df = spark.createDataFrame([("x", "y")], ["a", "b"])
        result = _numeric_feature_cols(df, ["a", "b"])
        assert result == []


# ── pearson_corr: null / insufficient-data safeguards ────────────────

class TestPearsonCorrSafeguards:
    def test_single_row_returns_none(self, spark: SparkSession):
        df = spark.createDataFrame([(0.5, 1.0)], ["feat", "score"])
        assert pearson_corr(df, "feat", "score") is None

    def test_zero_rows_returns_none(self, spark: SparkSession):
        df = spark.createDataFrame([], schema="feat DOUBLE, score DOUBLE")
        assert pearson_corr(df, "feat", "score") is None

    def test_constant_feature_returns_none(self, spark: SparkSession):
        # Zero variance in one column -> NaN correlation -> should return None.
        data = [(1.0, float(i)) for i in range(1, 11)]
        df = spark.createDataFrame(data, ["feat", "score"])
        assert pearson_corr(df, "feat", "score") is None

    def test_does_not_return_zero_when_insufficient(self, spark: SparkSession):
        # Guards the bug: returning 0.0 instead of None.
        df = spark.createDataFrame([(0.5, 1.0)], ["feat", "score"])
        result = pearson_corr(df, "feat", "score")
        assert result != 0.0  # Must be None, not silently 0.0


# ── _build_feature_score_relationship ───────────────────────────────

class TestBuildFeatureScoreRelationship:
    def _make_df(self, spark, channel="A", n=20):
        data = [
            ("2025-01", channel, float(i) / n, float(i) / n, "text")
            for i in range(1, n + 1)
        ]
        return spark.createDataFrame(
            data, ["vintage_month", "channel", "feat_num", "score", "feat_str"]
        )

    def test_non_numeric_feature_excluded(self, spark: SparkSession):
        current_df = self._make_df(spark)
        baseline_df = self._make_df(spark)
        result = _build_feature_score_relationship(
            current_df, baseline_df, spark,
            score_col="score",
            feature_cols=["feat_num", "feat_str"],
            channel_col="channel",
            channels=["A"],
        )
        feature_names = {r["feature_name"] for r in result.select("feature_name").collect()}
        assert "feat_str" not in feature_names
        assert "feat_num" in feature_names

    def test_corr_change_none_when_baseline_insufficient(self, spark: SparkSession):
        baseline_one_row = spark.createDataFrame(
            [("2025-01", "A", 0.5, 0.5)],
            ["vintage_month", "channel", "feat_num", "score"],
        )
        current_df = self._make_df(spark)
        result = _build_feature_score_relationship(
            current_df, baseline_one_row, spark,
            score_col="score",
            feature_cols=["feat_num"],
            channel_col="channel",
            channels=["A"],
        )
        row = result.collect()[0]
        assert row["baseline_corr"] is None
        assert row["corr_change"] is None

    def test_corr_change_none_when_current_insufficient(self, spark: SparkSession):
        baseline_df = self._make_df(spark)
        current_one_row = spark.createDataFrame(
            [("2025-01", "A", 0.5, 0.5)],
            ["vintage_month", "channel", "feat_num", "score"],
        )
        result = _build_feature_score_relationship(
            current_one_row, baseline_df, spark,
            score_col="score",
            feature_cols=["feat_num"],
            channel_col="channel",
            channels=["A"],
        )
        row = result.collect()[0]
        assert row["current_corr"] is None
        assert row["corr_change"] is None

    def test_corr_change_computed_when_both_available(self, spark: SparkSession):
        current_df = self._make_df(spark)
        baseline_df = self._make_df(spark)
        result = _build_feature_score_relationship(
            current_df, baseline_df, spark,
            score_col="score",
            feature_cols=["feat_num"],
            channel_col="channel",
            channels=["A"],
        )
        row = result.collect()[0]
        assert row["baseline_corr"] is not None
        assert row["current_corr"] is not None
        assert row["corr_change"] is not None


# ── _build_data_quality ──────────────────────────────────────────────

class TestBuildDataQuality:
    def test_schema_columns(self, spark: SparkSession):
        df = spark.createDataFrame(
            [("2025-01", "A", 0.5)], ["vintage_month", "channel", "score"]
        )
        result = _build_data_quality(
            df, spark,
            score_col="score",
            channel_col="channel",
            channels=["A"],
            outlier_lower=0.0,
            outlier_upper=1.0,
        )
        assert set(result.columns) == {
            "vintage_month", "channel", "missing_score_rate", "outlier_score_rate"
        }

    def test_all_scores_missing(self, spark: SparkSession):
        df = spark.createDataFrame(
            [("2025-01", "A", None), ("2025-01", "A", None)],
            schema="vintage_month STRING, channel STRING, score DOUBLE",
        )
        result = _build_data_quality(
            df, spark,
            score_col="score",
            channel_col="channel",
            channels=["A"],
            outlier_lower=0.0,
            outlier_upper=1.0,
        )
        row = result.collect()[0]
        assert row["missing_score_rate"] == 1.0

    def test_no_channel_match_produces_zero_rates(self, spark: SparkSession):
        df = spark.createDataFrame(
            [("2025-01", "A", 0.5)], ["vintage_month", "channel", "score"]
        )
        result = _build_data_quality(
            df, spark,
            score_col="score",
            channel_col="channel",
            channels=["X"],
            outlier_lower=0.0,
            outlier_upper=1.0,
        )
        row = result.collect()[0]
        assert row["missing_score_rate"] == 0.0
        assert row["outlier_score_rate"] == 0.0


# ── _build_feature_quality ───────────────────────────────────────────

class TestBuildFeatureQuality:
    def _make_df(self, spark):
        data = [
            ("2025-01", "A", float(i) / 10, float(i) / 10)
            for i in range(1, 11)
        ]
        return spark.createDataFrame(data, ["vintage_month", "channel", "f1", "f2"])

    def test_schema_columns(self, spark: SparkSession):
        df = self._make_df(spark)
        result = _build_feature_quality(
            df, df, spark,
            feature_cols=["f1", "f2"],
            channel_col="channel",
            channels=["A"],
            outlier_lower_q=0.01,
            outlier_upper_q=0.99,
        )
        expected = {
            "vintage_month", "channel", "feature_name",
            "missing_rate", "outlier_rate",
            "outlier_lower_bound", "outlier_upper_bound",
        }
        assert set(result.columns) == expected

    def test_one_row_per_feature(self, spark: SparkSession):
        df = self._make_df(spark)
        result = _build_feature_quality(
            df, df, spark,
            feature_cols=["f1", "f2"],
            channel_col="channel",
            channels=["A"],
            outlier_lower_q=0.01,
            outlier_upper_q=0.99,
        )
        feature_names = [r["feature_name"] for r in result.collect()]
        assert "f1" in feature_names
        assert "f2" in feature_names

    def test_bounds_derived_from_baseline(self, spark: SparkSession):
        baseline = self._make_df(spark)
        current = self._make_df(spark)
        result = _build_feature_quality(
            current, baseline, spark,
            feature_cols=["f1"],
            channel_col="channel",
            channels=["A"],
            outlier_lower_q=0.01,
            outlier_upper_q=0.99,
        )
        row = [r for r in result.collect() if r["feature_name"] == "f1"][0]
        assert row["outlier_lower_bound"] >= 0.0
        assert row["outlier_upper_bound"] <= 1.5


# ── _build_population_mix ────────────────────────────────────────────

class TestBuildPopulationMix:
    def _make_df(self, spark):
        data = [
            ("2025-01", "A", "online"),
            ("2025-01", "A", "online"),
            ("2025-01", "A", "branch"),
        ]
        return spark.createDataFrame(data, ["vintage_month", "channel", "source"])

    def test_segment_value_is_string_type(self, spark: SparkSession):
        df = self._make_df(spark)
        result = _build_population_mix(
            df, spark,
            segments=[{"column": "source", "segment_type": "source"}],
            channel_col="channel",
            channels=["A"],
        )
        for row in result.select("segment_value").collect():
            assert isinstance(row["segment_value"], str)

    def test_pct_sums_to_one_per_channel(self, spark: SparkSession):
        df = self._make_df(spark)
        result = _build_population_mix(
            df, spark,
            segments=[{"column": "source", "segment_type": "source"}],
            channel_col="channel",
            channels=["A"],
        )
        total_pct = result.agg(
            F.sum("pct_of_channel_accounts").alias("total")
        ).collect()[0]["total"]
        assert abs(total_pct - 1.0) < 1e-9

    def test_numeric_segment_column_cast_to_string(self, spark: SparkSession):
        df = spark.createDataFrame(
            [("2025-01", "A", 1), ("2025-01", "A", 2), ("2025-01", "A", 1)],
            ["vintage_month", "channel", "num_seg"],
        )
        result = _build_population_mix(
            df, spark,
            segments=[{"column": "num_seg", "segment_type": "num_seg"}],
            channel_col="channel",
            channels=["A"],
        )
        seg_vals = [r["segment_value"] for r in result.select("segment_value").collect()]
        assert all(isinstance(v, str) for v in seg_vals)
        assert set(seg_vals) == {"1", "2"}

    def test_schema_columns(self, spark: SparkSession):
        df = self._make_df(spark)
        result = _build_population_mix(
            df, spark,
            segments=[{"column": "source", "segment_type": "source"}],
            channel_col="channel",
            channels=["A"],
        )
        expected = {
            "vintage_month", "channel", "segment_type",
            "segment_value", "account_count", "pct_of_channel_accounts",
        }
        assert set(result.columns) == expected
