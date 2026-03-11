"""Unit tests for framework.metrics."""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from framework.metrics import (
    baseline_outlier_bounds,
    ks_statistic,
    missing_rate,
    outlier_rate,
    pearson_corr,
    psi,
)


# ── PSI ──────────────────────────────────────────────────────────────

class TestPsi:
    def test_identical_distributions_near_zero(self, spark: SparkSession):
        data = [(float(i) / 100,) for i in range(1, 101)]
        df = spark.createDataFrame(data, ["score"])
        result = psi(df, df, "score", 10)
        assert result < 0.05, "PSI of identical distributions should be near 0"

    def test_disjoint_distributions_positive(self, spark: SparkSession):
        baseline = spark.createDataFrame([(float(i) / 100,) for i in range(1, 51)], ["score"])
        current = spark.createDataFrame([(float(i) / 100,) for i in range(51, 101)], ["score"])
        result = psi(baseline, current, "score", 10)
        assert result > 0.1, "Disjoint distributions should have substantial PSI"

    def test_null_values_excluded(self, spark: SparkSession):
        # Adding nulls to one side should not crash; result still finite.
        baseline = spark.createDataFrame([(0.1,), (0.5,), (0.9,), (None,)], schema="score DOUBLE")
        current = spark.createDataFrame([(0.1,), (0.5,), (0.9,)], ["score"])
        result = psi(baseline, current, "score", 5)
        assert isinstance(result, float)
        assert result >= 0.0

    def test_empty_expected_returns_zero(self, spark: SparkSession):
        empty = spark.createDataFrame([], schema="score DOUBLE")
        current = spark.createDataFrame([(0.5,), (0.8,)], ["score"])
        result = psi(empty, current, "score", 5)
        assert result == 0.0

    def test_empty_actual_returns_zero(self, spark: SparkSession):
        baseline = spark.createDataFrame([(0.1,), (0.5,), (0.9,)], ["score"])
        empty = spark.createDataFrame([], schema="score DOUBLE")
        result = psi(baseline, empty, "score", 5)
        assert result == 0.0


# ── KS ───────────────────────────────────────────────────────────────

class TestKsStatistic:
    def test_identical_distributions_near_zero(self, spark: SparkSession):
        data = [(float(i) / 100,) for i in range(1, 51)]
        df = spark.createDataFrame(data, ["score"])
        result = ks_statistic(df, df, "score")
        assert result < 0.05

    def test_non_overlapping_distributions_equals_one(self, spark: SparkSession):
        df1 = spark.createDataFrame([(0.1,), (0.2,), (0.3,)], ["score"])
        df2 = spark.createDataFrame([(0.8,), (0.9,), (1.0,)], ["score"])
        result = ks_statistic(df1, df2, "score")
        assert result > 0.9, "Non-overlapping distributions should have KS near 1"

    def test_empty_df1_returns_zero(self, spark: SparkSession):
        empty = spark.createDataFrame([], schema="score DOUBLE")
        df2 = spark.createDataFrame([(0.5,)], ["score"])
        assert ks_statistic(empty, df2, "score") == 0.0

    def test_empty_df2_returns_zero(self, spark: SparkSession):
        df1 = spark.createDataFrame([(0.5,)], ["score"])
        empty = spark.createDataFrame([], schema="score DOUBLE")
        assert ks_statistic(df1, empty, "score") == 0.0

    def test_return_type_is_float(self, spark: SparkSession):
        df1 = spark.createDataFrame([(0.2,), (0.4,)], ["score"])
        df2 = spark.createDataFrame([(0.6,), (0.8,)], ["score"])
        assert isinstance(ks_statistic(df1, df2, "score"), float)


# ── pearson_corr ─────────────────────────────────────────────────────

class TestPearsonCorr:
    def test_zero_rows_returns_none(self, spark: SparkSession):
        df = spark.createDataFrame([], schema="a DOUBLE, b DOUBLE")
        assert pearson_corr(df, "a", "b") is None

    def test_one_row_returns_none(self, spark: SparkSession):
        df = spark.createDataFrame([(0.5, 1.0)], ["a", "b"])
        assert pearson_corr(df, "a", "b") is None

    def test_all_null_rows_returns_none(self, spark: SparkSession):
        df = spark.createDataFrame(
            [(None, None), (None, None)], schema="a DOUBLE, b DOUBLE"
        )
        assert pearson_corr(df, "a", "b") is None

    def test_perfect_positive_correlation(self, spark: SparkSession):
        data = [(float(i), float(i)) for i in range(1, 11)]
        df = spark.createDataFrame(data, ["a", "b"])
        result = pearson_corr(df, "a", "b")
        assert result is not None
        assert abs(result - 1.0) < 1e-6

    def test_constant_column_returns_none(self, spark: SparkSession):
        # Zero variance → correlation is NaN; should return None.
        data = [(float(i), 1.0) for i in range(1, 11)]
        df = spark.createDataFrame(data, ["a", "b"])
        result = pearson_corr(df, "a", "b")
        assert result is None

    def test_negative_correlation(self, spark: SparkSession):
        data = [(float(i), float(10 - i)) for i in range(1, 11)]
        df = spark.createDataFrame(data, ["a", "b"])
        result = pearson_corr(df, "a", "b")
        assert result is not None
        assert result < -0.9


# ── missing_rate ─────────────────────────────────────────────────────

class TestMissingRate:
    def test_no_missing(self, spark: SparkSession):
        df = spark.createDataFrame([(1.0, 2.0), (3.0, 4.0)], ["a", "b"])
        results = missing_rate(df, ["a", "b"])
        for r in results:
            assert r["missing_rate"] == 0.0

    def test_all_null(self, spark: SparkSession):
        df = spark.createDataFrame(
            [(None, None), (None, None)], schema="a DOUBLE, b DOUBLE"
        )
        results = missing_rate(df, ["a"])
        assert results[0]["missing_rate"] == 1.0

    def test_mixed_nulls(self, spark: SparkSession):
        df = spark.createDataFrame(
            [(1.0,), (None,), (None,), (4.0,)], schema="a DOUBLE"
        )
        results = missing_rate(df, ["a"])
        assert results[0]["missing_rate"] == 0.5

    def test_empty_df(self, spark: SparkSession):
        df = spark.createDataFrame([], schema="a DOUBLE")
        results = missing_rate(df, ["a"])
        assert results[0]["missing_rate"] == 0.0
        assert results[0]["total_count"] == 0

    def test_single_pass_multiple_cols(self, spark: SparkSession):
        df = spark.createDataFrame(
            [(1.0, None), (None, 2.0)], schema="a DOUBLE, b DOUBLE"
        )
        results = missing_rate(df, ["a", "b"])
        by_col = {r["column_name"]: r["missing_rate"] for r in results}
        assert by_col["a"] == 0.5
        assert by_col["b"] == 0.5


# ── outlier_rate ─────────────────────────────────────────────────────

class TestOutlierRate:
    def test_no_outliers(self, spark: SparkSession):
        df = spark.createDataFrame([(0.2,), (0.5,), (0.8,)], ["score"])
        result = outlier_rate(df, "score", 0.0, 1.0)
        assert result["outlier_rate"] == 0.0

    def test_all_outliers(self, spark: SparkSession):
        df = spark.createDataFrame([(-0.1,), (1.5,)], ["score"])
        result = outlier_rate(df, "score", 0.0, 1.0)
        assert result["outlier_rate"] == 1.0

    def test_mixed_outliers(self, spark: SparkSession):
        df = spark.createDataFrame([(0.5,), (1.5,), (0.9,), (-0.1,)], ["score"])
        result = outlier_rate(df, "score", 0.0, 1.0)
        assert result["outlier_rate"] == 0.5

    def test_empty_df_returns_zero(self, spark: SparkSession):
        df = spark.createDataFrame([], schema="score DOUBLE")
        result = outlier_rate(df, "score", 0.0, 1.0)
        assert result["outlier_rate"] == 0.0
        assert result["total_count"] == 0

    def test_nulls_excluded_from_denominator(self, spark: SparkSession):
        # 2 non-null values, 1 is outlier — rate should be 0.5, not 0.33.
        df = spark.createDataFrame(
            [(None,), (1.5,), (0.5,)], schema="score DOUBLE"
        )
        result = outlier_rate(df, "score", 0.0, 1.0)
        assert result["outlier_rate"] == 0.5
        assert result["total_count"] == 2


# ── baseline_outlier_bounds ───────────────────────────────────────────

class TestBaselineOutlierBounds:
    def test_returns_tuple_of_two_floats(self, spark: SparkSession):
        data = [(float(i) / 10,) for i in range(1, 101)]
        df = spark.createDataFrame(data, ["val"])
        lower, upper = baseline_outlier_bounds(df, "val", 0.01, 0.99)
        assert isinstance(lower, float)
        assert isinstance(upper, float)
        assert lower < upper

    def test_empty_df_falls_back_to_defaults(self, spark: SparkSession):
        df = spark.createDataFrame([], schema="val DOUBLE")
        lower, upper = baseline_outlier_bounds(df, "val", 0.01, 0.99)
        assert (lower, upper) == (0.0, 1.0)

    def test_bounds_are_within_data_range(self, spark: SparkSession):
        data = [(float(i),) for i in range(1, 101)]
        df = spark.createDataFrame(data, ["val"])
        lower, upper = baseline_outlier_bounds(df, "val", 0.01, 0.99)
        assert lower >= 1.0
        assert upper <= 100.0
