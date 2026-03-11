"""Lightweight end-to-end dry run using a fully synthetic Spark DataFrame.

No YAML files, no external tables, no writes to disk.
Verifies that all three layers produce DataFrames with the expected
row counts, column sets, and basic value constraints.
"""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from framework.config import ModelConfig
from framework.layer1 import enrich_layer1
from framework.layer2 import build_layer2
from framework.layer3 import build_layer3


# ── Minimal synthetic config ─────────────────────────────────────────

@pytest.fixture(scope="module")
def synthetic_config() -> ModelConfig:
    return ModelConfig(
        name="synth",
        display_name="Synthetic Test Model",
        source={
            "score_col": "score",
            "score_month_col": "month",
            "key_cols": ["account_id"],
            "channel_col": "channel",
            "channels": ["A", "B"],
        },
        binning={
            "dynamic": {
                "score": {
                    "n_bins": 5,
                    "partition_cols": ["month", "channel"],
                    "output_col_decile": "dyn_decile",
                    "output_col_band": "score_band",
                },
                "bands": [
                    {"name": "Low", "deciles": [1, 2]},
                    {"name": "Mid", "deciles": [3]},
                    {"name": "High", "deciles": [4, 5]},
                ],
            },
            "static": {
                "score": {
                    "baseline_window": ["2024-01"],
                    "n_bins": 5,
                    "partition_cols": ["channel"],
                    "output_col": "static_decile",
                }
            },
            "buckets": {
                "amount_bucket": {
                    "column": "amount",
                    "boundaries": [100.0, 500.0],
                    "labels": ["Low", "Mid", "High"],
                }
            },
        },
        layer1={
            "output_table": "synth_layer1_account",
            "extra_columns": [
                {"name": "is_missing_score", "expr": "score IS NULL"}
            ],
        },
        layer2={
            "standard_tables": {
                "overall_summary": {
                    "output_table": "synth_layer2_overall_summary",
                    "group_by": ["vintage_month", "channel"],
                    "metrics": [
                        {"func": "count", "col": None, "alias": "account_count"},
                        {"func": "avg", "col": "score", "alias": "avg_score"},
                    ],
                    "extra_columns": [
                        {
                            "name": "pct_of_channel_vintage_accounts",
                            "over": ["vintage_month", "channel"],
                            "source": "account_count",
                        }
                    ],
                }
            },
            "driver_tables": {},
        },
        layer3={
            "channel_col": "channel",
            "baseline_window": ["2024-01"],
            "score_col": "score",
            "feature_columns": ["feat_a", "feat_b"],
            "tables": {
                "score_drift": {
                    "output_table": "synth_layer3_score_drift",
                    "psi_n_bins": 5,
                    "quantiles": [0.50, 0.90],
                    "top_pct_means": [0.10],
                    "baseline_threshold_quantiles": [0.90],
                },
                "feature_psi": {
                    "output_table": "synth_layer3_feature_psi",
                    "n_bins": 5,
                    "columns": ["feat_a", "feat_b"],
                },
                "data_quality": {
                    "output_table": "synth_layer3_data_quality",
                    "score_outlier_bounds": {"lower": 0.0, "upper": 1.0},
                },
                "feature_quality": {
                    "output_table": "synth_layer3_feature_quality",
                    "columns": ["feat_a", "feat_b"],
                    "outlier_quantiles": [0.01, 0.99],
                },
                "feature_score_relationship": {
                    "output_table": "synth_layer3_feature_score_relationship",
                    "columns": ["feat_a", "feat_b"],
                },
                "population_mix": {
                    "output_table": "synth_layer3_population_mix",
                    "segments": [
                        {"column": "amount_bucket", "segment_type": "amount_bucket"}
                    ],
                },
            },
        },
        output={
            "database": "test_db",
            "format": "parquet",
            "partition_cols": ["vintage_month"],
        },
    )


# ── Synthetic data factories ─────────────────────────────────────────

@pytest.fixture(scope="module")
def raw_df(spark: SparkSession):
    """Small synthetic scored accounts table for 2025-03."""
    rows = []
    for i in range(1, 41):
        channel = "A" if i <= 20 else "B"
        rows.append((
            i,                      # account_id
            "2025-03",              # month
            round(i / 40.0, 4),     # score [0.025 .. 1.0]
            float(i * 25),          # amount
            channel,
            round(0.1 + (i % 10) * 0.08, 4),  # feat_a
            round(0.9 - (i % 7) * 0.05, 4),   # feat_b
        ))
    return spark.createDataFrame(
        rows,
        ["account_id", "month", "score", "amount", "channel", "feat_a", "feat_b"],
    )


@pytest.fixture(scope="module")
def baseline_df(spark: SparkSession):
    """Synthetic baseline data spanning channel A and B."""
    rows = []
    for i in range(1, 41):
        channel = "A" if i <= 20 else "B"
        rows.append((
            i,
            "2024-01",
            round(i / 40.0 * 0.9, 4),   # slightly shifted from current
            float(i * 20),
            channel,
            round(0.15 + (i % 10) * 0.07, 4),
            round(0.85 - (i % 7) * 0.04, 4),
        ))
    return spark.createDataFrame(
        rows,
        ["account_id", "month", "score", "amount", "channel", "feat_a", "feat_b"],
    )


# ── Layer 1 dry run ──────────────────────────────────────────────────

@pytest.fixture(scope="module")
def enriched_df(raw_df, baseline_df, synthetic_config):
    return enrich_layer1(
        raw_df, synthetic_config, baseline_df,
        score_month="2025-03",
        model_version="v1",
    )


class TestLayer1DryRun:
    def test_row_count_preserved(self, raw_df, enriched_df):
        assert enriched_df.count() == raw_df.count()

    def test_new_metadata_columns_present(self, enriched_df):
        assert "score_month" in enriched_df.columns
        assert "model_version" in enriched_df.columns
        assert "vintage_month" in enriched_df.columns

    def test_dynamic_decile_column_present(self, enriched_df):
        assert "dyn_decile" in enriched_df.columns

    def test_dynamic_decile_in_valid_range(self, enriched_df):
        out_of_range = enriched_df.filter(
            (F.col("dyn_decile") < 1) | (F.col("dyn_decile") > 5)
        ).count()
        assert out_of_range == 0

    def test_band_column_present(self, enriched_df):
        assert "score_band" in enriched_df.columns

    def test_band_values_valid(self, enriched_df):
        valid = {"Low", "Mid", "High"}
        actual = {r["score_band"] for r in enriched_df.select("score_band").collect()}
        assert actual.issubset(valid)

    def test_static_decile_column_present(self, enriched_df):
        assert "static_decile" in enriched_df.columns

    def test_bucket_column_present(self, enriched_df):
        assert "amount_bucket" in enriched_df.columns

    def test_bucket_values_valid(self, enriched_df):
        valid = {"Low", "Mid", "High"}
        actual = {r["amount_bucket"] for r in enriched_df.select("amount_bucket").collect()}
        assert actual.issubset(valid)

    def test_dq_flag_column_present(self, enriched_df):
        assert "is_missing_score" in enriched_df.columns

    def test_score_month_literal_correct(self, enriched_df):
        months = {r["score_month"] for r in enriched_df.select("score_month").collect()}
        assert months == {"2025-03"}

    def test_model_version_literal_correct(self, enriched_df):
        versions = {r["model_version"] for r in enriched_df.select("model_version").collect()}
        assert versions == {"v1"}


# ── Layer 2 dry run ──────────────────────────────────────────────────

@pytest.fixture(scope="module")
def layer2_tables(enriched_df, synthetic_config):
    return build_layer2(enriched_df, synthetic_config)


class TestLayer2DryRun:
    def test_expected_tables_produced(self, layer2_tables):
        assert "synth_layer2_overall_summary" in layer2_tables

    def test_overall_summary_row_count(self, layer2_tables):
        # 1 vintage_month × 2 channels = 2 rows.
        df = layer2_tables["synth_layer2_overall_summary"]
        assert df.count() == 2

    def test_overall_summary_columns(self, layer2_tables):
        df = layer2_tables["synth_layer2_overall_summary"]
        expected = {"vintage_month", "channel", "account_count", "avg_score",
                    "pct_of_channel_vintage_accounts"}
        assert expected.issubset(set(df.columns))

    def test_pct_sums_to_one(self, layer2_tables):
        df = layer2_tables["synth_layer2_overall_summary"]
        total_pct = df.agg(
            F.sum("pct_of_channel_vintage_accounts").alias("total")
        ).collect()[0]["total"]
        # 1 vintage_month × 2 channels, each partition has 1 row → total = 2.0
        assert abs(total_pct - 2.0) < 1e-9

    def test_account_count_sums_to_input_total(self, raw_df, layer2_tables):
        df = layer2_tables["synth_layer2_overall_summary"]
        total = df.agg(F.sum("account_count")).collect()[0][0]
        assert total == raw_df.count()


# ── Layer 3 dry run ──────────────────────────────────────────────────

@pytest.fixture(scope="module")
def layer3_tables(enriched_df, baseline_df, synthetic_config):
    return build_layer3(enriched_df, baseline_df, synthetic_config)


class TestLayer3DryRun:
    def test_all_six_tables_produced(self, layer3_tables):
        expected = {
            "synth_layer3_score_drift",
            "synth_layer3_feature_psi",
            "synth_layer3_data_quality",
            "synth_layer3_feature_quality",
            "synth_layer3_feature_score_relationship",
            "synth_layer3_population_mix",
        }
        assert expected == set(layer3_tables.keys())

    # score_drift
    def test_score_drift_row_count(self, layer3_tables):
        df = layer3_tables["synth_layer3_score_drift"]
        assert df.count() == 2  # 1 vintage_month × 2 channels

    def test_score_drift_psi_non_negative(self, layer3_tables):
        df = layer3_tables["synth_layer3_score_drift"]
        negatives = df.filter(F.col("score_psi") < 0).count()
        assert negatives == 0

    def test_score_drift_ks_between_0_and_1(self, layer3_tables):
        df = layer3_tables["synth_layer3_score_drift"]
        out_of_range = df.filter(
            (F.col("score_ks") < 0) | (F.col("score_ks") > 1)
        ).count()
        assert out_of_range == 0

    # feature_psi
    def test_feature_psi_row_count(self, layer3_tables):
        df = layer3_tables["synth_layer3_feature_psi"]
        # 1 vintage_month × 2 channels × 2 features = 4 rows.
        assert df.count() == 4

    def test_feature_psi_non_negative(self, layer3_tables):
        df = layer3_tables["synth_layer3_feature_psi"]
        assert df.filter(F.col("psi") < 0).count() == 0

    # data_quality
    def test_data_quality_row_count(self, layer3_tables):
        df = layer3_tables["synth_layer3_data_quality"]
        assert df.count() == 2  # 1 vintage_month × 2 channels

    def test_data_quality_rates_between_0_and_1(self, layer3_tables):
        df = layer3_tables["synth_layer3_data_quality"]
        invalid = df.filter(
            (F.col("missing_score_rate") < 0) | (F.col("missing_score_rate") > 1)
            | (F.col("outlier_score_rate") < 0) | (F.col("outlier_score_rate") > 1)
        ).count()
        assert invalid == 0

    # feature_quality
    def test_feature_quality_row_count(self, layer3_tables):
        df = layer3_tables["synth_layer3_feature_quality"]
        assert df.count() == 4  # 1 × 2 channels × 2 features

    # feature_score_relationship
    def test_feature_score_relationship_row_count(self, layer3_tables):
        df = layer3_tables["synth_layer3_feature_score_relationship"]
        assert df.count() == 4  # 1 × 2 channels × 2 features

    def test_feature_score_relationship_schema(self, layer3_tables):
        df = layer3_tables["synth_layer3_feature_score_relationship"]
        expected = {
            "vintage_month", "channel", "feature_name",
            "baseline_corr", "current_corr", "corr_change",
        }
        assert expected == set(df.columns)

    # population_mix
    def test_population_mix_segment_value_is_string(self, layer3_tables):
        df = layer3_tables["synth_layer3_population_mix"]
        for row in df.select("segment_value").collect():
            assert isinstance(row["segment_value"], str)

    def test_population_mix_pct_sums_to_one_per_channel(self, layer3_tables):
        df = layer3_tables["synth_layer3_population_mix"]
        sums = (
            df.groupBy("vintage_month", "channel", "segment_type")
            .agg(F.sum("pct_of_channel_accounts").alias("total_pct"))
            .collect()
        )
        for row in sums:
            assert abs(row["total_pct"] - 1.0) < 1e-9, (
                f"pct_of_channel_accounts does not sum to 1.0 for "
                f"({row['vintage_month']}, {row['channel']}, {row['segment_type']})"
            )
