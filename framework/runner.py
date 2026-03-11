"""Orchestrator: load config → read data → run layers → write outputs."""
from __future__ import annotations

from framework.config import ModelConfig, load_framework_config, load_model_config
from framework.io import read_source_table, write_output
from framework.layer1 import enrich_layer1
from framework.layer2 import build_layer2
from framework.layer3 import build_layer3
from framework.spark_session import get_or_create_spark
from framework.utils import get_logger

logger = get_logger(__name__)

_ALL_LAYERS = ["layer1", "layer2", "layer3"]


def run(
    model_name: str,
    score_month: str,
    model_version: str,
    layers: list[str] | None = None,
) -> None:
    """Main orchestrator for the reporting framework.

    Args:
        model_name: Key into ``conf/models/`` registry (e.g. ``"lfs"``).
        score_month: YYYY-MM string for the current scoring vintage.
        model_version: Version tag written into output columns.
        layers: Subset of ``['layer1', 'layer2', 'layer3']``.  Defaults to all.
    """
    layers = layers or _ALL_LAYERS
    logger.info("Starting run: model=%s, score_month=%s, version=%s, layers=%s",
                model_name, score_month, model_version, layers)

    # ── Setup ─────────────────────────────────────────────────────────
    config = load_model_config(model_name)
    fw_config = load_framework_config()

    spark = get_or_create_spark(
        app_name=fw_config.get("spark", {}).get("app_name", "model_reporting"),
        conf_overrides=fw_config.get("spark", {}).get("config"),
    )

    # ── Read source data ──────────────────────────────────────────────
    current_df = read_source_table(spark, config, score_month=score_month)

    baseline_window = config.binning["static"]["score"].get("baseline_window")
    baseline_df = None
    if baseline_window:
        baseline_df = read_source_table(
            spark, config,
            month_range=(baseline_window[0], baseline_window[1]),
        )

    # ── Layer 1: account-level enrichment ─────────────────────────────
    enriched_df = None
    if "layer1" in layers:
        logger.info("Running Layer 1")
        enriched_df = enrich_layer1(
            current_df, config, baseline_df, score_month, model_version,
        )
        write_output(
            enriched_df, config,
            config.layer1["output_table"],
            score_month, model_version,
        )

    # ── Layer 2: aggregated business tables ───────────────────────────
    if "layer2" in layers:
        logger.info("Running Layer 2")
        if enriched_df is None:
            enriched_df = enrich_layer1(
                current_df, config, baseline_df, score_month, model_version,
            )
        l2_tables = build_layer2(enriched_df, config)
        for table_name, table_df in l2_tables.items():
            write_output(
                table_df, config, table_name,
                score_month, model_version,
            )

    # ── Layer 3: model monitoring ─────────────────────────────────────
    if "layer3" in layers:
        logger.info("Running Layer 3")
        if enriched_df is None:
            enriched_df = enrich_layer1(
                current_df, config, baseline_df, score_month, model_version,
            )
        l3_tables = build_layer3(enriched_df, baseline_df, config)
        for table_name, table_df in l3_tables.items():
            write_output(
                table_df, config, table_name,
                score_month, model_version,
            )

    logger.info("Run complete for model=%s, score_month=%s", model_name, score_month)
