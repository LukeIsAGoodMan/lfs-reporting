"""Orchestrator: load config → read data → run layers → write outputs.

Supports two execution modes:

- **CLI / scheduled job**: call ``run(model="lfs", ...)`` without passing
  *spark*; the framework creates its own session from ``conf/framework.yaml``.
- **Notebook / interactive**: pass an existing ``spark`` session, set
  ``write_output=False`` for a dry run, and ``return_outputs=True`` to
  get all DataFrames back for inspection.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from framework.config import ModelConfig, load_framework_config, load_model_config
from framework.io import read_source_table
from framework.io import write_output as _write_output
from framework.layer1 import enrich_layer1
from framework.layer2 import build_layer2
from framework.layer3 import build_layer3
from framework.spark_session import get_or_create_spark
from framework.utils import get_logger

logger = get_logger(__name__)

_ALL_LAYERS = ["layer1", "layer2", "layer3"]


def run(
    model: str,
    score_month: str,
    model_version: str,
    layers: list[str] | None = None,
    *,
    spark: SparkSession | None = None,
    write_output: bool = True,
    return_outputs: bool = False,
) -> dict[str, DataFrame] | None:
    """Main orchestrator for the reporting framework.

    Args:
        model: Key into ``conf/models/`` registry (e.g. ``"lfs"``).
        score_month: YYYY-MM string for the current scoring vintage.
        model_version: Version tag written into output columns.
        layers: Subset of ``['layer1', 'layer2', 'layer3']``.
            Defaults to all three layers.
        spark: An existing ``SparkSession`` to reuse.  When *None*
            (CLI / scheduled mode) a new session is created from
            ``conf/framework.yaml``.
        write_output: Set to ``False`` to skip all ``saveAsTable`` calls.
            Useful for dry-run validation in notebooks.
        return_outputs: Set to ``True`` to return all produced DataFrames
            as a flat ``{table_name: DataFrame}`` dict.  When ``False``
            the function returns ``None``.

    Returns:
        ``{table_name: DataFrame}`` when *return_outputs* is ``True``,
        otherwise ``None``.

    Example — notebook dry run::

        outputs = run(
            model="lfs",
            score_month="2025-03",
            model_version="v1.0",
            spark=spark,
            write_output=False,
            return_outputs=True,
        )
        outputs["lfs_layer3_score_drift"].show()
    """
    layers = layers or _ALL_LAYERS
    logger.info(
        "Starting run: model=%s, score_month=%s, version=%s, layers=%s, "
        "write_output=%s, return_outputs=%s",
        model, score_month, model_version, layers, write_output, return_outputs,
    )

    # ── Setup ─────────────────────────────────────────────────────────
    config = load_model_config(model)
    fw_config = load_framework_config()

    if spark is None:
        spark = get_or_create_spark(
            app_name=fw_config.get("spark", {}).get("app_name", "model_reporting"),
            conf_overrides=fw_config.get("spark", {}).get("config"),
        )
        logger.info("Created new SparkSession")
    else:
        logger.info("Using provided SparkSession (app=%s)", spark.sparkContext.appName)

    # ── Read source data ──────────────────────────────────────────────
    current_df = read_source_table(spark, config, score_month=score_month)

    baseline_window = config.binning["static"]["score"].get("baseline_window")
    baseline_df = None
    if baseline_window:
        baseline_df = read_source_table(
            spark, config,
            month_range=(baseline_window[0], baseline_window[1]),
        )

    # Accumulate outputs when caller requests them.
    outputs: dict[str, DataFrame] = {}

    # ── Layer 1: account-level enrichment ─────────────────────────────
    enriched_df = None
    if "layer1" in layers:
        logger.info("Running Layer 1")
        enriched_df = enrich_layer1(
            current_df, config, baseline_df, score_month, model_version,
        )
        l1_table = config.layer1["output_table"]
        if write_output:
            _write_output(enriched_df, config, l1_table, score_month, model_version)
        if return_outputs:
            outputs[l1_table] = enriched_df

    # ── Layer 2: aggregated business tables ───────────────────────────
    if "layer2" in layers:
        logger.info("Running Layer 2")
        if enriched_df is None:
            enriched_df = enrich_layer1(
                current_df, config, baseline_df, score_month, model_version,
            )
        l2_tables = build_layer2(enriched_df, config)
        for table_name, table_df in l2_tables.items():
            if write_output:
                _write_output(table_df, config, table_name, score_month, model_version)
        if return_outputs:
            outputs.update(l2_tables)

    # ── Layer 3: model monitoring ─────────────────────────────────────
    if "layer3" in layers:
        logger.info("Running Layer 3")
        if enriched_df is None:
            enriched_df = enrich_layer1(
                current_df, config, baseline_df, score_month, model_version,
            )
        l3_tables = build_layer3(enriched_df, baseline_df, config)
        for table_name, table_df in l3_tables.items():
            if write_output:
                _write_output(table_df, config, table_name, score_month, model_version)
        if return_outputs:
            outputs.update(l3_tables)

    logger.info("Run complete for model=%s, score_month=%s", model, score_month)
    return outputs if return_outputs else None
