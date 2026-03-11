"""Layer 1: Account-level enrichment engine.

Adds metadata columns, score bins (dynamic + static), business buckets,
and data-quality flags to the raw scored table.  All rules are driven
by the model's YAML configuration.
"""
from __future__ import annotations

import importlib
from typing import Any

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from framework.config import ModelConfig
from framework.binning import (
    compute_bucket,
    compute_dynamic_bins,
    compute_static_bins,
    map_decile_to_band,
)
from framework.utils import add_expr_column, add_literal_column, get_logger

logger = get_logger(__name__)


def enrich_layer1(
    df: DataFrame,
    config: ModelConfig,
    baseline_df: DataFrame,
    score_month: str,
    model_version: str,
) -> DataFrame:
    """Apply all Layer 1 enrichments and return the enriched DataFrame.

    Steps:
        1. Add ``score_month`` and ``model_version`` as literal columns.
        2. Compute dynamic deciles (per vintage_month x channel).
        3. Map deciles to named bands.
        4. Compute static deciles from the baseline window.
        5. Add fixed-boundary business buckets.
        6. Add data-quality flag columns from config expressions.
        7. Invoke optional model hooks if configured.
    """
    src = config.source
    binning = config.binning
    l1_cfg = config.layer1

    score_col = src["score_col"]

    # ── 1. Metadata columns ──────────────────────────────────────────
    logger.info("Adding metadata columns")
    df = add_literal_column(df, "score_month", score_month)
    df = add_literal_column(df, "model_version", model_version)

    # Semantic alias: vintage_month mirrors the source month column.
    score_month_col = src["score_month_col"]
    df = df.withColumn("vintage_month", F.col(score_month_col))

    # ── 2. Dynamic deciles (vintage_month × channel) ─────────────────
    dyn_cfg = binning["dynamic"]["score"]
    logger.info(
        "Computing dynamic deciles: %s bins by %s",
        dyn_cfg["n_bins"],
        dyn_cfg["partition_cols"],
    )
    primary_key = src["key_cols"][0]  # creditaccountid
    df = compute_dynamic_bins(
        df,
        score_col=score_col,
        n_bins=dyn_cfg["n_bins"],
        partition_cols=dyn_cfg["partition_cols"],
        output_col=dyn_cfg["output_col_decile"],
        tiebreak_col=primary_key,
    )

    # ── 3. Map deciles → bands ───────────────────────────────────────
    band_mapping = binning["dynamic"].get("bands", [])
    if band_mapping:
        logger.info("Mapping deciles to bands")
        df = map_decile_to_band(
            df,
            decile_col=dyn_cfg["output_col_decile"],
            band_mapping=band_mapping,
            output_col=dyn_cfg["output_col_band"],
        )

    # ── 4. Static deciles (baseline window × channel) ────────────────
    static_cfg = binning["static"]["score"]
    logger.info(
        "Computing static deciles from baseline %s",
        static_cfg["baseline_window"],
    )
    df = compute_static_bins(
        df,
        baseline_df=baseline_df,
        score_col=score_col,
        n_bins=static_cfg["n_bins"],
        partition_cols=static_cfg["partition_cols"],
        output_col=static_cfg["output_col"],
    )

    # ── 5. Business buckets ──────────────────────────────────────────
    for bucket_name, bucket_cfg in binning.get("buckets", {}).items():
        logger.info("Adding bucket: %s", bucket_name)
        df = compute_bucket(
            df,
            col=bucket_cfg["column"],
            boundaries=bucket_cfg["boundaries"],
            labels=bucket_cfg["labels"],
            output_col=bucket_name,
        )

    # ── 6. Data-quality flags ────────────────────────────────────────
    for extra in l1_cfg.get("extra_columns", []):
        logger.info("Adding DQ column: %s", extra["name"])
        df = add_expr_column(df, extra["name"], extra["expr"])

    # ── 7. Model hooks ───────────────────────────────────────────────
    if config.hooks_module:
        df = _invoke_hook(df, config.hooks_module, "post_layer1_hook")

    logger.info("Layer 1 enrichment complete — %d columns", len(df.columns))
    return df


def _invoke_hook(df: DataFrame, hooks_module: str, func_name: str) -> DataFrame:
    """Dynamically import *hooks_module* and call *func_name*(df) if it exists."""
    try:
        mod = importlib.import_module(hooks_module)
    except ModuleNotFoundError:
        logger.warning("Hooks module '%s' not found — skipping", hooks_module)
        return df

    hook_fn = getattr(mod, func_name, None)
    if hook_fn is None:
        logger.debug("No '%s' in '%s' — skipping", func_name, hooks_module)
        return df

    logger.info("Invoking hook %s.%s", hooks_module, func_name)
    return hook_fn(df)
