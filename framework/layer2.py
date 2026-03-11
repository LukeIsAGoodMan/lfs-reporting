"""Layer 2: Aggregated business tables.

Reads the enriched Layer 1 DataFrame and produces one unified
aggregated DataFrame per table spec in the model config.  Channel
is kept as a column — tables are NOT physically split by channel.

Config distinguishes two categories of tables:

- **standard_tables** — core business summaries (overall, score
  distribution, segmentation by source / line).
- **driver_tables** — optional drill-down views cross-tabulated
  by business buckets (receivable, saleamount, etc.).

Both categories use the same aggregation engine.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

from framework.config import ModelConfig
from framework.utils import get_logger

logger = get_logger(__name__)

# Map from config func names to pyspark.sql.functions callables.
_AGG_DISPATCH: dict[str, callable] = {
    "count": lambda _col: F.count(F.lit(1)),
    "avg": lambda col: F.avg(col),
    "sum": lambda col: F.sum(col),
    "min": lambda col: F.min(col),
    "max": lambda col: F.max(col),
    "stddev": lambda col: F.stddev(col),
    "countDistinct": lambda col: F.countDistinct(col),
}


def build_layer2(
    enriched_df: DataFrame,
    config: ModelConfig,
) -> dict[str, DataFrame]:
    """Build all Layer 2 aggregation tables defined in ``config.layer2``.

    Iterates over both ``standard_tables`` and ``driver_tables`` in the
    config.  For each table spec:

        1. Group by the configured ``group_by`` columns (which include
           ``channel`` — no physical per-channel split).
        2. Compute each metric via ``_apply_metrics``.
        3. Compute any ``extra_columns`` (e.g. percent-of-total windows).

    Returns:
        ``{output_table_name: DataFrame}`` — the key is the
        ``output_table`` value from each table spec in the config
        (e.g. ``"lfs_layer2_overall_summary"``).
    """
    l2_cfg = config.layer2

    # Merge both table categories into a single processing list.
    all_specs: dict[str, dict] = {}
    all_specs.update(l2_cfg.get("standard_tables", {}))
    all_specs.update(l2_cfg.get("driver_tables", {}))

    results: dict[str, DataFrame] = {}

    for table_name, spec in all_specs.items():
        output_table: str = spec["output_table"]
        group_by: list[str] = spec["group_by"]
        metrics: list[dict] = spec["metrics"]
        extra_columns: list[dict] = spec.get("extra_columns", [])

        agg_df = _apply_metrics(enriched_df, group_by, metrics)
        agg_df = _apply_extra_columns(agg_df, extra_columns)

        results[output_table] = agg_df
        logger.info(
            "Built %s (%s, %d columns)",
            output_table, table_name, len(agg_df.columns),
        )

    logger.info("Layer 2 complete — %d tables produced", len(results))
    return results


def _apply_metrics(
    df: DataFrame,
    group_by: list[str],
    metrics: list[dict],
) -> DataFrame:
    """Generic group-by + multi-metric aggregation.

    Each metric dict has the form::

        {"func": "avg", "col": "lfs_score", "alias": "avg_lfs_score"}

    ``func`` is dispatched via ``_AGG_DISPATCH``.  For ``count``,
    ``col`` is ignored (counts all rows).
    """
    agg_exprs = []
    for m in metrics:
        func_name = m["func"]
        alias = m["alias"]
        col = m.get("col")

        agg_fn = _AGG_DISPATCH.get(func_name)
        if agg_fn is None:
            raise ValueError(
                f"Unsupported aggregation function '{func_name}'. "
                f"Supported: {list(_AGG_DISPATCH.keys())}"
            )

        agg_exprs.append(agg_fn(col).alias(alias))

    return df.groupBy(*group_by).agg(*agg_exprs)


def _apply_extra_columns(
    df: DataFrame,
    extra_columns: list[dict],
) -> DataFrame:
    """Add derived columns that depend on aggregated results.

    Currently supports one type — percent-of-total within a window::

        {"name": "pct_of_channel_vintage_accounts",
         "over": ["vintage_month", "channel"],
         "source": "account_count"}

    This computes ``source / sum(source) OVER (PARTITION BY over_cols)``.
    """
    for spec in extra_columns:
        name = spec["name"]
        over_cols = spec.get("over", [])
        source_col = spec.get("source")

        if over_cols and source_col:
            window_total = F.sum(F.col(source_col)).over(
                Window.partitionBy(*over_cols)
            )
            df = df.withColumn(name, F.col(source_col) / window_total)
        else:
            logger.warning(
                "Extra column '%s' missing 'over' or 'source' — skipping", name
            )

    return df
