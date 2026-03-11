"""Generic binning engine: dynamic quantile bins, static bins, fixed-boundary buckets."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

from framework.utils import get_logger

logger = get_logger(__name__)


def compute_dynamic_bins(
    df: DataFrame,
    score_col: str,
    n_bins: int,
    partition_cols: list[str],
    output_col: str,
    tiebreak_col: str | None = None,
) -> DataFrame:
    """Add quantile-based bin assignments computed per partition group.

    Uses ``ntile(n_bins)`` over a window partitioned by *partition_cols*,
    ordered by *score_col* ascending.  Bin 1 = lowest scores.

    When *tiebreak_col* is provided it is used as a secondary sort key
    so that rows with identical scores are assigned to bins
    deterministically across runs.

    Returns *df* with *output_col* appended (int 1 … n_bins).
    """
    order_cols = [F.col(score_col)]
    if tiebreak_col:
        order_cols.append(F.col(tiebreak_col))
    window = Window.partitionBy(*partition_cols).orderBy(*order_cols)
    return df.withColumn(output_col, F.ntile(n_bins).over(window))


def compute_static_bins(
    df: DataFrame,
    baseline_df: DataFrame,
    score_col: str,
    n_bins: int,
    partition_cols: list[str],
    output_col: str,
) -> DataFrame:
    """Compute bin edges from *baseline_df*, then apply them to *df*.

    Steps:
        1. For each distinct group in *partition_cols* within *baseline_df*,
           compute (n_bins - 1) quantile boundaries on *score_col*.
        2. Collect the boundary arrays into a small lookup (one row per group).
        3. Join to *df* on *partition_cols*, then use a UDF / CASE chain to
           assign each row to a bin based on where its score falls.
    """
    quantile_fractions = [i / n_bins for i in range(1, n_bins)]

    # Compute quantile boundaries per partition group from the baseline.
    boundary_rows: list = []
    for group_row in baseline_df.select(partition_cols).distinct().collect():
        group_filter = baseline_df
        group_vals: dict = {}
        for col_name in partition_cols:
            val = group_row[col_name]
            group_filter = group_filter.filter(F.col(col_name) == val)
            group_vals[col_name] = val

        approx_boundaries = group_filter.stat.approxQuantile(
            score_col, quantile_fractions, 0.01
        )
        group_vals["_bin_edges"] = approx_boundaries
        boundary_rows.append(group_vals)

    if not boundary_rows:
        logger.warning("No baseline groups found — returning df without static bins")
        return df.withColumn(output_col, F.lit(None).cast("int"))

    # Build a broadcast-friendly edges DataFrame.
    from pyspark.sql.types import ArrayType, DoubleType, StructType, StructField, StringType

    spark = df.sparkSession
    edges_schema_fields = [
        StructField(c, StringType(), True) for c in partition_cols
    ] + [StructField("_bin_edges", ArrayType(DoubleType()), True)]
    edges_schema = StructType(edges_schema_fields)
    edges_df = spark.createDataFrame(boundary_rows, schema=edges_schema)

    # Join edges onto the main df.
    df_with_edges = df.join(F.broadcast(edges_df), on=partition_cols, how="left")

    # Assign bin: null score → null bin; otherwise count how many edges
    # the score exceeds, then +1.
    edges_missing = F.col("_bin_edges").isNull()
    score_missing = F.col(score_col).isNull()
    bin_expr = (
        F.when(edges_missing | score_missing, F.lit(None).cast("int"))
        .otherwise(
            F.size(
                F.filter(
                    F.col("_bin_edges"),
                    lambda edge: F.col(score_col) > edge,
                )
            )
            + 1
        )
    )
    df_binned = df_with_edges.withColumn(output_col, bin_expr).drop("_bin_edges")

    return df_binned


def compute_bucket(
    df: DataFrame,
    col: str,
    boundaries: list[float],
    labels: list[str],
    output_col: str,
) -> DataFrame:
    """Assign a fixed-boundary bucket label to each row.

    *boundaries* defines the split points; *labels* has len(boundaries) entries
    corresponding to the intervals they delimit.

    Example:
        boundaries=[0, 500, 1000], labels=["0-500", "500-1K", "1K+"]
        → value 300 gets "0-500", value 700 gets "500-1K", value 2000 gets "1K+"
    """
    # bucket 0: col <= boundaries[0]
    # bucket i (1..n-2): boundaries[i-1] < col <= boundaries[i]
    # bucket n-1 (last): col > boundaries[-1]
    case_expr = F.when(F.col(col).isNull(), F.lit(None))
    for i, label in enumerate(labels):
        if i == 0:
            case_expr = case_expr.when(F.col(col) <= boundaries[0], F.lit(label))
        elif i < len(boundaries):
            case_expr = case_expr.when(
                (F.col(col) > boundaries[i - 1]) & (F.col(col) <= boundaries[i]),
                F.lit(label),
            )
        else:
            # Last bucket: everything above the final boundary.
            case_expr = case_expr.when(F.col(col) > boundaries[-1], F.lit(label))

    return df.withColumn(output_col, case_expr)


def map_decile_to_band(
    df: DataFrame,
    decile_col: str,
    band_mapping: list[dict],
    output_col: str,
) -> DataFrame:
    """Map numeric decile values to named bands (e.g. Low / Medium / High).

    *band_mapping*: ``[{"name": "Low", "deciles": [1,2,3]}, ...]``
    """
    case_expr = F.when(F.col(decile_col).isNull(), F.lit(None))
    for band in band_mapping:
        name = band["name"]
        deciles = band["deciles"]
        case_expr = case_expr.when(F.col(decile_col).isin(deciles), F.lit(name))

    return df.withColumn(output_col, case_expr)
