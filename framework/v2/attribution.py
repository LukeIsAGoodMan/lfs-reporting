"""Business attribution engine — explains month-over-month movement.

Runs ONLY when:
1. attribution is enabled in config
2. prior-period data is available
3. the observed delta exceeds min_materiality

Decomposes output movement (e.g. EDR90 change) into contributions from
configured driver columns using a mix-shift / within-shift decomposition.
"""
from __future__ import annotations
from dataclasses import dataclass, field
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from framework.v2.metrics.schemas import AttributionResult, AttributionDriver, NA_NO_ACTUALS
from framework.utils import get_logger

logger = get_logger(__name__)


@dataclass
class AttributionConfig:
    """Config for business attribution analysis."""
    enabled: bool = False
    compare_to: str = "prior_month"       # prior_month | baseline
    driver_columns: list[str] = field(default_factory=lambda: ["channel", "source"])
    max_drivers: int = 3
    min_materiality: float = 0.02
    narrative_mode: str = "plain_english"


def compute_attribution(
    current_df: DataFrame | None,
    prior_df: DataFrame | None,
    metric_col: str,
    target_col: str,
    config: AttributionConfig,
) -> AttributionResult:
    """Compute attribution of movement in a target metric.

    Uses mix-shift decomposition: for each driver_column, computes how
    much of the overall delta is explained by (a) population mix change
    vs (b) within-segment rate change.

    Args:
        current_df: Current period scored + actuals DataFrame.
        prior_df: Prior period DataFrame (same schema).
        metric_col: Display name (e.g. "EDR90").
        target_col: Column name for the outcome (e.g. "edr90_m9").
        config: Attribution configuration.

    Returns:
        AttributionResult with drivers and narrative.
    """
    if not config.enabled:
        return AttributionResult(na_reason="Attribution disabled in config")

    if current_df is None or prior_df is None:
        return AttributionResult(na_reason=NA_NO_ACTUALS)

    if target_col not in current_df.columns or target_col not in prior_df.columns:
        return AttributionResult(
            na_reason=f"Target column '{target_col}' not in data"
        )

    # Overall rates
    cur_rate = _safe_rate(current_df, target_col)
    pri_rate = _safe_rate(prior_df, target_col)

    if cur_rate is None or pri_rate is None:
        return AttributionResult(na_reason=NA_NO_ACTUALS)

    delta = cur_rate - pri_rate

    # Materiality check
    if abs(delta) < config.min_materiality:
        return AttributionResult(
            is_material=False,
            delta=delta,
            metric_name=metric_col,
            narrative=f"{metric_col} moved by {delta:+.2%}, below the {config.min_materiality:.0%} materiality threshold.",
        )

    # Decompose by each driver column
    all_drivers: list[AttributionDriver] = []
    for dim in config.driver_columns:
        if dim not in current_df.columns or dim not in prior_df.columns:
            continue
        drivers = _decompose_by_dimension(
            current_df, prior_df, target_col, dim, delta,
        )
        all_drivers.extend(drivers)

    # Sort by absolute contribution, take top N
    all_drivers.sort(key=lambda d: abs(d.contribution), reverse=True)
    top = all_drivers[:config.max_drivers]

    # Generate narrative
    narrative = _build_narrative(metric_col, delta, top)

    return AttributionResult(
        is_material=True,
        delta=delta,
        metric_name=metric_col,
        drivers=top,
        narrative=narrative,
    )


def _safe_rate(df: DataFrame, col: str) -> float | None:
    """Compute avg of a column, return None if empty."""
    row = df.agg(F.avg(col).alias("r"), F.count("*").alias("n")).collect()[0]
    if row["n"] == 0 or row["r"] is None:
        return None
    return float(row["r"])


def _decompose_by_dimension(
    current_df: DataFrame,
    prior_df: DataFrame,
    target_col: str,
    dim: str,
    total_delta: float,
) -> list[AttributionDriver]:
    """Mix-shift decomposition for one dimension."""
    cur_stats = _segment_stats(current_df, target_col, dim)
    pri_stats = _segment_stats(prior_df, target_col, dim)

    all_segments = set(cur_stats.keys()) | set(pri_stats.keys())
    cur_total = sum(s["n"] for s in cur_stats.values())
    pri_total = sum(s["n"] for s in pri_stats.values())

    drivers = []
    for seg in all_segments:
        cur = cur_stats.get(seg, {"rate": 0.0, "n": 0})
        pri = pri_stats.get(seg, {"rate": 0.0, "n": 0})

        cur_mix = cur["n"] / cur_total if cur_total > 0 else 0
        pri_mix = pri["n"] / pri_total if pri_total > 0 else 0

        # Mix effect: change in segment weight × prior rate
        mix_effect = (cur_mix - pri_mix) * pri.get("rate", 0)
        # Rate effect: change in segment rate × current weight
        rate_effect = (cur.get("rate", 0) - pri.get("rate", 0)) * cur_mix

        contribution = mix_effect + rate_effect
        if abs(contribution) < 0.001:
            continue

        direction = "up" if contribution > 0 else "down"
        explanation = _explain_driver(dim, seg, contribution, mix_effect, rate_effect)

        drivers.append(AttributionDriver(
            dimension=dim,
            segment=str(seg),
            contribution=round(contribution, 6),
            direction=direction,
            explanation=explanation,
        ))

    return drivers


def _segment_stats(df: DataFrame, target_col: str, dim: str) -> dict:
    """Compute rate and count per segment."""
    rows = (
        df.groupBy(dim)
        .agg(F.avg(target_col).alias("rate"), F.count("*").alias("n"))
        .collect()
    )
    return {
        str(r[dim]): {"rate": float(r["rate"]) if r["rate"] is not None else 0.0, "n": int(r["n"])}
        for r in rows if r[dim] is not None
    }


def _explain_driver(dim: str, seg: str, contribution: float, mix_eff: float, rate_eff: float) -> str:
    """Generate a plain-English explanation for one driver."""
    parts = []
    direction = "increased" if contribution > 0 else "decreased"

    if abs(mix_eff) > abs(rate_eff):
        mix_dir = "grew" if mix_eff > 0 else "shrank"
        parts.append(f"the {seg} segment {mix_dir} as a share of the portfolio")
    else:
        rate_dir = "worsened" if rate_eff > 0 else "improved"
        parts.append(f"charge-off rates {rate_dir} within the {seg} segment")

    return f"{dim.capitalize()} — {seg}: {', '.join(parts)} ({contribution:+.2%} contribution)."


def _build_narrative(metric: str, delta: float, drivers: list[AttributionDriver]) -> str:
    """Build a plain-English summary of the movement."""
    direction = "increased" if delta > 0 else "decreased"
    lines = [f"{metric} {direction} by {abs(delta):.2%} month-over-month."]

    if drivers:
        lines.append("Key drivers:")
        for d in drivers:
            lines.append(f"  - {d.explanation}")
    else:
        lines.append("No single driver explains a material portion of the movement.")

    return "\n".join(lines)
