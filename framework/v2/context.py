"""Shared report context — precomputed metrics consumed by both renderers.

Neither the MMR nor the Business report should recompute any metric.
Both consume this context object, which is built by the runner after
all metric computation is complete.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from framework.v2.metrics.schemas import (
    AttributionResult,
    na_display,
)
from framework.utils import get_logger

logger = get_logger(__name__)


@dataclass
class BaseReportContext:
    """Immutable snapshot of all monitoring results for report rendering."""

    # Identity
    reporting_month: str = ""
    model_name: str = ""
    display_name: str = ""
    data_mode: str = ""          # "MOCK/INJECTED" | "CONFIG-SOURCED"
    run_timestamp: str = ""

    # Precomputed metric results (per scorecard_id)
    stability: dict = field(default_factory=dict)
    separation: dict = field(default_factory=dict)
    performance: dict = field(default_factory=dict)
    calibration: dict = field(default_factory=dict)
    data_quality: dict = field(default_factory=dict)

    # Metadata
    baseline_info: dict = field(default_factory=dict)
    cohort_info: dict = field(default_factory=dict)
    flags: list = field(default_factory=list)

    # Optional
    attribution: AttributionResult | None = None
    explanation: dict | None = None

    # Derived (set by build_context)
    overall_health: str = "OK"
    section_visibility: dict = field(default_factory=dict)
    na_reasons: dict = field(default_factory=dict)


def build_context(result, config) -> BaseReportContext:
    """Build a BaseReportContext from a MonitoringResult.

    This is the single bridge between the metric engine and the
    report renderers. Called once by the runner after all metrics
    are computed.
    """
    from framework.v2.thresholds import ALERT, WARNING

    ctx = BaseReportContext(
        reporting_month=result.reporting_month,
        model_name=result.model_name,
        display_name=config.display_name,
        data_mode=getattr(result, "data_mode", ""),
        run_timestamp=result.run_timestamp,
        stability=result.stability,
        separation=result.separation,
        performance=result.performance,
        calibration=result.calibration,
        data_quality=result.data_quality,
        baseline_info=result.baseline_info,
        cohort_info=result.cohort_info,
        flags=result.flags,
        explanation=result.explanation,
    )

    # ── Overall health ────────────────────────────────────────────
    alerts = [f for f in result.flags if f.get("status") == ALERT]
    warnings = [f for f in result.flags if f.get("status") == WARNING]
    if alerts:
        ctx.overall_health = ALERT
    elif warnings:
        ctx.overall_health = WARNING
    else:
        ctx.overall_health = "OK"

    # ── Section visibility ────────────────────────────────────────
    ctx.section_visibility = compute_visibility(ctx)

    # ── N/A reasons ───────────────────────────────────────────────
    ctx.na_reasons = collect_na_reasons(ctx)

    return ctx


def compute_visibility(ctx: BaseReportContext) -> dict[str, bool]:
    """Determine which report sections have renderable content."""
    has_stability = bool(ctx.stability)
    has_separation = any(
        bool(sep_dict) for sep_dict in ctx.separation.values()
    )
    has_performance = any(
        bool(rows) for rows in ctx.performance.values()
    )
    has_calibration = any(
        bool(calib_dict) for calib_dict in ctx.calibration.values()
    )
    has_dq = bool(ctx.data_quality)
    has_flags = bool(ctx.flags)
    has_attribution = (
        ctx.attribution is not None
        and getattr(ctx.attribution, "is_material", False)
    )

    return {
        "stability": has_stability,
        "separation": has_separation,
        "performance": has_performance,
        "calibration": has_calibration,
        "data_quality": has_dq,
        "governance_flags": has_flags,
        "attribution": has_attribution,
    }


def collect_na_reasons(ctx: BaseReportContext) -> dict[str, str]:
    """Collect N/A reasons for unavailable sections."""
    from framework.v2.metrics.schemas import NA_NO_ACTUALS, NA_MATURITY

    reasons = {}

    if not ctx.section_visibility.get("separation"):
        reasons["separation"] = NA_NO_ACTUALS
    if not ctx.section_visibility.get("performance"):
        reasons["performance"] = NA_NO_ACTUALS
    if not ctx.section_visibility.get("calibration"):
        reasons["calibration"] = NA_MATURITY

    # Cohort-level reasons
    for label, info in ctx.cohort_info.items():
        if not info.get("available"):
            note = info.get("note", "")
            reasons[f"cohort_{label}"] = note or NA_MATURITY

    return reasons
