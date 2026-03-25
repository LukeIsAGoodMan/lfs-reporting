"""Business narrative generator for the Business Digest Report.

Language: concise, plain English, charge-off focused.
No raw PSI/KS/Gini terminology unless translated.
"""
from __future__ import annotations
from framework.v2.reports._fmt import pct
from framework.utils import get_logger

logger = get_logger(__name__)


def generate_business_narrative(context) -> str:
    """Generate business-facing executive summary from BaseReportContext."""
    lines = []

    # Health in business terms
    health_map = {
        "ALERT": "Action Required",
        "WARNING": "Under Observation",
        "OK": "On Track",
    }
    health_label = health_map.get(context.overall_health, context.overall_health)
    lines.append(f"**Overall Status: {health_label}**")
    lines.append("")

    # Key observations in plain English
    observations = _business_observations(context)
    if observations:
        for obs in observations:
            lines.append(f"- {obs}")
        lines.append("")

    # Attribution summary (if material)
    if context.attribution and getattr(context.attribution, "is_material", False):
        lines.append("**Movement Summary:**")
        lines.append(context.attribution.narrative if hasattr(context.attribution, "narrative") else "")
        lines.append("")

    return "\n".join(lines)


def _business_observations(context) -> list[str]:
    """Generate 3-5 bullet-point observations in business language."""
    obs = []

    # Score stability -> "scoring pattern"
    for sc_id, stab in context.stability.items():
        psi_val = stab.get("score_psi", 0)
        if psi_val >= 0.25:
            obs.append(
                "The scoring pattern has shifted significantly since the baseline period. "
                "This may affect charge-off prediction accuracy."
            )
        elif psi_val >= 0.10:
            obs.append(
                "A mild shift in the scoring pattern has been detected. "
                "Monitoring will continue."
            )

    # Performance -> charge-off language
    for sc_id, perf_rows in context.performance.items():
        if isinstance(perf_rows, list):
            for row in perf_rows:
                if not isinstance(row, dict):
                    continue
                if row.get("channel") != "all":
                    continue
                label = row.get("display_label") or row.get("maturity", "")
                edr = row.get("edr")
                if edr is not None and label:
                    obs.append(
                        f"{label} early delinquency rate is {pct(edr)}."
                    )

    # Calibration -> simple summary
    for sc_id, calib_dict in context.calibration.items():
        if isinstance(calib_dict, dict):
            m12 = calib_dict.get("M12")
            if m12 and isinstance(m12, list):
                gaps = [abs(float(r.get("calibration_gap", 0) or 0)) for r in m12 if isinstance(r, dict)]
                if gaps:
                    max_gap = max(gaps)
                    if max_gap > 0.05:
                        obs.append(
                            f"The model's charge-off predictions differ from observed outcomes "
                            f"by up to {pct(max_gap)} in some score segments."
                        )

    # Cohort availability
    unavailable = [
        label for label, info in context.cohort_info.items()
        if not info.get("available")
    ]
    if unavailable:
        labels = ", ".join(unavailable)
        obs.append(
            f"Performance data for {labels} is not yet available due to maturity requirements."
        )

    if not obs:
        obs.append("All monitored dimensions are within normal ranges.")

    return obs[:5]  # Cap at 5 observations
