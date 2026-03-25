"""Technical narrative generator for the Model Monitoring Report.

Language: precise, governance-friendly, no business euphemisms.
"""
from __future__ import annotations
from framework.v2.thresholds import ALERT, WARNING, OK
from framework.utils import get_logger

logger = get_logger(__name__)


def generate_mmr_narrative(context) -> str:
    """Generate technical executive narrative from BaseReportContext."""
    lines = []

    # Overall health
    lines.append(f"**Model Health: {context.overall_health}**")
    lines.append("")

    # Flag summary
    alerts = [f for f in context.flags if f.get("status") == ALERT]
    warnings = [f for f in context.flags if f.get("status") == WARNING]
    if alerts:
        lines.append(f"{len(alerts)} alert-level threshold breach(es) detected.")
    if warnings:
        lines.append(f"{len(warnings)} warning-level observation(s) noted.")
    if not alerts and not warnings:
        lines.append("All monitored metrics within acceptable thresholds.")
    lines.append("")

    # Stability findings
    for sc_id, stab in context.stability.items():
        psi = stab.get("score_psi", 0)
        status = stab.get("score_psi_status", "OK")
        if psi >= 0.10:
            lines.append(
                f"Score PSI = {psi:.4f} ({status}): "
                "distribution has shifted relative to baseline."
            )
        top_feats = [
            f for f in stab.get("feature_psi", [])
            if (f.get("psi") or f.get("psi", 0)) >= 0.10
        ][:3]
        if top_feats:
            names = ", ".join(
                f"{_feat_name(ft)} ({_feat_psi(ft):.3f})" for ft in top_feats
            )
            lines.append(f"Feature drift detected: {names}.")

    # Separation findings
    for sc_id, sep_dict in context.separation.items():
        for label, sep in sep_dict.items():
            if sep and _get(sep, "ks") is not None:
                ks = _get(sep, "ks")
                gini = _get(sep, "gini") or 0
                lines.append(
                    f"{label} separation: KS = {ks:.4f}, Gini = {gini:.4f}."
                )

    # Cohort availability
    for label, info in context.cohort_info.items():
        if not info.get("available"):
            note = info.get("note", "data not available")
            lines.append(f"Cohort {label}: {note}.")

    lines.append("")

    # Recommendations
    if alerts:
        lines.append("**Recommendation:** Escalate to model governance committee.")
    elif warnings:
        lines.append("**Recommendation:** Continue monitoring; review at next governance meeting.")
    else:
        lines.append("**Recommendation:** No action required.")

    return "\n".join(lines)


def _feat_name(ft) -> str:
    if isinstance(ft, dict):
        return ft.get("feature_name", "")
    return getattr(ft, "feature_name", "")


def _feat_psi(ft) -> float:
    if isinstance(ft, dict):
        return float(ft.get("psi", 0) or 0)
    return float(getattr(ft, "psi", 0) or 0)


def _get(obj, key, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)
