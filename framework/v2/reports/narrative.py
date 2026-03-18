"""Automated narrative generation for v2 monitoring reports."""
from __future__ import annotations

from framework.v2.config import MonitoringConfig
from framework.v2.thresholds import ThresholdEngine, ALERT, WARNING, OK
from framework.utils import get_logger

logger = get_logger(__name__)


def generate_executive_narrative(
    result,  # MonitoringResult (avoid circular import)
    config: MonitoringConfig,
    thresholds: ThresholdEngine,
) -> str:
    """Generate a human-readable executive narrative.

    Returns a multi-paragraph text summarising the model's health,
    key findings, and recommendations.  Suitable for inclusion in both
    the business-facing and technical monitoring reports.
    """
    lines: list[str] = []

    # Overall health
    health = _assess_overall_health(result, thresholds)
    lines.append(f"**Model Health: {health['status']}**")
    lines.append("")

    # Key findings
    lines.append("**Key Findings:**")
    for finding in health["findings"]:
        lines.append(f"- {finding}")
    lines.append("")

    # Performance outlook (if available)
    perf_summary = _summarize_performance(result, config)
    if perf_summary:
        lines.append("**Performance Outlook:**")
        for p in perf_summary:
            lines.append(f"- {p}")
        lines.append("")

    # Recommendations
    recs = _generate_recommendations(result, health, config)
    if recs:
        lines.append("**Recommendations:**")
        for r in recs:
            lines.append(f"- {r}")
        lines.append("")

    return "\n".join(lines)


def _assess_overall_health(result, thresholds: ThresholdEngine) -> dict:
    """Determine overall model health from all metrics."""
    alerts = [f for f in result.flags if f["status"] == ALERT]
    warnings = [f for f in result.flags if f["status"] == WARNING]

    findings: list[str] = []

    if alerts:
        status = ALERT
        findings.append(f"{len(alerts)} alert-level threshold breaches detected.")
    elif warnings:
        status = WARNING
        findings.append(f"{len(warnings)} warning-level observations noted.")
    else:
        status = OK
        findings.append("All monitored metrics are within acceptable thresholds.")

    # Stability findings
    for sc_id, stab in result.stability.items():
        psi_val = stab.get("score_psi", 0)
        psi_status = stab.get("score_psi_status", OK)
        if psi_val is not None and psi_val >= 0.10:
            findings.append(
                f"Score PSI = {psi_val:.4f} ({psi_status}) "
                f"-- score distribution has shifted vs baseline."
            )
        elif psi_val is not None and psi_val < 0.10:
            findings.append(
                f"Score PSI = {psi_val:.4f} -- score distribution is stable."
            )

        # Top drifting features
        top_feats = sorted(
            [f for f in stab.get("feature_psi", []) if f.get("psi", 0) >= 0.10],
            key=lambda f: f.get("psi", 0),
            reverse=True,
        )[:3]
        if top_feats:
            names = ", ".join(f"{f['feature_name']} ({f['psi']:.3f})" for f in top_feats)
            findings.append(f"Feature drift detected in: {names}.")

    # Separation findings
    for sc_id, sep_dict in result.separation.items():
        for label, sep in sep_dict.items():
            if sep and sep.get("ks") is not None:
                ks_val = sep["ks"]
                gini_val = sep.get("gini", 0)
                ks_status = sep.get("ks_drop_status", OK)
                findings.append(
                    f"{label} KS = {ks_val:.4f}, Gini = {gini_val:.4f} ({ks_status})."
                )

    # Data quality
    for sc_id, dq in result.data_quality.items():
        high_missing = [
            c for c in dq.get("columns", [])
            if c.get("missing_rate", 0) >= 0.01
        ]
        if high_missing:
            names = ", ".join(c["column"] for c in high_missing[:3])
            findings.append(f"Elevated missing rates in: {names}.")

    return {"status": status, "findings": findings, "alerts": alerts, "warnings": warnings}


def _summarize_performance(result, config: MonitoringConfig) -> list[str]:
    """Summarise performance metrics across maturity windows."""
    summaries: list[str] = []

    for sc_id, perf_rows in result.performance.items():
        for row in perf_rows:
            # Only include the 'all' channel row to avoid duplication
            if row.get("channel") not in ("all", None):
                continue

            if row.get("note"):
                summaries.append(f"{row.get('maturity', 'N/A')}: {row['note']}")
                continue

            edr = row.get("edr")
            bad_rate = row.get("bad_rate")
            label = row.get("maturity", "")
            acct = row.get("account_count", 0)

            if edr is not None:
                summaries.append(
                    f"{label} EDR = {edr:.2%} "
                    f"(bad rate: {bad_rate:.2%}, {acct:,} accounts)"
                )

    return summaries


def _generate_recommendations(result, health: dict, config: MonitoringConfig) -> list[str]:
    """Generate actionable recommendations based on health assessment."""
    recs: list[str] = []

    if health["status"] == ALERT:
        recs.append("Escalate to model governance committee for review.")

        # Specific alert types
        psi_alerts = [f for f in health["alerts"] if "psi" in f["metric"]]
        if psi_alerts:
            recs.append(
                "Investigate upstream data pipelines -- score distribution shift detected."
            )

        calib_alerts = [f for f in health["alerts"] if "calibration" in f["metric"]]
        if calib_alerts:
            recs.append(
                "Schedule model recalibration -- predicted vs actual rates diverging."
            )

        perf_alerts = [f for f in health["alerts"] if "edr" in f["metric"]]
        if perf_alerts:
            recs.append(
                "Review early default rates -- performance degradation observed."
            )

    elif health["status"] == WARNING:
        recs.append("Continue monitoring -- no immediate action required.")
        recs.append("Review trends at next monthly governance meeting.")

        psi_warnings = [f for f in health["warnings"] if "psi" in f["metric"]]
        if psi_warnings:
            recs.append("Watch for further score distribution drift in coming months.")

    else:
        recs.append("No action required. Model operating within normal parameters.")

    return recs
