"""Business-facing monitoring report builder (Markdown -> HTML).

Generates a report for non-technical stakeholders using plain language,
no raw metric names, and clear visual cues for model health status.
"""
from __future__ import annotations

import datetime
from pathlib import Path

from framework.v2.config import MonitoringConfig
from framework.v2.thresholds import ThresholdEngine, ALERT, WARNING, OK
from framework.v2.reports.narrative import generate_executive_narrative
from framework.report_builder import _md_to_html, _detect_html_backend
from framework.utils import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _f(val, decimals: int = 4) -> str:
    """Format a numeric value as a fixed-decimal string; returns '--' for None."""
    if val is None:
        return "--"
    try:
        return f"{float(val):.{decimals}f}"
    except (TypeError, ValueError):
        return str(val)


def _pct(val, decimals: int = 2) -> str:
    """Format a value as a percentage string; returns '--' for None."""
    if val is None:
        return "--"
    try:
        return f"{float(val) * 100:.{decimals}f}%"
    except (TypeError, ValueError):
        return str(val)


def _tbl(headers: list[str], rows: list[list[str]]) -> str:
    """Build a markdown table from headers and rows."""
    if not rows:
        return "_No data available._\n"
    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join("---" for _ in headers) + " |")
    for row in rows:
        lines.append("| " + " | ".join(str(c) for c in row) + " |")
    lines.append("")
    return "\n".join(lines)


def _status_label(status: str) -> str:
    """Convert a threshold status to a human-readable business label."""
    if status == ALERT:
        return "Requires Attention"
    if status == WARNING:
        return "Under Observation"
    return "Healthy"


def _status_icon(status: str) -> str:
    """Return a plain-text status indicator."""
    if status == ALERT:
        return "[ALERT]"
    if status == WARNING:
        return "[WARNING]"
    return "[OK]"


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------

def _section_executive_summary(result, config: MonitoringConfig, thresholds: ThresholdEngine) -> str:
    """Executive summary section with auto-generated narrative."""
    lines = ["## Executive Summary", ""]
    narrative = generate_executive_narrative(result, config, thresholds)
    lines.append(narrative)
    return "\n".join(lines)


def _section_portfolio_summary(result, config: MonitoringConfig) -> str:
    """Portfolio summary table: accounts, average score, bad rates by channel."""
    lines = ["## Portfolio Summary", ""]

    headers = ["Metric", "Value"]
    rows = []

    # Aggregate across all scorecard groups
    for sc_id, stab in result.stability.items():
        label = sc_id if sc_id != "overall" else "Overall"

        acct_count = stab.get("current_accounts", stab.get("account_count", "--"))
        avg_score = stab.get("current_mean", stab.get("mean_score"))
        bl_count = result.baseline_info.get("account_count", "--")

        rows.append(["Reporting Month", result.reporting_month])
        rows.append(["Population", label])
        rows.append(["Current Accounts", f"{acct_count:,}" if isinstance(acct_count, (int, float)) else str(acct_count)])
        rows.append(["Baseline Accounts", f"{bl_count:,}" if isinstance(bl_count, (int, float)) else str(bl_count)])
        rows.append(["Average Score (Current)", _f(avg_score, 2)])

    lines.append(_tbl(headers, rows))

    # Channel breakdown from performance
    channel_rows = []
    for sc_id, perf_rows in result.performance.items():
        for row in perf_rows:
            channel = row.get("channel", "all")
            display_label = row.get("display_label") or row.get("maturity", "")
            edr = row.get("edr")
            bad_rate = row.get("bad_rate")
            acct = row.get("account_count", 0)
            if edr is not None:
                channel_rows.append([
                    channel.title() if channel else "--",
                    display_label,
                    f"{acct:,}" if isinstance(acct, (int, float)) else str(acct),
                    _pct(bad_rate),
                    _pct(edr),
                ])

    if channel_rows:
        lines.append("### Early Performance by Channel")
        lines.append("")
        lines.append(_tbl(
            ["Channel", "Window", "Accounts", "Bad Rate", "Default Rate"],
            channel_rows,
        ))

    return "\n".join(lines)


def _section_segment_breakdown(result, config: MonitoringConfig) -> str:
    """Segment breakdown from stability channel data."""
    lines = ["## Segment Breakdown", ""]

    has_data = False
    for sc_id, stab in result.stability.items():
        channel_psi = stab.get("channel_psi", [])
        if channel_psi:
            has_data = True
            headers = ["Segment", "Score Shift (PSI)", "Status"]
            rows = []
            for ch in channel_psi:
                rows.append([
                    ch.get("channel", "--"),
                    _f(ch.get("psi"), 4),
                    _status_label(ch.get("status", OK)),
                ])
            lines.append(_tbl(headers, rows))

    if not has_data:
        lines.append("_No segment-level data available for this reporting period._")
        lines.append("")

    return "\n".join(lines)


def _section_early_performance(result, config: MonitoringConfig) -> str:
    """Early performance table by maturity window and channel."""
    lines = ["## Early Performance Indicators", ""]

    headers = ["Window", "Rate", "Status", "Note"]
    rows = []

    for sc_id, perf_rows in result.performance.items():
        for row in perf_rows:
            display_label = row.get("display_label") or row.get("maturity", "--")
            note = row.get("note")
            if note:
                rows.append([display_label, "--", "--", note])
                continue

            edr = row.get("edr")
            status = row.get("edr_delta_status", OK)

            rows.append([
                display_label,
                _pct(edr),
                _status_label(status),
                "",
            ])

    if rows:
        lines.append(_tbl(headers, rows))
    else:
        lines.append("_Performance data not yet available (cohorts still maturing)._")
        lines.append("")

    # M12 calibration one-line summary
    for sc_id, calib_dict in result.calibration.items():
        m12_rows = calib_dict.get("M12") or []
        if m12_rows:
            gaps = [abs(r.get("calibration_gap", 0)) for r in m12_rows if r.get("calibration_gap") is not None]
            if gaps:
                max_gap = max(gaps)
                if max_gap >= 0.05:
                    cal_status = "Alert"
                elif max_gap >= 0.02:
                    cal_status = "Warning"
                else:
                    cal_status = "On Track"
                lines.append(f"**M12 Calibration:** max gap {max_gap:.2%} -- {cal_status}")
                lines.append("")

    return "\n".join(lines)


def _section_business_flags(result, config: MonitoringConfig) -> str:
    """Business flags in plain language."""
    lines = ["## Observations", ""]

    if not result.flags:
        lines.append("No observations to report. All indicators are within expected ranges.")
        lines.append("")
        return "\n".join(lines)

    alert_flags = [f for f in result.flags if f["status"] == ALERT]
    warn_flags = [f for f in result.flags if f["status"] == WARNING]

    if alert_flags:
        lines.append("### Items Requiring Attention")
        lines.append("")
        for flag in alert_flags:
            desc = _flag_to_business_language(flag)
            lines.append(f"- {_status_icon(ALERT)} {desc}")
        lines.append("")

    if warn_flags:
        lines.append("### Items Under Observation")
        lines.append("")
        for flag in warn_flags:
            desc = _flag_to_business_language(flag)
            lines.append(f"- {_status_icon(WARNING)} {desc}")
        lines.append("")

    return "\n".join(lines)


def _flag_to_business_language(flag: dict) -> str:
    """Translate a governance flag into business-friendly language."""
    metric = flag.get("metric", "")
    value = flag.get("value")
    source = flag.get("source", "")

    if "score_psi" in metric:
        return (
            f"The model's score distribution has shifted compared to the baseline "
            f"(shift index: {_f(value, 4)}). This may indicate changes in the applicant population."
        )

    if "feature_psi" in metric:
        feat_name = metric.replace("feature_psi:", "")
        return (
            f"Input variable '{feat_name}' shows significant drift "
            f"(shift index: {_f(value, 4)}). Data pipeline or population changes may be the cause."
        )

    if "ks_drop" in metric:
        label = metric.replace("ks_drop:", "")
        return (
            f"Model ranking ability has declined at the {label} performance window "
            f"(drop: {_f(value, 4)}). The model may be losing predictive power."
        )

    if "calibration_gap" in metric:
        return (
            f"M12 calibration: predicted vs actual charge-off rates are diverging "
            f"(gap: {_pct(value)}). The model may need recalibration."
        )

    if "edr_delta" in metric:
        parts = metric.replace("edr_delta:", "").split(":")
        label = parts[0] if parts else ""
        channel = parts[1] if len(parts) > 1 else "overall"
        return (
            f"Early default rate has changed at {label} for {channel} "
            f"(delta: {_pct(value)}). Monitor for sustained trends."
        )

    if "missing_rate" in metric:
        col_name = metric.replace("missing_rate:", "")
        return (
            f"Data completeness issue: variable '{col_name}' has elevated missing rate "
            f"({_pct(value)}). Upstream data feeds should be investigated."
        )

    return f"{source}: {metric} = {_f(value, 4)} ({flag.get('status', 'N/A')})"


def _section_drivers_of_change(result, config: MonitoringConfig) -> str:
    """Drivers of change section (only if explanation is enabled and available)."""
    if result.explanation is None:
        return ""

    lines = ["## Drivers of Change", ""]

    top_drivers = result.explanation.get("top_drivers", [])
    if not top_drivers:
        lines.append("_No significant drivers of change identified._")
        lines.append("")
        return "\n".join(lines)

    headers = ["Driver", "Contribution", "Direction"]
    rows = []
    for driver in top_drivers[:10]:
        name = driver.get("feature", driver.get("name", "--"))
        contrib = driver.get("contribution", driver.get("importance"))
        direction = driver.get("direction", "--")
        rows.append([name, _f(contrib, 4), direction])

    lines.append(
        "The following factors are the primary contributors to changes in "
        "the model's score distribution compared to the baseline period:"
    )
    lines.append("")
    lines.append(_tbl(headers, rows))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def build_business_report(
    result,  # MonitoringResult
    config: MonitoringConfig,
    thresholds: ThresholdEngine,
    output_dir: str,
     context = None,
) -> str:
    """Build the business-facing monitoring report.

    Args:
        result: MonitoringResult from the monitoring pipeline.
        config: Monitoring configuration.
        thresholds: Threshold engine for status evaluation.
        output_dir: Directory to write the output HTML file.
        context: Optional BaseReportContext built by runner. Currently accepted
            for interface consistency with other renderers and future migration.

    Returns:
        Absolute path to the generated HTML file.
    """
    run_date = datetime.date.today().strftime("%Y%m%d")
    filename = f"{config.model_name}_business_{result.reporting_month}_{run_date}.html"

    # Build markdown
    sections = [
        f"# {config.display_name} -- Monthly Monitoring Report",
        "",
        f"**Reporting Period:** {result.reporting_month}",
        f"**Generated:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"**Data Source:** {'Mock Demo' if 'MOCK' in (getattr(result, 'data_mode', '') or '') else 'Production Actuals'}",
        "",
        "---",
        "",
        _section_executive_summary(result, config, thresholds),
        "",
        "---",
        "",
        _section_portfolio_summary(result, config),
        "",
        "---",
        "",
        _section_segment_breakdown(result, config),
        "",
        "---",
        "",
        _section_early_performance(result, config),
        "",
        "---",
        "",
        _section_business_flags(result, config),
    ]

    # Drivers of change (only if explanation enabled)
    drivers_section = _section_drivers_of_change(result, config)
    if drivers_section:
        sections.extend(["", "---", "", drivers_section])

    # Footer
    sections.extend([
        "",
        "---",
        "",
        f"_Report generated by {config.display_name} V2 Monitoring Framework._",
        f"_Baseline period: {result.baseline_info.get('start_month', 'N/A')} "
        f"to {result.baseline_info.get('end_month', 'N/A')}_",
        "",
    ])

    md_text = "\n".join(sections)

    # Convert to HTML
    out_path = Path(output_dir) / filename
    backend = _detect_html_backend()

    if backend:
        title = f"{config.display_name} - Business Report ({result.reporting_month})"
        html_content = _md_to_html(md_text, title=title)
        out_path.write_text(html_content, encoding="utf-8")
        logger.info("Business report (HTML): %s", out_path)
    else:
        # Fallback: write markdown directly
        out_path = out_path.with_suffix(".md")
        out_path.write_text(md_text, encoding="utf-8")
        logger.warning(
            "No HTML backend available; wrote markdown to %s. "
            "Install 'markdown' or 'markdown2' for HTML output.",
            out_path,
        )

    return str(out_path.resolve())
