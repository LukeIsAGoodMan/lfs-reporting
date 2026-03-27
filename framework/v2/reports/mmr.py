"""Model Monitoring Report (MMR) -- technical report builder.

Generates a comprehensive technical monitoring report with full metric
tables, threshold evaluations, governance flags, and diagnostics.
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


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------

def _section_run_metadata(result, config: MonitoringConfig) -> str:
    """Run metadata: model, reporting month, baseline info, cohort summary."""
    lines = ["## 1. Run Metadata", ""]

    data_mode = getattr(result, "data_mode", None) or "Unknown"
    rows = [
        ["Model", config.display_name],
        ["Model Name", config.model_name],
        ["Reporting Month", result.reporting_month],
        ["Run Timestamp", result.run_timestamp],
        ["Data Mode", data_mode],
        ["Score Column", config.score_col],
        ["Channel Column", config.channel_col],
        ["Channels", ", ".join(config.channels)],
        ["PSI Bins", str(config.psi_n_bins)],
        ["Scorecard Enabled", str(config.scorecard_enabled)],
    ]
    lines.append(_tbl(["Parameter", "Value"], rows))

    # Baseline info
    lines.append("### Baseline Configuration")
    lines.append("")
    bl = result.baseline_info
    bl_rows = [
        ["Type", bl.get("type", "--")],
        ["Start Month", bl.get("start_month", "--")],
        ["End Month", bl.get("end_month", "--")],
        ["Months Included", str(len(bl.get("months", [])))],
        ["Account Count", f"{bl.get('account_count', 0):,}"],
    ]
    lines.append(_tbl(["Parameter", "Value"], bl_rows))

    # Cohort summary
    if result.cohort_info:
        lines.append("### Cohort Summary")
        lines.append("")
        cohort_rows = []
        for label, info in result.cohort_info.items():
            cohort_rows.append([
                label,
                info.get("score_month", "--"),
                f"{info.get('account_count', 0):,}",
                f"{info.get('mature_count', 0):,}",
                "Yes" if info.get("available") else "No",
                info.get("note", ""),
            ])
        lines.append(_tbl(
            ["Window", "Score Month", "Accounts", "Mature", "Available", "Note"],
            cohort_rows,
        ))

    return "\n".join(lines)


def _section_health_summary(result, config: MonitoringConfig, thresholds: ThresholdEngine) -> str:
    """Model Health Summary with overall status and executive narrative."""
    lines = ["## 2. Model Health Summary", ""]

    # Overall status
    alerts = [f for f in result.flags if f["status"] == ALERT]
    warnings = [f for f in result.flags if f["status"] == WARNING]

    if alerts:
        overall = ALERT
    elif warnings:
        overall = WARNING
    else:
        overall = OK

    lines.append(f"**Overall Status: {overall}**")
    lines.append(f"- Alerts: {len(alerts)}")
    lines.append(f"- Warnings: {len(warnings)}")
    lines.append(f"- Total flags: {len(result.flags)}")
    lines.append("")

    # Executive narrative
    narrative = generate_executive_narrative(result, config, thresholds)
    lines.append(narrative)

    return "\n".join(lines)


def _section_psi(result, config: MonitoringConfig, thresholds: ThresholdEngine) -> str:
    """Section 3: Full PSI bucket table."""
    lines = ["## 3. Score Stability (PSI)", ""]

    for sc_id, stab in result.stability.items():
        sc_label = sc_id if sc_id != "overall" else "Overall"

        # Summary line
        psi_val = stab.get("score_psi")
        psi_status = stab.get("score_psi_status", "--")
        lines.append(f"### {sc_label}")
        lines.append(f"**Score PSI: {_f(psi_val, 4)} ({psi_status})**")
        lines.append("")

        # Full bucket table
        psi_table = stab.get("psi_table", [])
        if psi_table:
            headers = [
                "Score Interval", "Baseline Count", "Compare Count",
                "Baseline %", "Compare %", "Difference",
                "Obs/Baseline Ratio", "WoE", "Contribution",
            ]
            rows = []
            total_contribution = 0.0
            for row in psi_table:
                contribution = row.get("contribution", 0)
                total_contribution += contribution
                rows.append([
                    row.get("interval", "--"),
                    f"{row.get('baseline_count', 0):,}",
                    f"{row.get('compare_count', 0):,}",
                    _pct(row.get("baseline_pct")),
                    _pct(row.get("compare_pct")),
                    _f(row.get("difference"), 4),
                    _f(row.get("obs_to_baseline_ratio"), 4),
                    _f(row.get("woe"), 4),
                    _f(contribution, 6),
                ])
            # Total row
            rows.append([
                "**TOTAL**", "", "", "", "", "",
                "", "", f"**{_f(total_contribution, 6)}**",
            ])
            lines.append(_tbl(headers, rows))
        else:
            lines.append("_PSI bucket table not available._")
            lines.append("")

    return "\n".join(lines)


def _section_csi(result, config: MonitoringConfig, thresholds: ThresholdEngine) -> str:
    """Section 4: Full CSI bucket tables per feature."""
    lines = ["## 4. Feature Stability (CSI)", ""]

    for sc_id, stab in result.stability.items():
        sc_label = sc_id if sc_id != "overall" else "Overall"
        lines.append(f"### {sc_label}")
        lines.append("")

        csi_tables = stab.get("csi_tables", {})
        if not csi_tables:
            lines.append("_CSI tables not available._")
            lines.append("")
            continue

        # Feature summary first
        feat_psi = stab.get("feature_psi", [])
        if feat_psi:
            feat_warn = thresholds.resolve("feature_psi", "warning", sc_id if sc_id != "overall" else None)
            feat_alert = thresholds.resolve("feature_psi", "alert", sc_id if sc_id != "overall" else None)
            summary_rows = []
            for fp in sorted(feat_psi, key=lambda x: -(x.get("psi") or 0)):
                summary_rows.append([
                    fp.get("feature_name", "--"),
                    _f(fp.get("psi"), 4),
                    _f(feat_warn, 4),
                    _f(feat_alert, 4),
                    fp.get("status", "--"),
                ])
            lines.append("#### Feature PSI Summary")
            lines.append("")
            lines.append(_tbl(
                ["Feature", "CSI", "Warning", "Alert", "Status"],
                summary_rows,
            ))

        # Detailed CSI tables per feature
        for feat_name, csi_rows in csi_tables.items():
            if not csi_rows:
                continue
            lines.append(f"#### {feat_name}")
            lines.append("")
            headers = [
                "Bin", "Interval", "Min", "Max",
                "Baseline Count", "Compare Count",
                "Baseline %", "Compare %",
                "Difference", "IV Contribution",
            ]
            tbl_rows = []
            total_iv = 0.0
            for row in csi_rows:
                iv = row.get("information_value", 0)
                total_iv += iv
                tbl_rows.append([
                    str(row.get("bin_index", "--")),
                    row.get("interval", "--"),
                    _f(row.get("min_value"), 4),
                    _f(row.get("max_value"), 4),
                    f"{row.get('baseline_count', 0):,}",
                    f"{row.get('compare_count', 0):,}",
                    _pct(row.get("baseline_pct")),
                    _pct(row.get("compare_pct")),
                    _f(row.get("difference"), 4),
                    _f(iv, 6),
                ])
            tbl_rows.append([
                "", "**TOTAL**", "", "", "", "", "", "", "",
                f"**{_f(total_iv, 6)}**",
            ])
            lines.append(_tbl(headers, tbl_rows))

    return "\n".join(lines)


def _section_performance(result, config: MonitoringConfig, thresholds: ThresholdEngine) -> str:
    """Section 5: EDR rank ordering + capture summary per maturity window."""
    lines = ["## 5. Performance (Early Default Rates)", ""]

    for sc_id, perf_data in result.performance.items():
        sc_label = sc_id if sc_id != "overall" else "Overall"
        lines.append(f"### {sc_label}")
        lines.append("")

        # Handle both old (list) and new (dict) format
        if isinstance(perf_data, dict):
            summary = perf_data.get("summary", [])
            edr_details = perf_data.get("edr_details", {})
        else:
            summary = perf_data
            edr_details = {}

        # Summary table first
        if summary:
            edr_warn = thresholds.resolve("edr_delta", "warning", sc_id if sc_id != "overall" else None)
            edr_alert = thresholds.resolve("edr_delta", "alert", sc_id if sc_id != "overall" else None)
            sum_headers = ["Window", "Channel", "Accounts", "Bad Rate", "EDR", "Status"]
            sum_rows = []
            for row in summary:
                if row.get("note"):
                    sum_rows.append([
                        row.get("display_label", "--"), row.get("channel", "--"),
                        "--", "--", "--", row.get("note", "--"),
                    ])
                else:
                    sum_rows.append([
                        row.get("display_label", "--"),
                        row.get("channel", "all"),
                        f"{row.get('account_count', 0):,}",
                        _pct(row.get("bad_rate")),
                        _pct(row.get("edr")),
                        row.get("edr_status", "--"),
                    ])
            lines.append("#### EDR Summary")
            lines.append("")
            lines.append(_tbl(sum_headers, sum_rows))

        # Per-window rank ordering + capture
        for label in ("M3", "M6", "M9"):
            detail = edr_details.get(label, {})
            display = {"M3": "EDR30", "M6": "EDR60", "M9": "EDR90"}.get(label, label)

            rank_table = detail.get("rank_ordering")
            capture = detail.get("capture_summary")

            if rank_table:
                lines.append(f"#### {display} -- Rank Ordering ({label})")
                lines.append("")
                ro_headers = [
                    "Score Interval", "Min Score", "Max Score",
                    "Accounts (N)", "Accounts (%)", "Observation %", "Misrank",
                ]
                ro_rows = []
                for row in rank_table:
                    misrank = row.get("misrank", "NO")
                    misrank_display = f"**{misrank}**" if misrank == "YES" else misrank
                    ro_rows.append([
                        row.get("interval", "--"),
                        _f(row.get("min_score"), 2),
                        _f(row.get("max_score"), 2),
                        f"{row.get('accounts_n', 0):,}",
                        _pct(row.get("accounts_pct")),
                        _pct(row.get("observation_pct")),
                        misrank_display,
                    ])
                lines.append(_tbl(ro_headers, ro_rows))

            if capture:
                lines.append(f"#### {display} -- Risk Capture Summary ({label})")
                lines.append("")
                cap_rows = [[
                    _f(capture.get("threshold"), 2),
                    _pct(capture.get("pct_population_flagged")),
                    _pct(capture.get("pct_bad_captured")),
                ]]
                lines.append(_tbl(
                    ["Threshold", "% Population Flagged", "% Bad Captured"],
                    cap_rows,
                ))

    if not result.performance:
        lines.append("_No performance data available._")
        lines.append("")

    return "\n".join(lines)


def _section_separation(result, config: MonitoringConfig, thresholds: ThresholdEngine) -> str:
    """Section 6: KS tables with 20 bins per maturity window."""
    lines = ["## 6. Separation (KS / Gini)", ""]

    has_data = False
    for sc_id, sep_dict in result.separation.items():
        if not sep_dict:
            continue

        has_data = True
        sc_label = sc_id if sc_id != "overall" else "Overall"
        lines.append(f"### {sc_label}")
        lines.append("")

        # KS/Gini summary
        summary_rows = []
        for label in ("M3", "M6", "M9", "M12"):
            sep = sep_dict.get(label)
            if sep is None:
                summary_rows.append([label, "--", "--", "--", "--", "N/A"])
                continue
            summary_rows.append([
                label,
                _f(sep.get("ks"), 4),
                _f(sep.get("gini"), 4),
                _f(sep.get("ks_drop"), 4),
                str(sep.get("misrank_count", 0)),
                sep.get("ks_drop_status", "--"),
            ])
        lines.append("#### KS / Gini Summary")
        lines.append("")
        lines.append(_tbl(
            ["Window", "KS", "Gini", "KS Drop", "Misranks", "Status"],
            summary_rows,
        ))

        # Full KS tables
        ks_tables = sep_dict.get("ks_tables", {})
        for label in ("M3", "M6", "M9", "M12"):
            ks_tbl = ks_tables.get(label)
            if not ks_tbl:
                continue

            display = {"M3": "EDR30", "M6": "EDR60", "M9": "EDR90", "M12": "CO"}.get(label, label)
            lines.append(f"#### {label} ({display}) -- KS Table")
            lines.append("")

            ks_headers = [
                "Tier", "Min Score", "Max Score",
                "Accounts (N)", "Accounts (%)",
                "Goods (N)", "Goods (%)",
                "Bads (N)", "Bads (%)",
                "Bad Rate", "KS", "Lift",
            ]
            ks_rows = []
            for row in ks_tbl:
                tier = row.get("tier", "--")
                is_total = str(tier) == "TOTAL"
                ks_rows.append([
                    f"**{tier}**" if is_total else str(tier),
                    _f(row.get("min_score"), 4) if not is_total else "",
                    _f(row.get("max_score"), 4) if not is_total else "",
                    f"{row.get('accounts_n', 0):,}",
                    _pct(row.get("accounts_pct")),
                    f"{row.get('goods_n', 0):,}",
                    _pct(row.get("goods_pct")),
                    f"{row.get('bads_n', 0):,}",
                    _pct(row.get("bads_pct")),
                    _pct(row.get("bad_rate")),
                    _f(row.get("ks"), 4) if not is_total else f"**{_f(row.get('ks'), 4)}**",
                    _f(row.get("lift"), 2) if not is_total else "",
                ])
            lines.append(_tbl(ks_headers, ks_rows))

        # Odds tables (from existing separation results)
        for label, sep in sep_dict.items():
            if not isinstance(sep, dict):
                continue
            odds = sep.get("odds", [])
            if odds:
                lines.append(f"#### {label} -- Odds Table")
                lines.append("")
                odds_rows = []
                for row in odds:
                    odds_rows.append([
                        str(row.get("score_bin", "--")),
                        f"{row.get('goods', 0):,}",
                        f"{row.get('bads', 0):,}",
                        _f(row.get("odds"), 2),
                        _pct(row.get("bad_rate")),
                        "Yes" if row.get("is_monotonic") else "**No**",
                    ])
                lines.append(_tbl(
                    ["Score Bin", "Goods", "Bads", "Odds", "Bad Rate", "Monotonic"],
                    odds_rows,
                ))

    if not has_data:
        lines.append("_No separation data available._")
        lines.append("")

    return "\n".join(lines)


def _section_calibration(result, config: MonitoringConfig, thresholds: ThresholdEngine) -> str:
    """Calibration section: predicted vs actual by score bin (M12 only)."""
    lines = ["## 7. Calibration (M12 -- 1-Year Charge-Off)", ""]

    lines.append(
        "Calibration is assessed at the M12 window only. "
        "M3/M6/M9 are early-read performance diagnostics, not calibration targets."
    )
    lines.append("")

    calib_warn = thresholds.resolve("calibration_gap", "warning")
    calib_alert = thresholds.resolve("calibration_gap", "alert")

    has_data = False
    for sc_id, calib_dict in result.calibration.items():
        if not calib_dict:
            continue

        # Only render M12
        calib_rows = calib_dict.get("M12")
        if calib_rows is None:
            continue

        has_data = True
        sc_label = sc_id if sc_id != "overall" else "Overall"
        lines.append(f"### {sc_label}")
        lines.append("")

        headers = [
            "Scorecard", "Channel", "Score Bin", "Accounts", "Predicted Rate",
            "Actual Rate", "Gap", "Warn", "Alert", "Status",
        ]
        rows = []
        for row in calib_rows:
            rows.append([
                sc_label,
                row.get("channel", "all"),
                str(row.get("score_bin", "--")),
                f"{row.get('account_count', 0):,}" if isinstance(row.get("account_count"), (int, float)) else "--",
                _pct(row.get("predicted_rate")),
                _pct(row.get("actual_rate")),
                _f(row.get("calibration_gap"), 4),
                _f(calib_warn, 4),
                _f(calib_alert, 4),
                row.get("gap_status", "--"),
            ])
        lines.append(_tbl(headers, rows))

        # Summary statistics
        gaps = [abs(r.get("calibration_gap", 0)) for r in calib_rows if r.get("calibration_gap") is not None]
        if gaps:
            max_gap = max(gaps)
            mean_gap = sum(gaps) / len(gaps)
            worst_bin = None
            for r in calib_rows:
                if r.get("calibration_gap") is not None and abs(r["calibration_gap"]) == max_gap:
                    worst_bin = r.get("score_bin", "?")
                    break
            lines.append(f"**Summary:** Max |gap| = {max_gap:.4f}, Mean |gap| = {mean_gap:.4f}, Worst bin = {worst_bin}")
            lines.append("")

    if not has_data:
        lines.append("_No M12 calibration data available._")
        lines.append("")

    return "\n".join(lines)


def _section_data_quality(result, config: MonitoringConfig) -> str:
    """Data quality section."""
    lines = ["## 8. Data Quality", ""]

    for sc_id, dq in result.data_quality.items():
        sc_label = sc_id if sc_id != "overall" else "Overall"
        lines.append(f"### {sc_label}")
        lines.append("")

        # Summary
        summary_rows = []
        total_records = dq.get("total_records")
        if total_records is not None:
            summary_rows.append(["Total Records", f"{total_records:,}" if isinstance(total_records, (int, float)) else str(total_records)])
        summary_rows.append(["Duplicate Rate", _pct(dq.get("duplicate_rate"))])
        summary_rows.append(["Overall Missing Rate", _pct(dq.get("overall_missing_rate"))])

        if summary_rows:
            lines.append(_tbl(["Metric", "Value"], summary_rows))

        # Column-level detail
        col_dq = dq.get("columns", [])
        if col_dq:
            lines.append("#### Column-Level Data Quality")
            lines.append("")
            col_rows = []
            for c in col_dq:
                col_rows.append([
                    c.get("column", "--"),
                    _pct(c.get("missing_rate")),
                    c.get("missing_status", "--"),
                    _pct(c.get("outlier_rate")),
                    str(c.get("unique_count", "--")),
                ])
            lines.append(_tbl(
                ["Column", "Missing Rate", "Status", "Outlier Rate", "Unique Values"],
                col_rows,
            ))

    return "\n".join(lines)


def _section_governance_flags(result, config: MonitoringConfig, thresholds: ThresholdEngine) -> str:
    """Governance flags table -- MANDATORY section with ALL metrics and thresholds."""
    lines = ["## 9. Governance Flags", ""]

    # Build comprehensive flags table including ALL evaluated metrics
    all_rows: list[list[str]] = []

    # Stability metrics
    for sc_id, stab in result.stability.items():
        sc_key = sc_id if sc_id != "overall" else None

        # Score PSI
        psi = stab.get("score_psi")
        if psi is not None:
            all_rows.append([
                sc_id,
                "Score PSI",
                _f(psi, 4),
                _f(thresholds.resolve("psi", "warning", sc_key), 4),
                _f(thresholds.resolve("psi", "alert", sc_key), 4),
                stab.get("score_psi_status", "--"),
            ])

        # Feature PSI
        for fp in stab.get("feature_psi", []):
            feat_name = fp.get("feature_name", "--")
            all_rows.append([
                sc_id,
                f"Feature PSI: {feat_name}",
                _f(fp.get("psi"), 4),
                _f(thresholds.resolve("feature_psi", "warning", sc_key), 4),
                _f(thresholds.resolve("feature_psi", "alert", sc_key), 4),
                fp.get("status", "--"),
            ])

    # Separation metrics
    for sc_id, sep_dict in result.separation.items():
        sc_key = sc_id if sc_id != "overall" else None
        for label, sep in sep_dict.items():
            if isinstance(sep, dict) and sep.get("ks_drop") is not None:
                all_rows.append([
                    sc_id,
                    f"KS Drop ({label})",
                    _f(sep.get("ks_drop"), 4),
                    _f(thresholds.resolve("ks_drop", "warning", sc_key), 4),
                    _f(thresholds.resolve("ks_drop", "alert", sc_key), 4),
                    sep.get("ks_drop_status", "--"),
                ])

    # Performance metrics
    for sc_id, perf_data in result.performance.items():
        sc_key = sc_id if sc_id != "overall" else None
        perf_rows = perf_data.get("summary", perf_data) if isinstance(perf_data, dict) else perf_data
        if not isinstance(perf_rows, list):
            perf_rows = []
        for row in perf_rows:
            if row.get("edr_delta") is not None:
                all_rows.append([
                    sc_id,
                    f"EDR Delta ({row.get('maturity', '')}:{row.get('channel', 'all')})",
                    _f(row.get("edr_delta"), 4),
                    _f(thresholds.resolve("edr_delta", "warning", sc_key), 4),
                    _f(thresholds.resolve("edr_delta", "alert", sc_key), 4),
                    row.get("edr_delta_status", "--"),
                ])

    # Calibration metrics (M12 only)
    for sc_id, calib_dict in result.calibration.items():
        sc_key = sc_id if sc_id != "overall" else None
        m12_rows = calib_dict.get("M12") or []
        for crow in m12_rows:
            gap = crow.get("calibration_gap")
            if gap is not None:
                channel = crow.get("channel", "all")
                all_rows.append([
                    sc_id,
                    f"Calibration Gap (M12:{channel}:bin{crow.get('score_bin', '?')})",
                    _f(gap, 4),
                    _f(thresholds.resolve("calibration_gap", "warning", sc_key), 4),
                    _f(thresholds.resolve("calibration_gap", "alert", sc_key), 4),
                    crow.get("gap_status", "--"),
                ])

    # Data quality metrics
    for sc_id, dq in result.data_quality.items():
        sc_key = sc_id if sc_id != "overall" else None
        for col_dq in dq.get("columns", []):
            mr = col_dq.get("missing_rate")
            if mr is not None:
                all_rows.append([
                    sc_id,
                    f"Missing Rate: {col_dq.get('column', '?')}",
                    _pct(mr),
                    _pct(thresholds.resolve("missing_rate", "warning", sc_key)),
                    _pct(thresholds.resolve("missing_rate", "alert", sc_key)),
                    col_dq.get("missing_status", "--"),
                ])

    if all_rows:
        lines.append(_tbl(
            ["Scorecard", "Metric", "Value", "Warning Threshold", "Alert Threshold", "Status"],
            all_rows,
        ))
    else:
        lines.append("_No metrics evaluated._")
        lines.append("")

    # Flagged items summary
    flagged = [r for r in all_rows if r[-1] in (ALERT, WARNING)]
    if flagged:
        lines.append(f"**{len(flagged)} metric(s) outside acceptable thresholds.**")
    else:
        lines.append("**All metrics within acceptable thresholds.**")
    lines.append("")

    return "\n".join(lines)


def _section_diagnostics(result, config: MonitoringConfig, thresholds: ThresholdEngine) -> str:
    """Diagnostics: auto-diagnosis narrative summarising the full picture."""
    lines = ["## 10. Diagnostics", ""]

    # Auto-diagnosis
    diag_points: list[str] = []

    # Check for data availability issues
    empty_cohorts = [
        label for label, info in result.cohort_info.items()
        if not info.get("available")
    ]
    if empty_cohorts:
        diag_points.append(
            f"Cohorts not yet available: {', '.join(empty_cohorts)}. "
            "These windows have not reached maturity for the reporting month."
        )

    # Check for small sample sizes
    small_cohorts = [
        label for label, info in result.cohort_info.items()
        if info.get("available") and info.get("account_count", 0) < config.sample_rules.minimum_accounts_for_ks
    ]
    if small_cohorts:
        diag_points.append(
            f"Small sample sizes in: {', '.join(small_cohorts)} "
            f"(below {config.sample_rules.minimum_accounts_for_ks:,} account minimum). "
            "Metric reliability may be reduced."
        )

    # Stability diagnosis
    for sc_id, stab in result.stability.items():
        psi = stab.get("score_psi", 0)
        if psi is not None and psi >= 0.25:
            diag_points.append(
                f"Severe score distribution shift detected (PSI = {psi:.4f}). "
                "Potential causes: population mix change, upstream data issue, "
                "or macro-economic shift. Recommend root cause analysis."
            )
        elif psi is not None and psi >= 0.10:
            diag_points.append(
                f"Moderate score distribution shift (PSI = {psi:.4f}). "
                "Monitor for sustained drift over the next 1-2 months."
            )

        # Feature drift correlation
        drifting_feats = [
            f for f in stab.get("feature_psi", [])
            if f.get("psi", 0) >= 0.10
        ]
        if len(drifting_feats) >= 3:
            diag_points.append(
                f"{len(drifting_feats)} features showing simultaneous drift. "
                "This pattern suggests a systemic upstream data change "
                "rather than isolated variable issues."
            )

    # Separation diagnosis
    for sc_id, sep_dict in result.separation.items():
        for label, sep in sep_dict.items():
            if isinstance(sep, dict) and sep.get("ks_drop") is not None:
                drop = sep["ks_drop"]
                if drop <= -0.07:
                    diag_points.append(
                        f"Significant KS degradation at {label} (drop = {drop:.4f}). "
                        "Model discriminatory power may be compromised. "
                        "Consider champion/challenger analysis."
                    )

    # Calibration diagnosis (M12 only)
    for sc_id, calib_dict in result.calibration.items():
        m12_rows = calib_dict.get("M12") or []
        if not m12_rows:
            continue
        large_gaps = [
            r for r in m12_rows
            if r.get("calibration_gap") is not None and abs(r["calibration_gap"]) >= 0.05
        ]
        if large_gaps:
            diag_points.append(
                f"M12 Calibration divergence: {len(large_gaps)} bin(s) "
                f"with gap >= 5pp. Model recalibration should be evaluated."
            )

    if diag_points:
        for point in diag_points:
            lines.append(f"- {point}")
    else:
        lines.append("- No diagnostic issues identified. Model is operating normally.")

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def build_mmr_report(
    result,  # MonitoringResult
    config: MonitoringConfig,
    thresholds: ThresholdEngine,
    output_dir: str,
    context = None,
) -> str:
    """Build the Model Monitoring Report (MMR).

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
    filename = f"{config.model_name}_mmr_{result.reporting_month}_{run_date}.html"

    # Build markdown
    sections = [
        f"# Model Monitoring Report (MMR) -- {config.display_name}",
        "",
        f"**Model:** {config.display_name} (`{config.model_name}`)",
        f"**Reporting Month:** {result.reporting_month}",
        f"**Generated:** {result.run_timestamp}",
        "",
        "---",
        "",
        _section_run_metadata(result, config),
        "",
        "---",
        "",
        _section_health_summary(result, config, thresholds),
        "",
        "---",
        "",
        _section_psi(result, config, thresholds),
        "",
        "---",
        "",
        _section_csi(result, config, thresholds),
        "",
        "---",
        "",
        _section_performance(result, config, thresholds),
        "",
        "---",
        "",
        _section_separation(result, config, thresholds),
        "",
        "---",
        "",
        _section_calibration(result, config, thresholds),
        "",
        "---",
        "",
        _section_data_quality(result, config),
        "",
        "---",
        "",
        _section_governance_flags(result, config, thresholds),
        "",
        "---",
        "",
        _section_diagnostics(result, config, thresholds),
        "",
        "---",
        "",
        f"_Model Monitoring Report generated by V2 Framework | {config.display_name} | {result.reporting_month}_",
        "",
    ]

    md_text = "\n".join(sections)

    # Convert to HTML
    out_path = Path(output_dir) / filename
    backend = _detect_html_backend()

    if backend:
        title = f"MMR - {config.display_name} ({result.reporting_month})"
        html_content = _md_to_html(md_text, title=title)
        out_path.write_text(html_content, encoding="utf-8")
        logger.info("MMR report (HTML): %s", out_path)
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
