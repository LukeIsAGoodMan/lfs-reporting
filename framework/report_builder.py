"""Generate a human-readable markdown monitoring report.

Reads the already-built Layer 2 and Layer 3 output DataFrames and
produces a structured markdown document summarising:

A. Run metadata
B. Executive summary (rule-based health indicators)
C. Business summary (Layer 2)
D. Monitoring summary (Layer 3)
E. Flags and observations

Usage::

    from framework.report_builder import build_report

    report_md = build_report(
        outputs=outputs,          # {table_name: DataFrame} from runner.run()
        config=config,
        score_month="2025-05",
        model_version="v1.0",
        output_path="outputs/lfs_report.md",   # optional
    )
"""
from __future__ import annotations

import datetime
from pathlib import Path
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from framework.config import ModelConfig
from framework.utils import get_logger

logger = get_logger(__name__)


# ── Monitoring thresholds ─────────────────────────────────────────────

_PSI_WARN      = 0.10
_PSI_ALERT     = 0.25
_KS_WARN       = 0.10
_TAIL_WARN     = 0.15    # pct_accounts_above_baseline_pXX
_MISSING_WARN  = 0.01
_OUTLIER_WARN  = 0.05


# ── Formatting helpers ────────────────────────────────────────────────

def _f(val, decimals: int = 4) -> str:
    """Format a scalar as a fixed-decimal string; '—' for None."""
    if val is None:
        return "—"
    try:
        return f"{float(val):.{decimals}f}"
    except (TypeError, ValueError):
        return str(val)


def _pct(val, decimals: int = 1) -> str:
    """Format a fraction [0,1] as a percentage string."""
    if val is None:
        return "—"
    try:
        return f"{float(val) * 100:.{decimals}f}%"
    except (TypeError, ValueError):
        return str(val)


def _n(val) -> str:
    """Format an integer with thousands separator."""
    if val is None:
        return "—"
    try:
        return f"{int(val):,}"
    except (TypeError, ValueError):
        return str(val)


def _psi_cell(psi) -> str:
    """Return PSI value with inline flag text."""
    if psi is None:
        return "—"
    p = float(psi)
    if p >= _PSI_ALERT:
        return f"{_f(psi)} **[ALERT]**"
    if p >= _PSI_WARN:
        return f"{_f(psi)} [WARN]"
    return _f(psi)


def _ks_cell(ks) -> str:
    if ks is None:
        return "—"
    k = float(ks)
    if k >= _KS_WARN:
        return f"{_f(ks)} [WARN]"
    return _f(ks)


def _tail_cell(pct_val) -> str:
    if pct_val is None:
        return "—"
    p = float(pct_val)
    if p >= _TAIL_WARN:
        return f"{_pct(pct_val)} [WARN]"
    return _pct(pct_val)


def _tbl(headers: list[str], rows: list[list]) -> str:
    """Render a markdown table from headers and data rows."""
    if not rows:
        return "_No data available._"
    lines = ["| " + " | ".join(str(h) for h in headers) + " |"]
    lines += ["|" + "|".join(["---"] * len(headers)) + "|"]
    lines += ["| " + " | ".join(str(v) for v in row) + " |" for row in rows]
    return "\n".join(lines)


def _shift(base, curr) -> str:
    """Format a signed mean shift."""
    if base is None or curr is None:
        return "—"
    d = float(curr) - float(base)
    sign = "+" if d >= 0 else ""
    return f"{sign}{d:.4f}"


# ── Data access helpers ───────────────────────────────────────────────

def _get(outputs: dict[str, DataFrame], name: str) -> DataFrame | None:
    df = outputs.get(name)
    if df is None:
        logger.debug("Table '%s' not found in outputs — section will be skipped", name)
    return df


def _vm_rows(df: DataFrame | None, vintage_month: str) -> list:
    """Collect rows from *df* filtered to a single vintage_month."""
    if df is None:
        return []
    return [
        row.asDict()
        for row in df.filter(F.col("vintage_month") == vintage_month).collect()
    ]


def _latest_vm(df: DataFrame | None, score_month: str) -> str:
    """Return *score_month* if present in *df*, otherwise the latest available."""
    if df is None:
        return score_month
    vms = {r["vintage_month"] for r in df.select("vintage_month").distinct().collect()}
    return score_month if score_month in vms else (max(vms) if vms else score_month)


# ── Section builders ─────────────────────────────────────────────────

def _sec_metadata(
    config: ModelConfig,
    score_month: str,
    model_version: str,
    timestamp: str,
    l1_count: int | None,
) -> str:
    bw = config.layer3.get("baseline_window", [])
    bw_str = f"{bw[0]} → {bw[-1]}" if bw else "N/A"
    channels = ", ".join(config.source.get("channels", []))
    n_features = len(config.layer3.get("feature_columns", []))

    rows = [
        ["Model", f"{config.name} — {config.display_name}"],
        ["Score month", score_month],
        ["Model version", model_version],
        ["Baseline window", bw_str],
        ["Channels", channels],
        ["Features monitored", str(n_features)],
        ["Generated", timestamp],
    ]
    if l1_count is not None:
        rows.append(["Layer 1 accounts (current)", _n(l1_count)])

    return "\n".join([
        "## A. Run Metadata",
        "",
        _tbl(["Property", "Value"], rows),
    ])


def _sec_executive(
    drift_rows: list,
    feat_psi_rows: list,
    channels: list[str],
) -> list[str]:
    """Executive summary: score health table + top drifting features."""
    lines = ["## B. Executive Summary", ""]

    # ── Score health table ────────────────────────────────────────────
    lines += ["### Score Health", ""]
    if drift_rows:
        h = ["Channel", "Avg Score", "Std", "PSI vs Baseline", "KS vs Baseline",
             "% Above Baseline P90", "% Above Baseline P95"]
        tbl_rows = []
        for r in sorted(drift_rows, key=lambda x: x["channel"]):
            tbl_rows.append([
                r["channel"],
                _f(r.get("mean_lfs_score")),
                _f(r.get("std_lfs_score")),
                _psi_cell(r.get("score_psi")),
                _ks_cell(r.get("score_ks")),
                _tail_cell(r.get("pct_accounts_above_p90_baseline")),
                _tail_cell(r.get("pct_accounts_above_p95_baseline")),
            ])
        lines += [_tbl(h, tbl_rows), ""]
    else:
        lines += ["_Score drift data not available._", ""]

    # ── Top drifting features ─────────────────────────────────────────
    lines += ["### Top Drifting Features (by PSI)", ""]
    if feat_psi_rows:
        # Aggregate max PSI across channels per feature.
        feat_max: dict[str, tuple[float, float, float]] = {}
        for r in feat_psi_rows:
            fn = r["feature_name"]
            p = float(r["psi"]) if r["psi"] is not None else 0.0
            bm = float(r["baseline_mean"]) if r["baseline_mean"] is not None else 0.0
            cm = float(r["current_mean"]) if r["current_mean"] is not None else 0.0
            if fn not in feat_max or p > feat_max[fn][0]:
                feat_max[fn] = (p, bm, cm)

        top5 = sorted(feat_max.items(), key=lambda x: -x[1][0])[:5]
        h = ["Rank", "Feature", "Max PSI (across channels)", "Baseline Mean", "Current Mean", "Mean Shift"]
        tbl_rows = [
            [i + 1, fn, _psi_cell(psi), _f(bm), _f(cm), _shift(bm, cm)]
            for i, (fn, (psi, bm, cm)) in enumerate(top5)
        ]
        lines += [_tbl(h, tbl_rows), ""]
    else:
        lines += ["_Feature PSI data not available._", ""]

    return lines


def _sec_business(
    vm: str,
    overall_rows: list,
    dist_rows: list,
    by_source_rows: list,
    by_line_rows: list,
    band_decile_map: dict[str, list[int]],
) -> list[str]:
    lines = ["## C. Business Summary", ""]

    # ── Overall volume & score ────────────────────────────────────────
    lines += [f"### Overall Volume & Score ({vm})", ""]
    if overall_rows:
        h = ["Channel", "Accounts", "Avg Score", "Avg Receivable", "Avg Sale Amount"]
        tbl_rows = [
            [
                r["channel"],
                _n(r.get("account_count")),
                _f(r.get("avg_lfs_score")),
                _f(r.get("avg_endingreceivable"), 2),
                _f(r.get("avg_saleamount"), 2),
            ]
            for r in sorted(overall_rows, key=lambda x: x["channel"])
        ]
        lines += [_tbl(h, tbl_rows), ""]
    else:
        lines += ["_Overall summary not available._", ""]

    # ── Score band distribution ───────────────────────────────────────
    lines += [f"### Score Band Distribution ({vm})", ""]
    lines += ["Decile bins 1–3 = Low · 4–7 = Medium · 8–10 = High", ""]
    if dist_rows:
        # Compute band pct per channel from decile pct column.
        band_pct: dict[str, dict[str, float]] = {}
        for r in dist_rows:
            ch = r["channel"]
            dec = r.get("lfs_decile_dyn")
            pct_val = float(r.get("pct_of_channel_vintage_accounts") or 0)
            if ch not in band_pct:
                band_pct[ch] = {"Low": 0.0, "Medium": 0.0, "High": 0.0}
            if dec in (1, 2, 3):
                band_pct[ch]["Low"] += pct_val
            elif dec in (4, 5, 6, 7):
                band_pct[ch]["Medium"] += pct_val
            else:
                band_pct[ch]["High"] += pct_val

        h = ["Channel", "Low (D1–D3)", "Medium (D4–D7)", "High (D8–D10)"]
        tbl_rows = [
            [ch, _pct(bp["Low"]), _pct(bp["Medium"]), _pct(bp["High"])]
            for ch, bp in sorted(band_pct.items())
        ]
        lines += [_tbl(h, tbl_rows), ""]
    else:
        lines += ["_Score distribution data not available._", ""]

    # ── By source ─────────────────────────────────────────────────────
    lines += [f"### Score by Acquisition Source ({vm})", ""]
    if by_source_rows:
        h = ["Channel", "Source", "Accounts", "Avg Score"]
        tbl_rows = [
            [r["channel"], r.get("source", "—"), _n(r.get("account_count")),
             _f(r.get("avg_lfs_score"))]
            for r in sorted(by_source_rows, key=lambda x: (x["channel"], str(x.get("source"))))
        ]
        lines += [_tbl(h, tbl_rows), ""]
    else:
        lines += ["_By-source data not available._", ""]

    # ── By line ───────────────────────────────────────────────────────
    lines += [f"### Score by Product Line ({vm})", ""]
    if by_line_rows:
        h = ["Channel", "Line", "Accounts", "Avg Score"]
        tbl_rows = [
            [r["channel"], r.get("line", "—"), _n(r.get("account_count")),
             _f(r.get("avg_lfs_score"))]
            for r in sorted(by_line_rows, key=lambda x: (x["channel"], str(x.get("line"))))
        ]
        lines += [_tbl(h, tbl_rows), ""]
    else:
        lines += ["_By-line data not available._", ""]

    return lines


def _sec_monitoring(
    vm: str,
    drift_rows: list,
    feat_psi_rows: list,
    corr_rows: list,
    pop_mix_rows: list,
    dq_rows: list,
) -> list[str]:
    lines = ["## D. Monitoring Summary", ""]

    # ── D1. Score drift ───────────────────────────────────────────────
    lines += [f"### D1. Score Drift ({vm})", ""]
    if drift_rows:
        h = ["Channel", "Mean", "Std", "PSI", "KS",
             "P50", "P90", "P95", "Top-10% Mean",
             "% Above P90 Base", "% Above P95 Base"]
        tbl_rows = []
        for r in sorted(drift_rows, key=lambda x: x["channel"]):
            tbl_rows.append([
                r["channel"],
                _f(r.get("mean_lfs_score")),
                _f(r.get("std_lfs_score")),
                _psi_cell(r.get("score_psi")),
                _ks_cell(r.get("score_ks")),
                _f(r.get("q50_score")),
                _f(r.get("q90_score")),
                _f(r.get("q95_score")),
                _f(r.get("mean_top10pct_score")),
                _tail_cell(r.get("pct_accounts_above_p90_baseline")),
                _tail_cell(r.get("pct_accounts_above_p95_baseline")),
            ])
        lines += [_tbl(h, tbl_rows), ""]
    else:
        lines += ["_Score drift data not available._", ""]

    # ── D2. Feature PSI top 5 per channel ─────────────────────────────
    lines += [f"### D2. Feature PSI — Top 5 per Channel ({vm})", ""]
    if feat_psi_rows:
        h = ["Channel", "Feature", "PSI", "Base Mean", "Curr Mean",
             "Mean Shift", "Base Std", "Curr Std"]
        # Sort by PSI desc, take top 5 per channel.
        from collections import defaultdict
        by_ch: dict[str, list] = defaultdict(list)
        for r in feat_psi_rows:
            by_ch[r["channel"]].append(r)
        tbl_rows = []
        for ch in sorted(by_ch):
            top5 = sorted(by_ch[ch], key=lambda x: -(float(x["psi"]) if x["psi"] else 0))[:5]
            for r in top5:
                tbl_rows.append([
                    r["channel"],
                    r["feature_name"],
                    _psi_cell(r.get("psi")),
                    _f(r.get("baseline_mean")),
                    _f(r.get("current_mean")),
                    _shift(r.get("baseline_mean"), r.get("current_mean")),
                    _f(r.get("baseline_std")),
                    _f(r.get("current_std")),
                ])
        lines += [_tbl(h, tbl_rows), ""]
    else:
        lines += ["_Feature PSI data not available._", ""]

    # ── D3. Correlation drift top 5 ───────────────────────────────────
    lines += [f"### D3. Feature–Score Correlation Drift — Top 5 ({vm})", ""]
    if corr_rows:
        non_null = [r for r in corr_rows if r.get("corr_change") is not None]
        top5 = sorted(non_null, key=lambda x: -abs(float(x["corr_change"])))[:5]
        h = ["Channel", "Feature", "Baseline Corr", "Current Corr", "Change"]
        tbl_rows = [
            [
                r["channel"], r["feature_name"],
                _f(r.get("baseline_corr")),
                _f(r.get("current_corr")),
                _shift(r.get("baseline_corr"), r.get("current_corr")),
            ]
            for r in top5
        ]
        if tbl_rows:
            lines += [_tbl(h, tbl_rows), ""]
        else:
            lines += ["_Insufficient data for correlation computation._", ""]
    else:
        lines += ["_Correlation data not available._", ""]

    # ── D4. Population mix — source segment ───────────────────────────
    lines += [f"### D4. Population Mix — Source Segment ({vm})", ""]
    source_rows = [r for r in pop_mix_rows if r.get("segment_type") == "source"]
    if source_rows:
        h = ["Channel", "Source", "Accounts", "% of Channel"]
        tbl_rows = [
            [r["channel"], r.get("segment_value", "—"),
             _n(r.get("account_count")), _pct(r.get("pct_of_channel_accounts"))]
            for r in sorted(source_rows, key=lambda x: (x["channel"], str(x.get("segment_value"))))
        ]
        lines += [_tbl(h, tbl_rows), ""]
    else:
        lines += ["_Population mix data not available._", ""]

    # ── D5. Data quality ──────────────────────────────────────────────
    lines += [f"### D5. Data Quality ({vm})", ""]
    if dq_rows:
        h = ["Channel", "Missing Score Rate", "Outlier Score Rate"]
        tbl_rows = [
            [r["channel"],
             _pct(r.get("missing_score_rate"), 3),
             _pct(r.get("outlier_score_rate"), 3)]
            for r in sorted(dq_rows, key=lambda x: x["channel"])
        ]
        lines += [_tbl(h, tbl_rows), ""]
    else:
        lines += ["_Data quality data not available._", ""]

    return lines


def _sec_flags(flags: list[str]) -> list[str]:
    lines = ["## E. Flags and Observations", ""]
    if not flags:
        lines += ["No threshold violations detected for this score month.", ""]
    else:
        lines += [f"- {f}" for f in flags]
        lines += [""]
    return lines


def _collect_flags(
    drift_rows: list,
    feat_psi_rows: list,
    dq_rows: list,
    pop_mix_rows: list,
) -> list[str]:
    flags: list[str] = []

    # Score PSI / KS / tail.
    for r in drift_rows:
        ch = r["channel"]
        psi_val = r.get("score_psi")
        ks_val  = r.get("score_ks")
        p90_val = r.get("pct_accounts_above_p90_baseline")
        p95_val = r.get("pct_accounts_above_p95_baseline")

        if psi_val is not None:
            p = float(psi_val)
            if p >= _PSI_ALERT:
                flags.append(f"[ALERT] {ch}: score PSI = {_f(psi_val)} — exceeds alert threshold {_PSI_ALERT}")
            elif p >= _PSI_WARN:
                flags.append(f"[WARN]  {ch}: score PSI = {_f(psi_val)} — exceeds warning threshold {_PSI_WARN}")

        if ks_val is not None and float(ks_val) >= _KS_WARN:
            flags.append(f"[WARN]  {ch}: score KS = {_f(ks_val)} — exceeds threshold {_KS_WARN}")

        if p90_val is not None and float(p90_val) >= _TAIL_WARN:
            flags.append(
                f"[WARN]  {ch}: {_pct(p90_val)} of accounts above baseline P90 "
                f"(threshold {_pct(_TAIL_WARN)})"
            )
        if p95_val is not None and float(p95_val) >= _TAIL_WARN:
            flags.append(
                f"[WARN]  {ch}: {_pct(p95_val)} of accounts above baseline P95 "
                f"(threshold {_pct(_TAIL_WARN)})"
            )

    # Feature PSI flags.
    feat_flagged: set[str] = set()
    for r in feat_psi_rows:
        fn = r["feature_name"]
        ch = r["channel"]
        psi_val = r.get("psi")
        if psi_val is not None and float(psi_val) >= _PSI_WARN and fn not in feat_flagged:
            level = "[ALERT]" if float(psi_val) >= _PSI_ALERT else "[WARN] "
            flags.append(
                f"{level} Feature {fn} ({ch}): PSI = {_f(psi_val)}"
            )
            feat_flagged.add(fn)

    # Data quality flags.
    for r in dq_rows:
        ch = r["channel"]
        mr = r.get("missing_score_rate")
        orl = r.get("outlier_score_rate")
        if mr is not None and float(mr) >= _MISSING_WARN:
            flags.append(f"[WARN]  {ch}: score missing rate = {_pct(mr, 2)} — exceeds {_pct(_MISSING_WARN, 2)}")
        if orl is not None and float(orl) >= _OUTLIER_WARN:
            flags.append(f"[WARN]  {ch}: score outlier rate = {_pct(orl, 2)} — exceeds {_pct(_OUTLIER_WARN, 2)}")

    # Unknown segment values.
    unknown = [r for r in pop_mix_rows if r.get("segment_value") == "Unknown"]
    if unknown:
        seg_types = sorted({r.get("segment_type") for r in unknown})
        flags.append(
            f"[INFO]  Null segment values replaced with 'Unknown' in: "
            f"{', '.join(seg_types)}"
        )

    return flags


# ── HTML / PDF export ────────────────────────────────────────────────

_CSS = """
/* ── Reset & base ───────────────────────────────────────────────── */
* { box-sizing: border-box; margin: 0; padding: 0; }

body {
    font-family: 'Helvetica Neue', Arial, sans-serif;
    font-size: 11pt;
    line-height: 1.6;
    color: #1a1a1a;
    background: #ffffff;
    max-width: 794px;
    margin: 0 auto;
    padding: 28px 40px 40px;
}

/* ── Headings ──────────────────────────────────────────────────── */
h1 {
    font-size: 19pt;
    color: #1a3a5c;
    border-bottom: 2px solid #1a3a5c;
    padding-bottom: 8px;
    margin-bottom: 8px;
}
h2 {
    font-size: 14pt;
    color: #1a3a5c;
    border-bottom: 1px solid #b8cfe0;
    padding-bottom: 4px;
    margin-top: 32px;
    margin-bottom: 12px;
}
h3 {
    font-size: 11pt;
    color: #2c5282;
    margin-top: 20px;
    margin-bottom: 8px;
}

/* ── Paragraph & blockquote ──────────────────────────────────────── */
p { margin-bottom: 10px; }
blockquote {
    border-left: 3px solid #b8cfe0;
    padding: 6px 14px;
    color: #555;
    margin: 10px 0 14px;
    background: #f7fafc;
    border-radius: 0 3px 3px 0;
}

/* ── Tables ──────────────────────────────────────────────────────── */
table {
    width: 100%;
    table-layout: fixed;
    border-collapse: collapse;
    font-size: 9.5pt;
    margin-bottom: 18px;
    word-wrap: break-word;
}
thead tr {
    background: #1a3a5c;
    color: #ffffff;
}
thead th {
    padding: 7px 9px;
    text-align: left;
    font-weight: 600;
    letter-spacing: 0.02em;
}
tbody tr:nth-child(odd)  { background: #f7fafc; }
tbody tr:nth-child(even) { background: #ffffff; }
tbody td {
    padding: 5px 9px;
    border-bottom: 1px solid #e2e8f0;
    vertical-align: top;
}

/* ── Code ────────────────────────────────────────────────────────── */
code {
    font-family: 'Courier New', Consolas, monospace;
    font-size: 9pt;
    background: #f0f4f8;
    border-radius: 3px;
    padding: 1px 4px;
}

/* ── Horizontal rule ─────────────────────────────────────────────── */
hr {
    border: none;
    border-top: 1px solid #e2e8f0;
    margin: 22px 0;
}

/* ── Page-break helper ───────────────────────────────────────────── */
.page-break {
    page-break-before: always;
    break-before: page;
    padding-top: 12pt;
}

/* ── Print / @page ───────────────────────────────────────────────── */
@media print {
    body { max-width: 100%; padding: 0; }
    a    { text-decoration: none; color: inherit; }
}
@page {
    size: A4;
    margin: 18mm 22mm;
}
"""

# H2 section letter-prefixes that start a new page when printed.
_PAGE_BREAK_H2 = frozenset(["B.", "C.", "D."])


def _md_to_html(md_text: str, title: str = "Model Report") -> str:
    """Convert *md_text* to a self-contained HTML document with embedded CSS.

    Requires either the ``markdown`` or ``markdown2`` package::

        pip install markdown        # preferred
        pip install markdown2       # alternative

    Raises ``ImportError`` with an installation hint if neither is available.
    Page-break CSS class is injected on the B / C / D section headings.
    """
    try:
        import markdown as _md
        body_html = _md.markdown(md_text, extensions=["tables"])
    except ImportError:
        try:
            import markdown2 as _md2
            body_html = _md2.markdown(md_text, extras=["tables"])
        except ImportError:
            raise ImportError(
                "HTML export requires 'markdown' or 'markdown2'. "
                "Install with: pip install markdown"
            )

    # Inject page-break class on major section <h2> headings.
    import re

    def _maybe_break(m: re.Match) -> str:
        text = m.group(1)
        prefix = text.split()[0] if text.split() else ""
        if prefix in _PAGE_BREAK_H2:
            return f'<h2 class="page-break">{text}</h2>'
        return m.group(0)

    body_html = re.sub(r"<h2>(.*?)</h2>", _maybe_break, body_html, flags=re.DOTALL)

    return (
        '<!DOCTYPE html>\n'
        '<html lang="en">\n'
        '<head>\n'
        '  <meta charset="UTF-8">\n'
        '  <meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        f'  <title>{title}</title>\n'
        f'  <style>{_CSS}  </style>\n'
        '</head>\n'
        '<body>\n'
        f'{body_html}\n'
        '</body>\n'
        '</html>\n'
    )


def _html_to_pdf(html_text: str, pdf_path: Path) -> bool:
    """Write *html_text* as a PDF to *pdf_path*.

    Tries WeasyPrint first (preferred), then pdfkit (requires wkhtmltopdf).
    Returns ``True`` on success, ``False`` if no PDF library is available.
    Install a library to enable PDF output::

        pip install weasyprint        # recommended
        pip install pdfkit            # alternative (also needs wkhtmltopdf)
    """
    try:
        from weasyprint import HTML as _WP  # type: ignore[import]
        _WP(string=html_text).write_pdf(str(pdf_path))
        return True
    except ImportError:
        pass

    try:
        import pdfkit as _pk  # type: ignore[import]
        _pk.from_string(html_text, str(pdf_path))
        return True
    except ImportError:
        pass

    return False


def _detect_html_backend() -> str | None:
    """Return the name of the available HTML-conversion library, or ``None``."""
    for name in ("markdown", "markdown2"):
        try:
            __import__(name)
            return name
        except ImportError:
            pass
    return None


def _detect_pdf_backend() -> str | None:
    """Return the name of the available PDF-generation library, or ``None``."""
    for name in ("weasyprint", "pdfkit"):
        try:
            __import__(name)
            return name
        except ImportError:
            pass
    return None


# ── Public API ────────────────────────────────────────────────────────

def build_report(
    outputs: dict[str, DataFrame],
    config: ModelConfig,
    score_month: str,
    model_version: str,
    output_dir: str | None = None,
) -> dict[str, str | None]:
    """Build a monitoring report from Layer 2 / Layer 3 DataFrames.

    When *output_dir* is supplied, three files are written to that directory
    with a date-stamped filename::

        {model}_report_{score_month}_{model_version}_{YYYY-MM-DD}.md
        {model}_report_{score_month}_{model_version}_{YYYY-MM-DD}.html
        {model}_report_{score_month}_{model_version}_{YYYY-MM-DD}.pdf

    HTML requires ``markdown`` or ``markdown2`` (``pip install markdown``).
    PDF requires ``weasyprint`` or ``pdfkit`` (``pip install weasyprint``).
    Missing backends are reported via a printed message and the corresponding
    path in the return dict is ``None``.

    Args:
        outputs: Flat ``{table_name: DataFrame}`` dict as returned by
            ``runner.run(return_outputs=True)``.
        config: Model configuration (same object passed to the pipeline).
        score_month: YYYY-MM label for this pipeline run.  Used to filter
            per-vintage rows; falls back to the latest vintage if not present.
        model_version: Version tag written into the report header.
        output_dir: Directory where report files are written.  Created
            automatically if it does not exist.  When ``None`` no files are
            written.

    Returns:
        ``{"md": path|None, "html": path|None, "pdf": path|None}`` — absolute
        paths to the files that were written, or ``None`` for outputs that
        were skipped.
    """
    timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    logger.info("Building report for model=%s, score_month=%s", config.name, score_month)

    # ── Look up table names from config ──────────────────────────────
    l1_name    = config.layer1.get("output_table", "")
    l2_std     = config.layer2.get("standard_tables", {})
    l3_tbls    = config.layer3.get("tables", {})

    overall_name  = l2_std.get("overall_summary", {}).get("output_table", "")
    dist_name     = l2_std.get("score_distribution", {}).get("output_table", "")
    src_name      = l2_std.get("by_source", {}).get("output_table", "")
    line_name     = l2_std.get("by_line", {}).get("output_table", "")
    drift_name    = l3_tbls.get("score_drift", {}).get("output_table", "")
    fpsi_name     = l3_tbls.get("feature_psi", {}).get("output_table", "")
    dq_name       = l3_tbls.get("data_quality", {}).get("output_table", "")
    corr_name     = l3_tbls.get("feature_score_relationship", {}).get("output_table", "")
    pop_name      = l3_tbls.get("population_mix", {}).get("output_table", "")

    # ── Resolve effective vintage month ───────────────────────────────
    drift_df = _get(outputs, drift_name)
    vm = _latest_vm(drift_df, score_month)
    if vm != score_month:
        logger.info(
            "score_month '%s' not in drift table — using latest vintage '%s'",
            score_month, vm,
        )

    # ── Collect Layer 1 count ─────────────────────────────────────────
    l1_df = _get(outputs, l1_name)
    l1_count: int | None = None
    if l1_df is not None:
        try:
            l1_count = l1_df.count()
        except Exception:
            pass

    # ── Collect Layer 2 data ──────────────────────────────────────────
    overall_rows  = _vm_rows(_get(outputs, overall_name), vm)
    dist_rows     = _vm_rows(_get(outputs, dist_name), vm)
    by_source_rows = _vm_rows(_get(outputs, src_name), vm)
    by_line_rows  = _vm_rows(_get(outputs, line_name), vm)

    # ── Collect Layer 3 data ──────────────────────────────────────────
    drift_rows    = _vm_rows(drift_df, vm)
    feat_psi_rows = _vm_rows(_get(outputs, fpsi_name), vm)
    dq_rows       = _vm_rows(_get(outputs, dq_name), vm)
    corr_rows     = _vm_rows(_get(outputs, corr_name), vm)
    pop_mix_rows  = _vm_rows(_get(outputs, pop_name), vm)

    # ── Collect flags ─────────────────────────────────────────────────
    flags = _collect_flags(drift_rows, feat_psi_rows, dq_rows, pop_mix_rows)

    # ── Assemble sections ─────────────────────────────────────────────
    parts: list[str] = []

    # Title block.
    parts += [
        f"# {config.display_name} — Reporting Run {vm}",
        "",
        f"> **Score month**: {vm} &nbsp;|&nbsp; "
        f"**Model version**: {model_version} &nbsp;|&nbsp; "
        f"**Generated**: {timestamp}",
        "",
        "---",
        "",
    ]

    # Section A.
    parts += [_sec_metadata(config, vm, model_version, timestamp, l1_count), "", "---", ""]

    # Section B.
    channels = config.source.get("channels", [])
    parts += _sec_executive(drift_rows, feat_psi_rows, channels)
    parts += ["---", ""]

    # Section C.
    band_decile_map: dict[str, list[int]] = {
        "Low": [1, 2, 3], "Medium": [4, 5, 6, 7], "High": [8, 9, 10]
    }
    parts += _sec_business(vm, overall_rows, dist_rows, by_source_rows, by_line_rows, band_decile_map)
    parts += ["---", ""]

    # Section D.
    parts += _sec_monitoring(vm, drift_rows, feat_psi_rows, corr_rows, pop_mix_rows, dq_rows)
    parts += ["---", ""]

    # Section E.
    parts += _sec_flags(flags)
    parts += ["---", "", f"*Report generated by the model_reporting framework · {config.name} · {model_version}*"]

    report = "\n".join(parts)

    # ── Write to files ────────────────────────────────────────────────
    paths: dict[str, str | None] = {"md": None, "html": None, "pdf": None}

    if output_dir:
        run_date = datetime.date.today().strftime("%Y-%m-%d")
        stem = f"{config.name}_report_{score_month}_{model_version}_{run_date}"
        dest_dir = Path(output_dir).resolve()
        dest_dir.mkdir(parents=True, exist_ok=True)

        # Markdown — canonical source, always written.
        md_path = dest_dir / f"{stem}.md"
        md_path.write_text(report, encoding="utf-8")
        paths["md"] = str(md_path)
        logger.info("Markdown report -> %s", md_path)

        # HTML — self-contained with embedded CSS.
        html_text: str | None = None
        html_path = dest_dir / f"{stem}.html"
        try:
            html_text = _md_to_html(report, title=f"{config.display_name} — {vm}")
            html_path.write_text(html_text, encoding="utf-8")
            paths["html"] = str(html_path)
            logger.info("HTML report    -> %s", html_path)
        except ImportError:
            print("HTML export skipped: markdown library not installed")

        # PDF — requires weasyprint or pdfkit.
        if html_text is not None:
            pdf_path = dest_dir / f"{stem}.pdf"
            ok = _html_to_pdf(html_text, pdf_path)
            if ok:
                paths["pdf"] = str(pdf_path)
                logger.info("PDF report     -> %s", pdf_path)
            else:
                print("PDF export skipped: no supported PDF backend installed")

    return paths
