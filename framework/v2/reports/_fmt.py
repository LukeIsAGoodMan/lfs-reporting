"""Shared formatting helpers for V2 report renderers.

Single source of truth for value formatting, table rendering,
and status display. Both MMR and Business reports import from here.
"""
from __future__ import annotations


def f(val, decimals: int = 4) -> str:
    """Format a scalar as fixed-decimal; '--' for None."""
    if val is None:
        return "--"
    try:
        return f"{float(val):.{decimals}f}"
    except (TypeError, ValueError):
        return str(val)


def pct(val, decimals: int = 2) -> str:
    """Format as percentage; '--' for None."""
    if val is None:
        return "--"
    try:
        return f"{float(val) * 100:.{decimals}f}%"
    except (TypeError, ValueError):
        return str(val)


def tbl(headers: list[str], rows: list[list]) -> str:
    """Build a markdown table."""
    if not rows:
        return ""
    header_line = "| " + " | ".join(str(h) for h in headers) + " |"
    sep_line = "| " + " | ".join("---" for _ in headers) + " |"
    body_lines = []
    for row in rows:
        body_lines.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\n".join([header_line, sep_line] + body_lines)


def status_icon(status: str) -> str:
    """Technical status label for MMR."""
    return {"ALERT": "**[ALERT]**", "WARNING": "[WARN]", "OK": "OK"}.get(status, status)


def status_label_business(status: str) -> str:
    """Business-friendly status label."""
    return {
        "ALERT": "Requires Attention",
        "WARNING": "Under Observation",
        "OK": "Healthy",
    }.get(status, status)


def na_note(reason: str | None, audience: str = "technical") -> str:
    """Render an N/A note for a missing section."""
    if reason is None:
        return ""
    from framework.v2.metrics.schemas import na_display
    text = na_display(reason, audience)
    return f"*{text}*" if text else ""
