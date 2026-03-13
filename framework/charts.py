"""Chart generation for the monitoring report.

All charts use matplotlib with the Agg (non-interactive) backend so they
can be rendered in headless / notebook environments without a display.

Charts are returned as base64-encoded PNG strings suitable for embedding
in the HTML report as <img src="data:image/png;base64,..."> tags.

Install matplotlib to enable charts::

    pip install matplotlib
"""
from __future__ import annotations

import base64
import io

from framework.config import ModelConfig
from framework.utils import get_logger

logger = get_logger(__name__)

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mtick
    _MPL = True
except ImportError:
    _MPL = False


# ── Colour palette ────────────────────────────────────────────────────

_CHANNEL_COLORS: dict[str, str] = {
    "digital":    "#2c5282",
    "directmail": "#c05621",
}
_SEG_COLORS = ["#2c5282", "#c05621", "#276749", "#744210", "#6b46c1", "#285e61"]


def _ch_color(ch: str) -> str:
    return _CHANNEL_COLORS.get(ch, "#555555")


def _fig_to_b64(fig) -> str:
    """Serialise a matplotlib Figure to a base64-encoded PNG string."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight", facecolor="white")
    buf.seek(0)
    data = base64.b64encode(buf.read()).decode("utf-8")
    plt.close(fig)
    return data


def _style_ax(
    ax,
    title: str = "",
    xlabel: str = "",
    ylabel: str = "",
) -> None:
    """Apply a clean, consistent style to a matplotlib Axes."""
    if title:
        ax.set_title(title, fontsize=9.5, fontweight="bold", color="#1a3a5c", pad=7)
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=7.5, color="#555")
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=7.5, color="#555")
    ax.tick_params(labelsize=7, colors="#555")
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color("#e2e8f0")
    ax.yaxis.grid(True, color="#e2e8f0", linewidth=0.7, zorder=0)
    ax.set_axisbelow(True)


def _rot_xlabels(ax, rotation: int = 30) -> None:
    ax.set_xticklabels(ax.get_xticklabels(), rotation=rotation, ha="right")


# ── Individual chart builders ─────────────────────────────────────────

def _chart_score_drift_trend(
    drift_rows: list[dict],
    channels: list[str],
) -> str | None:
    """Dual-panel: avg score trend + PSI trend, one line per channel."""
    if not drift_rows:
        return None

    fig, axes = plt.subplots(1, 2, figsize=(9, 3.2))
    fig.suptitle(
        "Score Drift Trend by Channel",
        fontsize=10.5, fontweight="bold", color="#1a3a5c", y=1.02,
    )
    ax_mean, ax_psi = axes

    for ch in sorted(channels):
        ch_rows = sorted(
            [r for r in drift_rows if r.get("channel") == ch],
            key=lambda x: x.get("vintage_month", ""),
        )
        xs     = [r["vintage_month"] for r in ch_rows]
        means  = [float(r.get("mean_lfs_score") or 0) for r in ch_rows]
        psis   = [float(r.get("score_psi") or 0) for r in ch_rows]
        color  = _ch_color(ch)
        ax_mean.plot(xs, means, marker="o", markersize=4, linewidth=1.6,
                     label=ch, color=color)
        ax_psi.plot(xs, psis, marker="s", markersize=4, linewidth=1.6,
                    label=ch, color=color)

    ax_psi.axhline(0.10, color="#d69e2e", lw=0.9, ls="--", alpha=0.8)
    ax_psi.axhline(0.25, color="#c53030", lw=0.9, ls="--", alpha=0.8)

    _style_ax(ax_mean, "Average LFS Score", ylabel="Avg Score")
    _style_ax(ax_psi,  "Score PSI vs Baseline", ylabel="PSI")

    for ax in axes:
        _rot_xlabels(ax)
        ax.legend(fontsize=7, framealpha=0.4)

    fig.tight_layout()
    return _fig_to_b64(fig)


def _chart_feature_psi_bar(
    feat_psi_rows: list[dict],
    score_month: str,
) -> str | None:
    """Horizontal bar chart: top-10 features by max PSI across channels."""
    if not feat_psi_rows:
        return None

    latest = [r for r in feat_psi_rows if r.get("vintage_month") == score_month]
    if not latest:
        latest = feat_psi_rows

    feat_psi: dict[str, float] = {}
    for r in latest:
        fn = r.get("feature_name", "")
        p  = float(r.get("psi") or 0)
        feat_psi[fn] = max(feat_psi.get(fn, 0.0), p)

    top = sorted(feat_psi.items(), key=lambda x: -x[1])[:10]
    if not top:
        return None

    features = [t[0] for t in top][::-1]
    values   = [t[1] for t in top][::-1]
    colors   = [
        "#c53030" if v >= 0.25 else "#d69e2e" if v >= 0.10 else "#2c5282"
        for v in values
    ]

    fig, ax = plt.subplots(figsize=(7, max(3.0, len(features) * 0.38 + 1.0)))
    bars = ax.barh(features, values, color=colors, height=0.55, zorder=3)

    ax.axvline(0.10, color="#d69e2e", lw=0.9, ls="--", alpha=0.8, label="Warn 0.10")
    ax.axvline(0.25, color="#c53030", lw=0.9, ls="--", alpha=0.8, label="Alert 0.25")

    for bar, val in zip(bars, values):
        ax.text(
            val + max(values) * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"{val:.3f}", va="center", ha="left", fontsize=7, color="#333",
        )

    _style_ax(ax, f"Feature PSI — Top 10  (vintage {score_month})", xlabel="PSI")
    ax.yaxis.grid(False)
    ax.set_xlim(0, max(values) * 1.25 + 0.005)
    ax.legend(fontsize=7, framealpha=0.4, loc="lower right")
    fig.tight_layout()
    return _fig_to_b64(fig)


def _chart_calibration(
    calib_rows: list[dict],
    channels: list[str],
    score_month: str,
) -> str | None:
    """Grouped bar chart: predicted vs actual bad rate per score decile."""
    if not calib_rows:
        return None

    latest = [r for r in calib_rows if r.get("vintage_month") == score_month]
    if not latest:
        latest = calib_rows

    sorted_channels = sorted(channels)
    fig, axes = plt.subplots(
        1, len(sorted_channels),
        figsize=(4.8 * len(sorted_channels), 3.6),
        squeeze=False,
    )
    fig.suptitle(
        f"Calibration: Predicted vs Actual Bad Rate  (vintage {score_month})",
        fontsize=10.5, fontweight="bold", color="#1a3a5c", y=1.03,
    )

    for i, ch in enumerate(sorted_channels):
        ax = axes[0][i]
        ch_rows = sorted(
            [r for r in latest if r.get("channel") == ch],
            key=lambda x: x.get("score_bin") or 0,
        )
        if not ch_rows:
            ax.text(0.5, 0.5, "No data", ha="center", va="center",
                    transform=ax.transAxes, fontsize=8, color="#888")
            _style_ax(ax, ch.capitalize())
            continue

        pred   = [float(r.get("predicted_rate") or 0) for r in ch_rows]
        actual = [float(r.get("actual_rate") or 0) for r in ch_rows]
        bins   = [str(int(r.get("score_bin") or 0)) for r in ch_rows]

        x = list(range(len(bins)))
        w = 0.36
        ax.bar([xi - w / 2 for xi in x], pred,   w, label="Predicted",
               color=_ch_color(ch), alpha=0.85, zorder=3)
        ax.bar([xi + w / 2 for xi in x], actual, w, label="Actual",
               color="#68d391", alpha=0.85, zorder=3)

        ax.set_xticks(x)
        ax.set_xticklabels(bins, rotation=45, ha="right", fontsize=6.5)
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0, decimals=0))
        _style_ax(ax, ch.capitalize(), xlabel="Score Decile", ylabel="Bad Rate")
        ax.legend(fontsize=7, framealpha=0.4)

    fig.tight_layout()
    return _fig_to_b64(fig)


def _chart_population_mix(
    pop_mix_rows: list[dict],
    channels: list[str],
    score_month: str,
) -> str | None:
    """Stacked bar chart: acquisition source mix by vintage month."""
    source_rows = [r for r in pop_mix_rows if r.get("segment_type") == "source"]
    if not source_rows:
        return None

    sorted_channels = sorted(channels)
    fig, axes = plt.subplots(
        1, len(sorted_channels),
        figsize=(5 * len(sorted_channels), 3.4),
        squeeze=False,
    )
    fig.suptitle(
        "Population Mix — Acquisition Source",
        fontsize=10.5, fontweight="bold", color="#1a3a5c", y=1.03,
    )

    for i, ch in enumerate(sorted_channels):
        ax = axes[0][i]
        ch_rows = [r for r in source_rows if r.get("channel") == ch]
        months  = sorted({r["vintage_month"] for r in ch_rows})
        sources = sorted({str(r.get("segment_value", "")) for r in ch_rows})

        bottom = [0.0] * len(months)
        for j, src in enumerate(sources):
            vals = []
            for m in months:
                matches = [r for r in ch_rows
                           if r["vintage_month"] == m and r.get("segment_value") == src]
                vals.append(float(matches[0].get("pct_of_channel_accounts") or 0)
                            if matches else 0.0)
            ax.bar(
                range(len(months)), vals, bottom=bottom, label=src,
                color=_SEG_COLORS[j % len(_SEG_COLORS)], alpha=0.85, zorder=3,
            )
            bottom = [b + v for b, v in zip(bottom, vals)]

        ax.set_xticks(range(len(months)))
        ax.set_xticklabels(months, rotation=30, ha="right", fontsize=7)
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0, decimals=0))
        _style_ax(ax, ch.capitalize(), ylabel="Share of Channel")
        ax.legend(fontsize=7, framealpha=0.4, loc="upper right")
        ax.set_ylim(0, 1.08)

    fig.tight_layout()
    return _fig_to_b64(fig)


# ── Public API ────────────────────────────────────────────────────────

def build_charts(
    outputs: dict,
    config: ModelConfig,
    score_month: str,
) -> dict[str, str]:
    """Generate all monitoring charts from *outputs*.

    Args:
        outputs: ``{table_name: DataFrame}`` from the pipeline run.
        config: Model configuration.
        score_month: Latest score month (used for axis labels / filtering).

    Returns:
        ``{chart_name: base64_png_string}`` — only successfully generated
        charts are included.  Empty dict if matplotlib is not installed.
    """
    if not _MPL:
        logger.warning(
            "matplotlib not available — charts skipped.  "
            "Install with: pip install matplotlib"
        )
        return {}

    l3_tbls  = config.layer3.get("tables", {})
    channels = config.source.get("channels", [])

    def _rows(table_key: str) -> list[dict]:
        name = l3_tbls.get(table_key, {}).get("output_table", "")
        df   = outputs.get(name)
        if df is None:
            return []
        return [row.asDict() for row in df.collect()]

    drift_rows    = _rows("score_drift")
    fpsi_rows     = _rows("feature_psi")
    pop_mix_rows  = _rows("population_mix")
    calib_rows    = _rows("calibration")

    charts: dict[str, str] = {}

    def _try(name: str, fn, *args) -> None:
        try:
            result = fn(*args)
            if result:
                charts[name] = result
                logger.debug("Chart generated: %s", name)
        except Exception as exc:
            logger.warning("Chart '%s' failed: %s", name, exc)

    _try("score_drift_trend", _chart_score_drift_trend, drift_rows, channels)
    _try("feature_psi_bar",   _chart_feature_psi_bar,   fpsi_rows, score_month)
    _try("calibration",       _chart_calibration,       calib_rows, channels, score_month)
    _try("population_mix",    _chart_population_mix,    pop_mix_rows, channels, score_month)

    logger.info("Charts built: %d", len(charts))
    return charts
