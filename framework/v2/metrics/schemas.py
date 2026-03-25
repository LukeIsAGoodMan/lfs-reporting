"""Standardized output schemas for the frozen metric engine.

Every metric function returns a typed result. Renderers consume
these schemas — they never access raw DataFrames.
"""
from __future__ import annotations
from dataclasses import dataclass, field


# ── N/A reason codes ──────────────────────────────────────────────────

NA_MATURITY = "NOT_AVAILABLE_MATURITY"
NA_DATA_MODE = "NOT_AVAILABLE_DATA_MODE"
NA_NO_ACTUALS = "NOT_AVAILABLE_NO_ACTUALS"
NA_EMPTY_SEGMENT = "NOT_AVAILABLE_EMPTY_SEGMENT"
NA_SAMPLE_SIZE = "NOT_AVAILABLE_SAMPLE_SIZE"
NA_NOT_M12 = "NOT_AVAILABLE_NOT_M12"

# Human-readable messages keyed by audience
NA_DISPLAY = {
    "technical": {
        NA_MATURITY: "Maturity window data not available",
        NA_DATA_MODE: "Actuals not loaded (demo/stability-only mode)",
        NA_NO_ACTUALS: "No actual performance data provided",
        NA_EMPTY_SEGMENT: "Segment contains no accounts",
        NA_SAMPLE_SIZE: "Sample size below minimum threshold",
        NA_NOT_M12: "Calibration assessed at M12 only",
    },
    "business": {
        NA_MATURITY: "Not enough time has passed to observe this outcome",
        NA_DATA_MODE: "Actual charge-off data is not yet available",
        NA_NO_ACTUALS: "Performance data has not been loaded",
        NA_EMPTY_SEGMENT: "No accounts in this group",
        NA_SAMPLE_SIZE: "Too few accounts for a reliable estimate",
        NA_NOT_M12: "Calibration is only assessed at the 12-month window",
    },
}


def na_display(reason: str | None, audience: str = "technical") -> str:
    """Map a reason code to audience-appropriate display text."""
    if reason is None:
        return ""
    return NA_DISPLAY.get(audience, NA_DISPLAY["technical"]).get(reason, reason)


# ── Feature PSI row ──────────────────────────────────────────────────

@dataclass
class FeaturePSIRow:
    feature_name: str
    psi: float
    status: str
    baseline_mean: float | None
    current_mean: float | None
    mean_shift: float | None


@dataclass
class ChannelStability:
    score_psi: float
    score_psi_status: str
    mean_score: float
    std_score: float
    account_count: int


@dataclass
class StabilityResult:
    score_psi: float
    score_psi_status: str
    feature_psi: list[FeaturePSIRow]
    channels: dict[str, ChannelStability]
    na_reason: str | None = None


# ── Odds row ─────────────────────────────────────────────────────────

@dataclass
class OddsRow:
    score_bin: int
    total: int
    goods: int
    bads: int
    odds: float
    bad_rate: float
    is_monotonic: bool = True
    total_misranks: int = 0


@dataclass
class SeparationResult:
    label: str
    display_label: str
    score_month: str
    ks: float | None
    gini: float | None
    ks_drop: float | None = None
    ks_status: str = ""
    ks_drop_status: str = ""
    odds: list[OddsRow] = field(default_factory=list)
    odds_monotonic: bool = True
    misrank_count: int = 0
    account_count: int = 0
    na_reason: str | None = None


# ── Performance ──────────────────────────────────────────────────────

@dataclass
class PerformanceRow:
    maturity: str
    display_label: str
    score_month: str
    channel: str
    account_count: int = 0
    avg_score: float = 0.0
    edr: float | None = None
    edr_count: int = 0
    bad_rate: float | None = None
    bad_count: int = 0
    edr_status: str = ""
    note: str = ""


@dataclass
class PerformanceResult:
    rows: list[PerformanceRow] = field(default_factory=list)
    na_reason: str | None = None


# ── Calibration ──────────────────────────────────────────────────────

@dataclass
class CalibrationRow:
    channel: str
    score_bin: int
    account_count: int
    bad_count: int
    predicted_rate: float
    actual_rate: float
    calibration_gap: float
    gap_status: str
    sample_sufficient: bool = True


@dataclass
class CalibrationResult:
    rows: list[CalibrationRow] = field(default_factory=list)
    max_abs_gap: float = 0.0
    mean_abs_gap: float = 0.0
    worst_bin: int = 0
    na_reason: str | None = None


# ── Data Quality ─────────────────────────────────────────────────────

@dataclass
class DataQualityResult:
    score_missing_rate: float = 0.0
    score_outlier_rate: float = 0.0
    total_accounts: int = 0
    channels: dict = field(default_factory=dict)
    feature_quality: list = field(default_factory=list)
    na_reason: str | None = None


# ── Attribution ──────────────────────────────────────────────────────

@dataclass
class AttributionDriver:
    dimension: str          # e.g. "channel", "source"
    segment: str            # e.g. "digital", "organic"
    contribution: float     # signed contribution to delta
    direction: str          # "up" | "down"
    explanation: str        # plain English sentence


@dataclass
class AttributionResult:
    is_material: bool = False
    delta: float = 0.0
    metric_name: str = ""   # e.g. "EDR90"
    drivers: list[AttributionDriver] = field(default_factory=list)
    narrative: str = ""
    na_reason: str | None = None
