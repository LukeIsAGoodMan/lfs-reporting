"""V2 monitoring configuration."""
from __future__ import annotations
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import yaml
from framework.utils import get_logger

logger = get_logger(__name__)

_CONF_DIR = Path(__file__).resolve().parent.parent.parent / "conf"

@dataclass
class ThresholdLevel:
    warning: float
    alert: float

@dataclass
class ThresholdConfig:
    """Hierarchical threshold configuration."""
    default: dict[str, ThresholdLevel]  # metric_name -> ThresholdLevel
    model_override: dict[str, ThresholdLevel] = field(default_factory=dict)
    scorecard_override: dict[str, dict[str, ThresholdLevel]] = field(default_factory=dict)
    # scorecard_override: {scorecard_id: {metric_name: ThresholdLevel}}

@dataclass
class BaselineConfig:
    type: str  # "fixed_window" | "rolling_window" | "initial_val"
    start_month: str | None = None
    end_month: str | None = None
    minimum_history_months: int = 3
    fallback_method: str = "first_stable_window"

@dataclass
class SampleSizeRules:
    minimum_accounts_for_ks: int = 300
    minimum_bads_for_ks: int = 30
    minimum_accounts_for_calibration_bin: int = 50

@dataclass
class ActualSourceConfig:
    """Configuration for loading actual performance data from a production table."""
    enabled: bool = False
    database: str = ""
    table: str = ""
    key_cols: list[str] = field(default_factory=lambda: ["creditaccountid"])
    score_month_col: str = "score_month"

@dataclass
class TargetDefinition:
    """Defines the model target and maturity-window metric mapping.

    Calibration is assessed ONLY at M12 (1-year charge-off).
    M3/M6/M9 are early-read performance / separation windows.
    """
    target_name: str = "1-year charge-off"
    calibration_target: str = "badco_m12"
    early_read_targets: dict[str, str] = field(default_factory=lambda: {
        "M3": "edr30_m3", "M6": "edr60_m6", "M9": "edr90_m9",
    })
    separation_targets: dict[str, str] = field(default_factory=lambda: {
        "M3": "bad30_m3", "M6": "bad60_m6", "M9": "bad90_m9", "M12": "badco_m12",
    })

@dataclass
class MonitoringConfig:
    model_name: str
    display_name: str
    score_col: str
    channel_col: str
    channels: list[str]
    key_cols: list[str]
    feature_cols: list[str]
    segment_cols: list[dict]  # [{column, segment_type}]

    baseline: BaselineConfig
    thresholds: ThresholdConfig
    sample_rules: SampleSizeRules

    scorecard_enabled: bool = False
    scorecard_column: str = "scorecard_id"

    backfill_enabled: bool = False
    backfill_history_months: int = 18
    backfill_minimum_baseline: int = 12

    explanation_enabled: bool = False

    actual_source: ActualSourceConfig | None = None
    target: TargetDefinition = field(default_factory=TargetDefinition)

    maturity_on_missing: str = "skip_with_note"  # "skip_with_note" | "fail"

    psi_n_bins: int = 10
    n_bins: int = 10

    output_database: str = "reporting"
    output_format: str = "parquet"

def load_monitoring_config(config_path: str | None = None, model_name: str = "lfs") -> MonitoringConfig:
    """Load v2 monitoring config from YAML."""
    if config_path is None:
        config_path = str(_CONF_DIR / "models" / f"{model_name}_v2.yaml")

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"V2 config not found: {path}")

    with open(path) as fh:
        raw = yaml.safe_load(fh)

    return _parse_config(raw)

def _parse_config(raw: dict) -> MonitoringConfig:
    """Parse raw YAML dict into MonitoringConfig."""
    model = raw["model"]
    source = raw["source"]
    monitoring = raw.get("monitoring", {})

    # Parse baseline
    bl_raw = raw.get("baseline_definition", {})
    baseline = BaselineConfig(
        type=bl_raw.get("type", "fixed_window"),
        start_month=bl_raw.get("start_month"),
        end_month=bl_raw.get("end_month"),
        minimum_history_months=bl_raw.get("initial_val_config", {}).get("minimum_history_months", 3),
        fallback_method=bl_raw.get("initial_val_config", {}).get("fallback_method", "first_stable_window"),
    )

    # Parse thresholds
    th_raw = raw.get("threshold_config", {})
    thresholds = _parse_thresholds(th_raw)

    # Parse sample rules
    sr_raw = raw.get("sample_size_rules", {})
    sample_rules = SampleSizeRules(
        minimum_accounts_for_ks=sr_raw.get("minimum_accounts_for_ks", 300),
        minimum_bads_for_ks=sr_raw.get("minimum_bads_for_ks", 30),
        minimum_accounts_for_calibration_bin=sr_raw.get("minimum_accounts_for_calibration_bin", 50),
    )

    # Parse scorecard
    sc_raw = raw.get("scorecard_config", {})

    # Parse explanation
    ex_raw = raw.get("explanation_config", {})

    # Parse backfill
    bf_raw = raw.get("backfill_config", {})

    # Parse actual source
    as_raw = raw.get("actual_source", {})
    actual_source = ActualSourceConfig(
        enabled=as_raw.get("enabled", False),
        database=as_raw.get("database", ""),
        table=as_raw.get("table", ""),
        key_cols=as_raw.get("key_cols", ["creditaccountid"]),
        score_month_col=as_raw.get("score_month_col", "score_month"),
    ) if as_raw else None

    # Parse target definition
    td_raw = raw.get("target_definition", {})
    target = TargetDefinition(
        target_name=td_raw.get("target_name", "1-year charge-off"),
        calibration_target=td_raw.get("calibration_target", "badco_m12"),
        early_read_targets=td_raw.get("early_read_targets", {"M3": "edr30_m3", "M6": "edr60_m6", "M9": "edr90_m9"}),
        separation_targets=td_raw.get("separation_targets", {"M3": "bad30_m3", "M6": "bad60_m6", "M9": "bad90_m9", "M12": "badco_m12"}),
    )

    return MonitoringConfig(
        model_name=model["name"],
        display_name=model.get("display_name", model["name"]),
        score_col=source.get("score_col", "lfs_score"),
        channel_col=source.get("channel_col", "channel"),
        channels=source.get("channels", []),
        key_cols=source.get("key_cols", ["creditaccountid"]),
        feature_cols=source.get("columns", {}).get("features", []),
        segment_cols=monitoring.get("segments", []),
        baseline=baseline,
        thresholds=thresholds,
        sample_rules=sample_rules,
        scorecard_enabled=sc_raw.get("enabled", False),
        scorecard_column=sc_raw.get("scorecard_column", "scorecard_id"),
        backfill_enabled=bf_raw.get("enabled", False),
        backfill_history_months=bf_raw.get("history_months", 18),
        backfill_minimum_baseline=bf_raw.get("minimum_months_for_baseline", 12),
        explanation_enabled=ex_raw.get("enabled", False),
        actual_source=actual_source,
        target=target,
        maturity_on_missing=raw.get("maturity_handling", {}).get("on_missing", "skip_with_note"),
        psi_n_bins=monitoring.get("psi_n_bins", 10),
        n_bins=monitoring.get("n_bins", 10),
        output_database=raw.get("output", {}).get("database", "reporting"),
        output_format=raw.get("output", {}).get("format", "parquet"),
    )

def _parse_thresholds(raw: dict) -> ThresholdConfig:
    """Parse threshold_config section."""
    def _parse_levels(d: dict) -> dict[str, ThresholdLevel]:
        result = {}
        for metric, levels in d.items():
            if isinstance(levels, dict) and "warning" in levels:
                result[metric] = ThresholdLevel(
                    warning=float(levels["warning"]),
                    alert=float(levels["alert"]),
                )
        return result

    default = _parse_levels(raw.get("default", {}))
    model_override = _parse_levels(raw.get("model_override", {}))

    scorecard_override: dict[str, dict[str, ThresholdLevel]] = {}
    for sc_id, sc_metrics in raw.get("scorecard_override", {}).items():
        scorecard_override[str(sc_id)] = _parse_levels(sc_metrics)

    return ThresholdConfig(
        default=default,
        model_override=model_override,
        scorecard_override=scorecard_override,
    )
