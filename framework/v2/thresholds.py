"""Hierarchical threshold governance engine.

Resolution priority: scorecard_override -> model_override -> default.
"""
from __future__ import annotations
from framework.v2.config import MonitoringConfig, ThresholdLevel
from framework.utils import get_logger

logger = get_logger(__name__)

# Status constants
ALERT = "ALERT"
WARNING = "WARNING"
OK = "OK"

# Default fallbacks if metric not in config at all
_BUILTIN_DEFAULTS: dict[str, ThresholdLevel] = {
    "psi":             ThresholdLevel(warning=0.10, alert=0.25),
    "ks_drop":         ThresholdLevel(warning=-0.03, alert=-0.07),
    "calibration_gap": ThresholdLevel(warning=0.02, alert=0.05),
    "edr_delta":       ThresholdLevel(warning=0.01, alert=0.03),
    "missing_rate":    ThresholdLevel(warning=0.01, alert=0.05),
    "feature_psi":     ThresholdLevel(warning=0.10, alert=0.25),
}


class ThresholdEngine:
    """Resolve and evaluate monitoring thresholds with hierarchy."""

    def __init__(self, config: MonitoringConfig):
        self._config = config.thresholds

    def resolve(
        self,
        metric: str,
        level: str,
        scorecard_id: str | None = None,
    ) -> float:
        """Resolve the threshold value for a metric at a given level.

        Args:
            metric: Metric name (e.g. "psi", "ks_drop", "calibration_gap")
            level: "warning" or "alert"
            scorecard_id: Optional scorecard for scorecard-level override

        Returns:
            Threshold value with priority:
            scorecard_override -> model_override -> default -> builtin
        """
        # 1. Scorecard override
        if scorecard_id and scorecard_id in self._config.scorecard_override:
            sc_metrics = self._config.scorecard_override[scorecard_id]
            if metric in sc_metrics:
                tl = sc_metrics[metric]
                return tl.warning if level == "warning" else tl.alert

        # 2. Model override
        if metric in self._config.model_override:
            tl = self._config.model_override[metric]
            return tl.warning if level == "warning" else tl.alert

        # 3. Default
        if metric in self._config.default:
            tl = self._config.default[metric]
            return tl.warning if level == "warning" else tl.alert

        # 4. Builtin fallback
        if metric in _BUILTIN_DEFAULTS:
            tl = _BUILTIN_DEFAULTS[metric]
            return tl.warning if level == "warning" else tl.alert

        logger.warning("No threshold defined for metric '%s' — using 0.0", metric)
        return 0.0

    def evaluate(
        self,
        metric: str,
        value: float,
        scorecard_id: str | None = None,
        higher_is_worse: bool = True,
    ) -> str:
        """Evaluate a metric value against thresholds.

        Args:
            metric: Metric name
            value: Observed value
            scorecard_id: Optional scorecard
            higher_is_worse: If True, value >= alert -> ALERT.
                If False (e.g. ks_drop), value <= alert -> ALERT.

        Returns:
            "ALERT", "WARNING", or "OK"
        """
        warn_threshold = self.resolve(metric, "warning", scorecard_id)
        alert_threshold = self.resolve(metric, "alert", scorecard_id)

        if higher_is_worse:
            if value >= alert_threshold:
                return ALERT
            if value >= warn_threshold:
                return WARNING
            return OK
        else:
            # Lower is worse (e.g. KS drop)
            if value <= alert_threshold:
                return ALERT
            if value <= warn_threshold:
                return WARNING
            return OK

    def evaluate_row(
        self,
        metric: str,
        value: float,
        scorecard_id: str | None = None,
        higher_is_worse: bool = True,
    ) -> dict:
        """Evaluate and return a full status dict."""
        status = self.evaluate(metric, value, scorecard_id, higher_is_worse)
        return {
            "metric": metric,
            "value": value,
            "warning_threshold": self.resolve(metric, "warning", scorecard_id),
            "alert_threshold": self.resolve(metric, "alert", scorecard_id),
            "status": status,
            "scorecard_id": scorecard_id or "overall",
        }
