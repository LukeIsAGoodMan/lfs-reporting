"""Frozen metric engine — v2.2.

Domain modules:
- stability_metrics: PSI, CSI, feature drift
- performance_metrics: EDR30/60/90, CO rates
- separation_metrics: KS, Gini, Odds, monotonicity
- calibration_metrics: M12 calibration only

All metric functions are deterministic, testable, and return
schema-typed results. Renderers consume outputs — they never recompute.
"""
from __future__ import annotations

from framework.v2.metrics.stability_metrics import compute_stability
from framework.v2.metrics.performance_metrics import compute_performance
from framework.v2.metrics.separation_metrics import compute_separation
from framework.v2.metrics.calibration_metrics import compute_calibration
from framework.v2.metrics.stability_metrics import compute_data_quality

__all__ = [
    "compute_stability",
    "compute_separation",
    "compute_performance",
    "compute_calibration",
    "compute_data_quality",
]
