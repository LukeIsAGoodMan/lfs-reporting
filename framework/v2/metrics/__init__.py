"""Frozen metric engine — v2.2.

Domain modules:
- stability_metrics: PSI, CSI, feature drift, data quality
- performance_metrics: EDR30/60/90, CO rates
- separation_metrics: KS, Gini, Odds, monotonicity
- calibration_metrics: M12 calibration only

All metric functions are deterministic, testable, and return
schema-typed results. Renderers consume outputs — they never recompute.

Imports are lazy (via __getattr__) to prevent circular import chains.
Use explicit submodule imports in new code::

    from framework.v2.metrics.stability_metrics import compute_stability
"""
from __future__ import annotations

# Lazy re-exports — metric submodules are NOT imported at package init
# time.  This prevents the cycle:
#   config → attribution → schemas → metrics/__init__ → stability → config

_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    "compute_stability":   ("framework.v2.metrics.stability_metrics",  "compute_stability"),
    "compute_data_quality": ("framework.v2.metrics.stability_metrics", "compute_data_quality"),
    "compute_performance": ("framework.v2.metrics.performance_metrics", "compute_performance"),
    "compute_separation":  ("framework.v2.metrics.separation_metrics",  "compute_separation"),
    "compute_calibration": ("framework.v2.metrics.calibration_metrics", "compute_calibration"),
}

__all__ = list(_LAZY_IMPORTS.keys())


def __getattr__(name: str):
    if name in _LAZY_IMPORTS:
        module_path, attr = _LAZY_IMPORTS[name]
        import importlib
        mod = importlib.import_module(module_path)
        return getattr(mod, attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
