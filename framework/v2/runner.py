"""V2 monitoring pipeline orchestrator.

Coordinates all monitoring steps in sequence:
1. Validate inputs
2. Apply monitoring filter
3. Compute baseline
4. Build current cohort (for stability metrics)
5. Build performance cohorts (for separation/performance/calibration)
6. Get scorecard groups
7. For each scorecard group:
   - Compute stability, separation, performance, calibration, DQ
   - Evaluate thresholds
8. Generate business report
9. Generate MMR report
10. Return MonitoringResult
"""
from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from framework.v2.config import MonitoringConfig, load_monitoring_config
from framework.v2.data_model import validate_score_mart, validate_perf_mart
from framework.v2.cohort import build_current_cohort, build_performance_cohorts
from framework.v2.baseline import compute_baseline
from framework.v2.thresholds import ThresholdEngine
from framework.v2.scorecard import get_scorecard_groups
from framework.v2 import metrics as v2_metrics
from framework.v2.explanation import compute_explanation
from framework.v2.context import build_context, BaseReportContext
from framework.utils import get_logger

logger = get_logger(__name__)


@dataclass
class MonitoringResult:
    """Container for full monitoring pipeline results."""

    reporting_month: str
    model_name: str

    # Core results per scorecard group
    stability: dict = field(default_factory=dict)      # {scorecard_id: stability_dict}
    separation: dict = field(default_factory=dict)      # {scorecard_id: {maturity: separation_dict}}
    performance: dict = field(default_factory=dict)     # {scorecard_id: [perf_rows]}
    calibration: dict = field(default_factory=dict)     # {scorecard_id: {maturity: [calib_rows]}}
    data_quality: dict = field(default_factory=dict)    # {scorecard_id: dq_dict}

    # Metadata
    baseline_info: dict = field(default_factory=dict)
    cohort_info: dict = field(default_factory=dict)
    flags: list = field(default_factory=list)
    explanation: dict | None = None

    # Report paths
    business_report: str | None = None
    mmr_report: str | None = None

    # Shared report context
    context: BaseReportContext | None = None

    data_mode: str = ""
    run_timestamp: str = ""


def run_monitoring(
    score_mart: DataFrame,
    perf_mart: DataFrame | None,
    reporting_month: str,
    config: MonitoringConfig | str | None = None,
    *,
    spark: SparkSession | None = None,
    output_dir: str | None = None,
    model_name: str = "lfs",
) -> MonitoringResult:
    """Run the full v2 monitoring pipeline.

    Args:
        score_mart: Score snapshot mart (all months).
        perf_mart: Performance mart (optional -- skip perf/sep/calib if None).
        reporting_month: "YYYY-MM" format.
        config: MonitoringConfig, path to YAML, or None (auto-load).
        spark: SparkSession (auto-detected from score_mart if not given).
        output_dir: Directory for report files (default: ./outputs).
        model_name: Model name for config auto-loading.

    Returns:
        MonitoringResult with all metrics, flags, and report paths.
    """
    # 1. Load config
    if config is None:
        config = load_monitoring_config(model_name=model_name)
    elif isinstance(config, str):
        config = load_monitoring_config(config_path=config)

    if spark is None:
        spark = score_mart.sparkSession

    if output_dir is None:
        output_dir = "outputs"

    result = MonitoringResult(
        reporting_month=reporting_month,
        model_name=config.model_name,
        run_timestamp=datetime.datetime.now().isoformat(),
    )

    # ── Auto-load perf mart if not provided ───────────────────────────
    if perf_mart is None and config.actual_source and config.actual_source.enabled:
        from framework.v2.perf_mart import build_perf_mart_from_source
        logger.info("Loading perf mart from configured actual source")
        perf_mart = build_perf_mart_from_source(spark, config)

    data_mode = "CONFIG-SOURCED" if (config.actual_source and config.actual_source.enabled) else "MOCK/INJECTED"
    logger.info("Data mode: %s", data_mode)
    result.data_mode = data_mode

    # 2. Validate inputs
    score_warnings = validate_score_mart(score_mart)
    if score_warnings:
        for w in score_warnings:
            logger.warning("Score mart: %s", w)

    if perf_mart is not None:
        perf_warnings = validate_perf_mart(perf_mart)
        for w in perf_warnings:
            logger.warning("Perf mart: %s", w)

    # 3. Compute baseline
    logger.info("Computing baseline for %s", reporting_month)
    baseline_result = compute_baseline(score_mart, config, reporting_month)
    result.baseline_info = {
        "type": baseline_result.type,
        "start_month": baseline_result.start_month,
        "end_month": baseline_result.end_month,
        "months": baseline_result.months,
        "account_count": baseline_result.account_count,
    }

    # 4. Build current cohort (for stability)
    logger.info("Building current cohort for %s", reporting_month)
    current_df = build_current_cohort(score_mart, reporting_month, config)

    # 5. Build performance cohorts
    cohorts = {}
    if perf_mart is not None:
        logger.info("Building performance cohorts")
        cohorts = build_performance_cohorts(score_mart, perf_mart, reporting_month, config)
        result.cohort_info = {
            label: {
                "score_month": c.score_month,
                "account_count": c.account_count,
                "mature_count": c.mature_count,
                "available": c.is_available,
                "note": c.note,
            }
            for label, c in cohorts.items()
        }
    else:
        logger.info("No perf_mart provided; skipping performance cohorts")

    # 6. Threshold engine
    thresholds = ThresholdEngine(config)

    # 7. Scorecard groups
    sc_groups = get_scorecard_groups(current_df, config)
    logger.info("Scorecard groups: %s", list(sc_groups.keys()))

    # 8. Compute metrics per scorecard group
    for sc_id, sc_df in sc_groups.items():
        logger.info("Computing metrics for scorecard: %s", sc_id)

        # Baseline for this scorecard (filter if scorecard enabled)
        sc_baseline = baseline_result.df
        if (
            sc_id != "overall"
            and config.scorecard_enabled
            and config.scorecard_column in sc_baseline.columns
        ):
            from pyspark.sql import functions as _F

            sc_baseline = sc_baseline.filter(_F.col(config.scorecard_column) == sc_id)

        # Stability
        stab = v2_metrics.compute_stability(sc_df, sc_baseline, config, thresholds, sc_id)
        result.stability[sc_id] = stab

        # Data quality
        dq = v2_metrics.compute_data_quality(sc_df, config)
        result.data_quality[sc_id] = dq

        # Separation (per maturity)
        sep_results = {}
        for label, cohort in cohorts.items():
            sep = v2_metrics.compute_separation(cohort, config, thresholds, sc_baseline, sc_id)
            if sep is not None:
                sep_results[label] = sep
        result.separation[sc_id] = sep_results

        # Performance
        perf = v2_metrics.compute_performance(cohorts, config, thresholds, sc_id)

        # Calibration: M12 (target window) ONLY
        calib_results = {}
        m12_cohort = cohorts.get("M12")
        if m12_cohort is not None:
            calib = v2_metrics.compute_calibration(m12_cohort, config, thresholds, sc_id)
            if calib is not None:
                calib_results["M12"] = calib
        result.calibration[sc_id] = calib_results

        # ── Detailed tables for MMR ──────────────────────────────────
        from framework.v2.metrics.stability_metrics import compute_psi_table, compute_csi_table
        from framework.v2.metrics.performance_metrics import compute_edr_rank_ordering, compute_edr_capture_summary
        from framework.v2.metrics.separation_metrics import compute_ks_table

        # PSI bucket table
        psi_table = compute_psi_table(
            sc_df, sc_baseline, config.score_col, config.score_intervals,
        )
        if sc_id not in result.stability:
            result.stability[sc_id] = {}
        result.stability[sc_id]["psi_table"] = psi_table

        # CSI tables per feature
        csi_tables = {}
        for feat in config.feature_cols:
            csi = compute_csi_table(sc_df, sc_baseline, feat, config.psi_n_bins)
            csi_tables[feat] = csi
        result.stability[sc_id]["csi_tables"] = csi_tables

        # EDR rank ordering + capture per maturity window
        edr_details = {}
        for label, cohort in cohorts.items():
            rank_table = compute_edr_rank_ordering(cohort, config, config.score_intervals)
            capture = compute_edr_capture_summary(cohort, config, config.risk_threshold)
            edr_details[label] = {
                "rank_ordering": rank_table,
                "capture_summary": capture,
            }

        result.performance[sc_id] = {
            "summary": perf,
            "edr_details": edr_details,
        }

        # KS table per maturity window
        ks_tables = {}
        for label, cohort in cohorts.items():
            ks_tbl = compute_ks_table(cohort, config, config.ks_bins)
            if ks_tbl is not None:
                ks_tables[label] = ks_tbl
        result.separation[sc_id]["ks_tables"] = ks_tables

    # 9. Explanation (optional)
    if config.explanation_enabled:
        logger.info("Computing explanation")
        result.explanation = compute_explanation(
            current_df, baseline_result.df, config,
        )
    else:
        result.explanation = None

    # 10. Collect governance flags
    result.flags = _collect_governance_flags(result, thresholds)

    # 11. Build shared report context
    context = build_context(result, config)
    result.context = context

    # 12. Generate reports
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    from framework.v2.reports.business import build_business_report
    from framework.v2.reports.mmr import build_mmr_report

    result.business_report = build_business_report(result, config, thresholds, str(out), context=context)
    result.mmr_report = build_mmr_report(result, config, thresholds, str(out), context=context)

    logger.info("V2 monitoring complete for %s — reports at %s", reporting_month, out)
    return result


def _collect_governance_flags(result: MonitoringResult, thresholds: ThresholdEngine) -> list[dict]:
    """Collect all threshold violations as governance flags."""
    flags: list[dict] = []

    for sc_id, stab in result.stability.items():
        # Score PSI
        if stab.get("score_psi_status") in ("ALERT", "WARNING"):
            flags.append({
                "scorecard": sc_id,
                "metric": "score_psi",
                "value": stab.get("score_psi"),
                "status": stab["score_psi_status"],
                "source": "stability",
            })

        # Feature PSI flags
        for f in stab.get("feature_psi", []):
            if f.get("status") in ("ALERT", "WARNING"):
                flags.append({
                    "scorecard": sc_id,
                    "metric": f"feature_psi:{f['feature_name']}",
                    "value": f.get("psi"),
                    "status": f["status"],
                    "source": "stability",
                })

    for sc_id, sep_dict in result.separation.items():
        for label, sep in sep_dict.items():
            if sep and sep.get("ks_drop_status") in ("ALERT", "WARNING"):
                flags.append({
                    "scorecard": sc_id,
                    "metric": f"ks_drop:{label}",
                    "value": sep.get("ks_drop"),
                    "status": sep["ks_drop_status"],
                    "source": "separation",
                })

    for sc_id, perf_data in result.performance.items():
        perf_rows = perf_data.get("summary", perf_data) if isinstance(perf_data, dict) else perf_data
        if not isinstance(perf_rows, list):
            perf_rows = []
        for row in perf_rows:
            if row.get("edr_delta_status") in ("ALERT", "WARNING"):
                flags.append({
                    "scorecard": sc_id,
                    "metric": f"edr_delta:{row.get('maturity', '')}:{row.get('channel', '')}",
                    "value": row.get("edr_delta"),
                    "status": row["edr_delta_status"],
                    "source": "performance",
                })

    for sc_id, calib_dict in result.calibration.items():
        for label, calib_rows in calib_dict.items():
            for row in (calib_rows or []):
                if row.get("gap_status") in ("ALERT", "WARNING"):
                    flags.append({
                        "scorecard": sc_id,
                        "metric": f"calibration_gap:{label}:bin{row.get('score_bin')}",
                        "value": row.get("calibration_gap"),
                        "status": row["gap_status"],
                        "source": "calibration",
                    })

    for sc_id, dq in result.data_quality.items():
        for col_dq in dq.get("columns", []):
            if col_dq.get("missing_status") in ("ALERT", "WARNING"):
                flags.append({
                    "scorecard": sc_id,
                    "metric": f"missing_rate:{col_dq.get('column', '')}",
                    "value": col_dq.get("missing_rate"),
                    "status": col_dq["missing_status"],
                    "source": "data_quality",
                })

    logger.info("Governance flags: %d total", len(flags))
    return flags
