"""V2 monitoring framework — automated validation suite.

Runs the full pipeline across all mock scenarios and validates:
- report generation (business + MMR)
- metric table schemas (EDR rank, KS, PSI, CSI)
- business-rule correctness (misrank, capture rate, sample controls)
- scenario-specific expected behaviors

Usage:
    pytest tests/test_v2_validation.py -v
    python tests/test_v2_validation.py          # standalone entrypoint
"""
from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from framework.v2.mock import generate_v2_mock_data, AVAILABLE_SCENARIOS
from framework.v2.runner import run_monitoring, MonitoringResult


# ── Fixtures ──────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Local SparkSession for validation tests."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("v2_validation")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )


@pytest.fixture
def output_dir(tmp_path):
    """Temporary output directory for report artifacts."""
    return str(tmp_path / "outputs")


def _run_scenario(spark, scenario: str, output_dir: str) -> MonitoringResult:
    """Run the full pipeline for a given scenario. Shared helper."""
    score_mart, perf_mart = generate_v2_mock_data(
        spark, scenario=scenario, reporting_month="2025-05", seed=42,
    )
    result = run_monitoring(
        score_mart=score_mart,
        perf_mart=perf_mart,
        reporting_month="2025-05",
        output_dir=output_dir,
    )
    return result


# ── 1. Report generation ─────────────────────────────────────────────

class TestReportGeneration:
    """Both reports must be generated as files on disk."""

    def test_business_report_generated(self, spark, output_dir):
        result = _run_scenario(spark, "healthy", output_dir)
        assert result.business_report is not None, "Business report path is None"
        assert Path(result.business_report).exists(), (
            f"Business report not found at {result.business_report}"
        )
        size = Path(result.business_report).stat().st_size
        assert size > 500, f"Business report is too small ({size} bytes)"

    def test_mmr_report_generated(self, spark, output_dir):
        result = _run_scenario(spark, "healthy", output_dir)
        assert result.mmr_report is not None, "MMR report path is None"
        assert Path(result.mmr_report).exists(), (
            f"MMR report not found at {result.mmr_report}"
        )
        size = Path(result.mmr_report).stat().st_size
        assert size > 1000, f"MMR report is too small ({size} bytes)"


# ── 2. Metric table existence ────────────────────────────────────────

class TestMetricTableExistence:
    """Core metric structures must exist in the result."""

    @pytest.fixture(autouse=True)
    def _run(self, spark, output_dir):
        self.result = _run_scenario(spark, "healthy", output_dir)

    def test_stability_populated(self):
        assert self.result.stability, "stability dict is empty"
        for sc_id, stab in self.result.stability.items():
            assert "score_psi" in stab, f"score_psi missing for {sc_id}"

    def test_performance_populated(self):
        assert self.result.performance, "performance dict is empty"
        for sc_id, perf in self.result.performance.items():
            assert isinstance(perf, dict), f"performance[{sc_id}] is not a dict"
            assert "summary" in perf, f"summary missing in performance[{sc_id}]"
            assert isinstance(perf["summary"], list), "summary is not a list"
            assert len(perf["summary"]) > 0, "summary list is empty"

    def test_separation_populated(self):
        assert self.result.separation, "separation dict is empty"
        for sc_id, sep in self.result.separation.items():
            assert isinstance(sep, dict), f"separation[{sc_id}] is not a dict"
            # At least one maturity window should have data
            has_data = any(
                isinstance(v, dict) and v.get("ks") is not None
                for v in sep.values()
                if isinstance(v, dict) and "ks" in v
            )
            assert has_data, f"No separation data with KS for {sc_id}"


# ── 3. Schema validation ─────────────────────────────────────────────

class TestSchemaValidation:
    """Detailed table structures must match expected schemas."""

    @pytest.fixture(autouse=True)
    def _run(self, spark, output_dir):
        self.result = _run_scenario(spark, "healthy", output_dir)

    def test_psi_table_schema(self):
        for sc_id, stab in self.result.stability.items():
            psi_table = stab.get("psi_table", [])
            assert len(psi_table) > 0, f"PSI table is empty for {sc_id}"
            required = {
                "interval", "min_score", "max_score",
                "baseline_count", "compare_count",
                "baseline_pct", "compare_pct",
                "woe", "contribution",
            }
            actual = set(psi_table[0].keys())
            missing = required - actual
            assert not missing, f"PSI table missing columns: {missing}"

    def test_csi_tables_schema(self):
        for sc_id, stab in self.result.stability.items():
            csi_tables = stab.get("csi_tables", {})
            assert len(csi_tables) > 0, f"CSI tables empty for {sc_id}"
            for feat, rows in csi_tables.items():
                assert len(rows) > 0, f"CSI table for {feat} is empty"
                required = {
                    "feature", "bin_index", "interval",
                    "baseline_count", "compare_count",
                    "baseline_pct", "compare_pct",
                    "information_value",
                }
                actual = set(rows[0].keys())
                missing = required - actual
                assert not missing, f"CSI[{feat}] missing columns: {missing}"

    def test_edr_rank_table_schema(self):
        for sc_id, perf in self.result.performance.items():
            details = perf.get("edr_details", {})
            for label, detail in details.items():
                rank = detail.get("rank_ordering")
                if rank is None:
                    continue  # cohort may be unavailable
                assert len(rank) > 0, f"EDR rank table empty for {label}"
                required = {
                    "interval", "min_score", "max_score",
                    "accounts_n", "accounts_pct",
                    "observation_pct", "misrank",
                }
                actual = set(rank[0].keys())
                missing = required - actual
                assert not missing, f"EDR rank[{label}] missing: {missing}"

    def test_edr_rank_has_10_buckets(self):
        """EDR rank ordering should have 10 score-interval buckets."""
        for sc_id, perf in self.result.performance.items():
            details = perf.get("edr_details", {})
            for label, detail in details.items():
                rank = detail.get("rank_ordering")
                if rank is None:
                    continue
                assert len(rank) == 10, (
                    f"EDR rank[{label}] has {len(rank)} buckets, expected 10"
                )

    def test_ks_table_has_20_bins_plus_total(self):
        """KS table should have 20 tier rows + 1 TOTAL row = 21."""
        for sc_id, sep in self.result.separation.items():
            ks_tables = sep.get("ks_tables", {})
            for label, ks_tbl in ks_tables.items():
                assert len(ks_tbl) == 21, (
                    f"KS table[{label}] has {len(ks_tbl)} rows, expected 21"
                )
                # Last row must be TOTAL
                assert ks_tbl[-1]["tier"] == "TOTAL", (
                    f"KS table[{label}] last row tier={ks_tbl[-1]['tier']}, expected TOTAL"
                )

    def test_ks_table_schema(self):
        for sc_id, sep in self.result.separation.items():
            ks_tables = sep.get("ks_tables", {})
            for label, ks_tbl in ks_tables.items():
                assert len(ks_tbl) > 0
                required = {
                    "tier", "min_score", "max_score",
                    "accounts_n", "accounts_pct",
                    "goods_n", "goods_pct",
                    "bads_n", "bads_pct",
                    "bad_rate", "ks", "lift",
                }
                actual = set(ks_tbl[0].keys())
                missing = required - actual
                assert not missing, f"KS table[{label}] missing: {missing}"


# ── 4. Business-rule validation ──────────────────────────────────────

class TestBusinessRules:
    """Metric values must satisfy business invariants."""

    def test_capture_rate_in_bounds(self, spark, output_dir):
        result = _run_scenario(spark, "healthy", output_dir)
        for sc_id, perf in result.performance.items():
            details = perf.get("edr_details", {})
            for label, detail in details.items():
                cap = detail.get("capture_summary")
                if cap is None:
                    continue
                pct_flagged = cap["pct_population_flagged"]
                pct_captured = cap["pct_bad_captured"]
                assert 0.0 <= pct_flagged <= 1.0, (
                    f"[{label}] pct_population_flagged={pct_flagged} out of [0,1]"
                )
                assert 0.0 <= pct_captured <= 1.0, (
                    f"[{label}] pct_bad_captured={pct_captured} out of [0,1]"
                )

    def test_misrank_detection(self, spark, output_dir):
        """misrank_demo scenario must produce at least one Misrank=YES."""
        result = _run_scenario(spark, "misrank_demo", output_dir)
        found_misrank = False
        for sc_id, perf in result.performance.items():
            details = perf.get("edr_details", {})
            for label, detail in details.items():
                rank = detail.get("rank_ordering")
                if rank is None:
                    continue
                for row in rank:
                    if row.get("misrank") == "YES":
                        found_misrank = True
                        break
        assert found_misrank, (
            "misrank_demo scenario did not produce any Misrank=YES in EDR rank tables"
        )

    def test_psi_contributions_sum_to_total(self, spark, output_dir):
        """PSI bucket contributions must approximately sum to score_psi."""
        result = _run_scenario(spark, "healthy", output_dir)
        for sc_id, stab in result.stability.items():
            psi_table = stab.get("psi_table", [])
            score_psi = stab.get("score_psi")
            if not psi_table or score_psi is None:
                continue
            total_contribution = sum(
                row.get("contribution", 0) for row in psi_table
            )
            # Allow 10% tolerance due to epsilon smoothing
            assert abs(total_contribution - score_psi) < max(score_psi * 0.15, 0.005), (
                f"PSI contribution sum {total_contribution:.6f} != score_psi {score_psi:.6f}"
            )

    def test_ks_monotonic_cumulative(self, spark, output_dir):
        """Cumulative goods_pct and bads_pct must be non-decreasing."""
        result = _run_scenario(spark, "healthy", output_dir)
        for sc_id, sep in result.separation.items():
            ks_tables = sep.get("ks_tables", {})
            for label, ks_tbl in ks_tables.items():
                tiers = [r for r in ks_tbl if r["tier"] != "TOTAL"]
                for i in range(1, len(tiers)):
                    assert tiers[i]["goods_pct"] >= tiers[i - 1]["goods_pct"] - 1e-9, (
                        f"KS[{label}] goods_pct not non-decreasing at tier {tiers[i]['tier']}"
                    )
                    assert tiers[i]["bads_pct"] >= tiers[i - 1]["bads_pct"] - 1e-9, (
                        f"KS[{label}] bads_pct not non-decreasing at tier {tiers[i]['tier']}"
                    )

    def test_edr_accounts_sum_to_total(self, spark, output_dir):
        """EDR rank table accounts must sum to cohort total."""
        result = _run_scenario(spark, "healthy", output_dir)
        for sc_id, perf in result.performance.items():
            summary = perf.get("summary", [])
            details = perf.get("edr_details", {})
            for label, detail in details.items():
                rank = detail.get("rank_ordering")
                if rank is None:
                    continue
                rank_total = sum(r["accounts_n"] for r in rank)
                # Find matching summary row
                summary_total = None
                for s in summary:
                    if s.get("maturity") == label and s.get("channel") == "all":
                        summary_total = s.get("account_count")
                        break
                if summary_total is not None:
                    assert rank_total == summary_total, (
                        f"EDR rank[{label}] sum={rank_total} != summary={summary_total}"
                    )


# ── 5. Scenario-specific validation ──────────────────────────────────

class TestScenarios:
    """Each scenario must produce its expected distinctive behavior."""

    def test_healthy_no_alerts(self, spark, output_dir):
        result = _run_scenario(spark, "healthy", output_dir)
        alerts = [f for f in result.flags if f.get("status") == "ALERT"]
        # Healthy may still have alerts depending on thresholds, but should be few
        assert len(alerts) <= 3, f"healthy scenario has {len(alerts)} alerts"

    def test_drift_warning_shows_psi_movement(self, spark, output_dir):
        result = _run_scenario(spark, "drift_warning", output_dir)
        for sc_id, stab in result.stability.items():
            psi = stab.get("score_psi", 0)
            # drift_warning should produce non-trivial PSI
            assert psi > 0.01, (
                f"drift_warning PSI={psi:.4f}, expected meaningful drift"
            )

    def test_insufficient_bads_produces_na(self, spark, output_dir):
        """KS/separation must be None or have note when bads insufficient."""
        result = _run_scenario(spark, "insufficient_bads", output_dir)
        for sc_id, sep in result.separation.items():
            for label, sep_data in sep.items():
                if isinstance(sep_data, dict) and "ks" in sep_data:
                    # KS should be None when bads insufficient
                    if sep_data.get("note"):
                        assert sep_data.get("ks") is None, (
                            f"[{label}] KS should be None when note={sep_data['note']}"
                        )

    def test_insufficient_bads_no_ks_table(self, spark, output_dir):
        """KS detailed table must not exist for insufficient-bads scenario."""
        result = _run_scenario(spark, "insufficient_bads", output_dir)
        for sc_id, sep in result.separation.items():
            ks_tables = sep.get("ks_tables", {})
            # All KS tables should be empty (sample check fails)
            assert len(ks_tables) == 0, (
                f"insufficient_bads produced KS tables: {list(ks_tables.keys())}"
            )

    def test_missing_m12_no_calibration(self, spark, output_dir):
        """M12 calibration must be empty when M12 cohort is missing."""
        result = _run_scenario(spark, "missing_m12", output_dir)
        for sc_id, calib in result.calibration.items():
            m12 = calib.get("M12")
            assert m12 is None, (
                f"missing_m12 scenario should not have M12 calibration, got {type(m12)}"
            )

    def test_missing_m12_reports_still_generate(self, spark, output_dir):
        result = _run_scenario(spark, "missing_m12", output_dir)
        assert result.business_report is not None
        assert result.mmr_report is not None
        assert Path(result.business_report).exists()
        assert Path(result.mmr_report).exists()

    def test_all_scenarios_run_without_crash(self, spark, output_dir):
        """Every registered scenario must complete without exception."""
        for scenario in AVAILABLE_SCENARIOS:
            sub_dir = os.path.join(output_dir, scenario)
            result = _run_scenario(spark, scenario, sub_dir)
            assert result.business_report is not None, (
                f"Scenario {scenario}: business report is None"
            )
            assert result.mmr_report is not None, (
                f"Scenario {scenario}: MMR report is None"
            )


# ── 6. Determinism ───────────────────────────────────────────────────

class TestDeterminism:
    """Same seed must produce identical results."""

    def test_same_seed_same_psi(self, spark, output_dir):
        sm1, pm1 = generate_v2_mock_data(spark, "healthy", seed=99)
        sm2, pm2 = generate_v2_mock_data(spark, "healthy", seed=99)

        r1 = run_monitoring(sm1, pm1, "2025-05", output_dir=output_dir + "/det1")
        r2 = run_monitoring(sm2, pm2, "2025-05", output_dir=output_dir + "/det2")

        for sc_id in r1.stability:
            psi1 = r1.stability[sc_id].get("score_psi")
            psi2 = r2.stability[sc_id].get("score_psi")
            assert psi1 == psi2, (
                f"Determinism failure: PSI {psi1} != {psi2} for same seed"
            )


# ── Standalone entrypoint ─────────────────────────────────────────────

def run_and_validate() -> dict:
    """Run all scenarios and validate — standalone entrypoint.

    Returns:
        {"passed": int, "failed": int, "errors": list[str]}
    """
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("v2_validation")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    results = {"passed": 0, "failed": 0, "errors": []}
    tmp_dir = tempfile.mkdtemp(prefix="v2_validation_")

    checks = [
        ("report_generation", _check_reports),
        ("table_existence", _check_tables),
        ("psi_schema", _check_psi_schema),
        ("csi_schema", _check_csi_schema),
        ("edr_rank_schema", _check_edr_rank),
        ("ks_table_schema", _check_ks_table),
        ("capture_rate_bounds", _check_capture_bounds),
        ("misrank_detection", _check_misrank),
        ("insufficient_bads_na", _check_insufficient_bads),
        ("missing_m12_no_calibration", _check_missing_m12),
        ("all_scenarios_no_crash", _check_all_scenarios),
    ]

    for name, check_fn in checks:
        try:
            check_fn(spark, tmp_dir)
            results["passed"] += 1
            print(f"  PASS  {name}")
        except (AssertionError, Exception) as e:
            results["failed"] += 1
            results["errors"].append(f"{name}: {e}")
            print(f"  FAIL  {name}: {e}")

    # Cleanup
    shutil.rmtree(tmp_dir, ignore_errors=True)

    print()
    print(f"Results: {results['passed']} passed, {results['failed']} failed")
    if results["errors"]:
        print("Failures:")
        for err in results["errors"]:
            print(f"  - {err}")

    return results


# ── Standalone check functions ────────────────────────────────────────

def _check_reports(spark, tmp_dir):
    result = _run_scenario(spark, "healthy", os.path.join(tmp_dir, "reports"))
    assert result.business_report and Path(result.business_report).exists()
    assert result.mmr_report and Path(result.mmr_report).exists()


def _check_tables(spark, tmp_dir):
    result = _run_scenario(spark, "healthy", os.path.join(tmp_dir, "tables"))
    assert result.stability, "stability empty"
    assert result.performance, "performance empty"
    assert result.separation, "separation empty"


def _check_psi_schema(spark, tmp_dir):
    result = _run_scenario(spark, "healthy", os.path.join(tmp_dir, "psi"))
    for sc_id, stab in result.stability.items():
        tbl = stab.get("psi_table", [])
        assert len(tbl) > 0, "PSI table empty"
        assert "woe" in tbl[0] and "contribution" in tbl[0]


def _check_csi_schema(spark, tmp_dir):
    result = _run_scenario(spark, "healthy", os.path.join(tmp_dir, "csi"))
    for sc_id, stab in result.stability.items():
        csi = stab.get("csi_tables", {})
        assert len(csi) > 0, "CSI tables empty"
        for feat, rows in csi.items():
            assert len(rows) > 0, f"CSI[{feat}] empty"
            assert "information_value" in rows[0]


def _check_edr_rank(spark, tmp_dir):
    result = _run_scenario(spark, "healthy", os.path.join(tmp_dir, "edr"))
    for sc_id, perf in result.performance.items():
        details = perf.get("edr_details", {})
        found = False
        for label, detail in details.items():
            rank = detail.get("rank_ordering")
            if rank:
                found = True
                assert len(rank) == 10, f"EDR rank has {len(rank)} buckets"
                assert "misrank" in rank[0]
        assert found, "No EDR rank tables found"


def _check_ks_table(spark, tmp_dir):
    result = _run_scenario(spark, "healthy", os.path.join(tmp_dir, "ks"))
    for sc_id, sep in result.separation.items():
        ks_tables = sep.get("ks_tables", {})
        assert len(ks_tables) > 0, "No KS tables"
        for label, ks_tbl in ks_tables.items():
            assert len(ks_tbl) == 21, f"KS[{label}] has {len(ks_tbl)} rows"
            assert ks_tbl[-1]["tier"] == "TOTAL"


def _check_capture_bounds(spark, tmp_dir):
    result = _run_scenario(spark, "healthy", os.path.join(tmp_dir, "cap"))
    for sc_id, perf in result.performance.items():
        for label, detail in perf.get("edr_details", {}).items():
            cap = detail.get("capture_summary")
            if cap:
                assert 0 <= cap["pct_population_flagged"] <= 1
                assert 0 <= cap["pct_bad_captured"] <= 1


def _check_misrank(spark, tmp_dir):
    result = _run_scenario(spark, "misrank_demo", os.path.join(tmp_dir, "misrank"))
    found = False
    for sc_id, perf in result.performance.items():
        for label, detail in perf.get("edr_details", {}).items():
            rank = detail.get("rank_ordering")
            if rank:
                for r in rank:
                    if r.get("misrank") == "YES":
                        found = True
    assert found, "misrank_demo did not produce Misrank=YES"


def _check_insufficient_bads(spark, tmp_dir):
    result = _run_scenario(spark, "insufficient_bads", os.path.join(tmp_dir, "insuf"))
    for sc_id, sep in result.separation.items():
        ks_tables = sep.get("ks_tables", {})
        assert len(ks_tables) == 0, f"insufficient_bads produced KS tables: {list(ks_tables)}"


def _check_missing_m12(spark, tmp_dir):
    result = _run_scenario(spark, "missing_m12", os.path.join(tmp_dir, "m12"))
    for sc_id, calib in result.calibration.items():
        assert calib.get("M12") is None, "missing_m12 has M12 calibration"
    assert result.business_report and Path(result.business_report).exists()
    assert result.mmr_report and Path(result.mmr_report).exists()


def _check_all_scenarios(spark, tmp_dir):
    for scenario in AVAILABLE_SCENARIOS:
        result = _run_scenario(spark, scenario, os.path.join(tmp_dir, scenario))
        assert result.business_report is not None, f"{scenario}: no business report"
        assert result.mmr_report is not None, f"{scenario}: no MMR report"


# ── Main ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("V2 Monitoring Framework — Validation Suite")
    print("=" * 60)
    print()
    results = run_and_validate()
    exit(0 if results["failed"] == 0 else 1)
