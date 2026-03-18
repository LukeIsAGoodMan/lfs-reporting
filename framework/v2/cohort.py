"""Running-month cohort alignment engine.

Maps a reporting_month to the correct score_months for each maturity window,
joins score mart with performance mart, and applies maturity filters.
"""
from __future__ import annotations
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from framework.v2.config import MonitoringConfig
from framework.v2.data_model import apply_monitoring_filter
from framework.utils import get_logger

logger = get_logger(__name__)

# Maturity definitions: label -> (months_back, edr_col, bad_col, maturity_flag)
MATURITY_MAP = {
    "M3":  {"months_back": 3,  "edr_col": "edr30_m3",  "bad_col": "bad30_m3",  "maturity_flag": "is_mature_m3"},
    "M6":  {"months_back": 6,  "edr_col": "edr60_m6",  "bad_col": "bad60_m6",  "maturity_flag": "is_mature_m6"},
    "M9":  {"months_back": 9,  "edr_col": "edr90_m9",  "bad_col": "bad90_m9",  "maturity_flag": "is_mature_m9"},
    "M12": {"months_back": 12, "edr_col": "co_m12",    "bad_col": "badco_m12", "maturity_flag": "is_mature_m12"},
}


class CohortResult:
    """Container for a single maturity cohort."""
    __slots__ = ("label", "score_month", "df", "account_count", "mature_count", "note")

    def __init__(
        self,
        label: str,
        score_month: str,
        df: DataFrame | None,
        account_count: int,
        mature_count: int,
        note: str = "",
    ):
        self.label = label
        self.score_month = score_month
        self.df = df
        self.account_count = account_count
        self.mature_count = mature_count
        self.note = note

    @property
    def is_available(self) -> bool:
        return self.df is not None and self.mature_count > 0


def get_maturity_mapping(reporting_month: str) -> dict[str, str]:
    """Map maturity labels to their aligned score_months.

    Args:
        reporting_month: "YYYY-MM" format, e.g. "2025-05"

    Returns:
        {"M3": "2025-02", "M6": "2024-11", "M9": "2024-08", "M12": "2024-05"}
    """
    dt = datetime.strptime(reporting_month, "%Y-%m")
    result = {}
    for label, info in MATURITY_MAP.items():
        aligned = dt - relativedelta(months=info["months_back"])
        result[label] = aligned.strftime("%Y-%m")
    logger.info("Maturity mapping for %s: %s", reporting_month, result)
    return result


def build_current_cohort(
    score_mart: DataFrame,
    reporting_month: str,
    config: MonitoringConfig,
) -> DataFrame:
    """Build the current-period cohort (latest score_month = reporting_month).

    Used for stability metrics (PSI/CSI, feature drift, population mix, DQ)
    that don't require actuals.
    """
    filtered = apply_monitoring_filter(score_mart)
    current = filtered.filter(F.col("score_month") == reporting_month)
    n = current.count()
    logger.info("Current cohort (%s): %d accounts", reporting_month, n)
    return current


def build_performance_cohorts(
    score_mart: DataFrame,
    perf_mart: DataFrame,
    reporting_month: str,
    config: MonitoringConfig,
) -> dict[str, CohortResult]:
    """Build running-month-aligned performance cohorts.

    For each maturity window (M3, M6, M9, M12):
    1. Determine the aligned score_month
    2. Filter score_mart to that score_month
    3. Join with perf_mart on creditaccountid + score_month
    4. Filter to mature accounts (is_mature_mX == 1)
    5. Apply monitoring exclusion filter

    Returns:
        {maturity_label: CohortResult} — only cohorts with data are included.
    """
    maturity_map = get_maturity_mapping(reporting_month)
    filtered_score = apply_monitoring_filter(score_mart)

    results: dict[str, CohortResult] = {}

    for label, score_month in maturity_map.items():
        info = MATURITY_MAP[label]
        maturity_flag = info["maturity_flag"]
        edr_col = info["edr_col"]
        bad_col = info["bad_col"]

        # Filter score mart to aligned month
        score_cohort = filtered_score.filter(F.col("score_month") == score_month)
        score_count = score_cohort.count()

        if score_count == 0:
            logger.info("Cohort %s (%s): no scored accounts — skipping", label, score_month)
            continue

        # Check if perf mart has the maturity flag
        if maturity_flag not in perf_mart.columns:
            if config.maturity_on_missing == "skip_with_note":
                logger.warning(
                    "Cohort %s: maturity flag '%s' not in perf_mart — skipping with note",
                    label, maturity_flag,
                )
                results[label] = CohortResult(
                    label=label,
                    score_month=score_month,
                    df=None,
                    account_count=0,
                    mature_count=0,
                    note=f"Maturity flag '{maturity_flag}' not available",
                )
                continue
            else:
                raise ValueError(
                    f"Cohort {label}: maturity flag '{maturity_flag}' not in perf_mart"
                )

        # Select only needed columns from perf_mart to avoid ambiguity
        perf_cols = ["creditaccountid", "score_month"]
        for c in [maturity_flag, edr_col, bad_col]:
            if c in perf_mart.columns and c not in perf_cols:
                perf_cols.append(c)
        perf_slim = perf_mart.select(*perf_cols)

        # Join on creditaccountid + score_month
        joined = score_cohort.join(
            perf_slim, on=["creditaccountid", "score_month"], how="inner",
        )

        # Filter to mature accounts
        mature = joined.filter(F.col(maturity_flag) == 1)
        mature_count = mature.count()

        if mature_count == 0:
            logger.info("Cohort %s (%s): 0 mature accounts — skipping", label, score_month)
            results[label] = CohortResult(
                label=label,
                score_month=score_month,
                df=None,
                account_count=score_count,
                mature_count=0,
                note="No mature accounts available yet",
            )
            continue

        logger.info(
            "Cohort %s (%s): %d scored → %d mature",
            label, score_month, score_count, mature_count,
        )
        results[label] = CohortResult(
            label=label,
            score_month=score_month,
            df=mature,
            account_count=score_count,
            mature_count=mature_count,
        )

    return results
