# Model Reporting Framework

A PySpark-based model monitoring and reporting framework with two pipeline
architectures:

- **V1** — Three-layer batch pipeline (enrichment → aggregation → monitoring)
- **V2** — Production-grade running-month-aligned monitoring with dual reports,
  hierarchical threshold governance, and scorecard-level control

Built for the **Loss Forecast Suite (LFS)** credit risk model but designed to
be model-agnostic via YAML configuration.

## Project Structure

```
model_reporting/
├── framework/                  # Core framework modules
│   ├── runner.py               # V1 pipeline orchestrator
│   ├── config.py               # YAML config loader (ModelConfig)
│   ├── layer1.py               # Account-level enrichment
│   ├── layer2.py               # Aggregated business tables
│   ├── layer3.py               # Model monitoring tables
│   ├── metrics.py              # Statistical primitives (PSI, KS, correlation)
│   ├── binning.py              # Dynamic/static quantile + fixed-boundary bins
│   ├── charts.py               # Matplotlib chart generation
│   ├── report_builder.py       # Markdown + HTML report builder
│   ├── io.py                   # Spark read/write utilities
│   ├── spark_session.py        # SparkSession factory
│   ├── utils.py                # Logging and helpers
│   │
│   └── v2/                     # Production monitoring (v2.2.0)
│       ├── runner.py           # V2 orchestrator (run_monitoring)
│       ├── config.py           # MonitoringConfig with threshold hierarchy
│       ├── cohort.py           # Running-month cohort alignment engine
│       ├── baseline.py         # Baseline computation with fallback
│       ├── thresholds.py       # Hierarchical threshold governance
│       ├── metrics.py          # Stability / separation / performance / calibration
│       ├── scorecard.py        # Scorecard-level grouping
│       ├── sample_controls.py  # Sample size guards
│       ├── perf_mart.py        # Production actuals loader
│       ├── data_model.py       # Schema validation
│       ├── backfill.py         # Historical rebuild support
│       ├── explanation.py      # Optional attribution layer
│       ├── mock.py             # Synthetic data generators
│       └── reports/
│           ├── business.py     # Business-facing report
│           ├── mmr.py          # Model Monitoring Report (technical)
│           └── narrative.py    # Auto-narrative generation
│
├── conf/
│   ├── framework.yaml          # Global Spark and output settings
│   └── models/
│       ├── lfs.yaml            # V1 LFS model configuration
│       └── lfs_v2.yaml         # V2 LFS production configuration
│
├── models/lfs/hooks.py         # LFS-specific post-enrichment hooks
├── tests/                      # pytest test suite
├── notebooks/                  # Jupyter demo notebook + mock data
├── docs/                       # Guides and checklists
└── run.py                      # CLI entrypoint
```

## Quick Start

### Prerequisites

- Python 3.9+
- PySpark 3.x
- PyYAML
- python-dateutil

Optional (for report export and charts):

```bash
pip install markdown matplotlib
```

### Notebook Demo

The fastest way to see the framework in action:

```bash
cd model_reporting
jupyter notebook notebooks/LFS_Reporting_Runbook.ipynb
```

The notebook generates synthetic data, runs both V1 and V2 pipelines, and
produces HTML monitoring reports — no database connection needed.

### V1 Pipeline — Programmatic Usage

```python
from pyspark.sql import SparkSession
from framework.config import load_model_config
from framework.layer1 import enrich_layer1
from framework.layer2 import build_layer2
from framework.layer3 import build_layer3

spark = SparkSession.builder.appName("lfs").getOrCreate()
config = load_model_config("lfs")

# Layer 1: account-level enrichment
enriched_df = enrich_layer1(source_df, config, baseline_df, "2025-05", "v1.0")

# Layer 2: aggregated business tables
l2_tables = build_layer2(enriched_df, config)

# Layer 3: monitoring tables (actuals optional)
l3_tables = build_layer3(enriched_df, baseline_df, config, actuals_df=actuals_df)
```

### V2 Pipeline — Production Monitoring

```python
from framework.v2.runner import run_monitoring

result = run_monitoring(
    score_mart=score_mart,
    perf_mart=perf_mart,          # or None to auto-load from config
    reporting_month="2025-05",
    config="conf/models/lfs_v2.yaml",
    output_dir="outputs",
)

print(result.flags)               # Governance threshold violations
print(result.business_report)     # Path to business HTML report
print(result.mmr_report)          # Path to MMR HTML report
```

## Architecture

### V1: Three-Layer Pipeline

| Layer | Purpose | Output |
|-------|---------|--------|
| **Layer 1** | Account-level enrichment: deciles, bands, buckets, DQ flags | `lfs_layer1_account` |
| **Layer 2** | Aggregated business tables by channel, source, segment | 8 summary tables |
| **Layer 3** | Monitoring: score drift, feature PSI, DQ, correlation, population mix, performance, calibration | 8 monitoring tables |

### V2: Running-Month Monitoring

V2 uses **running-month cohort alignment** for performance metrics:

| Maturity | Score Month | Metric | Purpose |
|----------|-------------|--------|---------|
| M3 | R − 3 | EDR30, KS, Gini, Odds | Early-read performance |
| M6 | R − 6 | EDR60, KS, Gini, Odds | Early-read performance |
| M9 | R − 9 | EDR90, KS, Gini, Odds | Early-read performance |
| M12 | R − 12 | CO, KS, Gini, Odds, **Calibration** | Target window |

Calibration is assessed at M12 only (1-year charge-off target).

#### V2 Key Features

- **Hierarchical thresholds**: scorecard → model → default fallback chain
- **Dual reports**: Business (stakeholder-friendly) + MMR (technical governance)
- **Scorecard support**: per-scorecard monitoring and threshold overrides
- **Sample controls**: minimum account/bad counts before computing unstable metrics
- **Odds monotonicity**: automatic misrank detection across score deciles
- **Production actuals**: load performance data from Hive/Spark tables via config

## Configuration

All pipeline behaviour is driven by YAML configuration in `conf/models/`.

### Key Config Sections (V2)

```yaml
# Baseline definition
baseline_definition:
  type: fixed_window           # fixed_window | rolling_window | initial_val
  start_month: "2024-08"
  end_month: "2025-02"

# Hierarchical threshold governance
threshold_config:
  default:
    psi: { warning: 0.10, alert: 0.25 }
    calibration_gap: { warning: 0.02, alert: 0.05 }
  model_override:
    psi: { warning: 0.08, alert: 0.20 }
  scorecard_override:
    SC001:
      psi: { warning: 0.12, alert: 0.30 }

# Target definition
target_definition:
  calibration_target: badco_m12
  early_read_targets: { M3: edr30_m3, M6: edr60_m6, M9: edr90_m9 }
  separation_targets: { M3: bad30_m3, M6: bad60_m6, M9: bad90_m9, M12: badco_m12 }

# Production actuals (set enabled: true for production)
actual_source:
  enabled: false
  database: risk_perf
  table: lfs_performance_actuals
```

## Reports

The framework generates monitoring reports in Markdown and HTML.

### V1 Report Sections

- **A.** Run Metadata
- **B.** Executive Summary — model health, performance snapshot, key drivers
- **C.** Business Summary — Layer 2 aggregations
- **D.** Monitoring Summary — D1 Score Drift, D2 Feature PSI, D3 Correlation,
  D4 Population Mix, D5 Data Quality, D6 Performance, D7 Calibration
- **E.** Charts (HTML only — score drift, feature PSI, calibration, population mix)
- **F.** Flags and Diagnosis

### V2 Reports

**Business Report** — simple, non-technical:
executive summary, portfolio summary, early performance (EDR30/60/90),
observations

**MMR Report** — full technical governance:
stability (PSI/CSI), separation (KS/Gini/Odds with monotonicity),
performance (EDR by maturity window), calibration (M12 only),
data quality, mandatory governance flags table, diagnostics

## Testing

```bash
cd model_reporting
python -m pytest tests/ -v
```

Tests use a local SparkSession (`local[2]`) with synthetic data — no external
dependencies or database connections required.

| Test Suite | Coverage |
|------------|----------|
| `test_binning.py` | Dynamic/static quantile binning, fixed-boundary buckets |
| `test_metrics.py` | PSI, KS, correlation, missing/outlier rates |
| `test_layer2.py` | Aggregation engine, metric dispatch, grouping |
| `test_layer3.py` | All 8 monitoring tables |
| `test_e2e_dry_run.py` | Full end-to-end pipeline with synthetic data |

## Data Model

### V2 Score Snapshot Mart

```
creditaccountid | score_month | score | channel | scorecard_id
score_decile_static | feature_01..11 | is_excluded_from_monitoring
```

### V2 Performance Mart

```
creditaccountid | score_month
edr30_m3 | edr60_m6 | edr90_m9 | co_m12
bad30_m3 | bad60_m6 | bad90_m9 | badco_m12
is_mature_m3 | is_mature_m6 | is_mature_m9 | is_mature_m12
```

## Adding a New Model

1. Create `conf/models/{model_name}.yaml` following the LFS config structure
2. Optionally add model-specific hooks in `models/{model_name}/hooks.py`
3. Run: `python run.py --model {model_name} --score-month 2025-05 --version v1.0`

For V2 monitoring, create `conf/models/{model_name}_v2.yaml` with the
threshold, baseline, and target definitions appropriate for your model.
