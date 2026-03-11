# DSW Notebook Runbook — Model Reporting Framework

This guide explains how to run the reporting framework from a DSW (Data Science Workspace)
notebook where a Spark session is already initialised by the platform.

---

## 1. Setup

The framework lives in `.py` modules.  Add the project root to `sys.path` once at the
top of your notebook so Python can find the `framework` package.

```python
import sys
sys.path.insert(0, "/path/to/model_reporting")   # adjust to your mount point

from framework.runner import run
```

`spark` is assumed to be pre-initialised by the DSW environment.  You do **not** need
to create a session yourself.

---

## 2. Running the Full Pipeline

Pass your existing `spark` session directly.  The framework will skip session creation
and use yours instead.

```python
run(
    model="lfs",
    score_month="2025-03",
    model_version="v1.0",
    spark=spark,
)
```

This runs all three layers and writes results to the catalog defined in
`conf/models/lfs.yaml` → `output.database`.

---

## 3. Dry Run (No Writes)

Set `write_output=False` to execute all transformations without touching any catalog
tables.  This is the recommended first step when deploying to a new environment or
testing a new score month.

```python
run(
    model="lfs",
    score_month="2025-03",
    model_version="v1.0",
    spark=spark,
    write_output=False,
)
```

Logs will show each layer completing.  Nothing is written.

---

## 4. Inspecting Outputs in the Notebook

Set `return_outputs=True` to get all produced DataFrames back as a flat dict keyed
by table name.  Combine with `write_output=False` for a full dry-run inspection.

```python
outputs = run(
    model="lfs",
    score_month="2025-03",
    model_version="v1.0",
    spark=spark,
    write_output=False,
    return_outputs=True,
)
```

The returned dict contains one entry per output table:

```
outputs.keys()
# dict_keys([
#   'lfs_layer1_account',
#   'lfs_layer2_overall_summary',
#   'lfs_layer2_score_distribution',
#   'lfs_layer2_by_source',
#   'lfs_layer2_by_line',
#   'lfs_layer2_by_receivable_bucket',
#   'lfs_layer2_by_saleamount_bucket',
#   ... (all Layer2 driver tables)
#   'lfs_layer3_score_drift',
#   'lfs_layer3_feature_psi',
#   'lfs_layer3_data_quality',
#   'lfs_layer3_feature_quality',
#   'lfs_layer3_feature_score_relationship',
#   'lfs_layer3_population_mix',
# ])
```

Access any table as a normal PySpark DataFrame:

```python
# Inspect Layer 1 — account-level enrichment
outputs["lfs_layer1_account"].select(
    "creditaccountid", "lfs_score", "lfs_decile_dyn", "lfs_band_dyn", "static_decile"
).show(10)

# Check score drift PSI across channels
outputs["lfs_layer3_score_drift"].select(
    "vintage_month", "channel", "mean_lfs_score", "score_psi", "score_ks"
).show()

# Review feature quality
outputs["lfs_layer3_feature_quality"].filter(
    "missing_rate > 0.01 OR outlier_rate > 0.05"
).show()

# Convert to pandas for plotting (small tables only)
drift_pd = outputs["lfs_layer3_score_drift"].toPandas()
```

---

## 5. Running Selected Layers Only

Pass `layers` to run a subset.  Earlier layers are automatically re-run as
dependencies when needed (e.g. running `layer3` alone will still compute `layer1`
internally, but only write/return `layer3`).

```python
# Only Layer 1 — useful for data quality checks before aggregation
outputs = run(
    model="lfs",
    score_month="2025-03",
    model_version="v1.0",
    spark=spark,
    layers=["layer1"],
    write_output=False,
    return_outputs=True,
)

# Layer 2 + 3 without Layer 1 write
outputs = run(
    model="lfs",
    score_month="2025-03",
    model_version="v1.0",
    spark=spark,
    layers=["layer2", "layer3"],
    write_output=False,
    return_outputs=True,
)
```

---

## 6. Validating Outputs Before Writing

Use this two-step pattern to inspect outputs before committing to the catalog:

### Step 1 — Dry run and inspect

```python
outputs = run(
    model="lfs",
    score_month="2025-03",
    model_version="v1.0",
    spark=spark,
    write_output=False,
    return_outputs=True,
)

# ── Row count checks ─────────────────────────────────────
l1 = outputs["lfs_layer1_account"]
print("Layer1 row count:", l1.count())

# ── Aggregation reconciliation ───────────────────────────
from pyspark.sql import functions as F

l2_summary = outputs["lfs_layer2_overall_summary"]
l2_total = l2_summary.agg(F.sum("account_count")).collect()[0][0]
print("Layer2 total account_count:", l2_total)
assert l2_total == l1.count(), "Row count mismatch between Layer1 and Layer2"

# ── Percentage sum checks ────────────────────────────────
pct_sums = (
    l2_summary
    .groupBy("vintage_month", "channel")
    .agg(F.sum("pct_of_channel_vintage_accounts").alias("total_pct"))
    .collect()
)
for row in pct_sums:
    assert abs(row["total_pct"] - 1.0) < 1e-9, f"pct sum != 1.0: {row}"
print("Percentage sums OK")

# ── Score drift sanity check ─────────────────────────────
drift = outputs["lfs_layer3_score_drift"]
drift.select("vintage_month", "channel", "score_psi", "score_ks").show()
high_psi = drift.filter("score_psi > 0.25").count()
if high_psi > 0:
    print(f"WARNING: {high_psi} channel(s) with PSI > 0.25")

# ── DQ flags ────────────────────────────────────────────
dq = outputs["lfs_layer3_data_quality"]
dq.show()
```

### Step 2 — Write after validation passes

```python
# Re-run with write_output=True once checks pass.
# The framework re-reads source data and re-runs all transforms.
run(
    model="lfs",
    score_month="2025-03",
    model_version="v1.0",
    spark=spark,
    write_output=True,
)
print("Tables written to catalog.")
```

> **Tip**: If the dry-run DataFrames pass all checks, you can avoid re-running transforms
> by writing them directly from `outputs` using `df.write.saveAsTable(...)`.
> The `run()` approach is recommended for production runs as it guarantees a clean,
> logged execution.

---

## 7. Listing Registered Models

```python
from framework.config import list_registered_models
print(list_registered_models())
# ['lfs']
```

Adding a new model is as simple as dropping a new YAML into `conf/models/`.

---

## 8. CLI Reference (Scheduled Jobs)

For non-notebook execution (Airflow, cron, CI), use the CLI entrypoint:

```bash
# Full run
python run.py --model lfs --score_month 2025-03 --model_version v1.0

# Selected layers
python run.py --model lfs --score_month 2025-03 --model_version v1.0 \
              --layers layer1 layer2
```

The CLI always writes output and does not return DataFrames.

---

## 9. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `FileNotFoundError: No configuration found for model 'lfs'` | `sys.path` not pointing to project root | Add `sys.path.insert(0, "/path/to/model_reporting")` |
| `AnalysisException: Table not found` | Source table doesn't exist in current catalog | Verify `conf/models/lfs.yaml` → `source.database` and `source.table` match your environment |
| `missing_score_rate = 1.0` in data_quality | Score column is NULL for the month | Check source table for the requested `score_month` |
| High PSI (> 0.25) on first run | Score distribution has shifted significantly vs baseline window | Review `baseline_window` in lfs.yaml; consider extending it |
| `corr_change` is `None` | Channel slice has < 2 rows, or feature has zero variance | Expected for very small segments; review segment filters |
