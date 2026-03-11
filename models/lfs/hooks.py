"""Optional LFS-specific transforms beyond what YAML config can express."""
from __future__ import annotations

from pyspark.sql import DataFrame


def post_layer1_hook(df: DataFrame) -> DataFrame:
    """Optional post-enrichment transform for LFS.

    Called by the framework after standard Layer 1 enrichment if
    ``hooks_module`` is set in the model config.

    Currently a passthrough — implement custom derived columns here
    when a need arises that cannot be expressed as a config entry.
    """
    return df
