"""CLI entrypoint for the model reporting framework.

Usage:
    python run.py --model lfs --score_month 2025-03 --model_version v1.2
    python run.py --model lfs --score_month 2025-03 --model_version v1.2 --layers layer1 layer2
"""
from __future__ import annotations

import argparse

from framework.runner import run


def main() -> None:
    parser = argparse.ArgumentParser(description="Model Reporting Framework")
    parser.add_argument(
        "--model", required=True,
        help="Model name (must match a YAML file in conf/models/)",
    )
    parser.add_argument(
        "--score_month", required=True,
        help="Score month in YYYY-MM format",
    )
    parser.add_argument(
        "--model_version", required=True,
        help="Model version string (e.g. v1.2)",
    )
    parser.add_argument(
        "--layers", nargs="*", default=["layer1", "layer2", "layer3"],
        help="Which layers to run (default: all)",
    )
    args = parser.parse_args()

    run(
        model_name=args.model,
        score_month=args.score_month,
        model_version=args.model_version,
        layers=args.layers,
    )


if __name__ == "__main__":
    main()
