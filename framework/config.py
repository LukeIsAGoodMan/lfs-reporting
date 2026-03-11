"""Configuration loader and model registry.

The filesystem is the registry: each YAML in conf/models/ is a registered model.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


_CONF_DIR = Path(__file__).resolve().parent.parent / "conf"


@dataclass
class ModelConfig:
    """Typed representation of a model's YAML configuration.

    Top-level sections map 1:1 to the YAML structure.
    Inner dicts are kept loose so each layer module can destructure
    only the keys it needs — this keeps the schema extensible.
    """

    name: str
    display_name: str
    source: dict[str, Any]
    binning: dict[str, Any]
    layer1: dict[str, Any]
    layer2: dict[str, Any]
    layer3: dict[str, Any]
    output: dict[str, Any]
    hooks_module: str | None = None


def load_model_config(model_name: str) -> ModelConfig:
    """Load ``conf/models/{model_name}.yaml`` and return a validated ModelConfig.

    Raises:
        FileNotFoundError: No config file for *model_name*.
        ValueError: YAML is missing required top-level keys.
    """
    config_path = _CONF_DIR / "models" / f"{model_name}.yaml"
    if not config_path.exists():
        raise FileNotFoundError(
            f"No configuration found for model '{model_name}' at {config_path}"
        )

    with open(config_path, "r") as fh:
        raw: dict[str, Any] = yaml.safe_load(fh)

    required_sections = ["model", "source", "binning", "layer1", "layer2", "layer3", "output"]
    missing = [s for s in required_sections if s not in raw]
    if missing:
        raise ValueError(
            f"Model config '{model_name}' is missing required sections: {missing}"
        )

    model_section = raw["model"]
    return ModelConfig(
        name=model_section["name"],
        display_name=model_section.get("display_name", model_section["name"]),
        source=raw["source"],
        binning=raw["binning"],
        layer1=raw["layer1"],
        layer2=raw["layer2"],
        layer3=raw["layer3"],
        output=raw["output"],
        hooks_module=model_section.get("hooks_module"),
    )


def load_framework_config() -> dict[str, Any]:
    """Load ``conf/framework.yaml`` and return as a plain dict."""
    framework_path = _CONF_DIR / "framework.yaml"
    if not framework_path.exists():
        return {}
    with open(framework_path, "r") as fh:
        return yaml.safe_load(fh) or {}


def list_registered_models() -> list[str]:
    """Return names of all models with a YAML file in ``conf/models/``."""
    models_dir = _CONF_DIR / "models"
    if not models_dir.exists():
        return []
    return sorted(p.stem for p in models_dir.glob("*.yaml"))
