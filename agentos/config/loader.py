"""Place this at: agentos/config/loader.py

Small cached YAML loader so every module reads config once, not per-call.
Requires: pip install pyyaml (add to requirements.txt)
"""
from __future__ import annotations

import os
from functools import lru_cache

import yaml

_CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))


@lru_cache(maxsize=None)
def load_config(filename: str) -> dict:
    path = os.path.join(_CONFIG_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def team_roles() -> dict:
    return load_config("actor_team.yml")


def guardrail_policies() -> dict:
    return load_config("guardrail_policies.yaml")


def runtime_tuning() -> dict:
    return load_config("runtime_tuning.yaml")