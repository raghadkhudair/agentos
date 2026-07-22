from __future__ import annotations

import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, cast

import yaml

_CONFIG_DIR = Path(__file__).resolve().parent
_ENV_PATTERN = re.compile(r"\$\{([A-Z][A-Z0-9_]*)(?::-([^}]*))?\}")
_MAX_CONFIG_BYTES = 1_048_576


def _expand_environment(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _expand_environment(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand_environment(item) for item in value]
    if not isinstance(value, str):
        return value

    def replace(match: re.Match[str]) -> str:
        name, default = match.group(1), match.group(2)
        if name in os.environ:
            return os.environ[name]
        if default is not None:
            return default
        raise ValueError(f"required configuration environment variable is missing: {name}")

    return _ENV_PATTERN.sub(replace, value)


@lru_cache(maxsize=64)
def _load_yaml_path(path_text: str) -> dict[str, Any]:
    path = Path(path_text).resolve()
    if path.suffix.lower() not in {".yaml", ".yml"}:
        raise ValueError("configuration path must use a .yaml or .yml extension")
    if not path.is_file():
        raise FileNotFoundError(path)
    if path.stat().st_size > _MAX_CONFIG_BYTES:
        raise ValueError(f"configuration file exceeds {_MAX_CONFIG_BYTES} bytes: {path}")
    with path.open("r", encoding="utf-8") as stream:
        raw = yaml.safe_load(stream) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"configuration root must be a mapping: {path}")
    return cast(dict[str, Any], _expand_environment(raw))


def load_config(filename: str) -> dict[str, Any]:
    """Load a packaged configuration without allowing directory traversal."""

    path = (_CONFIG_DIR / filename).resolve()
    if path.parent != _CONFIG_DIR:
        raise ValueError("configuration filename must resolve inside agentos/config")
    return _load_yaml_path(str(path))


def load_config_path(path: Path | str) -> dict[str, Any]:
    """Safely load an operator-selected YAML file.

    The operator is allowed to locate the provider registry outside the package,
    but the file must be a bounded regular YAML document and is always parsed with
    ``yaml.safe_load``. Its schema is validated by the consuming subsystem.
    """

    return _load_yaml_path(str(Path(path).expanduser().resolve()))


def clear_config_cache() -> None:
    _load_yaml_path.cache_clear()


def team_roles() -> dict[str, Any]:
    return load_config("actor_team.yml")


def guardrail_policies() -> dict[str, Any]:
    return load_config("guardrail_policies.yaml")


def runtime_tuning() -> dict[str, Any]:
    return load_config("runtime_tuning.yaml")


def provider_registry(path: Path | str | None = None) -> dict[str, Any]:
    if path is None:
        return load_config("providers.yaml")
    return load_config_path(path)
