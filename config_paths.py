from __future__ import annotations

import os
from pathlib import Path


SYNC_PACKAGE_ROOT = Path(__file__).resolve().parent
DEFAULT_SYNC_CONFIG_ROOT = SYNC_PACKAGE_ROOT / "config" / "sync"
DEFAULT_SYNC_PLAN_ROOT = DEFAULT_SYNC_CONFIG_ROOT / "plans"
DEFAULT_RUNTIME_CONFIG_PATH = SYNC_PACKAGE_ROOT / "config" / "runtime.local.yaml"
DEFAULT_RUNTIME_EXAMPLE_PATH = SYNC_PACKAGE_ROOT / "config" / "runtime.example.yaml"
DEFAULT_PROVIDER_ROOT = SYNC_PACKAGE_ROOT / "providers"


def resolve_sync_config_root(project_root: str | Path | None = None) -> Path:
    root = Path(project_root).resolve() if project_root is not None else SYNC_PACKAGE_ROOT
    return root / "config" / "sync"


def resolve_sync_plan_root(project_root: str | Path | None = None) -> Path:
    return resolve_sync_config_root(project_root) / "plans"


def resolve_provider_root(project_root: str | Path | None = None) -> Path:
    root = Path(project_root).resolve() if project_root is not None else SYNC_PACKAGE_ROOT
    return root / "providers"


def resolve_runtime_config_path(path_like: str | Path | None = None) -> Path:
    env_path = (
        os.environ.get("SYNC_DATA_RUNTIME_CONFIG")
        or os.environ.get("ALPHABLOCKS_SYNC_DATA_RUNTIME_CONFIG")
        or os.environ.get("ALPHABLOCKS_RUNTIME_CONFIG")
        or os.environ.get("RUNTIME_CONFIG_PATH")
    )
    if path_like is None:
        if env_path:
            return Path(env_path).expanduser().resolve()
        return DEFAULT_RUNTIME_CONFIG_PATH

    candidate = Path(path_like).expanduser()
    if candidate.suffix in {".yaml", ".yml"}:
        if candidate.is_absolute():
            return candidate
        cwd_candidate = (Path.cwd() / candidate).resolve()
        if cwd_candidate.exists():
            return cwd_candidate
        local_candidate = (SYNC_PACKAGE_ROOT / candidate).resolve()
        if local_candidate.exists():
            return local_candidate
        return cwd_candidate

    root = candidate.resolve() if candidate.is_absolute() else (Path.cwd() / candidate).resolve()
    local_runtime = root / "config" / "runtime.local.yaml"
    return local_runtime


def resolve_config_candidate(path_like: str | Path, project_root: str | Path | None = None) -> Path:
    candidate = Path(path_like).expanduser()
    if candidate.is_absolute():
        return candidate

    roots = []
    root = Path(project_root).resolve() if project_root is not None else SYNC_PACKAGE_ROOT
    roots.append(root)
    roots.append(root.parent)
    roots.append(resolve_sync_plan_root(root))
    roots.append(resolve_sync_config_root(root))

    for base in roots:
        resolved = (base / candidate).resolve()
        if resolved.exists():
            return resolved
    return candidate
