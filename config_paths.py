from __future__ import annotations

from pathlib import Path


SYNC_PACKAGE_ROOT = Path(__file__).resolve().parent
OUTER_PROJECT_ROOT = SYNC_PACKAGE_ROOT.parent
DEFAULT_SYNC_CONFIG_ROOT = OUTER_PROJECT_ROOT / "config" / "sync"
DEFAULT_SYNC_PLAN_ROOT = DEFAULT_SYNC_CONFIG_ROOT / "plans"
DEFAULT_RUNTIME_CONFIG_PATH = OUTER_PROJECT_ROOT / "config" / "runtime.local.yaml"


def resolve_sync_config_root(project_root: str | Path | None = None) -> Path:
    root = Path(project_root).resolve() if project_root is not None else SYNC_PACKAGE_ROOT
    return root.parent / "config" / "sync"


def resolve_sync_plan_root(project_root: str | Path | None = None) -> Path:
    return resolve_sync_config_root(project_root) / "plans"


def resolve_runtime_config_path(path_like: str | Path | None = None) -> Path:
    if path_like is None:
        return DEFAULT_RUNTIME_CONFIG_PATH

    candidate = Path(path_like).expanduser()
    if candidate.suffix in {".yaml", ".yml"}:
        if candidate.is_absolute():
            return candidate
        cwd_candidate = (Path.cwd() / candidate).resolve()
        if cwd_candidate.exists():
            return cwd_candidate
        outer_candidate = (OUTER_PROJECT_ROOT / candidate).resolve()
        if outer_candidate.exists():
            return outer_candidate
        return cwd_candidate

    root = candidate.resolve() if candidate.is_absolute() else (Path.cwd() / candidate).resolve()
    return root.parent / "config" / "runtime.local.yaml"


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
