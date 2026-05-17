#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Validation for provider sync plan TOML files."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sync_data_system.config_paths import resolve_sync_plan_root
from sync_data_system.core.config import load_toml_config
from sync_data_system.core.providers import ProviderManifest, load_provider_registry


TOP_LEVEL_FIELDS = frozenset({"source", "runtime_path", "log_level", "continue_on_error", "database", "defaults", "tasks"})
TASK_META_FIELDS = frozenset({"task", "enabled"})
BOOL_FIELDS = frozenset({"continue_on_error", "enabled", "force", "resume", "fill_data", "incrementally", "complete"})
NON_NEGATIVE_INT_FIELDS = frozenset({"limit"})
INT_FIELDS = frozenset({"count", "year", "quarter"})
LIST_OR_STRING_FIELDS = frozenset({"codes", "fields", "table_names"})
DATE_OR_STRING_FIELDS = frozenset({"begin_date", "end_date", "begin_time", "end_time", "day"})
STRING_FIELDS = frozenset(
    {
        "source",
        "runtime_path",
        "log_level",
        "database",
        "task",
        "symbol",
        "market",
        "index_code",
        "stock_code",
        "sector_name",
        "code_market",
        "period",
        "adjust_type",
        "year_type",
        "adjustflag",
        "frequency",
    }
)


@dataclass(frozen=True)
class SyncPlanValidationResult:
    path: Path
    source: str
    total_tasks: int
    enabled_tasks: int
    disabled_tasks: int


def discover_sync_plan_paths(project_root: str | Path | None = None) -> list[Path]:
    plan_root = resolve_sync_plan_root(project_root)
    if not plan_root.exists():
        return []
    return sorted(path for path in plan_root.glob("run_sync*.toml") if path.is_file())


def validate_sync_plan(path: str | Path, *, project_root: str | Path | None = None) -> SyncPlanValidationResult:
    config_path, data = load_toml_config(path, project_root=project_root)
    registry = load_provider_registry(project_root)

    _reject_unknown_fields(data, TOP_LEVEL_FIELDS, scope="top-level")
    source = _required_string(data.get("source"), "source")
    if not registry.has(source):
        raise ValueError(f"{config_path}: source {source!r} is not a registered provider; available={registry.names()}")
    manifest = registry.get(source)

    _validate_top_level_values(config_path, data)
    _validate_task_defaults(config_path, manifest, data.get("defaults", {}) or {})
    total_tasks, enabled_tasks = _validate_tasks(config_path, manifest, data.get("tasks"))
    return SyncPlanValidationResult(
        path=config_path,
        source=source,
        total_tasks=total_tasks,
        enabled_tasks=enabled_tasks,
        disabled_tasks=total_tasks - enabled_tasks,
    )


def validate_sync_plans(
    paths: list[str | Path] | tuple[str | Path, ...],
    *,
    project_root: str | Path | None = None,
) -> list[SyncPlanValidationResult]:
    return [validate_sync_plan(path, project_root=project_root) for path in paths]


def _validate_top_level_values(config_path: Path, data: dict[str, Any]) -> None:
    _validate_field_value("source", data.get("source"), scope=str(config_path))
    for field_name in ("runtime_path", "log_level", "database"):
        if field_name in data:
            _validate_field_value(field_name, data[field_name], scope=str(config_path))
    if "continue_on_error" in data:
        _validate_field_value("continue_on_error", data["continue_on_error"], scope=str(config_path))
    if "defaults" in data and not isinstance(data["defaults"], dict):
        raise ValueError(f"{config_path}: [defaults] must be a TOML table")
    if "tasks" not in data:
        raise ValueError(f"{config_path}: at least one [[tasks]] table is required")


def _validate_task_defaults(config_path: Path, manifest: ProviderManifest, defaults: Any) -> None:
    if not isinstance(defaults, dict):
        raise ValueError(f"{config_path}: [defaults] must be a TOML table")
    allowed = set(manifest.plan_fields)
    _reject_unknown_fields(defaults, allowed, scope=f"{config_path} [defaults]")
    for field_name, value in defaults.items():
        _validate_field_value(field_name, value, scope=f"{config_path} [defaults]")


def _validate_tasks(config_path: Path, manifest: ProviderManifest, raw_tasks: Any) -> tuple[int, int]:
    if not isinstance(raw_tasks, list) or not raw_tasks:
        raise ValueError(f"{config_path}: at least one [[tasks]] table is required")

    allowed = set(TASK_META_FIELDS) | set(manifest.plan_fields)
    task_names = set(manifest.task_names)
    enabled_count = 0
    for index, raw_task in enumerate(raw_tasks, start=1):
        scope = f"{config_path} tasks[{index}]"
        if not isinstance(raw_task, dict):
            raise ValueError(f"{scope}: must be a TOML table")
        _reject_unknown_fields(raw_task, allowed, scope=scope)
        task_name = _required_string(raw_task.get("task"), f"tasks[{index}].task")
        if task_name not in task_names:
            raise ValueError(f"{scope}: task {task_name!r} is not declared by provider {manifest.name!r}")
        enabled = raw_task.get("enabled", True)
        _validate_field_value("enabled", enabled, scope=scope)
        if enabled:
            enabled_count += 1
        for field_name, value in raw_task.items():
            _validate_field_value(field_name, value, scope=scope)

    if enabled_count == 0:
        raise ValueError(f"{config_path}: all [[tasks]] tables are disabled")
    return len(raw_tasks), enabled_count


def _reject_unknown_fields(data: dict[str, Any], allowed: set[str] | frozenset[str], *, scope: str) -> None:
    unexpected = sorted(set(data) - set(allowed))
    if unexpected:
        raise ValueError(f"{scope}: unknown field(s): {unexpected}")


def _required_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")
    text = value.strip()
    if not text:
        raise ValueError(f"{field_name} must be a non-empty string")
    return text


def _validate_field_value(field_name: str, value: Any, *, scope: str) -> None:
    if field_name in BOOL_FIELDS:
        if not isinstance(value, bool):
            raise ValueError(f"{scope}: {field_name} must be boolean")
        return
    if field_name in NON_NEGATIVE_INT_FIELDS:
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError(f"{scope}: {field_name} must be a non-negative integer")
        return
    if field_name in INT_FIELDS:
        if value is None:
            return
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"{scope}: {field_name} must be an integer")
        return
    if field_name in LIST_OR_STRING_FIELDS:
        _validate_string_or_scalar_list(field_name, value, scope=scope)
        return
    if field_name in DATE_OR_STRING_FIELDS:
        if not isinstance(value, (int, str)) or isinstance(value, bool):
            raise ValueError(f"{scope}: {field_name} must be an integer or string")
        return
    if field_name in STRING_FIELDS:
        if not isinstance(value, str):
            raise ValueError(f"{scope}: {field_name} must be a string")
        return


def _validate_string_or_scalar_list(field_name: str, value: Any, *, scope: str) -> None:
    if isinstance(value, str):
        return
    if not isinstance(value, list):
        raise ValueError(f"{scope}: {field_name} must be a string or array")
    for item in value:
        if isinstance(item, (dict, list)):
            raise ValueError(f"{scope}: {field_name} array items must be scalar values")


__all__ = [
    "SyncPlanValidationResult",
    "discover_sync_plan_paths",
    "validate_sync_plan",
    "validate_sync_plans",
]
