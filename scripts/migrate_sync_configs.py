#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""One-time migration from the five formal TOML plans to business configs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from program_bootstrap import install_sync_data_system_alias

install_sync_data_system_alias(PROJECT_ROOT)

from sync_data_system.service.sync_config_manager import SyncConfigManager
from sync_data_system.service.task_registry import TASK_REGISTRY
from sync_data_system.toml_compat import tomllib


FORMAL_CONFIGS = (
    ("run_sync.amazingdata.full.toml", "sync_config_amazingdata_full", "AmazingData 全量同步"),
    (
        "run_sync.amazingdata.minute_000001_sz.toml",
        "sync_config_amazingdata_minute_000001_sz",
        "AmazingData 单股分钟同步",
    ),
    ("run_sync.amazingdata.special.toml", "sync_config_amazingdata_special", "AmazingData 特殊数据同步"),
    ("run_sync.baostock.daily.toml", "sync_config_baostock_daily", "BaoStock 日常同步"),
    ("run_sync.baostock.full.toml", "sync_config_baostock_full", "BaoStock 全量同步"),
)
LEGACY_TARGETS = {filename: config_id for filename, config_id, _ in FORMAL_CONFIGS}
DATE_PARAMETER_KEYS = {"day", "begin_date", "end_date", "year", "quarter", "year_type"}


def migrate(project_root: Path, state_dir: Path | None = None) -> dict[str, Any]:
    project_root = project_root.resolve()
    plans_dir = project_root / "config" / "sync" / "plans"
    manager = SyncConfigManager(project_root, state_dir=state_dir)
    existing_ids = {item["id"] for item in manager.list_configs()}
    migrated: list[dict[str, Any]] = []

    for filename, config_id, display_name in FORMAL_CONFIGS:
        path = plans_dir / filename
        if not path.is_file():
            if config_id in existing_ids:
                migrated.append(manager.get_config(config_id))
                continue
            raise FileNotFoundError(f"formal legacy config not found: {path}")
        with path.open("rb") as handle:
            legacy = tomllib.load(handle)
        payload = _convert_legacy_config(legacy, filename=filename, display_name=display_name)
        if config_id in existing_ids:
            record = manager.update_config(config_id, payload)
        else:
            record = manager.create_config(payload, config_id=config_id)
            existing_ids.add(config_id)
        migrated.append(record)

    schedules = _migrate_schedules(manager.state_dir / "schedules")
    return {
        "configs": [
            {"id": item["id"], "name": item["name"], "task_count": len(item["tasks"])}
            for item in migrated
        ],
        "schedules": schedules,
    }


def _convert_legacy_config(
    legacy: dict[str, Any],
    *,
    filename: str,
    display_name: str,
) -> dict[str, Any]:
    provider = str(legacy.get("source") or "amazingdata").strip()
    defaults = legacy.get("defaults") if isinstance(legacy.get("defaults"), dict) else {}
    raw_tasks = legacy.get("tasks")
    if not isinstance(raw_tasks, list) or not raw_tasks:
        raise ValueError(f"legacy config has no tasks: {filename}")
    tasks: list[dict[str, Any]] = []
    for index, raw_task in enumerate(raw_tasks, start=1):
        if not isinstance(raw_task, dict):
            raise ValueError(f"invalid task #{index} in {filename}")
        local_name = str(raw_task.get("task") or raw_task.get("name") or "").strip()
        task_name = local_name if "." in local_name else f"{provider}.{local_name}"
        metadata = TASK_REGISTRY.get_task_metadata(task_name)
        request_fields = set(metadata.get("request_fields") or []) - {"name", "log_level", "runtime_path"}
        merged = {**defaults, **raw_task}
        parameters = {
            key: value
            for key, value in merged.items()
            if key in request_fields and key not in {"task", "enabled"}
        }
        tasks.append(
            {
                "id": f"{config_id_fragment(filename)}_{index:03d}",
                "name": task_name,
                "enabled": _coerce_bool(raw_task.get("enabled", True)),
                "date_mode": "fixed" if DATE_PARAMETER_KEYS.intersection(parameters) else "provider_default",
                "parameters": parameters,
                "entity_assets": [],
            }
        )
    return {
        "name": display_name,
        "description": f"由 {filename} 迁移",
        "log_level": str(legacy.get("log_level") or "INFO"),
        "continue_on_error": _coerce_bool(legacy.get("continue_on_error", True)),
        "legacy_source": filename,
        "tasks": tasks,
    }


def _migrate_schedules(schedules_dir: Path) -> list[dict[str, str]]:
    migrated: list[dict[str, str]] = []
    if not schedules_dir.exists():
        return migrated
    for path in sorted(schedules_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if payload.get("target_type") != "config":
            continue
        old_target = str(payload.get("target") or "")
        new_target = LEGACY_TARGETS.get(old_target)
        if not new_target:
            continue
        payload["target"] = new_target
        temp_path = path.with_suffix(".json.tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(path)
        migrated.append({"id": str(payload.get("id") or path.stem), "from": old_target, "to": new_target})
    return migrated


def config_id_fragment(filename: str) -> str:
    return "sync_task_" + "_".join(
        part for part in filename.removesuffix(".toml").replace(".", "_").split("_") if part
    )


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", default=str(PROJECT_ROOT))
    parser.add_argument("--state-dir")
    args = parser.parse_args()
    result = migrate(
        Path(args.project_root),
        state_dir=Path(args.state_dir).resolve() if args.state_dir else None,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
