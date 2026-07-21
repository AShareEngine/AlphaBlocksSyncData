#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Persistent cross-provider sync configuration records."""

from __future__ import annotations

import json
import re
import threading
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from sync_data_system.service.task_registry import TASK_REGISTRY


CONFIG_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
TIME_RE = re.compile(r"^(\d{1,2}):(\d{2})$")
DATE_MODES = {"incremental", "fixed", "provider_default"}
SCHEDULE_FREQUENCIES = {"daily", "weekly", "interval"}
DEFAULT_WEEKDAYS = ["1", "2", "3", "4", "5"]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class SyncConfigManager:
    def __init__(self, project_root: Path, state_dir: Optional[Path] = None) -> None:
        self.project_root = Path(project_root).resolve()
        self.state_dir = (state_dir or (self.project_root / ".service_state")).resolve()
        self.configs_dir = self.state_dir / "sync_configs"
        self.configs_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._configs: dict[str, dict[str, Any]] = {}
        self._load_existing_configs()

    def list_configs(self) -> list[dict[str, Any]]:
        with self._lock:
            items = [deepcopy(item) for item in self._configs.values()]
        return sorted(items, key=lambda item: str(item.get("created_at") or ""), reverse=True)

    def get_config(self, config_id: str) -> dict[str, Any]:
        clean_id = self._clean_id(config_id)
        with self._lock:
            if clean_id not in self._configs:
                raise KeyError(clean_id)
            return deepcopy(self._configs[clean_id])

    def create_config(self, payload: dict[str, Any], *, config_id: str | None = None) -> dict[str, Any]:
        now = utc_now_iso()
        clean_id = self._clean_id(config_id or f"sync_config_{uuid.uuid4().hex[:12]}")
        with self._lock:
            if clean_id in self._configs:
                raise ValueError(f"sync config already exists: {clean_id}")
            record = self._normalize_record(
                {
                    **payload,
                    "id": clean_id,
                    "created_at": now,
                    "updated_at": now,
                    "last_job_id": None,
                    "last_run_at": None,
                }
            )
            self._ensure_unique_name(record["name"])
            self._configs[clean_id] = record
            self._save_config(record)
            return deepcopy(record)

    def update_config(self, config_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        clean_id = self._clean_id(config_id)
        with self._lock:
            if clean_id not in self._configs:
                raise KeyError(clean_id)
            current = self._configs[clean_id]
            record = self._normalize_record(
                {
                    **current,
                    **updates,
                    "id": clean_id,
                    "created_at": current["created_at"],
                    "updated_at": utc_now_iso(),
                    "last_job_id": current.get("last_job_id"),
                    "last_run_at": current.get("last_run_at"),
                }
            )
            self._ensure_unique_name(record["name"], exclude_id=clean_id)
            self._configs[clean_id] = record
            self._save_config(record)
            return deepcopy(record)

    def delete_config(self, config_id: str) -> dict[str, Any]:
        clean_id = self._clean_id(config_id)
        with self._lock:
            if clean_id not in self._configs:
                raise KeyError(clean_id)
            record = self._configs.pop(clean_id)
            path = self._config_path(clean_id)
            if path.exists():
                path.unlink()
            return deepcopy(record)

    def get_schedule(self, config_id: str) -> dict[str, Any]:
        clean_id = self._clean_id(config_id)
        with self._lock:
            if clean_id not in self._configs:
                raise KeyError(clean_id)
            return deepcopy(self._configs[clean_id]["schedule"])

    def update_schedule(self, config_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        clean_id = self._clean_id(config_id)
        with self._lock:
            if clean_id not in self._configs:
                raise KeyError(clean_id)
            current = self._configs[clean_id]
            schedule = self._normalize_schedule(
                {
                    **current["schedule"],
                    **updates,
                    "created_at": current["schedule"]["created_at"],
                    "updated_at": utc_now_iso(),
                }
            )
            current["schedule"] = schedule
            current["updated_at"] = utc_now_iso()
            self._save_config(current)
            return deepcopy(schedule)

    def mark_started(self, config_id: str, job_id: str, *, started_at: str | None = None) -> dict[str, Any]:
        clean_id = self._clean_id(config_id)
        with self._lock:
            if clean_id not in self._configs:
                raise KeyError(clean_id)
            current = self._configs[clean_id]
            current["last_job_id"] = str(job_id or "").strip() or None
            current["last_run_at"] = str(started_at or utc_now_iso())
            current["updated_at"] = utc_now_iso()
            self._save_config(current)
            return deepcopy(current)

    def _normalize_record(self, payload: dict[str, Any]) -> dict[str, Any]:
        config_id = self._clean_id(payload.get("id"))
        name = self._clean_text(payload.get("name"))
        if not name:
            raise ValueError("sync config name is required")
        raw_tasks = payload.get("tasks")
        if not isinstance(raw_tasks, list) or not raw_tasks:
            raise ValueError("sync config must contain at least one task")
        tasks = [self._normalize_task(item) for item in raw_tasks]
        if not any(item["enabled"] for item in tasks):
            raise ValueError("sync config must contain at least one enabled task")
        created_at = self._clean_text(payload.get("created_at")) or utc_now_iso()
        updated_at = self._clean_text(payload.get("updated_at")) or created_at
        return {
            "id": config_id,
            "name": name,
            "description": self._clean_text(payload.get("description")),
            "log_level": self._clean_text(payload.get("log_level")) or "INFO",
            "continue_on_error": self._coerce_bool(payload.get("continue_on_error"), default=True),
            "tasks": tasks,
            "legacy_source": self._clean_text(payload.get("legacy_source")) or None,
            "last_job_id": self._clean_text(payload.get("last_job_id")) or None,
            "last_run_at": self._clean_text(payload.get("last_run_at")) or None,
            "schedule": self._normalize_schedule(payload.get("schedule") or {}),
            "created_at": created_at,
            "updated_at": updated_at,
        }

    def _normalize_schedule(self, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("sync config schedule must be an object")
        frequency = self._clean_text(payload.get("frequency")) or "daily"
        if frequency not in SCHEDULE_FREQUENCIES:
            raise ValueError(f"schedule frequency must be one of {sorted(SCHEDULE_FREQUENCIES)}")
        time_value = self._clean_text(payload.get("time")) or "18:00"
        if frequency != "interval":
            match = TIME_RE.match(time_value)
            if not match or int(match.group(1)) > 23 or int(match.group(2)) > 59:
                raise ValueError("schedule time must use HH:mm")
        weekdays = payload.get("weekdays") or list(DEFAULT_WEEKDAYS)
        if not isinstance(weekdays, list):
            weekdays = [weekdays]
        clean_weekdays = sorted({self._clean_text(item) for item in weekdays if self._clean_text(item)})
        if any(item not in {"1", "2", "3", "4", "5", "6", "7"} for item in clean_weekdays):
            raise ValueError("schedule weekdays must use values 1 through 7")
        if frequency == "weekly" and not clean_weekdays:
            raise ValueError("schedule weekdays are required for weekly frequency")
        try:
            interval_minutes = int(payload.get("interval_minutes") or 60)
        except (TypeError, ValueError) as exc:
            raise ValueError("schedule interval_minutes must be an integer") from exc
        if frequency == "interval" and interval_minutes < 5:
            raise ValueError("schedule interval_minutes must be at least 5")
        enabled = self._coerce_bool(payload.get("enabled"), default=False)
        created_at = self._clean_text(payload.get("created_at")) or utc_now_iso()
        updated_at = self._clean_text(payload.get("updated_at")) or created_at
        return {
            "enabled": enabled,
            "frequency": frequency,
            "time": time_value,
            "weekdays": clean_weekdays or list(DEFAULT_WEEKDAYS),
            "interval_minutes": interval_minutes,
            "next_run_at": self._clean_text(payload.get("next_run_at")) if enabled else "",
            "last_trigger_at": self._clean_text(payload.get("last_trigger_at")) or None,
            "last_trigger_result": self._clean_text(payload.get("last_trigger_result")) or None,
            "last_trigger_message": self._clean_text(payload.get("last_trigger_message")) or None,
            "created_at": created_at,
            "updated_at": updated_at,
        }

    def _normalize_task(self, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("sync config task must be an object")
        task_name = self._clean_text(payload.get("name") or payload.get("task"))
        if not task_name:
            raise ValueError("sync config task name is required")
        try:
            metadata = TASK_REGISTRY.get_task_metadata(task_name)
        except KeyError as exc:
            raise ValueError(f"unknown registered task: {task_name}") from exc

        supplied_provider = self._clean_text(payload.get("provider") or payload.get("source"))
        if supplied_provider and supplied_provider != metadata["source"]:
            raise ValueError(
                f"task {task_name} belongs to provider {metadata['source']}, not {supplied_provider}"
            )
        parameters = payload.get("parameters") or {}
        if not isinstance(parameters, dict):
            raise ValueError(f"task {task_name} parameters must be an object")
        allowed_fields = set(metadata.get("request_fields") or []) - {"name", "log_level", "runtime_path"}
        unexpected = sorted(set(parameters) - allowed_fields)
        if unexpected:
            raise ValueError(f"task {task_name} has unsupported parameters: {unexpected}")
        date_mode = self._clean_text(payload.get("date_mode")) or "provider_default"
        if date_mode not in DATE_MODES:
            raise ValueError(f"task {task_name} date_mode must be one of {sorted(DATE_MODES)}")
        entity_assets = payload.get("entity_assets") or []
        if not isinstance(entity_assets, list):
            entity_assets = [entity_assets]
        clean_assets = [self._clean_text(item) for item in entity_assets if self._clean_text(item)]
        return {
            "id": self._clean_id(payload.get("id") or f"sync_task_{uuid.uuid4().hex[:12]}", label="task id"),
            "name": task_name,
            "provider": metadata["source"],
            "database": metadata.get("database"),
            "target": metadata.get("target"),
            "enabled": self._coerce_bool(payload.get("enabled"), default=True),
            "date_mode": date_mode,
            "parameters": deepcopy(parameters),
            "entity_assets": clean_assets,
        }

    def _ensure_unique_name(self, name: str, *, exclude_id: str | None = None) -> None:
        normalized = name.casefold()
        for config_id, item in self._configs.items():
            if config_id == exclude_id:
                continue
            if str(item.get("name") or "").casefold() == normalized:
                raise ValueError(f"sync config name already exists: {name}")

    def _load_existing_configs(self) -> None:
        for path in sorted(self.configs_dir.glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                record = self._normalize_record(payload)
                self._configs[record["id"]] = record
                if payload != record:
                    self._save_config(record)
            except Exception:
                continue

    def _save_config(self, record: dict[str, Any]) -> None:
        path = self._config_path(record["id"])
        temp_path = path.with_suffix(".json.tmp")
        temp_path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(path)

    def _config_path(self, config_id: str) -> Path:
        return self.configs_dir / f"{self._clean_id(config_id)}.json"

    @staticmethod
    def _clean_text(value: Any) -> str:
        return str(value or "").strip()

    @staticmethod
    def _coerce_bool(value: Any, *, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() not in {"0", "false", "no", "off"}

    @staticmethod
    def _clean_id(value: Any, *, label: str = "sync config id") -> str:
        clean_value = str(value or "").strip()
        if not clean_value or not CONFIG_ID_RE.match(clean_value):
            raise ValueError(f"{label} is invalid")
        return clean_value


__all__ = ["DATE_MODES", "SyncConfigManager", "utc_now_iso"]
