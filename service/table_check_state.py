#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Persistent per-table sync check state."""

from __future__ import annotations

import json
import os
import re
import threading
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


SAFE_PART_RE = re.compile(r"[^A-Za-z0-9_.-]+")
SUCCESS_STATUSES = {"success", "skipped"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class TableCheckStateStore:
    """Store the latest check result without coupling it to business tables."""

    def __init__(self, project_root: Path, state_dir: Optional[Path] = None) -> None:
        self.project_root = Path(project_root).resolve()
        self.state_dir = (state_dir or (self.project_root / ".service_state")).resolve()
        self.checks_dir = self.state_dir / "table_checks"
        self.checks_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()

    def record(
        self,
        *,
        provider: str,
        task: str,
        database: str,
        table: str,
        status: str,
        job_id: str,
        attempted_at: str | None = None,
        finished_at: str | None = None,
        rows_written: int = 0,
        error: str = "",
    ) -> dict[str, Any]:
        identity = self._identity(provider=provider, task=task, database=database, table=table)
        now = str(finished_at or utc_now_iso())
        normalized_status = str(status or "failed").strip().lower() or "failed"
        path = self._state_path(**identity)
        with self._lock:
            previous = self._read_path(path)
            record = {
                **identity,
                "last_attempt_at": str(attempted_at or now),
                "last_success_at": previous.get("last_success_at") or None,
                "last_status": "success" if normalized_status in SUCCESS_STATUSES else normalized_status,
                "last_job_id": str(job_id or "").strip(),
                "rows_written": max(0, int(rows_written or 0)),
                "last_error": "" if normalized_status in SUCCESS_STATUSES else str(error or "").strip(),
                "updated_at": now,
            }
            if normalized_status in SUCCESS_STATUSES:
                record["last_success_at"] = now
            self._atomic_write(path, record)
        return deepcopy(record)

    def list_states(self) -> list[dict[str, Any]]:
        with self._lock:
            items = [self._read_path(path) for path in sorted(self.checks_dir.glob("*.json"))]
        return [deepcopy(item) for item in items if item]

    def latest_for_tasks(self, task_names: list[str]) -> dict[str, Any] | None:
        names = {str(name or "").strip() for name in task_names if str(name or "").strip()}
        if not names:
            return None
        matches = [item for item in self.list_states() if str(item.get("task") or "") in names]
        if not matches:
            return None
        return max(matches, key=lambda item: str(item.get("updated_at") or ""))

    def _identity(self, *, provider: str, task: str, database: str, table: str) -> dict[str, str]:
        identity = {
            "provider": str(provider or "").strip(),
            "task": str(task or "").strip(),
            "database": str(database or "").strip(),
            "table": str(table or "").strip(),
        }
        if not identity["task"]:
            raise ValueError("table check task is required")
        if not identity["table"]:
            raise ValueError("table check target is required")
        return identity

    def _state_path(self, *, provider: str, task: str, database: str, table: str) -> Path:
        parts = [provider or "unknown", task, database or "default", table]
        filename = "__".join(SAFE_PART_RE.sub("_", part).strip("._") or "unknown" for part in parts)
        return self.checks_dir / f"{filename}.json"

    @staticmethod
    def _read_path(path: Path) -> dict[str, Any]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _atomic_write(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_suffix(f"{path.suffix}.tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temp_path, path)


__all__ = ["TableCheckStateStore"]
