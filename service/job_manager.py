#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Background job manager with global per-provider FIFO lanes."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
import time
import uuid
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from sync_data_system.config_paths import resolve_runtime_config_path
from sync_data_system.core.providers import load_provider_registry
from sync_data_system.runtime_config import load_runtime_config
from sync_data_system.service.log_redaction import redact_sensitive_text
from sync_data_system.service.log_time import log_timestamp
from sync_data_system.service.sync_config_manager import (
    WIDE_TABLE_TASK_KIND,
    WIDE_TABLE_TASK_PROVIDER,
)
from sync_data_system.service.task_registry import TASK_REGISTRY


DEFAULT_MAX_PARALLEL_PROVIDERS = 3
ERROR_SUMMARY_MAX_CHARS = 2000
ERROR_LOG_PATTERN = re.compile(
    r"traceback|exception|\berror\b|\bfailed\b|\bfailure\b|\bfatal\b|\bcritical\b|"
    r"timed?\s*out|timeout|permission\s+denied|connectionerror|"
    r"请求失败|登录失败|同步失败|错误|异常|无可用代码池|找不到",
    re.IGNORECASE,
)
ERROR_LOG_CONTEXT_BEFORE = 1
ERROR_LOG_CONTEXT_AFTER = 6
ACTIVE_STATUSES = {"queued", "running", "cancelling"}
PROCESS_ACTIVE_STATUSES = {"running", "cancelling"}
TERMINAL_STATUSES = {
    "success",
    "partial_success",
    "failed",
    "cancelled",
    "interrupted",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def utc_iso_from_timestamp(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, timezone.utc).replace(microsecond=0).isoformat()


@dataclass
class JobRecord:
    job_id: str
    kind: str
    status: str
    created_at: str
    started_at: Optional[str]
    finished_at: Optional[str]
    cwd: str
    command: list[str]
    log_path: str
    config_path: Optional[str] = None
    task: Optional[str] = None
    source: Optional[str] = None
    target: Optional[str] = None
    pid: Optional[int] = None
    return_code: Optional[int] = None
    error: Optional[str] = None
    request_payload: Optional[dict[str, Any]] = None
    updated_at: Optional[str] = None
    config_id: Optional[str] = None
    config_name: Optional[str] = None
    task_results_path: Optional[str] = None
    trigger: Optional[str] = None
    parent_job_id: Optional[str] = None
    child_job_ids: list[str] = field(default_factory=list)
    restart_count: int = 0
    last_restarted_at: Optional[str] = None


class SyncJobManager:
    def __init__(
        self,
        project_root: Path,
        state_dir: Optional[Path] = None,
        *,
        max_parallel_providers: Optional[int] = None,
    ) -> None:
        self.project_root = Path(project_root).resolve()
        self.state_dir = (state_dir or (self.project_root / ".service_state")).resolve()
        self.jobs_dir = self.state_dir / "jobs"
        self.logs_dir = self.state_dir / "logs"
        self.queue_path = self.state_dir / "job_queue.json"
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.max_parallel_providers = self._resolve_max_parallel_providers(
            max_parallel_providers
        )
        self._lock = threading.RLock()
        self._jobs: dict[str, JobRecord] = {}
        self._processes: dict[str, subprocess.Popen] = {}
        self._restart_job_ids: set[str] = set()
        self._shutting_down = False
        # Only executable jobs live in this queue. Parent jobs aggregate child state.
        self._queue: list[str] = []
        self._orphaned_job_ids: list[str] = []
        self._load_existing_jobs()
        self._load_queue()
        self._start_orphan_watchers()
        self._dispatch_next()

    def list_jobs(
        self,
        *,
        status: Optional[str] = None,
        task: Optional[str] = None,
        kind: Optional[str] = None,
        include_children: bool = False,
    ) -> list[JobRecord]:
        with self._lock:
            job_ids = [
                job.job_id
                for job in self._jobs.values()
                if include_children or not job.parent_job_id
            ]
            active_job_ids = [
                job_id
                for job_id in job_ids
                if self._jobs[job_id].status in ACTIVE_STATUSES
            ]
        # Terminal jobs are immutable. Refresh only live process/parent state so
        # paginated history requests do not repeatedly walk every old record.
        for job_id in active_job_ids:
            self._refresh_job(job_id)
        with self._lock:
            items = [
                job
                for job in self._jobs.values()
                if include_children or not job.parent_job_id
            ]
        if status:
            items = [job for job in items if job.status == status]
        if task:
            items = [job for job in items if job.task == task]
        if kind:
            items = [job for job in items if job.kind == kind]
        return sorted(items, key=lambda item: item.created_at, reverse=True)

    def get_job(self, job_id: str) -> JobRecord:
        self._refresh_job(job_id)
        with self._lock:
            if job_id not in self._jobs:
                raise KeyError(f"job not found: {job_id}")
            return self._jobs[job_id]

    def get_child_jobs(self, job_id: str) -> list[JobRecord]:
        self._refresh_job(job_id)
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(f"job not found: {job_id}")
            return [
                self._jobs[child_id]
                for child_id in job.child_job_ids
                if child_id in self._jobs
            ]

    def get_running_jobs(self) -> list[JobRecord]:
        return [
            job
            for job in self.list_jobs()
            if job.status in {"running", "cancelling"}
        ]

    def get_active_jobs(self, *, config_id: Optional[str] = None) -> list[JobRecord]:
        with self._lock:
            job_ids = [
                job.job_id
                for job in self._jobs.values()
                if not job.parent_job_id
                and job.status in ACTIVE_STATUSES
                and (not config_id or job.config_id == config_id)
            ]
        for job_id in job_ids:
            self._refresh_job(job_id)
        with self._lock:
            items = [
                self._jobs[job_id]
                for job_id in job_ids
                if job_id in self._jobs
                and self._jobs[job_id].status in ACTIVE_STATUSES
                and (not config_id or self._jobs[job_id].config_id == config_id)
            ]
        return sorted(items, key=lambda item: item.created_at, reverse=True)

    def find_active_config_job(self, config_id: str) -> Optional[JobRecord]:
        items = self.get_active_jobs(config_id=config_id)
        return sorted(items, key=lambda item: item.created_at)[0] if items else None

    def provider_queue_positions(self, job_id: str) -> list[dict[str, Any]]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(f"job not found: {job_id}")
            child_ids = job.child_job_ids or [job.job_id]
            positions: list[dict[str, Any]] = []
            for child_id in child_ids:
                child = self._jobs.get(child_id)
                if child is None:
                    continue
                positions.append(
                    {
                        "job_id": child.job_id,
                        "provider": child.source,
                        "status": child.status,
                        "queue_position": self._provider_queue_position_locked(child),
                        "started_at": child.started_at,
                        "finished_at": child.finished_at,
                        "return_code": child.return_code,
                        "error": child.error,
                        "restart_count": child.restart_count,
                        "last_restarted_at": child.last_restarted_at,
                    }
                )
            return positions

    def queue_position(self, job_id: str) -> Optional[int]:
        positions = [
            item["queue_position"]
            for item in self.provider_queue_positions(job_id)
            if item["queue_position"] is not None
        ]
        return min(positions) if positions else None

    def cancel_pending_jobs(
        self,
        *,
        config_id: str,
        trigger: Optional[str] = None,
    ) -> list[JobRecord]:
        with self._lock:
            job_ids = [
                job.job_id
                for job in self._jobs.values()
                if not job.parent_job_id
                and job.status == "queued"
                and job.config_id == config_id
                and (not trigger or job.trigger == trigger)
            ]
        cancelled = [self.cancel_job(job_id) for job_id in job_ids]
        return cancelled

    def create_task_batch_job(
        self,
        *,
        name: str,
        tasks: list[dict[str, Any]],
        continue_on_error: bool = True,
        log_level: str = "INFO",
        runtime_path: Optional[str] = None,
        config_id: Optional[str] = None,
        trigger: str = "manual",
    ) -> JobRecord:
        clean_name = str(name or "").strip()
        if not clean_name:
            raise ValueError("task batch name is required")
        if not tasks or not any(item.get("enabled", True) for item in tasks):
            raise ValueError("task batch must contain at least one enabled task")
        clean_config_id = str(config_id or "").strip() or None
        if clean_config_id:
            active = self.find_active_config_job(clean_config_id)
            if active is not None:
                raise RuntimeError(
                    "sync config already queued or running "
                    f"config_id={clean_config_id} job_id={active.job_id}"
                )

        parent_id = uuid.uuid4().hex[:12]
        now = utc_now_iso()
        parent_log_path = self.logs_dir / f"{parent_id}.log"
        parent_results_path = self.jobs_dir / f"{parent_id}.results.json"
        parent_payload_path = self.jobs_dir / f"{parent_id}.batch.json"
        task_ids = [
            str(task.get("id") or f"{parent_id}_task_{index}")
            for index, task in enumerate(tasks, start=1)
        ]
        snapshot = {
            "job_id": parent_id,
            "name": clean_name,
            "config_id": clean_config_id,
            "continue_on_error": bool(continue_on_error),
            "log_level": str(log_level or "INFO").strip() or "INFO",
            "runtime_path": runtime_path,
            "tasks": tasks,
            "task_ids": task_ids,
        }
        parent_payload_path.write_text(
            json.dumps(snapshot, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        grouped: dict[str, list[dict[str, Any]]] = {}
        for index, task in enumerate(tasks):
            if not task.get("enabled", True):
                continue
            provider = self._task_provider(task)
            executable_task = dict(task)
            executable_task["id"] = task_ids[index]
            executable_task["provider"] = provider
            grouped.setdefault(provider, []).append(executable_task)

        parent = JobRecord(
            job_id=parent_id,
            kind="sync_config" if clean_config_id else "task_batch",
            status="queued",
            created_at=now,
            started_at=None,
            finished_at=None,
            cwd=str(self.project_root),
            command=[],
            log_path=str(parent_log_path),
            request_payload=snapshot,
            updated_at=now,
            config_id=clean_config_id,
            config_name=clean_name,
            task_results_path=str(parent_results_path),
            trigger=str(trigger or "manual").strip() or "manual",
        )
        children: list[JobRecord] = []
        for provider, provider_tasks in grouped.items():
            child = self._build_batch_child(
                parent=parent,
                provider=provider,
                tasks=provider_tasks,
                continue_on_error=continue_on_error,
                log_level=log_level,
                runtime_path=runtime_path,
            )
            children.append(child)
        parent.child_job_ids = [child.job_id for child in children]

        with self._lock:
            if clean_config_id:
                duplicate = next(
                    (
                        job
                        for job in self._jobs.values()
                        if not job.parent_job_id
                        and job.config_id == clean_config_id
                        and job.status in ACTIVE_STATUSES
                    ),
                    None,
                )
                if duplicate is not None:
                    raise RuntimeError(
                        "sync config already queued or running "
                        f"config_id={clean_config_id} job_id={duplicate.job_id}"
                    )
            self._jobs[parent.job_id] = parent
            for child in children:
                self._jobs[child.job_id] = child
                self._queue.append(child.job_id)
                self._save_job(child)
            self._append_scheduler_log_locked(
                parent,
                "status=queued "
                f"providers={','.join(grouped)} max_parallel={self.max_parallel_providers}",
            )
            self._save_job(parent)
            self._save_queue()
            self._write_parent_results_locked(parent)
        self._dispatch_next()
        return self.get_job(parent.job_id)

    def create_registered_task_job(
        self,
        *,
        task: str,
        codes: list[str] | None = None,
        day: Optional[int] = None,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        year_type: Optional[str] = None,
        market: Optional[str] = None,
        index_code: Optional[str] = None,
        table_names: Optional[str] = None,
        sector_name: Optional[str] = None,
        code_market: Optional[str] = None,
        period: Optional[str] = None,
        fields: Optional[str] = None,
        params: Optional[dict[str, Any]] = None,
        adjust_type: Optional[str] = None,
        qmt_adjust_type: Optional[str] = None,
        fill_data: Optional[bool] = None,
        count: Optional[int] = None,
        incrementally: Optional[bool] = None,
        complete: Optional[bool] = None,
        limit: int = 0,
        force: bool = False,
        resume: bool = False,
        adjustflag: Optional[str] = None,
        frequency: Optional[str] = None,
        universe_mode: Optional[str] = None,
        continue_on_error: bool = False,
        log_level: Optional[str] = None,
        runtime_path: Optional[str] = None,
    ) -> JobRecord:
        definition = TASK_REGISTRY.get_task(task)
        code_items = [str(item).strip() for item in (codes or []) if str(item).strip()]
        request_payload = {
            "name": task,
            "codes": code_items,
            "day": day,
            "begin_date": begin_date,
            "end_date": end_date,
            "year": year,
            "quarter": quarter,
            "year_type": year_type,
            "market": market,
            "index_code": index_code,
            "table_names": table_names,
            "sector_name": sector_name,
            "code_market": code_market,
            "period": period,
            "fields": fields,
            "params": dict(params or {}),
            "adjust_type": adjust_type,
            "qmt_adjust_type": qmt_adjust_type,
            "fill_data": fill_data,
            "count": count,
            "incrementally": incrementally,
            "complete": complete,
            "limit": limit,
            "force": force,
            "resume": resume,
            "adjustflag": adjustflag,
            "frequency": frequency,
            "universe_mode": universe_mode,
            "continue_on_error": continue_on_error,
            "log_level": log_level,
            "runtime_path": runtime_path,
        }
        duplicate = self._find_active_registered_duplicate(
            task=task,
            source=definition.source,
            request_payload=request_payload,
        )
        if duplicate is not None:
            return duplicate

        parent_id = uuid.uuid4().hex[:12]
        child_id = uuid.uuid4().hex[:12]
        now = utc_now_iso()
        child_log_path = self.logs_dir / f"{child_id}.{definition.source}.log"
        command = [
            self._python_executable(),
            str(self.project_root / "scripts" / "run_provider_sync.py"),
            "--job-id",
            child_id,
            "--task",
            task,
            "--log-path",
            str(child_log_path),
        ]
        if runtime_path:
            command.extend(["--runtime-path", runtime_path])
        if code_items:
            command.extend(["--codes", ",".join(code_items)])
        if day is not None:
            command.extend(["--day", str(day)])
        if begin_date is not None:
            command.extend(["--begin-date", str(begin_date)])
        if end_date is not None:
            command.extend(["--end-date", str(end_date)])
        if year is not None:
            command.extend(["--year", str(year)])
        if quarter is not None:
            command.extend(["--quarter", str(quarter)])
        if year_type:
            command.extend(["--year-type", str(year_type)])
        if market:
            command.extend(["--market", str(market)])
        if index_code:
            command.extend(["--index-code", str(index_code)])
        if table_names:
            command.extend(["--table-names", str(table_names)])
        if sector_name:
            command.extend(["--sector-name", str(sector_name)])
        if code_market:
            command.extend(["--code-market", str(code_market)])
        if period:
            command.extend(["--period", str(period)])
        if fields:
            command.extend(["--fields", str(fields)])
        if params:
            command.extend(
                ["--params", json.dumps(params, ensure_ascii=False, separators=(",", ":"))]
            )
        resolved_qmt_adjust_type = qmt_adjust_type or adjust_type
        if resolved_qmt_adjust_type:
            command.extend(["--adjust-type", str(resolved_qmt_adjust_type)])
        if fill_data is not None:
            command.append("--fill-data" if fill_data else "--no-fill-data")
        if count is not None:
            command.extend(["--count", str(count)])
        if incrementally:
            command.append("--incrementally")
        if complete:
            command.append("--complete")
        if limit:
            command.extend(["--limit", str(limit)])
        if force:
            command.append("--force")
        if resume:
            command.append("--resume")
        if adjustflag:
            command.extend(["--adjustflag", str(adjustflag)])
        if frequency:
            command.extend(["--frequency", str(frequency)])
        if universe_mode:
            command.extend(["--universe-mode", str(universe_mode)])
        if continue_on_error:
            command.append("--continue-on-error")
        if log_level:
            command.extend(["--log-level", str(log_level)])

        parent = JobRecord(
            job_id=parent_id,
            kind="registered_task",
            status="queued",
            created_at=now,
            started_at=None,
            finished_at=None,
            cwd=str(self.project_root),
            command=command,
            log_path=str(self.logs_dir / f"{parent_id}.log"),
            task=task,
            source=definition.source,
            target=definition.target,
            request_payload=request_payload,
            updated_at=now,
            trigger="manual",
            child_job_ids=[child_id],
        )
        child = JobRecord(
            job_id=child_id,
            kind="provider_task",
            status="queued",
            created_at=now,
            started_at=None,
            finished_at=None,
            cwd=str(self.project_root),
            command=command,
            log_path=str(child_log_path),
            task=task,
            source=definition.source,
            target=definition.target,
            request_payload=request_payload,
            updated_at=now,
            trigger="manual",
            parent_job_id=parent_id,
        )
        with self._lock:
            duplicate = next(
                (
                    job
                    for job in self._jobs.values()
                    if not job.parent_job_id
                    and job.kind == "registered_task"
                    and job.status in ACTIVE_STATUSES
                    and job.task == task
                    and job.source == definition.source
                    and job.request_payload == request_payload
                ),
                None,
            )
            if duplicate is not None:
                return duplicate
            self._jobs[parent_id] = parent
            self._jobs[child_id] = child
            self._queue.append(child_id)
            self._append_scheduler_log_locked(
                parent,
                f"status=queued provider={definition.source} task={task}",
            )
            self._save_job(parent)
            self._save_job(child)
            self._save_queue()
        self._dispatch_next()
        return self.get_job(parent_id)

    def cancel_job(self, job_id: str) -> JobRecord:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(f"job not found: {job_id}")
            targets = [
                self._jobs[child_id]
                for child_id in job.child_job_ids
                if child_id in self._jobs
            ]
            if not targets:
                targets = [job]
            for target in targets:
                self._cancel_executable_locked(target)
            if job.child_job_ids:
                if any(target.status == "cancelling" for target in targets):
                    job.status = "cancelling"
                    job.updated_at = utc_now_iso()
                    self._save_job(job)
                else:
                    self._refresh_parent_locked(job)
                self._append_scheduler_log_locked(job, "status=cancellation_requested")
            elif job.parent_job_id:
                parent = self._jobs.get(job.parent_job_id)
                if parent is not None:
                    self._refresh_parent_locked(parent)
            self._save_queue()
        self._dispatch_next()
        return self.get_job(job_id)

    def read_job_log(self, job_id: str, tail_lines: int = 200) -> str:
        job = self.get_job(job_id)
        entries = self._read_job_log_entries(job)
        if tail_lines <= 0:
            return "\n".join(entries)
        return "\n".join(entries[-tail_lines:])

    def read_job_error_log(self, job_id: str, max_lines: int = 500) -> str:
        """Return error-related log segments with enough surrounding context."""

        job = self.get_job(job_id)
        entries = self._read_job_log_entries(job)
        matched = [
            index
            for index, line in enumerate(entries)
            if ERROR_LOG_PATTERN.search(line)
        ]
        if not matched:
            return ""
        selected_indexes: set[int] = set()
        for index in matched:
            start = max(0, index - ERROR_LOG_CONTEXT_BEFORE)
            end = min(len(entries), index + ERROR_LOG_CONTEXT_AFTER + 1)
            selected_indexes.update(range(start, end))
        selected: list[str] = []
        previous_index: int | None = None
        for index, line in enumerate(entries):
            if index not in selected_indexes:
                continue
            if previous_index is not None and index > previous_index + 1:
                selected.append("…")
            selected.append(line)
            previous_index = index
        limit = max(1, int(max_lines or 500))
        return "\n".join(selected[-limit:])

    def _read_job_log_entries(self, job: JobRecord) -> list[str]:
        entries: list[str] = []
        path = Path(job.log_path)
        if path.exists():
            text = redact_sensitive_text(
                path.read_text(encoding="utf-8", errors="ignore")
            )
            label = "scheduler" if job.child_job_ids else (job.source or "job")
            entries.extend(f"[{label}] {line}" for line in text.splitlines())
        for child in self.get_child_jobs(job.job_id) if job.child_job_ids else []:
            child_path = Path(child.log_path)
            if not child_path.exists():
                continue
            text = redact_sensitive_text(
                child_path.read_text(encoding="utf-8", errors="ignore")
            )
            label = f"provider={child.source or 'unknown'} job={child.job_id}"
            entries.extend(f"[{label}] {line}" for line in text.splitlines())
        return entries

    def read_task_results(self, job_id: str) -> dict[str, Any]:
        job = self.get_job(job_id)
        with self._lock:
            if job.child_job_ids and job.task_results_path:
                return self._aggregate_parent_results_locked(job)
        path = Path(job.task_results_path) if job.task_results_path else None
        if path is None or not path.exists():
            return {"job_id": job_id, "status": job.status, "tasks": []}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"job_id": job_id, "status": job.status, "tasks": []}
        if isinstance(payload, dict):
            return payload
        return {"job_id": job_id, "status": job.status, "tasks": []}

    def list_tasks(self) -> list[str]:
        return sorted(task.name for task in TASK_REGISTRY.list_tasks())

    def list_registered_tasks(self) -> list[dict[str, str | None]]:
        return TASK_REGISTRY.list_task_metadata()

    def list_providers(self) -> list[dict[str, Any]]:
        return load_provider_registry(self.project_root).to_metadata()

    def _build_batch_child(
        self,
        *,
        parent: JobRecord,
        provider: str,
        tasks: list[dict[str, Any]],
        continue_on_error: bool,
        log_level: str,
        runtime_path: Optional[str],
    ) -> JobRecord:
        child_id = uuid.uuid4().hex[:12]
        log_path = self.logs_dir / f"{child_id}.{provider}.log"
        payload_path = self.jobs_dir / f"{child_id}.batch.json"
        results_path = self.jobs_dir / f"{child_id}.results.json"
        snapshot = {
            "job_id": child_id,
            "parent_job_id": parent.job_id,
            "name": parent.config_name,
            "config_id": parent.config_id,
            "provider": provider,
            "continue_on_error": bool(continue_on_error),
            "log_level": str(log_level or "INFO").strip() or "INFO",
            "runtime_path": runtime_path,
            "tasks": tasks,
        }
        payload_path.write_text(
            json.dumps(snapshot, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        command = [
            self._python_executable(),
            str(self.project_root / "scripts" / "run_task_batch.py"),
            "--payload",
            str(payload_path),
            "--results",
            str(results_path),
            "--log-path",
            str(log_path),
        ]
        return JobRecord(
            job_id=child_id,
            kind="provider_batch",
            status="queued",
            created_at=parent.created_at,
            started_at=None,
            finished_at=None,
            cwd=str(self.project_root),
            command=command,
            log_path=str(log_path),
            source=provider,
            request_payload=snapshot,
            updated_at=parent.created_at,
            config_id=parent.config_id,
            config_name=parent.config_name,
            task_results_path=str(results_path),
            trigger=parent.trigger,
            parent_job_id=parent.job_id,
        )

    def _find_active_registered_duplicate(
        self,
        *,
        task: str,
        source: str,
        request_payload: dict[str, Any],
    ) -> Optional[JobRecord]:
        with self._lock:
            candidates = [
                job
                for job in self._jobs.values()
                if not job.parent_job_id
                and job.kind == "registered_task"
                and job.status in ACTIVE_STATUSES
                and job.task == task
                and job.source == source
                and job.request_payload == request_payload
            ]
        return sorted(candidates, key=lambda item: item.created_at)[0] if candidates else None

    def _watch_process(
        self,
        job_id: str,
        process: subprocess.Popen,
        log_fp,
    ) -> None:
        return_code = process.wait()
        log_fp.close()
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            self._finish_executable_locked(job, return_code)
        self._dispatch_next()

    def _refresh_job(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            child_ids = list(job.child_job_ids)
        if child_ids:
            for child_id in child_ids:
                self._refresh_job(child_id)
            with self._lock:
                parent = self._jobs.get(job_id)
                if parent is not None:
                    self._refresh_parent_locked(parent)
            return

        with self._lock:
            process = self._processes.get(job_id)
            job = self._jobs.get(job_id)
        if process is None or job is None:
            return
        return_code = process.poll()
        if return_code is None:
            self._refresh_running_job_updated_at(job_id)
            return
        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None:
                self._finish_executable_locked(job, return_code)
        self._dispatch_next()

    def _finish_executable_locked(self, job: JobRecord, return_code: int) -> None:
        if job.job_id in self._restart_job_ids:
            # prepare_for_restart() has already persisted this job as queued.
            # The old process exits only to let the service shut down cleanly;
            # its signal return code must not overwrite the resumable state.
            self._processes.pop(job.job_id, None)
            return
        if job.job_id not in self._processes and job.status in TERMINAL_STATUSES:
            return
        job.return_code = return_code
        job.finished_at = utc_now_iso()
        job.updated_at = job.finished_at
        if job.status in {"cancelling", "cancelled"}:
            job.status = "cancelled"
        else:
            job.status = self._status_from_return_code(return_code)
        if job.status in {"failed", "partial_success", "interrupted"}:
            job.error = self._derive_job_error(job, return_code)
        elif job.status == "success":
            job.error = None
        self._processes.pop(job.job_id, None)
        self._save_job(job)
        if job.parent_job_id:
            parent = self._jobs.get(job.parent_job_id)
            if parent is not None:
                self._append_scheduler_log_locked(
                    parent,
                    f"provider={job.source} child={job.job_id} status={job.status} "
                    f"return_code={return_code}",
                )
                self._refresh_parent_locked(parent)

    def _refresh_parent_locked(self, parent: JobRecord) -> None:
        children = [
            self._jobs[child_id]
            for child_id in parent.child_job_ids
            if child_id in self._jobs
        ]
        if not children:
            return
        statuses = [child.status for child in children]
        started_values = [child.started_at for child in children if child.started_at]
        updated_values = [
            child.updated_at
            for child in children
            if child.updated_at
        ]
        parent.restart_count = sum(int(child.restart_count or 0) for child in children)
        restarted_values = [
            child.last_restarted_at
            for child in children
            if child.last_restarted_at
        ]
        parent.last_restarted_at = max(restarted_values) if restarted_values else None
        if started_values:
            parent.started_at = min(started_values)
        if any(status == "cancelling" for status in statuses):
            status = "cancelling"
        elif any(status == "running" for status in statuses):
            status = "running"
        elif any(status == "queued" for status in statuses):
            status = "running" if parent.started_at else "queued"
        else:
            status = self._aggregate_terminal_status(statuses)
        status_changed = status != parent.status
        parent.status = status
        if updated_values:
            parent.updated_at = max(updated_values)
        if status in TERMINAL_STATUSES:
            finished_values = [
                child.finished_at
                for child in children
                if child.finished_at
            ]
            parent.finished_at = max(finished_values) if finished_values else utc_now_iso()
            parent.updated_at = parent.finished_at
            parent.return_code = self._return_code_from_status(status)
            failures = [
                f"{child.source or child.task or child.job_id}: "
                f"{child.error or child.status}"
                for child in children
                if child.status != "success"
            ]
            parent.error = (
                self._truncate_error("; ".join(failures))
                if failures
                else None
            )
        else:
            parent.finished_at = None
            parent.return_code = None
            live_failures = [
                f"{child.source or child.task or child.job_id}: {child.error}"
                for child in children
                if child.error
            ]
            parent.error = (
                self._truncate_error("; ".join(live_failures))
                if live_failures
                else None
            )
        self._save_job(parent)
        if parent.task_results_path:
            self._write_parent_results_locked(parent)
        if status_changed:
            self._append_scheduler_log_locked(parent, f"status={status}")

    @staticmethod
    def _aggregate_terminal_status(statuses: list[str]) -> str:
        unique = set(statuses)
        if unique == {"success"}:
            return "success"
        if unique == {"cancelled"}:
            return "cancelled"
        if unique == {"interrupted"}:
            return "interrupted"
        if unique == {"failed"}:
            return "failed"
        if "success" in unique or "partial_success" in unique:
            return "partial_success"
        if "failed" in unique:
            return "failed"
        if "interrupted" in unique:
            return "interrupted"
        return "cancelled"

    def _cancel_executable_locked(self, job: JobRecord) -> None:
        if job.status == "queued":
            if job.job_id in self._queue:
                self._queue.remove(job.job_id)
            job.status = "cancelled"
            job.finished_at = utc_now_iso()
            job.updated_at = job.finished_at
            job.error = "cancelled before execution"
            self._save_job(job)
            return
        process = self._processes.get(job.job_id)
        if process is None:
            return
        if job.status == "running":
            job.status = "cancelling"
            job.updated_at = utc_now_iso()
            self._save_job(job)
        process.terminate()

    def _provider_queue_position_locked(self, job: JobRecord) -> Optional[int]:
        if job.status != "queued" or job.job_id not in self._queue:
            return None
        lane = self._lane_key(job)
        position = 0
        for queued_id in self._queue:
            queued = self._jobs.get(queued_id)
            if queued is None or queued.status != "queued":
                continue
            if self._lane_key(queued) == lane:
                position += 1
            if queued_id == job.job_id:
                return position
        return None

    def _save_job(self, job: JobRecord) -> None:
        path = self.jobs_dir / f"{job.job_id}.json"
        path.write_text(
            json.dumps(asdict(job), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _refresh_running_job_updated_at(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.status not in PROCESS_ACTIVE_STATUSES:
                return
            task_error = self._task_result_error(job)
            updated_at = (
                self._log_updated_at(job)
                or job.updated_at
                or job.started_at
                or job.created_at
            )
            if updated_at == job.updated_at and (not task_error or task_error == job.error):
                return
            job.updated_at = updated_at
            if task_error:
                job.error = task_error
            self._save_job(job)
            if job.parent_job_id:
                parent = self._jobs.get(job.parent_job_id)
                if parent is not None:
                    self._refresh_parent_locked(parent)

    def _log_updated_at(self, job: JobRecord) -> Optional[str]:
        if not job.log_path:
            return None
        try:
            return utc_iso_from_timestamp(Path(job.log_path).stat().st_mtime)
        except OSError:
            return None

    def _derive_job_error(self, job: JobRecord, return_code: int) -> str:
        task_error = self._task_result_error(job)
        if task_error:
            return task_error
        log_error = self._log_error_summary(job)
        if log_error:
            return log_error
        return f"provider process exited with return code {return_code}"

    def _task_result_error(self, job: JobRecord) -> str:
        path = Path(job.task_results_path) if job.task_results_path else None
        if path is None or not path.exists():
            return ""
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return ""
        results = payload.get("tasks") if isinstance(payload, dict) else None
        if not isinstance(results, list):
            return ""
        failures: list[str] = []
        for item in results:
            if not isinstance(item, dict):
                continue
            if str(item.get("status") or "") not in {"failed", "partial_success"}:
                continue
            error = str(item.get("error") or "").strip()
            if not error:
                continue
            label = str(item.get("name") or item.get("task_id") or "task").strip()
            failures.append(f"{label}: {error}")
        return self._truncate_error("; ".join(failures)) if failures else ""

    def _log_error_summary(self, job: JobRecord) -> str:
        text = self._read_log_tail(Path(job.log_path))
        if not text:
            return ""
        lines = [
            redact_sensitive_text(line).strip()
            for line in text.splitlines()
            if line.strip()
        ]
        exception_pattern = re.compile(
            r"^(?:[A-Za-z_][A-Za-z0-9_.]*(?:Error|Exception)|"
            r"KeyboardInterrupt|SystemExit):\s*.+$"
        )
        for line in reversed(lines):
            if exception_pattern.match(line):
                return self._truncate_error(line)
        for line in reversed(lines):
            if re.search(r"(?:^|\s)(?:ERROR|CRITICAL)(?::|\s)", line):
                return self._truncate_error(line)
        for line in reversed(lines):
            lowered = line.lower()
            if " failed" in lowered or "失败" in line:
                return self._truncate_error(line)
        return self._truncate_error(lines[-1]) if lines else ""

    @staticmethod
    def _read_log_tail(path: Path, max_bytes: int = 256 * 1024) -> str:
        try:
            with path.open("rb") as handle:
                handle.seek(0, os.SEEK_END)
                size = handle.tell()
                handle.seek(max(0, size - max_bytes))
                data = handle.read()
        except OSError:
            return ""
        return data.decode("utf-8", errors="ignore")

    @staticmethod
    def _truncate_error(value: str) -> str:
        text = redact_sensitive_text(str(value or "").strip())
        if len(text) <= ERROR_SUMMARY_MAX_CHARS:
            return text
        return text[: ERROR_SUMMARY_MAX_CHARS - 1].rstrip() + "…"

    def _load_existing_jobs(self) -> None:
        loaded: dict[str, JobRecord] = {}
        for path in sorted(self.jobs_dir.glob("*.json")):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                job = JobRecord(**data)
                loaded[job.job_id] = job
            except Exception:
                continue
        self._jobs = loaded
        for job in self._jobs.values():
            is_parent = bool(job.child_job_ids)
            if job.status == "running" and not is_parent:
                if self._recorded_process_is_running(job):
                    self._orphaned_job_ids.append(job.job_id)
                    continue
                self._requeue_after_restart_locked(job)
            elif job.status == "cancelling" and not is_parent:
                job.status = "cancelled"
                job.finished_at = job.finished_at or utc_now_iso()
                job.updated_at = job.finished_at
                job.error = job.error or "cancelled because the service restarted"
                self._save_job(job)
            elif (
                job.status in {"failed", "partial_success", "interrupted"}
                and not is_parent
                and not job.error
            ):
                job.error = self._derive_job_error(job, job.return_code or 1)
                self._save_job(job)
            elif not job.updated_at:
                job.updated_at = job.finished_at or job.started_at or job.created_at
                self._save_job(job)
        for job in self._jobs.values():
            if job.child_job_ids:
                self._refresh_parent_locked(job)

    def prepare_for_restart(self, *, timeout_seconds: float = 5.0) -> None:
        """Persist running jobs as resumable work before service shutdown.

        Without this transition, the child process can receive SIGINT/SIGTERM
        before the API process exits and its watcher records a business failure.
        Persisting the queued state first makes a graceful service restart follow
        the same recovery path as an abrupt machine restart.
        """

        processes: list[subprocess.Popen] = []
        with self._lock:
            if self._shutting_down:
                return
            self._shutting_down = True
            running = sorted(
                (
                    job
                    for job in self._jobs.values()
                    if not job.child_job_ids
                    and job.status == "running"
                    and job.job_id in self._processes
                ),
                key=lambda item: (item.started_at or item.created_at, item.job_id),
            )
            resumed_ids: list[str] = []
            for job in running:
                process = self._processes[job.job_id]
                processes.append(process)
                self._restart_job_ids.add(job.job_id)
                self._requeue_after_restart_locked(job)
                resumed_ids.append(job.job_id)
                if job.parent_job_id:
                    parent = self._jobs.get(job.parent_job_id)
                    if parent is not None:
                        self._refresh_parent_locked(parent)
            if resumed_ids:
                resumed_set = set(resumed_ids)
                # A task that was already running must remain ahead of later
                # queued work in the same provider lane after the restart.
                self._queue = resumed_ids + [
                    job_id for job_id in self._queue if job_id not in resumed_set
                ]
                self._save_queue()

        for process in processes:
            try:
                if process.poll() is None:
                    process.terminate()
            except (OSError, ProcessLookupError):
                continue

        deadline = time.monotonic() + max(0.0, float(timeout_seconds))
        pending = list(processes)
        while pending and time.monotonic() < deadline:
            pending = [process for process in pending if process.poll() is None]
            if pending:
                time.sleep(0.05)
        for process in pending:
            try:
                process.kill()
            except (AttributeError, OSError, ProcessLookupError):
                pass

    def _requeue_after_restart_locked(self, job: JobRecord) -> None:
        restarted_at = utc_now_iso()
        self._enable_resume_for_job_locked(job)
        job.status = "queued"
        job.started_at = None
        job.finished_at = None
        job.pid = None
        job.return_code = None
        job.error = None
        job.updated_at = restarted_at
        job.restart_count = int(job.restart_count or 0) + 1
        job.last_restarted_at = restarted_at
        self._save_job(job)
        if job.parent_job_id:
            parent = self._jobs.get(job.parent_job_id)
            if parent is not None:
                self._append_scheduler_log_locked(
                    parent,
                    f"provider={job.source} child={job.job_id} "
                    "status=requeued reason=service_restart resume=true force=false",
                )

    def _enable_resume_for_job_locked(self, job: JobRecord) -> None:
        payload = deepcopy(job.request_payload or {})
        if job.kind == "provider_batch":
            payload["resume_after_restart"] = True
            tasks = []
            for raw_task in payload.get("tasks") or []:
                task = deepcopy(raw_task)
                parameters = dict(task.get("parameters") or {})
                parameters["resume"] = True
                # A restarted job is a continuation, even when the original
                # manual run requested a forced backfill. Keeping force=True
                # would make target-table incremental tasks (notably AmazingData
                # minute_kline) ignore their persisted max(trade_time) and fetch
                # the entire requested history again.
                parameters["force"] = False
                task["parameters"] = parameters
                tasks.append(task)
            payload["tasks"] = tasks
            payload_path = self._command_option(job.command, "--payload")
            if payload_path:
                self._write_json_atomic(Path(payload_path), payload)
        elif job.kind == "provider_task":
            payload["resume"] = True
            payload["force"] = False
            job.command = [argument for argument in job.command if argument != "--force"]
            if "--resume" not in job.command:
                job.command.append("--resume")
        job.request_payload = payload or job.request_payload

    def _start_orphan_watchers(self) -> None:
        for job_id in self._orphaned_job_ids:
            watcher = threading.Thread(
                target=self._watch_orphaned_process,
                args=(job_id,),
                daemon=True,
            )
            watcher.start()

    def _watch_orphaned_process(self, job_id: str) -> None:
        while True:
            with self._lock:
                job = self._jobs.get(job_id)
                if job is None or job.status != "running":
                    return
                still_running = self._recorded_process_is_running(job)
            if not still_running:
                break
            time.sleep(1)
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.status != "running":
                return
            self._requeue_after_restart_locked(job)
            self._load_queue()
            if job.parent_job_id:
                parent = self._jobs.get(job.parent_job_id)
                if parent is not None:
                    self._refresh_parent_locked(parent)
        self._dispatch_next()

    @staticmethod
    def _recorded_process_is_running(job: JobRecord) -> bool:
        if not job.pid or job.pid <= 0 or not sys.platform.startswith("linux"):
            return False
        try:
            os.kill(job.pid, 0)
        except (OSError, ProcessLookupError):
            return False
        try:
            command_line = Path(f"/proc/{job.pid}/cmdline").read_bytes().split(b"\0")
        except OSError:
            return False
        actual = [
            item.decode("utf-8", errors="ignore")
            for item in command_line
            if item
        ]
        if not actual:
            return False
        joined = "\0".join(actual)
        return job.job_id in joined

    @staticmethod
    def _command_option(command: list[str], option: str) -> Optional[str]:
        try:
            index = command.index(option)
        except ValueError:
            return None
        if index + 1 >= len(command):
            return None
        return command[index + 1]

    @staticmethod
    def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_suffix(path.suffix + ".tmp")
        temp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp_path.replace(path)

    def _load_queue(self) -> None:
        persisted: list[str] = []
        try:
            payload = json.loads(self.queue_path.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                persisted = [str(item) for item in payload]
        except (OSError, json.JSONDecodeError):
            persisted = []
        queued_ids = {
            job.job_id
            for job in self._jobs.values()
            if job.status == "queued" and not job.child_job_ids
        }
        ordered = [job_id for job_id in persisted if job_id in queued_ids]
        remaining = sorted(
            (self._jobs[job_id] for job_id in queued_ids if job_id not in ordered),
            key=lambda item: (item.created_at, item.job_id),
        )
        self._queue = ordered + [job.job_id for job in remaining]
        self._save_queue()

    def _dispatch_next(self) -> None:
        while True:
            watcher: Optional[threading.Thread] = None
            with self._lock:
                if self._shutting_down:
                    return
                active_jobs = [
                    job
                    for job in self._jobs.values()
                    if not job.child_job_ids
                    and job.status in PROCESS_ACTIVE_STATUSES
                ]
                if len(active_jobs) >= self.max_parallel_providers:
                    return
                active_lanes = {self._lane_key(job) for job in active_jobs}
                selected_index: Optional[int] = None
                selected: Optional[JobRecord] = None
                queue_changed = False
                for index, job_id in enumerate(self._queue):
                    candidate = self._jobs.get(job_id)
                    if candidate is None or candidate.status != "queued":
                        queue_changed = True
                        continue
                    if self._lane_key(candidate) in active_lanes:
                        continue
                    selected_index = index
                    selected = candidate
                    break
                if queue_changed:
                    self._queue = [
                        job_id
                        for job_id in self._queue
                        if job_id in self._jobs
                        and self._jobs[job_id].status == "queued"
                    ]
                if selected is None or selected_index is None:
                    if queue_changed:
                        self._save_queue()
                    return
                # Recalculate after stale queue entries were removed.
                selected_index = self._queue.index(selected.job_id)
                self._queue.pop(selected_index)
                self._save_queue()

                log_path = Path(selected.log_path)
                log_path.parent.mkdir(parents=True, exist_ok=True)
                log_fp = log_path.open("a", encoding="utf-8")
                try:
                    process = subprocess.Popen(
                        selected.command,
                        cwd=str(self.project_root),
                        env=self._build_subprocess_env(),
                        stdout=log_fp,
                        stderr=subprocess.STDOUT,
                        text=True,
                    )
                except Exception as exc:
                    log_fp.close()
                    selected.status = "failed"
                    selected.error = str(exc)
                    selected.finished_at = utc_now_iso()
                    selected.updated_at = selected.finished_at
                    self._save_job(selected)
                    if selected.parent_job_id:
                        parent = self._jobs.get(selected.parent_job_id)
                        if parent is not None:
                            self._append_scheduler_log_locked(
                                parent,
                                f"provider={selected.source} child={selected.job_id} "
                                f"status=failed error={exc}",
                            )
                            self._refresh_parent_locked(parent)
                    continue

                now = utc_now_iso()
                selected.status = "running"
                selected.started_at = now
                selected.updated_at = now
                selected.pid = process.pid
                self._processes[selected.job_id] = process
                self._save_job(selected)
                if selected.parent_job_id:
                    parent = self._jobs.get(selected.parent_job_id)
                    if parent is not None:
                        self._append_scheduler_log_locked(
                            parent,
                            f"provider={selected.source} child={selected.job_id} status=running",
                        )
                        self._refresh_parent_locked(parent)
                watcher = threading.Thread(
                    target=self._watch_process,
                    args=(selected.job_id, process, log_fp),
                    daemon=True,
                )
            if watcher is not None:
                watcher.start()

    def _save_queue(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        temp_path = self.queue_path.with_suffix(".json.tmp")
        temp_path.write_text(
            json.dumps(self._queue, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp_path.replace(self.queue_path)

    def _aggregate_parent_results_locked(self, parent: JobRecord) -> dict[str, Any]:
        snapshot = parent.request_payload or {}
        original_tasks = list(snapshot.get("tasks") or [])
        task_ids = list(snapshot.get("task_ids") or [])
        child_results: dict[str, dict[str, Any]] = {}
        task_child_status: dict[str, str] = {}
        for child_id in parent.child_job_ids:
            child = self._jobs.get(child_id)
            if child is None:
                continue
            child_task_ids = {
                str(task.get("id") or "")
                for task in (child.request_payload or {}).get("tasks", [])
            }
            for task_id in child_task_ids:
                if task_id:
                    task_child_status[task_id] = child.status
            path = Path(child.task_results_path) if child.task_results_path else None
            if path is None or not path.exists():
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            for result in payload.get("tasks", []) if isinstance(payload, dict) else []:
                if isinstance(result, dict):
                    child_results[str(result.get("task_id") or "")] = result

        results: list[dict[str, Any]] = []
        for index, task in enumerate(original_tasks):
            task_id = str(
                task.get("id")
                or (task_ids[index] if index < len(task_ids) else f"task_{index + 1}")
            )
            if not task.get("enabled", True):
                results.append(self._pending_task_result(task, task_id, "disabled"))
                continue
            result = child_results.get(task_id)
            if result is not None:
                results.append(result)
                continue
            child_status = task_child_status.get(task_id, "queued")
            status = (
                "not_run"
                if child_status in TERMINAL_STATUSES
                else ("running" if child_status == "running" else "queued")
            )
            results.append(self._pending_task_result(task, task_id, status))
        return {
            "job_id": parent.job_id,
            "status": parent.status,
            "updated_at": parent.updated_at or parent.created_at,
            "tasks": results,
        }

    def _write_parent_results_locked(self, parent: JobRecord) -> None:
        if not parent.task_results_path:
            return
        payload = self._aggregate_parent_results_locked(parent)
        path = Path(parent.task_results_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_suffix(".json.tmp")
        temp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp_path.replace(path)

    @staticmethod
    def _pending_task_result(
        task: dict[str, Any],
        task_id: str,
        status: str,
    ) -> dict[str, Any]:
        return {
            "task_id": task_id,
            "name": str(task.get("name") or task.get("task") or ""),
            "provider": str(task.get("provider") or task.get("source") or ""),
            "database": str(task.get("database") or ""),
            "target": str(task.get("target") or ""),
            "status": status,
            "started_at": None,
            "finished_at": None,
            "error": None,
            "effective_parameters": {},
        }

    def _append_scheduler_log_locked(self, parent: JobRecord, message: str) -> None:
        path = Path(parent.log_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(
                f"{log_timestamp()} scheduler parent={parent.job_id} "
                f"{redact_sensitive_text(message)}\n"
            )

    def _task_provider(self, task: dict[str, Any]) -> str:
        if str(task.get("kind") or "").strip() == WIDE_TABLE_TASK_KIND:
            if not isinstance(task.get("payload"), dict):
                raise ValueError("wide table sync task payload is required")
            supplied = str(task.get("provider") or task.get("source") or "").strip()
            if supplied and supplied != WIDE_TABLE_TASK_PROVIDER:
                raise ValueError(
                    "wide table task belongs to provider "
                    f"{WIDE_TABLE_TASK_PROVIDER}, not {supplied}"
                )
            return WIDE_TABLE_TASK_PROVIDER
        name = str(task.get("name") or task.get("task") or "").strip()
        if not name:
            raise ValueError("task name is required")
        try:
            definition = TASK_REGISTRY.get_task(name)
        except KeyError as exc:
            raise ValueError(f"unknown registered task: {name}") from exc
        supplied = str(task.get("provider") or task.get("source") or "").strip()
        if supplied and supplied != definition.source:
            raise ValueError(
                f"task {name} belongs to provider {definition.source}, not {supplied}"
            )
        return definition.source

    @staticmethod
    def _lane_key(job: JobRecord) -> str:
        return str(job.source or "__global__").strip().casefold() or "__global__"

    def _resolve_max_parallel_providers(self, explicit: Optional[int]) -> int:
        value: Any = explicit
        if value is None:
            value = (
                os.environ.get("SYNC_MAX_PARALLEL_PROVIDERS")
                or os.environ.get("ALPHABLOCKS_SYNC_MAX_PARALLEL_PROVIDERS")
            )
        if value is None:
            try:
                path = resolve_runtime_config_path()
                if path.exists():
                    value = load_runtime_config(
                        path
                    ).sync.scheduler.max_parallel_providers
            except Exception:
                value = None
        if value is None:
            value = DEFAULT_MAX_PARALLEL_PROVIDERS
        try:
            return max(1, int(value))
        except (TypeError, ValueError):
            return DEFAULT_MAX_PARALLEL_PROVIDERS

    def _build_subprocess_env(self) -> dict[str, str]:
        env = os.environ.copy()
        parent = str(self.project_root)
        current = env.get("PYTHONPATH", "")
        items = [item for item in current.split(os.pathsep) if item]
        if parent not in items:
            items.insert(0, parent)
        env["PYTHONPATH"] = os.pathsep.join(items)
        return env

    def _python_executable(self) -> str:
        configured = (
            os.environ.get("SYNC_JOB_PYTHON_BIN")
            or os.environ.get("ALPHABLOCKS_SYNC_JOB_PYTHON_BIN")
            or ""
        ).strip()
        return configured or sys.executable

    @staticmethod
    def _status_from_return_code(return_code: int) -> str:
        if return_code == 0:
            return "success"
        if return_code == 2:
            return "partial_success"
        return "failed"

    @staticmethod
    def _return_code_from_status(status: str) -> int:
        if status == "success":
            return 0
        if status == "partial_success":
            return 2
        return 1


__all__ = ["JobRecord", "SyncJobManager"]
