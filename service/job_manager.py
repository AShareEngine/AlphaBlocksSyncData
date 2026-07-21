#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Background job manager for provider sync jobs."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from sync_data_system.core.providers import load_provider_registry
from sync_data_system.service.task_registry import TASK_REGISTRY


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


class SyncJobManager:
    def __init__(self, project_root: Path, state_dir: Optional[Path] = None) -> None:
        self.project_root = Path(project_root).resolve()
        self.state_dir = (state_dir or (self.project_root / ".service_state")).resolve()
        self.jobs_dir = self.state_dir / "jobs"
        self.logs_dir = self.state_dir / "logs"
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._jobs: dict[str, JobRecord] = {}
        self._processes: dict[str, subprocess.Popen] = {}
        self._load_existing_jobs()

    def list_jobs(
        self,
        *,
        status: Optional[str] = None,
        task: Optional[str] = None,
        kind: Optional[str] = None,
    ) -> list[JobRecord]:
        with self._lock:
            jobs = list(self._jobs.values())
        for job in jobs:
            self._refresh_job(job.job_id)
        with self._lock:
            items = list(self._jobs.values())
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

    def get_running_jobs(self) -> list[JobRecord]:
        jobs = self.list_jobs()
        return [job for job in jobs if job.status in {"running", "cancelling"}]

    def create_task_batch_job(
        self,
        *,
        name: str,
        tasks: list[dict[str, Any]],
        continue_on_error: bool = True,
        log_level: str = "INFO",
        runtime_path: Optional[str] = None,
        config_id: Optional[str] = None,
    ) -> JobRecord:
        self._ensure_no_running_jobs()
        clean_name = str(name or "").strip()
        if not clean_name:
            raise ValueError("task batch name is required")
        if not tasks or not any(item.get("enabled", True) for item in tasks):
            raise ValueError("task batch must contain at least one enabled task")

        job_id = uuid.uuid4().hex[:12]
        log_path = self.logs_dir / f"{job_id}.log"
        payload_path = self.jobs_dir / f"{job_id}.batch.json"
        results_path = self.jobs_dir / f"{job_id}.results.json"
        snapshot = {
            "job_id": job_id,
            "name": clean_name,
            "config_id": str(config_id or "").strip() or None,
            "continue_on_error": bool(continue_on_error),
            "log_level": str(log_level or "INFO").strip() or "INFO",
            "runtime_path": runtime_path,
            "tasks": tasks,
        }
        payload_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
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
        return self._start_job(
            kind="sync_config" if config_id else "task_batch",
            command=command,
            config_path=None,
            task=None,
            request_payload=snapshot,
            job_id=job_id,
            log_path=log_path,
            config_id=str(config_id or "").strip() or None,
            config_name=clean_name,
            task_results_path=str(results_path),
        )

    def cancel_job(self, job_id: str) -> JobRecord:
        with self._lock:
            process = self._processes.get(job_id)
            if process is None:
                return self.get_job(job_id)
            job = self._jobs.get(job_id)
            if job is not None and job.status == "running":
                job.status = "cancelling"
                job.updated_at = utc_now_iso()
                self._save_job(job)
            process.terminate()
        return self.get_job(job_id)

    def read_job_log(self, job_id: str, tail_lines: int = 200) -> str:
        job = self.get_job(job_id)
        path = Path(job.log_path)
        if not path.exists():
            return ""
        text = path.read_text(encoding="utf-8", errors="ignore")
        lines = text.splitlines()
        if tail_lines <= 0:
            return text
        return "\n".join(lines[-tail_lines:])

    def read_task_results(self, job_id: str) -> dict[str, Any]:
        job = self.get_job(job_id)
        path = Path(job.task_results_path) if job.task_results_path else None
        if path is None or not path.exists():
            return {"job_id": job_id, "status": job.status, "tasks": []}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"job_id": job_id, "status": job.status, "tasks": []}
        return payload if isinstance(payload, dict) else {"job_id": job_id, "status": job.status, "tasks": []}

    def list_tasks(self) -> list[str]:
        return sorted(task.name for task in TASK_REGISTRY.list_tasks())

    def list_registered_tasks(self) -> list[dict[str, str | None]]:
        return TASK_REGISTRY.list_task_metadata()

    def list_providers(self) -> list[dict[str, Any]]:
        return load_provider_registry(self.project_root).to_metadata()

    def _start_job(
        self,
        *,
        kind: str,
        command: list[str],
        config_path: Optional[str],
        task: Optional[str],
        source: Optional[str] = None,
        target: Optional[str] = None,
        request_payload: Optional[dict[str, Any]] = None,
        job_id: Optional[str] = None,
        log_path: Optional[Path] = None,
        config_id: Optional[str] = None,
        config_name: Optional[str] = None,
        task_results_path: Optional[str] = None,
    ) -> JobRecord:
        job_id = job_id or uuid.uuid4().hex[:12]
        log_path = log_path or (self.logs_dir / f"{job_id}.log")
        log_fp = log_path.open("a", encoding="utf-8")
        process = subprocess.Popen(
            command,
            cwd=str(self.project_root),
            env=self._build_subprocess_env(),
            stdout=log_fp,
            stderr=subprocess.STDOUT,
            text=True,
        )
        now = utc_now_iso()
        job = JobRecord(
            job_id=job_id,
            kind=kind,
            status="running",
            created_at=now,
            started_at=now,
            finished_at=None,
            cwd=str(self.project_root),
            command=command,
            log_path=str(log_path),
            config_path=config_path,
            task=task,
            source=source,
            target=target,
            pid=process.pid,
            return_code=None,
            error=None,
            request_payload=request_payload,
            updated_at=now,
            config_id=config_id,
            config_name=config_name,
            task_results_path=task_results_path,
        )
        with self._lock:
            self._jobs[job_id] = job
            self._processes[job_id] = process
            self._save_job(job)
        watcher = threading.Thread(target=self._watch_process, args=(job_id, process, log_fp), daemon=True)
        watcher.start()
        return job

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
        log_level: Optional[str] = None,
        runtime_path: Optional[str] = None,
    ) -> JobRecord:
        self._ensure_no_running_jobs()
        definition = TASK_REGISTRY.get_task(task)
        job_id = uuid.uuid4().hex[:12]
        log_path = self.logs_dir / f"{job_id}.log"
        command = [
            self._python_executable(),
            str(self.project_root / "scripts" / "run_provider_sync.py"),
            "--job-id",
            job_id,
            "--task",
            task,
            "--log-path",
            str(log_path),
        ]
        code_items = [str(item).strip() for item in (codes or []) if str(item).strip()]
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
        if log_level:
            command.extend(["--log-level", str(log_level)])
        return self._start_job(
            kind="registered_task",
            command=command,
            config_path=None,
            task=task,
            source=definition.source,
            target=definition.target,
            request_payload={
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
                "log_level": log_level,
                "runtime_path": runtime_path,
            },
        )

    def _watch_process(self, job_id: str, process: subprocess.Popen, log_fp) -> None:
        return_code = process.wait()
        log_fp.close()
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            job.return_code = return_code
            job.finished_at = utc_now_iso()
            job.updated_at = job.finished_at
            if job.status == "cancelling":
                job.status = "cancelled"
            elif job.status != "cancelled":
                job.status = self._status_from_return_code(return_code)
            self._processes.pop(job_id, None)
            self._save_job(job)

    def _refresh_job(self, job_id: str) -> None:
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
            job.return_code = return_code
            job.finished_at = utc_now_iso()
            job.updated_at = job.finished_at
            if job.status != "cancelled":
                job.status = self._status_from_return_code(return_code)
            self._processes.pop(job_id, None)
            self._save_job(job)

    def _ensure_no_running_jobs(self) -> None:
        running_jobs = self.get_running_jobs()
        if not running_jobs:
            return
        running = running_jobs[0]
        raise RuntimeError(
            f"another sync job is running job_id={running.job_id} task={running.task or running.config_path}; wait for it to finish before starting a new sync job"
        )

    def _save_job(self, job: JobRecord) -> None:
        path = self.jobs_dir / f"{job.job_id}.json"
        path.write_text(json.dumps(asdict(job), ensure_ascii=False, indent=2), encoding="utf-8")

    def _refresh_running_job_updated_at(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.status not in {"running", "cancelling"}:
                return
            updated_at = self._log_updated_at(job) or job.updated_at or job.started_at or job.created_at
            if updated_at == job.updated_at:
                return
            job.updated_at = updated_at
            self._save_job(job)

    def _log_updated_at(self, job: JobRecord) -> Optional[str]:
        if not job.log_path:
            return None
        try:
            return utc_iso_from_timestamp(Path(job.log_path).stat().st_mtime)
        except OSError:
            return None

    def _load_existing_jobs(self) -> None:
        for path in sorted(self.jobs_dir.glob("*.json")):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                job = JobRecord(**data)
                if job.status == "running":
                    job.status = "interrupted"
                    job.finished_at = job.finished_at or utc_now_iso()
                    job.updated_at = job.updated_at or job.finished_at
                    self._save_job(job)
                elif not job.updated_at:
                    job.updated_at = job.finished_at or job.started_at or job.created_at
                    self._save_job(job)
                self._jobs[job.job_id] = job
            except Exception:
                continue

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


__all__ = ["JobRecord", "SyncJobManager"]
