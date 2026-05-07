#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Background job manager for run_sync.py."""

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

from sync_data_system.config_paths import resolve_config_candidate, resolve_sync_plan_root
from sync_data_system.service.task_registry import TASK_REGISTRY


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


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


class SyncJobManager:
    def __init__(self, project_root: Path, state_dir: Optional[Path] = None) -> None:
        self.project_root = Path(project_root).resolve()
        self.config_root = resolve_sync_plan_root(self.project_root)
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

    def create_config_job(
        self,
        config_path: str,
        log_level: Optional[str] = None,
        runtime_path: Optional[str] = None,
    ) -> JobRecord:
        self._ensure_no_running_jobs()
        resolved_config = self._resolve_config_path(config_path)
        relative_config_path = str(resolved_config.relative_to(self.config_root))
        command = [sys.executable, str(self.project_root / "run_sync.py"), "--config", str(resolved_config)]
        if runtime_path:
            command.extend(["--runtime-path", runtime_path])
        if log_level:
            command.extend(["--log-level", str(log_level)])
        return self._start_job(
            kind="config",
            command=command,
            config_path=relative_config_path,
            task=None,
            request_payload={
                "config": relative_config_path,
                "log_level": log_level,
                "runtime_path": runtime_path,
            },
        )

    def create_task_job(
        self,
        *,
        task: str,
        codes: list[str] | None = None,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
        limit: int = 0,
        force: bool = False,
        resume: bool = False,
        log_level: Optional[str] = None,
        runtime_path: Optional[str] = None,
    ) -> JobRecord:
        self._ensure_no_running_jobs()
        command = [sys.executable, str(self.project_root / "run_sync.py"), task]
        if runtime_path:
            command.extend(["--runtime-path", runtime_path])
        code_items = [item.strip() for item in (codes or []) if str(item).strip()]
        if code_items:
            command.extend(["--codes", ",".join(code_items)])
        if begin_date is not None:
            command.extend(["--begin-date", str(begin_date)])
        if end_date is not None:
            command.extend(["--end-date", str(end_date)])
        if limit:
            command.extend(["--limit", str(limit)])
        if force:
            command.append("--force")
        if resume:
            command.append("--resume")
        if log_level:
            command.extend(["--log-level", str(log_level)])
        return self._start_job(
            kind="task",
            command=command,
            config_path=None,
            task=task,
            request_payload={
                "task": task,
                "codes": code_items,
                "begin_date": begin_date,
                "end_date": end_date,
                "limit": limit,
                "force": force,
                "resume": resume,
                "log_level": log_level,
                "runtime_path": runtime_path,
            },
        )

    def cancel_job(self, job_id: str) -> JobRecord:
        with self._lock:
            process = self._processes.get(job_id)
            if process is None:
                return self.get_job(job_id)
            job = self._jobs.get(job_id)
            if job is not None and job.status == "running":
                job.status = "cancelling"
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

    def list_configs(self) -> list[str]:
        paths: dict[str, Path] = {}
        for root in (self.config_root, self.project_root):
            if not root.exists():
                continue
            for path in root.glob("run_sync*.toml"):
                if path.is_file():
                    paths.setdefault(path.name, path)
        return sorted(paths)

    def list_tasks(self) -> list[str]:
        names = set()
        try:
            from sync_data_system.run_sync import TASK_CHOICES

            names.update(TASK_CHOICES)
        except Exception:
            pass
        names.update(task.name for task in TASK_REGISTRY.list_tasks())
        return sorted(names)

    def list_registered_tasks(self) -> list[dict[str, str | None]]:
        return TASK_REGISTRY.list_task_metadata()

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
    ) -> JobRecord:
        job_id = uuid.uuid4().hex[:12]
        log_path = self.logs_dir / f"{job_id}.log"
        log_fp = log_path.open("a", encoding="utf-8")
        process = subprocess.Popen(
            command,
            cwd=str(self.project_root),
            env=self._build_subprocess_env(),
            stdout=log_fp,
            stderr=subprocess.STDOUT,
            text=True,
        )
        job = JobRecord(
            job_id=job_id,
            kind=kind,
            status="running",
            created_at=utc_now_iso(),
            started_at=utc_now_iso(),
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
            sys.executable,
            str(self.project_root / "scripts" / "run_registered_task.py"),
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
            if job.status == "cancelling":
                job.status = "cancelled"
            elif job.status != "cancelled":
                job.status = "success" if return_code == 0 else "failed"
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
            return
        with self._lock:
            job.return_code = return_code
            job.finished_at = utc_now_iso()
            if job.status != "cancelled":
                job.status = "success" if return_code == 0 else "failed"
            self._processes.pop(job_id, None)
            self._save_job(job)

    def _resolve_config_path(self, config_path: str) -> Path:
        candidate = resolve_config_candidate(config_path, project_root=self.project_root)
        if not candidate.is_file():
            raise FileNotFoundError(f"config not found: {candidate}")
        try:
            candidate.relative_to(self.config_root)
        except ValueError as exc:
            raise ValueError(f"config must be inside config root: {candidate}") from exc
        return candidate

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

    def _load_existing_jobs(self) -> None:
        for path in sorted(self.jobs_dir.glob("*.json")):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                job = JobRecord(**data)
                if job.status == "running":
                    job.status = "interrupted"
                    job.finished_at = job.finished_at or utc_now_iso()
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


__all__ = ["JobRecord", "SyncJobManager"]
