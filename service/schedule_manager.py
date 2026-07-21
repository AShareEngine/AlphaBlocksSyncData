#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Persistent schedule records for sync jobs."""

from __future__ import annotations

import json
import logging
import re
import threading
import uuid
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sync_data_system.service.job_manager import JobRecord, SyncJobManager, utc_now_iso
from sync_data_system.service.sync_config_manager import SyncConfigManager


SCHEDULE_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
TIME_RE = re.compile(r"^(\d{1,2}):(\d{2})$")
TARGET_TYPES = {"config", "task"}
FREQUENCIES = {"daily", "weekly", "interval"}
CONCURRENCY_POLICIES = {"skip", "replace", "allow"}
DEFAULT_WEEKDAYS = ["1", "2", "3", "4", "5"]
RUNNING_STATUSES = {"running", "cancelling"}
logger = logging.getLogger(__name__)


@dataclass
class ScheduleRecord:
    id: str
    name: str
    enabled: bool
    target_type: str
    target: str
    frequency: str
    time: str
    weekdays: list[str]
    interval_minutes: int
    timezone: str
    log_level: Optional[str]
    concurrency_policy: str
    retry_attempts: int
    next_run_at: str
    last_run_at: Optional[str] = None
    last_status: str = "pending"
    last_job_id: Optional[str] = None
    last_error: Optional[str] = None
    created_at: str = ""
    updated_at: str = ""


class SyncScheduleManager:
    def __init__(
        self,
        project_root: Path,
        job_manager: SyncJobManager,
        config_manager: SyncConfigManager,
        state_dir: Optional[Path] = None,
    ) -> None:
        self.project_root = Path(project_root).resolve()
        self.job_manager = job_manager
        self.config_manager = config_manager
        self.state_dir = (state_dir or job_manager.state_dir).resolve()
        self.schedules_dir = self.state_dir / "schedules"
        self.schedules_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._schedules: dict[str, ScheduleRecord] = {}
        self._scheduler_stop = threading.Event()
        self._scheduler_thread: threading.Thread | None = None
        self._load_existing_schedules()

    def list_schedules(
        self,
        *,
        enabled: Optional[bool] = None,
        target_type: Optional[str] = None,
    ) -> list[ScheduleRecord]:
        self._ensure_next_run_times()
        self._refresh_last_job_statuses()
        with self._lock:
            items = list(self._schedules.values())
        if enabled is not None:
            items = [item for item in items if item.enabled is enabled]
        if target_type:
            items = [item for item in items if item.target_type == target_type]
        return sorted(items, key=lambda item: item.created_at, reverse=True)

    def get_schedule(self, schedule_id: str) -> ScheduleRecord:
        self._ensure_next_run_time(schedule_id)
        self._refresh_last_job_status(schedule_id)
        with self._lock:
            if schedule_id not in self._schedules:
                raise KeyError(f"schedule not found: {schedule_id}")
            return self._schedules[schedule_id]

    def create_schedule(self, payload: dict[str, Any]) -> ScheduleRecord:
        now = utc_now_iso()
        schedule_id = self._new_schedule_id()
        schedule = self._normalize_payload(
            {
                **payload,
                "id": schedule_id,
                "created_at": now,
                "updated_at": now,
                "last_status": "pending",
                "last_run_at": None,
                "last_job_id": None,
                "last_error": None,
            }
        )
        with self._lock:
            self._schedules[schedule.id] = schedule
            self._save_schedule(schedule)
        return schedule

    def update_schedule(self, schedule_id: str, updates: dict[str, Any]) -> ScheduleRecord:
        with self._lock:
            if schedule_id not in self._schedules:
                raise KeyError(f"schedule not found: {schedule_id}")
            current = self._schedules[schedule_id]
        payload = {
            **asdict(current),
            **updates,
            "id": current.id,
            "created_at": current.created_at,
            "updated_at": utc_now_iso(),
        }
        schedule = self._normalize_payload(payload)
        with self._lock:
            self._schedules[schedule.id] = schedule
            self._save_schedule(schedule)
        return schedule

    def delete_schedule(self, schedule_id: str) -> ScheduleRecord:
        with self._lock:
            if schedule_id not in self._schedules:
                raise KeyError(f"schedule not found: {schedule_id}")
            schedule = self._schedules.pop(schedule_id)
            path = self._schedule_path(schedule_id)
            if path.exists():
                path.unlink()
        return schedule

    def delete_by_target(self, target_type: str, target: str) -> list[ScheduleRecord]:
        """Delete schedules that directly reference a removed business object."""
        with self._lock:
            schedule_ids = [
                item.id
                for item in self._schedules.values()
                if item.target_type == target_type and item.target == target
            ]
        return [self.delete_schedule(schedule_id) for schedule_id in schedule_ids]

    def set_enabled(self, schedule_id: str, enabled: bool) -> ScheduleRecord:
        with self._lock:
            if schedule_id not in self._schedules:
                raise KeyError(f"schedule not found: {schedule_id}")
            current = self._schedules[schedule_id]
        last_status = current.last_status
        if enabled and last_status == "paused":
            last_status = "pending"
        if not enabled:
            last_status = "paused"
        return self.update_schedule(
            schedule_id,
            {
                "enabled": enabled,
                "last_status": last_status,
            },
        )

    def run_schedule_now(self, schedule_id: str) -> tuple[ScheduleRecord, JobRecord]:
        schedule = self.get_schedule(schedule_id)
        job = self._start_schedule_job(schedule)

        with self._lock:
            current = self._schedules[schedule_id]
            current.last_run_at = job.started_at or utc_now_iso()
            current.last_status = job.status
            current.last_job_id = job.job_id
            current.last_error = job.error
            current.updated_at = utc_now_iso()
            current.next_run_at = self.compute_next_run_at(current) if current.enabled else ""
            self._save_schedule(current)
            return current, job

    def run_due_schedules(self, now: Optional[datetime] = None) -> list[tuple[ScheduleRecord, Optional[JobRecord]]]:
        """Run enabled schedules whose next_run_at is due.

        The list endpoint intentionally does not advance overdue plans. This
        method is the single place that consumes a due next_run_at by starting a
        job or recording why the due run could not be started.
        """
        now_utc = self._normalize_now(now)
        self._refresh_last_job_statuses()
        due_ids: list[str] = []
        with self._lock:
            for schedule in self._schedules.values():
                if not schedule.enabled:
                    continue
                next_run = self._parse_iso_datetime(schedule.next_run_at)
                if next_run is None:
                    schedule.next_run_at = self.compute_next_run_at(schedule, now=now_utc)
                    schedule.updated_at = utc_now_iso()
                    self._save_schedule(schedule)
                    continue
                if next_run <= now_utc:
                    due_ids.append(schedule.id)

        results: list[tuple[ScheduleRecord, Optional[JobRecord]]] = []
        for schedule_id in due_ids:
            results.append(self._run_due_schedule(schedule_id, now_utc))
        return results

    def start_scheduler(self, *, interval_seconds: float = 30.0) -> bool:
        with self._lock:
            if self._scheduler_thread is not None and self._scheduler_thread.is_alive():
                return False
            self._scheduler_stop.clear()
            self._scheduler_thread = threading.Thread(
                target=self._scheduler_loop,
                args=(max(float(interval_seconds), 1.0),),
                name="sync-schedule-runner",
                daemon=True,
            )
            self._scheduler_thread.start()
            return True

    def stop_scheduler(self, *, timeout: float = 5.0) -> None:
        self._scheduler_stop.set()
        thread = self._scheduler_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)

    def _scheduler_loop(self, interval_seconds: float) -> None:
        while not self._scheduler_stop.is_set():
            try:
                self.run_due_schedules()
            except Exception:
                logger.exception("scheduled sync polling failed")
            self._scheduler_stop.wait(interval_seconds)

    def _run_due_schedule(
        self,
        schedule_id: str,
        now_utc: datetime,
    ) -> tuple[ScheduleRecord, Optional[JobRecord]]:
        replace_job_id: Optional[str] = None
        with self._lock:
            schedule = self._schedules.get(schedule_id)
            if schedule is None:
                raise KeyError(f"schedule not found: {schedule_id}")
            if not schedule.enabled:
                return schedule, None
            next_run = self._parse_iso_datetime(schedule.next_run_at)
            if next_run is None or next_run > now_utc:
                return schedule, None
            if schedule.last_status in RUNNING_STATUSES:
                if schedule.concurrency_policy == "skip":
                    schedule.next_run_at = self.compute_next_run_at(schedule, now=now_utc)
                    schedule.updated_at = utc_now_iso()
                    self._save_schedule(schedule)
                    return schedule, None
                if schedule.concurrency_policy == "replace":
                    replace_job_id = schedule.last_job_id
            schedule_snapshot = ScheduleRecord(**asdict(schedule))

        if replace_job_id:
            try:
                self.job_manager.cancel_job(replace_job_id)
            except Exception as exc:
                logger.warning("failed to cancel previous scheduled job %s: %s", replace_job_id, exc)

        try:
            job = self._start_schedule_job(schedule_snapshot)
        except Exception as exc:
            if schedule_snapshot.concurrency_policy == "skip" and "another sync job is running" in str(exc):
                return self._skip_due_schedule(schedule_id, now_utc, reason=str(exc)), None
            return self._record_due_schedule_error(schedule_id, exc, now_utc), None

        with self._lock:
            current = self._schedules[schedule_id]
            current.last_run_at = job.started_at or utc_now_iso()
            current.last_status = job.status
            current.last_job_id = job.job_id
            current.last_error = job.error
            current.updated_at = utc_now_iso()
            current.next_run_at = self.compute_next_run_at(current, now=now_utc) if current.enabled else ""
            self._save_schedule(current)
            return current, job

    def _record_due_schedule_error(
        self,
        schedule_id: str,
        exc: Exception,
        now_utc: datetime,
    ) -> ScheduleRecord:
        with self._lock:
            current = self._schedules[schedule_id]
            current.last_run_at = utc_now_iso()
            current.last_status = "failed"
            current.last_error = str(exc)
            current.updated_at = utc_now_iso()
            current.next_run_at = self.compute_next_run_at(current, now=now_utc) if current.enabled else ""
            self._save_schedule(current)
            return current

    def _skip_due_schedule(
        self,
        schedule_id: str,
        now_utc: datetime,
        *,
        reason: str,
    ) -> ScheduleRecord:
        with self._lock:
            current = self._schedules[schedule_id]
            current.last_error = reason
            current.updated_at = utc_now_iso()
            current.next_run_at = self.compute_next_run_at(current, now=now_utc) if current.enabled else ""
            self._save_schedule(current)
            return current

    def _start_schedule_job(self, schedule: ScheduleRecord) -> JobRecord:
        if schedule.target_type == "config":
            config = self.config_manager.get_config(schedule.target)
            job = self.job_manager.create_task_batch_job(
                name=config["name"],
                tasks=config["tasks"],
                continue_on_error=config["continue_on_error"],
                log_level=schedule.log_level or config["log_level"],
                config_id=config["id"],
            )
            self.config_manager.mark_started(
                config["id"],
                job.job_id,
                started_at=job.started_at,
            )
            return job
        elif schedule.target_type == "task":
            known_tasks = {item["name"] for item in self.job_manager.list_registered_tasks()}
            if schedule.target not in known_tasks:
                raise ValueError(f"unknown registered task: {schedule.target}")
            return self.job_manager.create_registered_task_job(
                task=schedule.target,
                log_level=schedule.log_level,
            )
        raise ValueError(f"unsupported schedule target_type: {schedule.target_type}")

    def compute_next_run_at(
        self,
        schedule: ScheduleRecord,
        now: Optional[datetime] = None,
    ) -> str:
        if not schedule.enabled:
            return ""
        now_utc = now or datetime.now(timezone.utc)
        if now_utc.tzinfo is None:
            now_utc = now_utc.replace(tzinfo=timezone.utc)
        now_utc = now_utc.astimezone(timezone.utc).replace(microsecond=0)

        if schedule.frequency == "interval":
            return (now_utc + timedelta(minutes=schedule.interval_minutes)).isoformat()

        local_zone = self._resolve_timezone(schedule.timezone)
        now_local = now_utc.astimezone(local_zone)
        hours, minutes = self._parse_time(schedule.time)
        if schedule.frequency == "weekly":
            selected = {int(item) for item in schedule.weekdays}
            for offset in range(14):
                candidate = now_local + timedelta(days=offset)
                candidate = candidate.replace(hour=hours, minute=minutes, second=0, microsecond=0)
                if candidate.isoweekday() in selected and candidate > now_local:
                    return candidate.astimezone(timezone.utc).isoformat()
        candidate = now_local.replace(hour=hours, minute=minutes, second=0, microsecond=0)
        if candidate <= now_local:
            candidate = candidate + timedelta(days=1)
        return candidate.astimezone(timezone.utc).isoformat()

    def _normalize_payload(self, payload: dict[str, Any]) -> ScheduleRecord:
        name = self._clean_string(payload.get("name"))
        if not name:
            raise ValueError("schedule name is required")

        target_type = self._clean_string(payload.get("target_type") or "config")
        if target_type not in TARGET_TYPES:
            raise ValueError(f"target_type must be one of {sorted(TARGET_TYPES)}")

        target = self._clean_string(payload.get("target"))
        if not target:
            raise ValueError("schedule target is required")
        self._validate_target(target_type, target)

        frequency = self._clean_string(payload.get("frequency") or "daily")
        if frequency not in FREQUENCIES:
            raise ValueError(f"frequency must be one of {sorted(FREQUENCIES)}")

        time_value = self._clean_string(payload.get("time") or "18:00")
        if frequency != "interval":
            self._parse_time(time_value)

        weekdays = self._normalize_weekdays(payload.get("weekdays"))
        if frequency == "weekly" and not weekdays:
            raise ValueError("weekdays is required for weekly schedules")

        interval_minutes = self._coerce_int(payload.get("interval_minutes"), default=60)
        if frequency == "interval" and interval_minutes < 5:
            raise ValueError("interval_minutes must be at least 5")

        timezone_name = self._clean_string(payload.get("timezone") or "Asia/Shanghai")
        self._resolve_timezone(timezone_name)

        concurrency_policy = self._clean_string(payload.get("concurrency_policy") or "skip")
        if concurrency_policy not in CONCURRENCY_POLICIES:
            raise ValueError(f"concurrency_policy must be one of {sorted(CONCURRENCY_POLICIES)}")

        retry_attempts = max(0, min(self._coerce_int(payload.get("retry_attempts"), default=0), 10))
        enabled = self._coerce_bool(payload.get("enabled", True))
        schedule_id = self._clean_string(payload.get("id"))
        if not schedule_id or not SCHEDULE_ID_RE.match(schedule_id):
            raise ValueError("schedule id is invalid")

        created_at = self._clean_string(payload.get("created_at") or utc_now_iso())
        updated_at = self._clean_string(payload.get("updated_at") or created_at)
        last_status = self._clean_string(payload.get("last_status") or ("pending" if enabled else "paused"))
        if not enabled:
            last_status = "paused"
        elif last_status == "paused":
            last_status = "pending"

        schedule = ScheduleRecord(
            id=schedule_id,
            name=name,
            enabled=enabled,
            target_type=target_type,
            target=target,
            frequency=frequency,
            time=time_value,
            weekdays=weekdays or list(DEFAULT_WEEKDAYS),
            interval_minutes=interval_minutes,
            timezone=timezone_name,
            log_level=self._clean_optional_string(payload.get("log_level")) or "INFO",
            concurrency_policy=concurrency_policy,
            retry_attempts=retry_attempts,
            next_run_at="",
            last_run_at=self._clean_optional_string(payload.get("last_run_at")),
            last_status=last_status,
            last_job_id=self._clean_optional_string(payload.get("last_job_id")),
            last_error=self._clean_optional_string(payload.get("last_error")),
            created_at=created_at,
            updated_at=updated_at,
        )
        schedule.next_run_at = self.compute_next_run_at(schedule) if schedule.enabled else ""
        return schedule

    def _validate_target(self, target_type: str, target: str) -> None:
        if target_type == "config":
            try:
                self.config_manager.get_config(target)
            except KeyError:
                raise ValueError(f"unknown sync config: {target}")
            return
        known_tasks = {item["name"] for item in self.job_manager.list_registered_tasks()}
        if target not in known_tasks:
            raise ValueError(f"unknown registered task: {target}")

    def _refresh_last_job_statuses(self) -> None:
        with self._lock:
            schedule_ids = list(self._schedules.keys())
        for schedule_id in schedule_ids:
            self._refresh_last_job_status(schedule_id)

    def _ensure_next_run_times(self) -> None:
        with self._lock:
            schedule_ids = list(self._schedules.keys())
        for schedule_id in schedule_ids:
            self._ensure_next_run_time(schedule_id)

    def _ensure_next_run_time(self, schedule_id: str) -> None:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        with self._lock:
            schedule = self._schedules.get(schedule_id)
            if schedule is None or not schedule.enabled:
                return
            if schedule.last_status == "paused":
                schedule.last_status = "pending"
            next_run = self._parse_iso_datetime(schedule.next_run_at)
            if next_run is not None:
                return
            schedule.next_run_at = self.compute_next_run_at(schedule, now=now)
            schedule.updated_at = utc_now_iso()
            self._save_schedule(schedule)

    def _refresh_last_job_status(self, schedule_id: str) -> None:
        with self._lock:
            schedule = self._schedules.get(schedule_id)
        if schedule is None or not schedule.last_job_id or schedule.last_status not in RUNNING_STATUSES:
            return
        try:
            job = self.job_manager.get_job(schedule.last_job_id)
        except KeyError:
            return
        with self._lock:
            current = self._schedules.get(schedule_id)
            if current is None:
                return
            current.last_status = job.status
            current.last_error = job.error
            if job.started_at:
                current.last_run_at = job.started_at
            current.updated_at = utc_now_iso()
            self._save_schedule(current)

    def _load_existing_schedules(self) -> None:
        field_names = {field.name for field in fields(ScheduleRecord)}
        for path in sorted(self.schedules_dir.glob("*.json")):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                record_payload = {key: value for key, value in data.items() if key in field_names}
                schedule = ScheduleRecord(**record_payload)
                if not SCHEDULE_ID_RE.match(schedule.id):
                    continue
                self._schedules[schedule.id] = schedule
            except Exception:
                continue

    def _save_schedule(self, schedule: ScheduleRecord) -> None:
        path = self._schedule_path(schedule.id)
        path.write_text(json.dumps(asdict(schedule), ensure_ascii=False, indent=2), encoding="utf-8")

    def _schedule_path(self, schedule_id: str) -> Path:
        clean_id = Path(schedule_id).name
        return self.schedules_dir / f"{clean_id}.json"

    def _new_schedule_id(self) -> str:
        return f"schedule_{uuid.uuid4().hex[:12]}"

    @staticmethod
    def _clean_string(value: Any) -> str:
        return str(value or "").strip()

    @staticmethod
    def _clean_optional_string(value: Any) -> Optional[str]:
        text = str(value or "").strip()
        return text or None

    @staticmethod
    def _coerce_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() not in {"0", "false", "no", "off"}
        return bool(value)

    @staticmethod
    def _coerce_int(value: Any, *, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _parse_time(value: str) -> tuple[int, int]:
        match = TIME_RE.match(str(value or ""))
        if not match:
            raise ValueError("time must use HH:mm format")
        hours = int(match.group(1))
        minutes = int(match.group(2))
        if hours > 23 or minutes > 59:
            raise ValueError("time must use HH:mm format")
        return hours, minutes

    @staticmethod
    def _normalize_weekdays(value: Any) -> list[str]:
        if value is None:
            return list(DEFAULT_WEEKDAYS)
        if not isinstance(value, list):
            value = [value]
        selected: set[str] = set()
        for item in value:
            text = str(item).strip()
            if text in {"1", "2", "3", "4", "5", "6", "7"}:
                selected.add(text)
        return sorted(selected, key=int)

    @staticmethod
    def _resolve_timezone(value: str):
        try:
            return ZoneInfo(value)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown timezone: {value}") from exc

    @staticmethod
    def _parse_iso_datetime(value: Any) -> Optional[datetime]:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).replace(microsecond=0)

    @staticmethod
    def _normalize_now(value: Optional[datetime]) -> datetime:
        current = value or datetime.now(timezone.utc)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        return current.astimezone(timezone.utc).replace(microsecond=0)


__all__ = ["ScheduleRecord", "SyncScheduleManager"]
