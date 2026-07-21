#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""One-to-one schedule settings owned by sync configurations."""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

from sync_data_system.service.job_manager import JobRecord, SyncJobManager, utc_now_iso
from sync_data_system.service.sync_config_manager import SyncConfigManager


FREQUENCIES = {"daily", "weekly", "interval"}
DEFAULT_WEEKDAYS = ["1", "2", "3", "4", "5"]
SHANGHAI_ZONE = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)


class SyncScheduleManager:
    """Manage the schedule settings embedded in each sync config.

    A schedule is not independently created or deleted. Every config owns one
    disabled-by-default schedule and all executions enter SyncJobManager's
    persistent FIFO queue.
    """

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
        self.legacy_schedules_dir = self.state_dir / "schedules"
        self._lock = threading.RLock()
        self._scheduler_stop = threading.Event()
        self._scheduler_thread: Optional[threading.Thread] = None
        self._migrate_legacy_schedules()

    def get_schedule(self, config_id: str) -> dict[str, Any]:
        return self.config_manager.get_schedule(config_id)

    def update_schedule(self, config_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        current = self.get_schedule(config_id)
        if not current["enabled"]:
            raise ValueError("enable the schedule before editing its settings")
        allowed = {"frequency", "time", "weekdays", "interval_minutes"}
        unexpected = sorted(set(updates) - allowed)
        if unexpected:
            raise ValueError(f"unsupported schedule settings: {unexpected}")
        candidate = {**current, **updates, "enabled": True}
        next_run_at = self.compute_next_run_at(candidate)
        return self.config_manager.update_schedule(
            config_id,
            {**updates, "next_run_at": next_run_at},
        )

    def set_enabled(self, config_id: str, enabled: bool) -> dict[str, Any]:
        current = self.get_schedule(config_id)
        desired = bool(enabled)
        if current["enabled"] == desired:
            return current
        if desired:
            next_run_at = self.compute_next_run_at({**current, "enabled": True})
            return self.config_manager.update_schedule(
                config_id,
                {"enabled": True, "next_run_at": next_run_at},
            )
        self.job_manager.cancel_pending_jobs(config_id=config_id, trigger="schedule")
        return self.config_manager.update_schedule(
            config_id,
            {"enabled": False, "next_run_at": ""},
        )

    def run_due_schedules(
        self,
        now: Optional[datetime] = None,
    ) -> list[tuple[dict[str, Any], Optional[JobRecord]]]:
        now_utc = self._normalize_now(now)
        due: list[dict[str, Any]] = []
        for config in self.config_manager.list_configs():
            schedule = config["schedule"]
            if not schedule["enabled"]:
                continue
            next_run = self._parse_iso_datetime(schedule.get("next_run_at"))
            if next_run is None:
                self.config_manager.update_schedule(
                    config["id"],
                    {"next_run_at": self.compute_next_run_at(schedule, now=now_utc)},
                )
                continue
            if next_run <= now_utc:
                due.append(config)

        due.sort(
            key=lambda item: (
                str(item["schedule"].get("next_run_at") or ""),
                str(item["id"]),
            )
        )
        return [self._enqueue_due_config(config, now_utc) for config in due]

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

    def compute_next_run_at(
        self,
        schedule: dict[str, Any],
        now: Optional[datetime] = None,
    ) -> str:
        if not schedule.get("enabled"):
            return ""
        now_utc = self._normalize_now(now)
        frequency = str(schedule.get("frequency") or "daily")
        if frequency not in FREQUENCIES:
            raise ValueError(f"frequency must be one of {sorted(FREQUENCIES)}")
        if frequency == "interval":
            interval_minutes = int(schedule.get("interval_minutes") or 60)
            if interval_minutes < 5:
                raise ValueError("interval_minutes must be at least 5")
            return (now_utc + timedelta(minutes=interval_minutes)).isoformat()

        now_local = now_utc.astimezone(SHANGHAI_ZONE)
        hours, minutes = self._parse_time(str(schedule.get("time") or "18:00"))
        if frequency == "weekly":
            selected = {int(item) for item in schedule.get("weekdays") or DEFAULT_WEEKDAYS}
            for offset in range(14):
                candidate = now_local + timedelta(days=offset)
                candidate = candidate.replace(hour=hours, minute=minutes, second=0, microsecond=0)
                if candidate.isoweekday() in selected and candidate > now_local:
                    return candidate.astimezone(timezone.utc).isoformat()
            raise ValueError("unable to calculate next weekly schedule")

        candidate = now_local.replace(hour=hours, minute=minutes, second=0, microsecond=0)
        if candidate <= now_local:
            candidate += timedelta(days=1)
        return candidate.astimezone(timezone.utc).isoformat()

    def _scheduler_loop(self, interval_seconds: float) -> None:
        while not self._scheduler_stop.is_set():
            try:
                self.run_due_schedules()
            except Exception:
                logger.exception("scheduled sync polling failed")
            self._scheduler_stop.wait(interval_seconds)

    def _enqueue_due_config(
        self,
        config: dict[str, Any],
        now_utc: datetime,
    ) -> tuple[dict[str, Any], Optional[JobRecord]]:
        config_id = config["id"]
        job: Optional[JobRecord] = None
        trigger_result = "queued"
        trigger_message = ""
        try:
            active = self.job_manager.find_active_config_job(config_id)
            if active is not None:
                trigger_result = "coalesced"
                trigger_message = f"coalesced with active job {active.job_id}"
            else:
                job = self.job_manager.create_task_batch_job(
                    name=config["name"],
                    tasks=config["tasks"],
                    continue_on_error=config["continue_on_error"],
                    log_level=config["log_level"],
                    config_id=config_id,
                    trigger="schedule",
                )
                self.config_manager.mark_started(
                    config_id,
                    job.job_id,
                    started_at=job.started_at or job.created_at,
                )
                if job.status in {"failed", "cancelled", "interrupted"}:
                    trigger_result = "error"
                    trigger_message = job.error or f"job ended with status {job.status}"
                else:
                    trigger_result = "queued" if job.status == "queued" else "started"
                    trigger_message = f"job {job.job_id} {trigger_result}"
        except Exception as exc:
            active = self.job_manager.find_active_config_job(config_id)
            if active is not None:
                trigger_result = "coalesced"
                trigger_message = f"coalesced with active job {active.job_id}"
            else:
                trigger_result = "error"
                trigger_message = str(exc)

        schedule = self.config_manager.update_schedule(
            config_id,
            {
                "next_run_at": self.compute_next_run_at(config["schedule"], now=now_utc),
                "last_trigger_at": utc_now_iso(),
                "last_trigger_result": trigger_result,
                "last_trigger_message": trigger_message or None,
            },
        )
        return schedule, job

    def _migrate_legacy_schedules(self) -> None:
        if not self.legacy_schedules_dir.exists():
            return
        migrated_targets: set[str] = set()
        for path in sorted(self.legacy_schedules_dir.glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                target = str(payload.get("target") or "").strip()
                if payload.get("target_type") == "config" and target and target not in migrated_targets:
                    self.config_manager.get_config(target)
                    enabled = self._coerce_bool(payload.get("enabled"), default=False)
                    schedule_updates = {
                        "enabled": enabled,
                        "frequency": payload.get("frequency") or "daily",
                        "time": payload.get("time") or "18:00",
                        "weekdays": payload.get("weekdays") or list(DEFAULT_WEEKDAYS),
                        "interval_minutes": payload.get("interval_minutes") or 60,
                        "next_run_at": payload.get("next_run_at") if enabled else "",
                        "last_trigger_at": payload.get("last_run_at"),
                        "last_trigger_result": "started" if payload.get("last_job_id") else None,
                        "last_trigger_message": None,
                    }
                    self.config_manager.update_schedule(target, schedule_updates)
                    migrated_targets.add(target)
            except Exception:
                logger.warning("discarding invalid legacy schedule %s", path, exc_info=True)
            finally:
                try:
                    path.unlink()
                except OSError:
                    pass

    @staticmethod
    def _parse_time(value: str) -> tuple[int, int]:
        parts = value.split(":", 1)
        if len(parts) != 2:
            raise ValueError("time must use HH:mm")
        try:
            hours, minutes = int(parts[0]), int(parts[1])
        except ValueError as exc:
            raise ValueError("time must use HH:mm") from exc
        if not (0 <= hours <= 23 and 0 <= minutes <= 59):
            raise ValueError("time must use HH:mm")
        return hours, minutes

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
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _normalize_now(value: Optional[datetime]) -> datetime:
        current = value or datetime.now(timezone.utc)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        return current.astimezone(timezone.utc).replace(microsecond=0)

    @staticmethod
    def _coerce_bool(value: Any, *, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() not in {"0", "false", "no", "off"}


__all__ = ["DEFAULT_WEEKDAYS", "FREQUENCIES", "SyncScheduleManager"]
