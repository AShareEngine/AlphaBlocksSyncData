#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

from sync_data_system.service.job_manager import JobRecord
from sync_data_system.service.schedule_manager import SyncScheduleManager
from sync_data_system.service.sync_config_manager import SyncConfigManager


def _fake_job(job_id: str = "job1", status: str = "queued") -> JobRecord:
    return JobRecord(
        job_id=job_id,
        kind="sync_config",
        status=status,
        created_at="2026-01-01T00:00:00+00:00",
        started_at="2026-01-01T00:00:00+00:00" if status == "running" else None,
        finished_at=None,
        cwd="/tmp",
        command=["python", "scripts/run_task_batch.py"],
        log_path="/tmp/job1.log",
        config_id="sync_config_daily",
        config_name="每日基础数据",
        trigger="schedule",
    )


def _config_payload(name: str = "每日基础数据") -> dict:
    return {
        "name": name,
        "tasks": [{"id": "task1", "name": "amazingdata.daily_kline", "enabled": True}],
        "continue_on_error": True,
        "log_level": "INFO",
    }


def _managers(root: Path):
    state_dir = root / ".service_state"
    config_manager = SyncConfigManager(root, state_dir=state_dir)
    config_manager.create_config(_config_payload(), config_id="sync_config_daily")
    job_manager = Mock()
    job_manager.state_dir = state_dir
    job_manager.find_active_config_job.return_value = None
    job_manager.cancel_pending_jobs.return_value = []
    manager = SyncScheduleManager(root, job_manager, config_manager, state_dir=state_dir)
    return manager, job_manager, config_manager


class SyncScheduleManagerTest(unittest.TestCase):
    def test_config_always_has_disabled_default_schedule(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            manager, _, config_manager = _managers(root)

            schedule = manager.get_schedule("sync_config_daily")

            self.assertFalse(schedule["enabled"])
            self.assertEqual(schedule["frequency"], "daily")
            self.assertEqual(schedule["time"], "18:00")
            stored = json.loads(
                (config_manager.configs_dir / "sync_config_daily.json").read_text(encoding="utf-8")
            )
            self.assertEqual(stored["schedule"], schedule)

    def test_compute_next_run_at_is_fixed_to_shanghai_timezone(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manager, _, _ = _managers(Path(tmpdir))
            schedule = {
                **manager.get_schedule("sync_config_daily"),
                "enabled": True,
                "frequency": "daily",
                "time": "18:30",
            }

            next_run = manager.compute_next_run_at(
                schedule,
                now=datetime(2026, 5, 16, 10, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(next_run, "2026-05-16T10:30:00+00:00")

    def test_disabled_schedule_cannot_be_edited(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manager, _, _ = _managers(Path(tmpdir))
            with self.assertRaisesRegex(ValueError, "enable the schedule"):
                manager.update_schedule("sync_config_daily", {"time": "19:00"})

    def test_switch_enable_and_disable_updates_schedule_and_cancels_pending(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manager, job_manager, _ = _managers(Path(tmpdir))

            enabled = manager.set_enabled("sync_config_daily", True)
            disabled = manager.set_enabled("sync_config_daily", False)

            self.assertTrue(enabled["next_run_at"])
            self.assertFalse(disabled["enabled"])
            self.assertEqual(disabled["next_run_at"], "")
            job_manager.cancel_pending_jobs.assert_called_once_with(
                config_id="sync_config_daily",
                trigger="schedule",
            )

    def test_due_schedule_enters_fifo_queue_and_records_trigger(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manager, job_manager, config_manager = _managers(Path(tmpdir))
            job_manager.create_task_batch_job.return_value = _fake_job()
            config_manager.update_schedule(
                "sync_config_daily",
                {
                    "enabled": True,
                    "next_run_at": "2026-05-16T10:30:00+00:00",
                },
            )

            schedule, job = manager.run_due_schedules(
                now=datetime(2026, 5, 16, 10, 31, tzinfo=timezone.utc)
            )[0]

            self.assertEqual(job.job_id, "job1")
            self.assertEqual(schedule["last_trigger_result"], "queued")
            self.assertGreater(schedule["next_run_at"], "2026-05-16T10:31:00+00:00")
            self.assertEqual(config_manager.get_config("sync_config_daily")["last_job_id"], "job1")
            job_manager.create_task_batch_job.assert_called_once_with(
                name="每日基础数据",
                tasks=config_manager.get_config("sync_config_daily")["tasks"],
                continue_on_error=True,
                log_level="INFO",
                config_id="sync_config_daily",
                trigger="schedule",
            )

    def test_due_schedule_coalesces_when_same_config_is_active(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manager, job_manager, config_manager = _managers(Path(tmpdir))
            job_manager.find_active_config_job.return_value = _fake_job("active")
            config_manager.update_schedule(
                "sync_config_daily",
                {"enabled": True, "next_run_at": "2026-05-16T10:30:00+00:00"},
            )

            schedule, job = manager.run_due_schedules(
                now=datetime(2026, 5, 16, 10, 31, tzinfo=timezone.utc)
            )[0]

            self.assertIsNone(job)
            self.assertEqual(schedule["last_trigger_result"], "coalesced")
            job_manager.create_task_batch_job.assert_not_called()

    def test_legacy_config_schedule_is_migrated_and_old_file_removed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state_dir = root / ".service_state"
            config_manager = SyncConfigManager(root, state_dir=state_dir)
            config_manager.create_config(_config_payload(), config_id="sync_config_daily")
            schedules_dir = state_dir / "schedules"
            schedules_dir.mkdir(parents=True)
            legacy_path = schedules_dir / "schedule_old.json"
            legacy_path.write_text(
                json.dumps(
                    {
                        "id": "schedule_old",
                        "name": "旧计划",
                        "enabled": True,
                        "target_type": "config",
                        "target": "sync_config_daily",
                        "frequency": "daily",
                        "time": "19:30",
                        "weekdays": ["1", "2", "3", "4", "5"],
                        "interval_minutes": 60,
                        "next_run_at": "2026-05-16T11:30:00+00:00",
                    }
                ),
                encoding="utf-8",
            )
            job_manager = Mock(state_dir=state_dir)

            manager = SyncScheduleManager(root, job_manager, config_manager, state_dir=state_dir)

            schedule = manager.get_schedule("sync_config_daily")
            self.assertTrue(schedule["enabled"])
            self.assertEqual(schedule["time"], "19:30")
            self.assertFalse(legacy_path.exists())


if __name__ == "__main__":
    unittest.main()
