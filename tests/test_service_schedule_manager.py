#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

from sync_data_system.service.job_manager import JobRecord
from sync_data_system.service.schedule_manager import SyncScheduleManager


def _fake_job(job_id: str = "job1", status: str = "running") -> JobRecord:
    return JobRecord(
        job_id=job_id,
        kind="sync_config",
        status=status,
        created_at="2026-01-01T00:00:00+00:00",
        started_at="2026-01-01T00:00:00+00:00",
        finished_at=None,
        cwd="/tmp",
        command=["python", "scripts/run_task_batch.py"],
        log_path="/tmp/job1.log",
        config_path=None,
        task=None,
        source=None,
        target=None,
        pid=123,
        return_code=None,
        error=None,
        config_id="sync_config_daily",
        config_name="每日基础数据",
    )


def _fake_config() -> dict:
    return {
        "id": "sync_config_daily",
        "name": "每日基础数据",
        "tasks": [{"id": "task1", "name": "amazingdata.daily_kline", "enabled": True}],
        "continue_on_error": True,
        "log_level": "INFO",
    }


def _managers(root: Path):
    job_manager = Mock()
    job_manager.state_dir = root / ".service_state"
    job_manager.list_registered_tasks.return_value = [{"name": "amazingdata.daily_kline"}]
    config_manager = Mock()
    config_manager.get_config.side_effect = lambda config_id: (
        _fake_config() if config_id == "sync_config_daily" else (_ for _ in ()).throw(KeyError(config_id))
    )
    manager = SyncScheduleManager(
        root,
        job_manager,
        config_manager,
        state_dir=job_manager.state_dir,
    )
    return manager, job_manager, config_manager


class SyncScheduleManagerTest(unittest.TestCase):
    def test_create_schedule_persists_business_config_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            manager, job_manager, config_manager = _managers(root)
            schedule = manager.create_schedule(
                {
                    "name": "每日基础数据",
                    "target_type": "config",
                    "target": "sync_config_daily",
                    "frequency": "daily",
                    "time": "18:30",
                    "timezone": "Asia/Shanghai",
                }
            )

            self.assertEqual(schedule.target, "sync_config_daily")
            self.assertTrue(schedule.next_run_at)
            self.assertTrue((job_manager.state_dir / "schedules" / f"{schedule.id}.json").is_file())

            reloaded = SyncScheduleManager(root, job_manager, config_manager, state_dir=job_manager.state_dir)
            self.assertEqual(reloaded.get_schedule(schedule.id).name, "每日基础数据")

    def test_compute_next_run_at_uses_configured_timezone(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manager, _, _ = _managers(Path(tmpdir))
            schedule = manager.create_schedule(
                {
                    "name": "每日基础数据",
                    "target_type": "config",
                    "target": "sync_config_daily",
                    "frequency": "daily",
                    "time": "18:30",
                    "timezone": "Asia/Shanghai",
                }
            )
            next_run = manager.compute_next_run_at(
                schedule,
                now=datetime(2026, 5, 16, 10, 0, tzinfo=timezone.utc),
            )
            self.assertEqual(next_run, "2026-05-16T10:30:00+00:00")

    def test_create_schedule_rejects_unknown_targets(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manager, _, _ = _managers(Path(tmpdir))
            with self.assertRaisesRegex(ValueError, "unknown sync config"):
                manager.create_schedule(
                    {"name": "未知配置", "target_type": "config", "target": "missing"}
                )
            with self.assertRaisesRegex(ValueError, "unknown registered task"):
                manager.create_schedule(
                    {"name": "未知任务", "target_type": "task", "target": "amazingdata.missing"}
                )

    def test_run_schedule_now_uses_config_snapshot_and_marks_started(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manager, job_manager, config_manager = _managers(Path(tmpdir))
            job_manager.create_task_batch_job.return_value = _fake_job()
            schedule = manager.create_schedule(
                {"name": "日线同步", "target_type": "config", "target": "sync_config_daily"}
            )

            updated, job = manager.run_schedule_now(schedule.id)

            self.assertEqual(job.job_id, "job1")
            self.assertEqual(updated.last_job_id, "job1")
            job_manager.create_task_batch_job.assert_called_once_with(
                name="每日基础数据",
                tasks=_fake_config()["tasks"],
                continue_on_error=True,
                log_level="INFO",
                config_id="sync_config_daily",
            )
            config_manager.mark_started.assert_called_once_with(
                "sync_config_daily", "job1", started_at="2026-01-01T00:00:00+00:00"
            )

    def test_run_due_schedules_skips_overlap_without_marking_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manager, job_manager, _ = _managers(Path(tmpdir))
            job_manager.create_task_batch_job.side_effect = RuntimeError("another sync job is running job_id=abc")
            schedule = manager.create_schedule(
                {
                    "name": "每日基础数据",
                    "target_type": "config",
                    "target": "sync_config_daily",
                    "frequency": "daily",
                    "time": "18:30",
                    "timezone": "Asia/Shanghai",
                    "concurrency_policy": "skip",
                }
            )
            schedule.next_run_at = "2026-05-16T10:30:00+00:00"
            manager._save_schedule(schedule)

            updated, job = manager.run_due_schedules(
                now=datetime(2026, 5, 16, 10, 31, tzinfo=timezone.utc)
            )[0]

            self.assertIsNone(job)
            self.assertEqual(updated.last_status, "pending")
            self.assertIn("another sync job is running", updated.last_error or "")

    def test_delete_by_target_cascades_config_schedules(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manager, _, _ = _managers(Path(tmpdir))
            schedule = manager.create_schedule(
                {"name": "每日基础数据", "target_type": "config", "target": "sync_config_daily"}
            )
            deleted = manager.delete_by_target("config", "sync_config_daily")
            self.assertEqual([item.id for item in deleted], [schedule.id])
            self.assertEqual(manager.list_schedules(), [])


if __name__ == "__main__":
    unittest.main()
