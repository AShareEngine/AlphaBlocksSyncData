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
        kind="registered_task",
        status=status,
        created_at="2026-01-01T00:00:00+00:00",
        started_at="2026-01-01T00:00:00+00:00",
        finished_at=None,
        cwd="/tmp",
        command=["python", "scripts/run_provider_sync.py"],
        log_path="/tmp/job1.log",
        config_path=None,
        task="amazingdata.daily_kline",
        source="amazingdata",
        target="ad_market_kline_daily",
        pid=123,
        return_code=None,
        error=None,
        request_payload={"name": "amazingdata.daily_kline"},
    )


class SyncScheduleManagerTest(unittest.TestCase):
    def test_create_schedule_persists_record_and_computes_next_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            job_manager = Mock()
            job_manager.state_dir = root / ".service_state"
            job_manager.list_configs.return_value = ["run_sync.daily.toml"]
            job_manager.list_registered_tasks.return_value = []
            manager = SyncScheduleManager(root, job_manager, state_dir=job_manager.state_dir)

            schedule = manager.create_schedule(
                {
                    "name": "每日基础数据",
                    "target_type": "config",
                    "target": "run_sync.daily.toml",
                    "frequency": "daily",
                    "time": "18:30",
                    "timezone": "Asia/Shanghai",
                }
            )

            self.assertTrue(schedule.id.startswith("schedule_"))
            self.assertEqual(schedule.target, "run_sync.daily.toml")
            self.assertTrue(schedule.next_run_at)
            self.assertTrue((job_manager.state_dir / "schedules" / f"{schedule.id}.json").is_file())

            reloaded = SyncScheduleManager(root, job_manager, state_dir=job_manager.state_dir)
            self.assertEqual(reloaded.get_schedule(schedule.id).name, "每日基础数据")

    def test_compute_next_run_at_uses_configured_timezone(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            job_manager = Mock()
            job_manager.state_dir = root / ".service_state"
            job_manager.list_configs.return_value = ["run_sync.daily.toml"]
            job_manager.list_registered_tasks.return_value = []
            manager = SyncScheduleManager(root, job_manager, state_dir=job_manager.state_dir)
            schedule = manager.create_schedule(
                {
                    "name": "每日基础数据",
                    "target_type": "config",
                    "target": "run_sync.daily.toml",
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

    def test_create_schedule_rejects_unknown_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            job_manager = Mock()
            job_manager.state_dir = root / ".service_state"
            job_manager.list_configs.return_value = []
            job_manager.list_registered_tasks.return_value = [{"name": "amazingdata.daily_kline"}]
            manager = SyncScheduleManager(root, job_manager, state_dir=job_manager.state_dir)

            with self.assertRaisesRegex(ValueError, "unknown registered task"):
                manager.create_schedule(
                    {
                        "name": "未知任务",
                        "target_type": "task",
                        "target": "amazingdata.missing",
                    }
                )

    def test_run_schedule_now_starts_task_job_and_updates_record(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            job_manager = Mock()
            job_manager.state_dir = root / ".service_state"
            job_manager.list_configs.return_value = []
            job_manager.list_registered_tasks.return_value = [{"name": "amazingdata.daily_kline"}]
            job_manager.create_registered_task_job.return_value = _fake_job()
            manager = SyncScheduleManager(root, job_manager, state_dir=job_manager.state_dir)
            schedule = manager.create_schedule(
                {
                    "name": "日线同步",
                    "target_type": "task",
                    "target": "amazingdata.daily_kline",
                    "frequency": "interval",
                    "interval_minutes": 30,
                }
            )

            updated, job = manager.run_schedule_now(schedule.id)

            self.assertEqual(job.job_id, "job1")
            self.assertEqual(updated.last_job_id, "job1")
            self.assertEqual(updated.last_status, "running")
            job_manager.create_registered_task_job.assert_called_once_with(
                task="amazingdata.daily_kline",
                log_level="INFO",
            )

    def test_list_schedules_advances_stale_next_run_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            job_manager = Mock()
            job_manager.state_dir = root / ".service_state"
            job_manager.list_configs.return_value = ["run_sync.daily.toml"]
            job_manager.list_registered_tasks.return_value = []
            manager = SyncScheduleManager(root, job_manager, state_dir=job_manager.state_dir)
            schedule = manager.create_schedule(
                {
                    "name": "每日基础数据",
                    "target_type": "config",
                    "target": "run_sync.daily.toml",
                    "frequency": "interval",
                    "interval_minutes": 30,
                }
            )
            schedule.next_run_at = "2020-01-01T00:00:00+00:00"
            manager._save_schedule(schedule)

            items = manager.list_schedules()

            self.assertEqual(len(items), 1)
            self.assertGreater(
                datetime.fromisoformat(items[0].next_run_at).astimezone(timezone.utc),
                datetime.now(timezone.utc),
            )


if __name__ == "__main__":
    unittest.main()
