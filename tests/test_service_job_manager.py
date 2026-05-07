#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from sync_data_system.service.job_manager import JobRecord, SyncJobManager


class SyncJobManagerTest(unittest.TestCase):
    def test_list_configs_discovers_run_sync_toml_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            config_root = root.parent / "config" / "sync" / "plans"
            config_root.mkdir(parents=True, exist_ok=True)
            (config_root / "run_sync.full.toml").write_text("source = 'amazingdata'\n[[tasks]]\ntask='code_info'\n", encoding="utf-8")
            (config_root / "run_sync.baostock.full.toml").write_text("source = 'baostock'\n[[tasks]]\ntask='all_stock'\n", encoding="utf-8")
            manager = SyncJobManager(root, state_dir=root / ".service_state")
            self.assertEqual(manager.list_configs(), ["run_sync.baostock.full.toml", "run_sync.full.toml"])

    def test_resolve_config_path_rejects_outside_project(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            other = Path(tmpdir).parent / "outside.toml"
            other.write_text("source = 'amazingdata'\n[[tasks]]\ntask='code_info'\n", encoding="utf-8")
            try:
                manager = SyncJobManager(root, state_dir=root / ".service_state")
                with self.assertRaises(ValueError):
                    manager._resolve_config_path(str(other))
            finally:
                if other.exists():
                    other.unlink()

    def test_list_registered_tasks_returns_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(root, state_dir=root / ".service_state")
            items = manager.list_registered_tasks()
            self.assertTrue(any(item["name"] == "daily_kline" for item in items))
            daily = next(item for item in items if item["name"] == "daily_kline")
            self.assertEqual(daily["source"], "amazingdata")
            self.assertEqual(daily["target"], "ad_market_kline_daily")

    def test_rejects_new_job_when_running_job_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(root, state_dir=root / ".service_state")
            manager._jobs["job1"] = JobRecord(
                job_id="job1",
                kind="task",
                status="running",
                created_at="2026-01-01T00:00:00+00:00",
                started_at="2026-01-01T00:00:00+00:00",
                finished_at=None,
                cwd=str(root),
                command=["python", "run_sync.py"],
                log_path=str(root / "job1.log"),
                config_path=None,
                task="daily_kline",
                source="amazingdata",
                target="ad_market_kline_daily",
                pid=None,
                return_code=None,
                error=None,
            )
            with self.assertRaisesRegex(RuntimeError, "another sync job is running"):
                manager._ensure_no_running_jobs()

    def test_list_jobs_supports_filters(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(root, state_dir=root / ".service_state")
            manager._jobs["job1"] = JobRecord(
                job_id="job1",
                kind="task",
                status="running",
                created_at="2026-01-01T00:00:00+00:00",
                started_at="2026-01-01T00:00:00+00:00",
                finished_at=None,
                cwd=str(root),
                command=["python", "run_sync.py"],
                log_path=str(root / "job1.log"),
                config_path=None,
                task="daily_kline",
                source="amazingdata",
                target="ad_market_kline_daily",
                pid=None,
                return_code=None,
                error=None,
            )
            manager._jobs["job2"] = JobRecord(
                job_id="job2",
                kind="config",
                status="failed",
                created_at="2026-01-02T00:00:00+00:00",
                started_at="2026-01-02T00:00:00+00:00",
                finished_at="2026-01-02T00:01:00+00:00",
                cwd=str(root),
                command=["python", "run_sync.py", "--config"],
                log_path=str(root / "job2.log"),
                config_path="run_sync.full.toml",
                task=None,
                source=None,
                target=None,
                pid=None,
                return_code=1,
                error="boom",
            )
            self.assertEqual([job.job_id for job in manager.list_jobs(status="running")], ["job1"])
            self.assertEqual([job.job_id for job in manager.list_jobs(kind="config")], ["job2"])
            self.assertEqual([job.job_id for job in manager.list_jobs(task="daily_kline")], ["job1"])

    def test_cancel_job_marks_job_cancelling_before_exit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(root, state_dir=root / ".service_state")
            manager._jobs["job1"] = JobRecord(
                job_id="job1",
                kind="task",
                status="running",
                created_at="2026-01-01T00:00:00+00:00",
                started_at="2026-01-01T00:00:00+00:00",
                finished_at=None,
                cwd=str(root),
                command=["python", "run_sync.py"],
                log_path=str(root / "job1.log"),
                config_path=None,
                task="daily_kline",
                source="amazingdata",
                target="ad_market_kline_daily",
                pid=123,
                return_code=None,
                error=None,
            )
            fake_process = Mock()
            fake_process.poll.return_value = None
            manager._processes["job1"] = fake_process
            job = manager.cancel_job("job1")
            self.assertEqual(job.status, "cancelling")
            fake_process.terminate.assert_called_once()


if __name__ == "__main__":
    unittest.main()
