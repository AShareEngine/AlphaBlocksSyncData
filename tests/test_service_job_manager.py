#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from sync_data_system.service.job_manager import JobRecord, SyncJobManager


class SyncJobManagerTest(unittest.TestCase):
    def test_list_registered_tasks_returns_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(root, state_dir=root / ".service_state")
            items = manager.list_registered_tasks()
            self.assertTrue(any(item["name"] == "amazingdata.daily_kline" for item in items))
            daily = next(item for item in items if item["name"] == "amazingdata.daily_kline")
            self.assertEqual(daily["source"], "amazingdata")
            self.assertEqual(daily["target"], "ad_market_kline_daily")

    def test_create_task_batch_job_persists_cross_provider_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(root, state_dir=root / ".service_state")
            fake_process = Mock()
            fake_process.pid = 123
            fake_process.wait.return_value = 0
            tasks = [
                {"id": "a", "name": "amazingdata.daily_kline", "enabled": True},
                {"id": "b", "name": "baostock.daily_kline", "enabled": True},
            ]

            with patch("sync_data_system.service.job_manager.subprocess.Popen", return_value=fake_process) as popen:
                job = manager.create_task_batch_job(
                    name="跨源日线",
                    tasks=tasks,
                    log_level="INFO",
                    config_id="sync_config_daily",
                )

            command = popen.call_args.args[0]
            self.assertEqual(job.kind, "sync_config")
            self.assertEqual(job.config_id, "sync_config_daily")
            self.assertEqual(job.request_payload["tasks"], tasks)
            self.assertIsNotNone(job.updated_at)
            self.assertEqual(Path(command[1]).name, "run_task_batch.py")
            snapshot = Path(job.request_payload and manager.jobs_dir / f"{job.job_id}.batch.json")
            self.assertTrue(snapshot.is_file())

    def test_create_task_batch_job_uses_configured_job_python(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(root, state_dir=root / ".service_state")
            fake_process = Mock()
            fake_process.pid = 123
            fake_process.wait.return_value = 0

            with (
                patch.dict(os.environ, {"SYNC_JOB_PYTHON_BIN": "/opt/conda/envs/amazing_data/bin/python3"}),
                patch("sync_data_system.service.job_manager.subprocess.Popen", return_value=fake_process) as popen,
            ):
                manager.create_task_batch_job(
                    name="日线",
                    tasks=[{"id": "a", "name": "amazingdata.daily_kline", "enabled": True}],
                )

            command = popen.call_args.args[0]
            self.assertEqual(command[0], "/opt/conda/envs/amazing_data/bin/python3")

    def test_list_jobs_refreshes_running_job_updated_at_from_log_mtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(root, state_dir=root / ".service_state")
            log_path = root / "job1.log"
            log_path.write_text("running\n", encoding="utf-8")
            updated_at = datetime(2026, 1, 1, 0, 10, 0, tzinfo=timezone.utc)
            os.utime(log_path, (updated_at.timestamp(), updated_at.timestamp()))
            manager._jobs["job1"] = JobRecord(
                job_id="job1",
                kind="task",
                status="running",
                created_at="2026-01-01T00:00:00+00:00",
                started_at="2026-01-01T00:00:00+00:00",
                finished_at=None,
                cwd=str(root),
                command=["python", "scripts/run_provider_sync.py"],
                log_path=str(log_path),
                config_path=None,
                task="amazingdata.daily_kline",
                source="amazingdata",
                target="ad_market_kline_daily",
                pid=None,
                return_code=None,
                error=None,
                updated_at="2026-01-01T00:00:00+00:00",
            )
            process = Mock()
            process.poll.return_value = None
            manager._processes["job1"] = process

            jobs = manager.list_jobs()

            self.assertEqual(jobs[0].updated_at, "2026-01-01T00:10:00+00:00")

    def test_new_job_is_queued_when_another_job_is_running(self) -> None:
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
                command=["python", "scripts/run_provider_sync.py"],
                log_path=str(root / "job1.log"),
                config_path=None,
                task="amazingdata.daily_kline",
                source="amazingdata",
                target="ad_market_kline_daily",
                pid=None,
                return_code=None,
                error=None,
            )
            job = manager.create_task_batch_job(
                name="排队任务",
                tasks=[{"id": "a", "name": "baostock.daily_kline", "enabled": True}],
            )

            self.assertEqual(job.status, "queued")
            self.assertEqual(manager.queue_position(job.job_id), 1)

    def test_same_config_cannot_be_queued_twice(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(root, state_dir=root / ".service_state")
            manager._jobs["job1"] = JobRecord(
                job_id="job1",
                kind="sync_config",
                status="running",
                created_at="2026-01-01T00:00:00+00:00",
                started_at="2026-01-01T00:00:00+00:00",
                finished_at=None,
                cwd=str(root),
                command=["python"],
                log_path=str(root / "job1.log"),
                config_id="sync_config_daily",
            )

            with self.assertRaisesRegex(RuntimeError, "already queued or running"):
                manager.create_task_batch_job(
                    name="重复配置",
                    tasks=[{"id": "a", "name": "baostock.daily_kline", "enabled": True}],
                    config_id="sync_config_daily",
                )

    def test_cancel_pending_scheduled_jobs_does_not_cancel_manual_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(root, state_dir=root / ".service_state")
            manager._jobs["running"] = JobRecord(
                job_id="running",
                kind="registered_task",
                status="running",
                created_at="2026-01-01T00:00:00+00:00",
                started_at="2026-01-01T00:00:00+00:00",
                finished_at=None,
                cwd=str(root),
                command=["python"],
                log_path=str(root / "running.log"),
            )
            scheduled = manager.create_task_batch_job(
                name="定时配置",
                tasks=[{"id": "a", "name": "baostock.daily_kline", "enabled": True}],
                config_id="sync_config_daily",
                trigger="schedule",
            )
            manual = manager.create_task_batch_job(
                name="手动临时任务",
                tasks=[{"id": "b", "name": "baostock.daily_kline", "enabled": True}],
            )

            cancelled = manager.cancel_pending_jobs(
                config_id="sync_config_daily",
                trigger="schedule",
            )

            self.assertEqual([item.job_id for item in cancelled], [scheduled.job_id])
            self.assertEqual(manager.get_job(scheduled.job_id).status, "cancelled")
            self.assertEqual(manager.get_job(manual.job_id).status, "queued")

    def test_persisted_queue_resumes_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            state_dir = root / ".service_state"
            manager = SyncJobManager(root, state_dir=state_dir)
            blocker = JobRecord(
                job_id="blocker",
                kind="registered_task",
                status="running",
                created_at="2026-01-01T00:00:00+00:00",
                started_at="2026-01-01T00:00:00+00:00",
                finished_at=None,
                cwd=str(root),
                command=["python"],
                log_path=str(root / "blocker.log"),
            )
            manager._jobs[blocker.job_id] = blocker
            manager._save_job(blocker)
            queued = manager.create_task_batch_job(
                name="恢复任务",
                tasks=[{"id": "a", "name": "baostock.daily_kline", "enabled": True}],
            )
            self.assertEqual(queued.status, "queued")

            fake_process = Mock()
            fake_process.pid = 321
            fake_process.poll.return_value = 0
            fake_process.wait.return_value = 0
            with patch("sync_data_system.service.job_manager.subprocess.Popen", return_value=fake_process) as popen:
                reloaded = SyncJobManager(root, state_dir=state_dir)
                resumed = reloaded.get_job(queued.job_id)

            self.assertTrue(popen.called)
            self.assertEqual(resumed.status, "success")
            self.assertEqual(reloaded.get_job("blocker").status, "interrupted")

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
                command=["python", "scripts/run_provider_sync.py"],
                log_path=str(root / "job1.log"),
                config_path=None,
                task="amazingdata.daily_kline",
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
                command=["python", "scripts/run_provider_sync.py", "--config"],
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
            self.assertEqual([job.job_id for job in manager.list_jobs(task="amazingdata.daily_kline")], ["job1"])

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
                command=["python", "scripts/run_provider_sync.py"],
                log_path=str(root / "job1.log"),
                config_path=None,
                task="amazingdata.daily_kline",
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
