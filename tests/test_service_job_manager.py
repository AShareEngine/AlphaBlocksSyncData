#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from sync_data_system.service.job_manager import JobRecord, SyncJobManager


class ControlledProcess:
    _next_pid = 1000

    def __init__(self) -> None:
        type(self)._next_pid += 1
        self.pid = type(self)._next_pid
        self._done = threading.Event()
        self._return_code: int | None = None
        self.terminated = False

    def wait(self) -> int:
        self._done.wait(timeout=5)
        return 1 if self._return_code is None else self._return_code

    def poll(self) -> int | None:
        return self._return_code if self._done.is_set() else None

    def finish(self, return_code: int = 0) -> None:
        self._return_code = return_code
        self._done.set()

    def terminate(self) -> None:
        self.terminated = True
        self.finish(-15)


def wait_for_status(
    manager: SyncJobManager,
    job_id: str,
    expected: set[str],
    timeout: float = 3,
) -> JobRecord:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = manager.get_job(job_id)
        if job.status in expected:
            return job
        time.sleep(0.01)
    raise AssertionError(
        f"job {job_id} did not reach {sorted(expected)}; "
        f"current={manager.get_job(job_id).status}"
    )


class SyncJobManagerTest(unittest.TestCase):
    def test_create_task_batch_job_accepts_wide_table_lane(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(root, state_dir=root / ".service_state")
            fake_process = ControlledProcess()
            tasks = [
                {
                    "id": "wide_one",
                    "kind": "wide_table",
                    "name": "wide_table.stock_daily_real",
                    "provider": "wide_table",
                    "enabled": True,
                    "payload": {"wide_table": {"name": "stock_daily_real"}},
                }
            ]

            with patch(
                "sync_data_system.service.job_manager.subprocess.Popen",
                return_value=fake_process,
            ):
                job = manager.create_task_batch_job(name="宽表同步", tasks=tasks)

            children = manager.get_child_jobs(job.job_id)
            self.assertEqual(len(children), 1)
            self.assertEqual(children[0].source, "wide_table")
            self.assertEqual(children[0].request_payload["tasks"], tasks)
            fake_process.finish()
            self.assertEqual(
                wait_for_status(manager, job.job_id, {"success"}).status,
                "success",
            )

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
            processes = [ControlledProcess(), ControlledProcess()]
            tasks = [
                {"id": "a", "name": "amazingdata.daily_kline", "enabled": True},
                {"id": "b", "name": "baostock.daily_kline", "enabled": True},
            ]

            with patch(
                "sync_data_system.service.job_manager.subprocess.Popen",
                side_effect=processes,
            ) as popen:
                job = manager.create_task_batch_job(
                    name="跨源日线",
                    tasks=tasks,
                    log_level="INFO",
                    config_id="sync_config_daily",
                )

            self.assertEqual(job.kind, "sync_config")
            self.assertEqual(job.config_id, "sync_config_daily")
            self.assertEqual(job.request_payload["tasks"], tasks)
            self.assertIsNotNone(job.updated_at)
            self.assertEqual(popen.call_count, 2)
            self.assertEqual(
                {item.source for item in manager.get_child_jobs(job.job_id)},
                {"amazingdata", "baostock"},
            )
            self.assertTrue(
                all(
                    Path(call.args[0][1]).name == "run_task_batch.py"
                    for call in popen.call_args_list
                )
            )
            snapshot = Path(job.request_payload and manager.jobs_dir / f"{job.job_id}.batch.json")
            self.assertTrue(snapshot.is_file())
            for process in processes:
                process.finish()
            self.assertEqual(
                wait_for_status(manager, job.job_id, {"success"}).status,
                "success",
            )

    def test_create_task_batch_job_uses_configured_job_python(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(root, state_dir=root / ".service_state")
            fake_process = ControlledProcess()

            with (
                patch.dict(os.environ, {"SYNC_JOB_PYTHON_BIN": "/opt/conda/envs/amazing_data/bin/python3"}),
                patch("sync_data_system.service.job_manager.subprocess.Popen", return_value=fake_process) as popen,
            ):
                job = manager.create_task_batch_job(
                    name="日线",
                    tasks=[{"id": "a", "name": "amazingdata.daily_kline", "enabled": True}],
                )

            command = popen.call_args.args[0]
            self.assertEqual(command[0], "/opt/conda/envs/amazing_data/bin/python3")
            fake_process.finish()
            wait_for_status(manager, job.job_id, {"success"})

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

    def test_different_provider_starts_while_another_provider_is_running(self) -> None:
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
            process = ControlledProcess()
            with patch(
                "sync_data_system.service.job_manager.subprocess.Popen",
                return_value=process,
            ) as popen:
                job = manager.create_task_batch_job(
                    name="跨供应商并发",
                    tasks=[{"id": "a", "name": "baostock.daily_kline", "enabled": True}],
                )

            self.assertEqual(job.status, "running")
            self.assertEqual(popen.call_count, 1)
            self.assertIsNone(manager.queue_position(job.job_id))
            process.finish()
            wait_for_status(manager, job.job_id, {"success"})

    def test_same_provider_jobs_run_fifo_one_at_a_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(
                root,
                state_dir=root / ".service_state",
                max_parallel_providers=3,
            )
            first_process = ControlledProcess()
            second_process = ControlledProcess()
            with patch(
                "sync_data_system.service.job_manager.subprocess.Popen",
                side_effect=[first_process, second_process],
            ) as popen:
                first = manager.create_registered_task_job(
                    task="amazingdata.daily_kline",
                    day=20260729,
                )
                second = manager.create_registered_task_job(
                    task="amazingdata.daily_kline",
                    day=20260730,
                )
                self.assertEqual(first.status, "running")
                self.assertEqual(second.status, "queued")
                self.assertEqual(manager.queue_position(second.job_id), 1)
                self.assertEqual(popen.call_count, 1)

                first_process.finish()
                wait_for_status(manager, second.job_id, {"running"})
                self.assertEqual(popen.call_count, 2)
                second_process.finish()
                wait_for_status(manager, second.job_id, {"success"})

    def test_scheduler_skips_busy_lane_and_starts_oldest_eligible_provider(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(
                root,
                state_dir=root / ".service_state",
                max_parallel_providers=2,
            )
            amazing_first = ControlledProcess()
            baostock = ControlledProcess()
            akshare = ControlledProcess()
            amazing_second = ControlledProcess()
            with patch(
                "sync_data_system.service.job_manager.subprocess.Popen",
                side_effect=[
                    amazing_first,
                    baostock,
                    akshare,
                    amazing_second,
                ],
            ) as popen:
                amazing_first_job = manager.create_registered_task_job(
                    task="amazingdata.daily_kline",
                    day=20260729,
                )
                baostock_job = manager.create_registered_task_job(
                    task="baostock.daily_kline",
                    day=20260729,
                )
                amazing_queued = manager.create_registered_task_job(
                    task="amazingdata.daily_kline",
                    day=20260730,
                )
                akshare_job = manager.create_registered_task_job(
                    task="akshare.us_daily_kline",
                    day=20260729,
                )
                self.assertEqual(popen.call_count, 2)
                self.assertEqual(amazing_queued.status, "queued")
                self.assertEqual(akshare_job.status, "queued")

                baostock.finish()
                wait_for_status(manager, akshare_job.job_id, {"running"})
                self.assertEqual(popen.call_count, 3)
                self.assertEqual(manager.get_job(amazing_queued.job_id).status, "queued")

                amazing_first.finish()
                wait_for_status(manager, amazing_queued.job_id, {"running"})
                self.assertEqual(popen.call_count, 4)

                akshare.finish()
                amazing_second.finish()
                wait_for_status(manager, amazing_queued.job_id, {"success"})
                wait_for_status(manager, baostock_job.job_id, {"success"})
                wait_for_status(manager, amazing_first_job.job_id, {"success"})
                wait_for_status(manager, akshare_job.job_id, {"success"})

    def test_manual_exact_duplicate_returns_existing_active_job(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(root, state_dir=root / ".service_state")
            process = ControlledProcess()
            with patch(
                "sync_data_system.service.job_manager.subprocess.Popen",
                return_value=process,
            ) as popen:
                first = manager.create_registered_task_job(
                    task="amazingdata.daily_kline",
                    day=20260730,
                    runtime_path="/tmp/runtime.yaml",
                )
                duplicate = manager.create_registered_task_job(
                    task="amazingdata.daily_kline",
                    day=20260730,
                    runtime_path="/tmp/runtime.yaml",
                )
                self.assertEqual(duplicate.job_id, first.job_id)
                self.assertEqual(popen.call_count, 1)
                process.finish()
                wait_for_status(manager, first.job_id, {"success"})

    def test_cancelling_parent_stops_running_children_and_removes_queued_child(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(
                root,
                state_dir=root / ".service_state",
                max_parallel_providers=2,
            )
            processes = [ControlledProcess(), ControlledProcess()]
            tasks = [
                {"id": "a", "name": "amazingdata.daily_kline", "enabled": True},
                {"id": "b", "name": "baostock.daily_kline", "enabled": True},
                {"id": "c", "name": "akshare.us_daily_kline", "enabled": True},
            ]
            with patch(
                "sync_data_system.service.job_manager.subprocess.Popen",
                side_effect=processes,
            ):
                parent = manager.create_task_batch_job(name="取消批次", tasks=tasks)
                manager.cancel_job(parent.job_id)
                cancelled = wait_for_status(
                    manager,
                    parent.job_id,
                    {"cancelled"},
                )

            self.assertEqual(cancelled.status, "cancelled")
            self.assertTrue(all(process.terminated for process in processes))
            self.assertEqual(
                {child.status for child in manager.get_child_jobs(parent.job_id)},
                {"cancelled"},
            )

    def test_parent_aggregates_child_results_and_partial_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(root, state_dir=root / ".service_state")
            processes = [ControlledProcess(), ControlledProcess()]
            tasks = [
                {"id": "a", "name": "amazingdata.daily_kline", "enabled": True},
                {"id": "b", "name": "baostock.daily_kline", "enabled": True},
            ]
            with patch(
                "sync_data_system.service.job_manager.subprocess.Popen",
                side_effect=processes,
            ):
                parent = manager.create_task_batch_job(name="结果聚合", tasks=tasks)
                children = manager.get_child_jobs(parent.job_id)
                for child, status in zip(children, ("success", "failed")):
                    Path(child.task_results_path).write_text(
                        json.dumps(
                            {
                                "job_id": child.job_id,
                                "status": status,
                                "tasks": [
                                    {
                                        "task_id": child.request_payload["tasks"][0]["id"],
                                        "name": child.request_payload["tasks"][0]["name"],
                                        "provider": child.source,
                                        "status": status,
                                    }
                                ],
                            }
                        ),
                        encoding="utf-8",
                    )
                processes[0].finish(0)
                processes[1].finish(1)
                wait_for_status(manager, parent.job_id, {"partial_success"})

            result = manager.read_task_results(parent.job_id)
            self.assertEqual(result["status"], "partial_success")
            self.assertEqual(
                [item["status"] for item in result["tasks"]],
                ["success", "failed"],
            )
            self.assertIn(
                "provider process exited with return code 1",
                manager.get_job(parent.job_id).error,
            )

    def test_failed_job_persists_exception_summary_from_log(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(root, state_dir=root / ".service_state")
            log_path = root / "failed.log"
            log_path.write_text(
                "Traceback (most recent call last):\n"
                "  File \"sync.py\", line 1, in <module>\n"
                "ImportError: missing tables package\n",
                encoding="utf-8",
            )
            job = JobRecord(
                job_id="failed_job",
                kind="provider_task",
                status="running",
                created_at="2026-01-01T00:00:00+00:00",
                started_at="2026-01-01T00:00:00+00:00",
                finished_at=None,
                cwd=str(root),
                command=["python"],
                log_path=str(log_path),
            )
            manager._jobs[job.job_id] = job

            with manager._lock:
                manager._finish_executable_locked(job, 1)

            persisted = json.loads(
                (manager.jobs_dir / "failed_job.json").read_text(encoding="utf-8")
            )
            self.assertEqual(job.error, "ImportError: missing tables package")
            self.assertEqual(
                persisted["error"],
                "ImportError: missing tables package",
            )

    def test_read_job_error_log_filters_noise_and_keeps_traceback_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(root, state_dir=root / ".service_state")
            log_path = root / "failed.log"
            log_path.write_text(
                "task=one status=success\n"
                "task=two status=started\n"
                "Traceback (most recent call last):\n"
                "  File \"sync.py\", line 8, in run\n"
                "    raise RuntimeError('upstream timeout')\n"
                "RuntimeError: upstream timeout\n"
                "task=three status=started\n"
                "task=three progress=1/100\n",
                encoding="utf-8",
            )
            job = JobRecord(
                job_id="error_log_job",
                kind="provider_task",
                status="failed",
                created_at="2026-01-01T00:00:00+00:00",
                started_at="2026-01-01T00:00:00+00:00",
                finished_at="2026-01-01T00:01:00+00:00",
                cwd=str(root),
                command=["python"],
                log_path=str(log_path),
                error="RuntimeError: upstream timeout",
            )
            manager._jobs[job.job_id] = job

            error_log = manager.read_job_error_log(job.job_id)

            self.assertNotIn("task=one status=success", error_log)
            self.assertIn("task=two status=started", error_log)
            self.assertIn("Traceback (most recent call last):", error_log)
            self.assertIn("File \"sync.py\", line 8", error_log)
            self.assertIn("RuntimeError: upstream timeout", error_log)

    def test_parent_job_log_aggregates_child_logs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(root, state_dir=root / ".service_state")
            parent_log_path = root / "parent.log"
            child_log_path = root / "child.log"
            parent_log_path.write_text(
                "scheduler parent=parent status=running\n",
                encoding="utf-8",
            )
            child_log_path.write_text(
                "RuntimeError: child sync failed\n",
                encoding="utf-8",
            )
            parent = JobRecord(
                job_id="parent",
                kind="task_batch",
                status="failed",
                created_at="2026-01-01T00:00:00+00:00",
                started_at="2026-01-01T00:00:00+00:00",
                finished_at="2026-01-01T00:01:00+00:00",
                cwd=str(root),
                command=[],
                log_path=str(parent_log_path),
                child_job_ids=["child"],
            )
            child = JobRecord(
                job_id="child",
                kind="provider_task",
                status="failed",
                created_at="2026-01-01T00:00:00+00:00",
                started_at="2026-01-01T00:00:00+00:00",
                finished_at="2026-01-01T00:01:00+00:00",
                cwd=str(root),
                command=["python"],
                log_path=str(child_log_path),
                source="tushare",
                parent_job_id="parent",
            )
            manager._jobs.update({parent.job_id: parent, child.job_id: child})

            log = manager.read_job_log(parent.job_id)

            self.assertIn("[scheduler] scheduler parent=parent status=running", log)
            self.assertIn(
                "[provider=tushare job=child] RuntimeError: child sync failed",
                log,
            )

    def test_batch_failure_prefers_structured_task_error_over_log_tail(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(root, state_dir=root / ".service_state")
            results_path = root / "results.json"
            results_path.write_text(
                json.dumps(
                    {
                        "tasks": [
                            {
                                "task_id": "daily",
                                "name": "tushare.daily",
                                "status": "failed",
                                "error": "API rate limit exceeded",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            log_path = root / "failed.log"
            log_path.write_text("RuntimeError: fallback error\n", encoding="utf-8")
            job = JobRecord(
                job_id="batch_failed",
                kind="provider_batch",
                status="running",
                created_at="2026-01-01T00:00:00+00:00",
                started_at="2026-01-01T00:00:00+00:00",
                finished_at=None,
                cwd=str(root),
                command=["python"],
                log_path=str(log_path),
                task_results_path=str(results_path),
            )
            manager._jobs[job.job_id] = job

            with manager._lock:
                manager._finish_executable_locked(job, 1)

            self.assertEqual(
                job.error,
                "tushare.daily: API rate limit exceeded",
            )

    def test_running_batch_exposes_completed_task_error_before_process_exits(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(root, state_dir=root / ".service_state")
            results_path = root / "results.json"
            results_path.write_text(
                json.dumps(
                    {
                        "status": "running",
                        "tasks": [
                            {
                                "task_id": "daily",
                                "name": "tushare.daily",
                                "status": "failed",
                                "error": "permission denied",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            job = JobRecord(
                job_id="running_batch",
                kind="provider_batch",
                status="running",
                created_at="2026-01-01T00:00:00+00:00",
                started_at="2026-01-01T00:00:00+00:00",
                finished_at=None,
                cwd=str(root),
                command=["python"],
                log_path=str(root / "running.log"),
                task_results_path=str(results_path),
            )
            process = Mock()
            process.poll.return_value = None
            manager._jobs[job.job_id] = job
            manager._processes[job.job_id] = process

            refreshed = manager.get_job(job.job_id)

            self.assertEqual(
                refreshed.error,
                "tushare.daily: permission denied",
            )

    def test_max_parallel_provider_env_override_has_minimum_one(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            with patch.dict(os.environ, {"SYNC_MAX_PARALLEL_PROVIDERS": "0"}):
                manager = SyncJobManager(root, state_dir=root / ".service_state")
            self.assertEqual(manager.max_parallel_providers, 1)

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
                source="baostock",
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
                source="baostock",
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
            self.assertEqual(reloaded.get_job("blocker").status, "success")
            self.assertEqual(reloaded.get_job("blocker").restart_count, 1)

    def test_running_provider_child_is_requeued_with_resume_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            state_dir = root / ".service_state"
            manager = SyncJobManager(root, state_dir=state_dir)
            payload_path = state_dir / "jobs" / "child.batch.json"
            results_path = state_dir / "jobs" / "child.results.json"
            log_path = state_dir / "logs" / "child.log"
            payload = {
                "job_id": "child",
                "tasks": [
                    {
                        "id": "a",
                        "name": "amazingdata.daily_kline",
                        "parameters": {"force": True},
                    }
                ],
            }
            payload_path.write_text(json.dumps(payload), encoding="utf-8")
            parent = JobRecord(
                job_id="parent",
                kind="task_batch",
                status="running",
                created_at="2026-07-30T01:00:00+00:00",
                started_at="2026-07-30T01:00:01+00:00",
                finished_at=None,
                cwd=str(root),
                command=[],
                log_path=str(root / "parent.log"),
                child_job_ids=["child"],
            )
            child = JobRecord(
                job_id="child",
                kind="provider_batch",
                status="running",
                created_at=parent.created_at,
                started_at=parent.started_at,
                finished_at=None,
                cwd=str(root),
                command=[
                    "python",
                    str(root / "scripts" / "run_task_batch.py"),
                    "--payload",
                    str(payload_path),
                    "--results",
                    str(results_path),
                    "--log-path",
                    str(log_path),
                ],
                log_path=str(log_path),
                source="amazingdata",
                request_payload=payload,
                task_results_path=str(results_path),
                parent_job_id=parent.job_id,
            )
            manager._jobs = {"parent": parent, "child": child}
            manager._save_job(parent)
            manager._save_job(child)
            manager._save_queue()

            process = ControlledProcess()
            with patch(
                "sync_data_system.service.job_manager.subprocess.Popen",
                return_value=process,
            ) as popen:
                reloaded = SyncJobManager(root, state_dir=state_dir)

            self.assertEqual(reloaded.get_job("child").status, "running")
            self.assertEqual(reloaded.get_job("child").restart_count, 1)
            self.assertEqual(reloaded.get_job("parent").status, "running")
            rewritten = json.loads(payload_path.read_text(encoding="utf-8"))
            self.assertTrue(rewritten["resume_after_restart"])
            self.assertTrue(rewritten["tasks"][0]["parameters"]["resume"])
            self.assertFalse(rewritten["tasks"][0]["parameters"]["force"])
            popen.assert_called_once()
            process.finish()
            self.assertEqual(
                wait_for_status(reloaded, "parent", {"success"}).status,
                "success",
            )

    def test_provider_task_restart_disables_force_and_enables_resume(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "sync_project"
            root.mkdir()
            manager = SyncJobManager(root, state_dir=root / ".service_state")
            job = JobRecord(
                job_id="minute_child",
                kind="provider_task",
                status="running",
                created_at="2026-07-30T01:00:00+00:00",
                started_at="2026-07-30T01:00:01+00:00",
                finished_at=None,
                cwd=str(root),
                command=[
                    "python",
                    "scripts/run_provider_sync.py",
                    "--task",
                    "amazingdata.minute_kline",
                    "--force",
                ],
                log_path=str(root / "minute_child.log"),
                source="amazingdata",
                request_payload={
                    "name": "amazingdata.minute_kline",
                    "force": True,
                    "resume": False,
                },
            )

            manager._enable_resume_for_job_locked(job)

            self.assertNotIn("--force", job.command)
            self.assertIn("--resume", job.command)
            self.assertFalse(job.request_payload["force"])
            self.assertTrue(job.request_payload["resume"])

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
