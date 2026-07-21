#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sync_data_system.service.task_batch import run_task_batch


class TaskBatchTest(unittest.TestCase):
    def test_batch_continues_after_failure_and_records_partial_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            results_path = root / "results.json"
            log_path = root / "batch.log"
            payload = {
                "job_id": "job_partial",
                "continue_on_error": True,
                "tasks": [
                    {"id": "one", "name": "baostock.daily_kline", "enabled": True},
                    {"id": "two", "name": "amazingdata.daily_kline", "enabled": True},
                ],
            }

            with patch(
                "sync_data_system.service.task_batch.run_registered_task",
                side_effect=[1, 0],
            ) as runner:
                return_code = run_task_batch(payload, results_path=results_path, log_path=log_path)

            results = json.loads(results_path.read_text(encoding="utf-8"))
            self.assertEqual(return_code, 2)
            self.assertEqual(results["status"], "partial_success")
            self.assertEqual([item["status"] for item in results["tasks"]], ["failed", "success"])
            self.assertEqual(runner.call_count, 2)

    def test_batch_stops_after_failure_when_continue_is_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            results_path = root / "results.json"
            payload = {
                "job_id": "job_stop",
                "continue_on_error": False,
                "tasks": [
                    {"id": "one", "name": "baostock.daily_kline", "enabled": True},
                    {"id": "two", "name": "amazingdata.daily_kline", "enabled": True},
                ],
            }

            with patch(
                "sync_data_system.service.task_batch.run_registered_task",
                return_value=1,
            ) as runner:
                return_code = run_task_batch(
                    payload,
                    results_path=results_path,
                    log_path=root / "batch.log",
                )

            results = json.loads(results_path.read_text(encoding="utf-8"))
            self.assertEqual(return_code, 1)
            self.assertEqual(results["status"], "failed")
            self.assertEqual(len(results["tasks"]), 1)
            self.assertEqual(runner.call_count, 1)


if __name__ == "__main__":
    unittest.main()
