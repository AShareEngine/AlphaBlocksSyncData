#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import Mock, call, patch

from sync_data_system.service.task_batch import _resolve_incremental_parameters, run_task_batch


class TaskBatchTest(unittest.TestCase):
    def test_code_scoped_incremental_keeps_per_code_floor_instead_of_table_max(self) -> None:
        connection = Mock()
        metadata = {
            "incremental_scope": "code",
            "request_fields": ["begin_date", "end_date"],
        }

        with patch(
            "sync_data_system.service.task_batch.expected_business_date",
            return_value=date(2026, 7, 28),
        ):
            parameters = _resolve_incremental_parameters(metadata, {}, connection)

        self.assertEqual(
            parameters,
            {"begin_date": 20100101, "end_date": 20260728},
        )
        connection.query_rows.assert_not_called()
        connection.query_value.assert_not_called()

    def test_code_scoped_incremental_preserves_explicit_begin_date(self) -> None:
        metadata = {
            "incremental_scope": "code",
            "request_fields": ["begin_date", "end_date"],
        }

        with patch(
            "sync_data_system.service.task_batch.expected_business_date",
            return_value=date(2026, 7, 28),
        ):
            parameters = _resolve_incremental_parameters(
                metadata,
                {"begin_date": 20150101},
                None,
            )

        self.assertEqual(
            parameters,
            {"begin_date": 20150101, "end_date": 20260728},
        )

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

            baostock_context = Mock()
            amazingdata_context = Mock()
            with (
                patch(
                    "sync_data_system.service.task_batch.run_registered_task",
                    side_effect=[1, 0],
                ) as runner,
                patch(
                    "sync_data_system.service.task_batch.build_provider_context",
                    side_effect=[baostock_context, amazingdata_context],
                ),
            ):
                return_code = run_task_batch(payload, results_path=results_path, log_path=log_path)

            results = json.loads(results_path.read_text(encoding="utf-8"))
            self.assertEqual(return_code, 2)
            self.assertEqual(results["status"], "partial_success")
            self.assertEqual([item["status"] for item in results["tasks"]], ["failed", "success"])
            self.assertEqual(runner.call_count, 2)
            baostock_context.close.assert_called_once_with()
            amazingdata_context.close.assert_called_once_with()

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

            context = Mock()
            with (
                patch(
                    "sync_data_system.service.task_batch.run_registered_task",
                    return_value=1,
                ) as runner,
                patch(
                    "sync_data_system.service.task_batch.build_provider_context",
                    return_value=context,
                ),
            ):
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
            context.close.assert_called_once_with()

    def test_restart_resume_skips_tasks_already_completed_in_results_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            results_path = root / "results.json"
            results_path.write_text(
                json.dumps(
                    {
                        "job_id": "job_resume",
                        "status": "running",
                        "tasks": [
                            {
                                "task_id": "one",
                                "name": "baostock.daily_kline",
                                "provider": "baostock",
                                "status": "success",
                            },
                            {
                                "task_id": "two",
                                "name": "amazingdata.daily_kline",
                                "provider": "amazingdata",
                                "status": "running",
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            payload = {
                "job_id": "job_resume",
                "resume_after_restart": True,
                "continue_on_error": True,
                "tasks": [
                    {"id": "one", "name": "baostock.daily_kline", "enabled": True},
                    {"id": "two", "name": "amazingdata.daily_kline", "enabled": True},
                ],
            }
            context = Mock()
            with (
                patch(
                    "sync_data_system.service.task_batch.run_registered_task",
                    return_value=0,
                ) as runner,
                patch(
                    "sync_data_system.service.task_batch.build_provider_context",
                    return_value=context,
                ),
            ):
                return_code = run_task_batch(
                    payload,
                    results_path=results_path,
                    log_path=root / "batch.log",
                )

            results = json.loads(results_path.read_text(encoding="utf-8"))
            self.assertEqual(return_code, 0)
            self.assertEqual(
                [item["status"] for item in results["tasks"]],
                ["success", "success"],
            )
            runner.assert_called_once()
            self.assertEqual(runner.call_args.args[0].task, "amazingdata.daily_kline")
            context.close.assert_called_once_with()

    def test_amazingdata_tasks_share_one_context_across_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            amazingdata_context = Mock()
            baostock_context = Mock()
            payload = {
                "job_id": "job_shared_amazingdata",
                "continue_on_error": True,
                "runtime_path": "/tmp/runtime.local.yaml",
                "tasks": [
                    {"id": "one", "name": "amazingdata.code_info", "enabled": True},
                    {"id": "two", "name": "baostock.daily_kline", "enabled": True},
                    {"id": "three", "name": "amazingdata.long_hu_bang", "enabled": True},
                ],
            }

            with (
                patch(
                    "sync_data_system.service.task_batch.run_registered_task",
                    return_value=0,
                ) as runner,
                patch(
                    "sync_data_system.service.task_batch.build_provider_context",
                    side_effect=[amazingdata_context, baostock_context],
                ) as build_context,
            ):
                return_code = run_task_batch(
                    payload,
                    results_path=root / "results.json",
                    log_path=root / "batch.log",
            )

            self.assertEqual(return_code, 0)
            self.assertEqual(
                build_context.call_args_list,
                [
                    call(
                        "amazingdata",
                        runtime_path="/tmp/runtime.local.yaml",
                        database="starlight",
                    ),
                    call(
                        "baostock",
                        runtime_path="/tmp/runtime.local.yaml",
                        database="baostock",
                    ),
                ],
            )
            self.assertIs(
                runner.call_args_list[0].kwargs["context"],
                amazingdata_context,
            )
            self.assertIs(
                runner.call_args_list[1].kwargs["context"],
                baostock_context,
            )
            self.assertIs(
                runner.call_args_list[2].kwargs["context"],
                amazingdata_context,
            )
            amazingdata_context.close.assert_called_once_with()
            baostock_context.close.assert_called_once_with()

    def test_akshare_tasks_share_one_context_across_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            shared_context = Mock()
            payload = {
                "job_id": "job_shared_akshare",
                "continue_on_error": True,
                "runtime_path": "/tmp/runtime.local.yaml",
                "tasks": [
                    {"id": "one", "name": "akshare.us_spot", "enabled": True},
                    {"id": "two", "name": "akshare.us_daily_kline", "enabled": True},
                ],
            }

            with (
                patch(
                    "sync_data_system.service.task_batch.run_registered_task",
                    return_value=0,
                ) as runner,
                patch(
                    "sync_data_system.service.task_batch.build_provider_context",
                    return_value=shared_context,
                ) as build_context,
            ):
                return_code = run_task_batch(
                    payload,
                    results_path=root / "results.json",
                    log_path=root / "batch.log",
                )

            self.assertEqual(return_code, 0)
            build_context.assert_called_once_with(
                "akshare",
                runtime_path="/tmp/runtime.local.yaml",
                database="akshare",
            )
            self.assertIs(runner.call_args_list[0].kwargs["context"], shared_context)
            self.assertIs(runner.call_args_list[1].kwargs["context"], shared_context)
            shared_context.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
