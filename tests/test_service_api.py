#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import yaml
from fastapi.testclient import TestClient

from sync_data_system.service.api import app
from sync_data_system.service.job_manager import JobRecord
from sync_data_system.wide_table_sync import WideTableRunResult


class ServiceApiTest(unittest.TestCase):
    def test_provider_configs_include_runtime_values(self) -> None:
        client = TestClient(app)
        with TemporaryDirectory() as tmp:
            runtime_path = Path(tmp) / "runtime.local.yaml"
            runtime_path.write_text(
                """
sync:
  amazingdata:
    username: demo-user
    password: demo-pass
    host: 127.0.0.1
    port: 8600
""",
                encoding="utf-8",
            )

            response = client.get("/api/sync/provider-configs", params={"runtime_path": str(runtime_path)})

        self.assertEqual(response.status_code, 200)
        providers = {item["provider"]: item for item in response.json()["providers"]}
        self.assertIn("amazingdata", providers)
        self.assertEqual(providers["amazingdata"]["values"]["username"], "demo-user")
        self.assertTrue(providers["amazingdata"]["configured"])

    def test_update_provider_config_writes_runtime_yaml(self) -> None:
        client = TestClient(app)
        with TemporaryDirectory() as tmp:
            runtime_path = Path(tmp) / "runtime.local.yaml"
            runtime_path.write_text(
                """
sync:
  qmt:
    timeout: 30
""",
                encoding="utf-8",
            )

            response = client.patch(
                "/api/sync/provider-configs/qmt",
                params={"runtime_path": str(runtime_path)},
                json={
                    "values": {
                        "base_url": "http://127.0.0.1:8000",
                        "api_key": "secret",
                        "timeout": "45",
                    }
                },
            )
            payload = yaml.safe_load(runtime_path.read_text(encoding="utf-8"))

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["provider_config"]["configured"])
        self.assertEqual(payload["sync"]["qmt"]["base_url"], "http://127.0.0.1:8000")
        self.assertEqual(payload["sync"]["qmt"]["api_key"], "secret")
        self.assertEqual(payload["sync"]["qmt"]["timeout"], 45)

    def test_export_provider_config_package_contains_sync_sections(self) -> None:
        client = TestClient(app)
        with TemporaryDirectory() as tmp:
            runtime_path = Path(tmp) / "runtime.local.yaml"
            runtime_path.write_text(
                """
sync:
  baostock:
    user_id: demo
    password: pass
""",
                encoding="utf-8",
            )

            response = client.get(
                "/api/sync/provider-configs/baostock/export",
                params={"runtime_path": str(runtime_path)},
            )

        self.assertEqual(response.status_code, 200)
        package = response.json()
        self.assertEqual(package["kind"], "alphablocks.sync.provider-package")
        self.assertEqual(package["provider"], "baostock")
        self.assertEqual(package["sections"]["runtime"]["values"]["user_id"], "demo")
        self.assertIsNotNone(package["sections"]["provider_manifest"])
        self.assertGreaterEqual(len(package["sections"]["provider_plans"]), 1)
        self.assertEqual(package["sections"]["code_files"], [])

    def test_export_provider_config_package_can_include_code(self) -> None:
        client = TestClient(app)
        response = client.get(
            "/api/sync/provider-configs/baostock/export",
            params={"include_code": "true"},
        )

        self.assertEqual(response.status_code, 200)
        code_paths = [item["path"] for item in response.json()["sections"]["code_files"]]
        self.assertIn("providers/baostock/provider.py", code_paths)

    def test_import_provider_config_package_writes_runtime_values(self) -> None:
        client = TestClient(app)
        with TemporaryDirectory() as tmp:
            runtime_path = Path(tmp) / "runtime.local.yaml"
            package = {
                "kind": "alphablocks.sync.provider-package",
                "version": 1,
                "provider": "qmt",
                "sections": {
                    "runtime": {
                        "values": {
                            "base_url": "http://127.0.0.1:8999",
                            "api_key": "imported",
                            "timeout": "15",
                        }
                    },
                    "provider_plans": [],
                    "sync_configs": [],
                    "code_files": [],
                },
            }

            response = client.post(
                "/api/sync/provider-configs/import",
                json={"package": package, "runtime_path": str(runtime_path)},
            )
            payload = yaml.safe_load(runtime_path.read_text(encoding="utf-8"))

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertEqual(payload["sync"]["qmt"]["base_url"], "http://127.0.0.1:8999")
        self.assertEqual(payload["sync"]["qmt"]["api_key"], "imported")
        self.assertEqual(payload["sync"]["qmt"]["timeout"], 15)

    def test_sync_table_status(self) -> None:
        client = TestClient(app)

        class _FakeClient:
            def query_rows(self, sql, parameters=None):
                if "FROM system.tables" in sql:
                    return [("starlight", "ad_market_kline_daily")]
                if "FROM system.columns" in sql:
                    return [
                        ("starlight", "ad_market_kline_daily", "code"),
                        ("starlight", "ad_market_kline_daily", "trade_time"),
                    ]
                if "FROM system.parts" in sql:
                    return [("starlight", "ad_market_kline_daily", 123, "2026-04-22 10:00:00")]
                return []

            def query_value(self, sql, parameters=None):
                return "2026-04-22 00:00:00"

            def close(self):
                return None

        with patch("sync_data_system.service.api.JOB_MANAGER.list_registered_tasks", return_value=[{"name": "amazingdata.daily_kline", "target": "ad_market_kline_daily"}]), patch(
            "sync_data_system.service.api.ClickHouseConfig.from_env",
            return_value=object(),
        ), patch(
            "sync_data_system.service.api.create_clickhouse_client",
            return_value=_FakeClient(),
        ):
            response = client.get("/api/sync-table-status")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["target"], "ad_market_kline_daily")
        self.assertEqual(payload["items"][0]["latest_date"], "2026-04-22 00:00:00")
        self.assertEqual(payload["items"][0]["row_count"], 123)

    def test_run_batch_endpoint_creates_transient_task_snapshot(self) -> None:
        client = TestClient(app)
        fake_job = JobRecord(
            job_id="job-batch",
            kind="task_batch",
            status="running",
            created_at="2026-01-01T00:00:00+00:00",
            started_at="2026-01-01T00:00:00+00:00",
            finished_at=None,
            cwd="/tmp/project",
            command=["python", "scripts/run_task_batch.py"],
            log_path="/tmp/job.log",
            config_path=None,
            task=None,
            source=None,
            target=None,
            pid=123,
            return_code=None,
            error=None,
        )

        tasks = [
            {"id": "one", "name": "amazingdata.daily_kline", "enabled": True},
            {"id": "two", "name": "baostock.daily_kline", "enabled": True},
        ]
        with patch("sync_data_system.service.api.JOB_MANAGER.create_task_batch_job", return_value=fake_job) as create_job:
            response = client.post(
                "/api/sync/jobs/run-batch",
                json={
                    "name": "临时日线同步",
                    "tasks": tasks,
                    "log_level": "INFO",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["job"]["job_id"], "job-batch")
        create_job.assert_called_once_with(
            name="临时日线同步",
            tasks=tasks,
            continue_on_error=True,
            log_level="INFO",
            runtime_path=None,
        )

    def test_run_wide_table_inline_uses_payload_execution(self) -> None:
        client = TestClient(app)
        payload = {
            "wide_table": {
                "id": "demo_wide",
                "name": "demo_wide",
                "source_node": "stock_daily_real",
                "target": {
                    "database": "starlight",
                    "table": "demo_wide",
                    "engine": "Memory",
                    "partition_by": [],
                    "order_by": ["market_code", "trade_date"],
                    "version_field": "",
                },
                "fields": ["market_code", "trade_date", "close"],
                "key_fields": ["market_code", "trade_date"],
                "status": "enabled",
            },
            "materialization_bundle": {
                "query_plan": {},
                "base_context": {},
                "preview_sql": "SELECT 1",
            },
        }
        with patch(
            "sync_data_system.service.api.build_wide_table_metadata",
            return_value=object(),
        ), patch(
            "sync_data_system.service.api.run_wide_table_sync_payloads_with_clickhouse",
            return_value=[
                WideTableRunResult(
                    wide_table_name="demo_wide",
                    action="create_and_sync",
                    status="success",
                    message="ok",
                )
            ],
        ), patch(
            "sync_data_system.service.api.ClickHouseConfig.from_env",
            return_value=object(),
        ):
            response = client.post(
                "/api/sync/wide-tables/run-inline",
                json={
                    "id": "demo_wide",
                    "nodes_path": "/tmp/alphablocks/config/nodes",
                    "payload": payload,
                },
            )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["results"][0]["wide_table_name"], "demo_wide")

    def test_removed_wide_table_spec_routes_return_not_found(self) -> None:
        client = TestClient(app)
        removed_prefix = "/api/" + "wide-tables"
        for method, url in [
            ("get", f"{removed_prefix}/specs"),
            ("post", f"{removed_prefix}/plan"),
            ("post", f"{removed_prefix}/run"),
            ("post", f"{removed_prefix}/run/stock_daily_real"),
        ]:
            response = getattr(client, method)(url)
            self.assertEqual(response.status_code, 404)

    def test_get_job_includes_logs_tail(self) -> None:
        client = TestClient(app)
        fake_job = JobRecord(
            job_id="job1",
            kind="task",
            status="running",
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
        )
        with patch("sync_data_system.service.api.JOB_MANAGER.get_job", return_value=fake_job), patch(
            "sync_data_system.service.api.JOB_MANAGER.read_job_log",
            return_value="line1\nline2",
        ):
            response = client.get("/api/jobs/job1")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["job_id"], "job1")
        self.assertEqual(payload["logs_tail"], "line1\nline2")

    def test_list_jobs_supports_status_filter(self) -> None:
        client = TestClient(app)
        fake_job = JobRecord(
            job_id="job1",
            kind="task",
            status="running",
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
        )
        with patch("sync_data_system.service.api.JOB_MANAGER.list_jobs", return_value=[fake_job]):
            response = client.get("/api/jobs?status=running")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload["jobs"]), 1)
        self.assertEqual(payload["jobs"][0]["status"], "running")

    def test_run_task_returns_task_metadata(self) -> None:
        client = TestClient(app)
        fake_job = JobRecord(
            job_id="job1",
            kind="registered_task",
            status="running",
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
        with patch(
            "sync_data_system.service.api.JOB_MANAGER.create_registered_task_job",
            return_value=fake_job,
        ), patch(
            "sync_data_system.service.api.JOB_MANAGER.list_registered_tasks",
            return_value=[
                {
                    "name": "amazingdata.daily_kline",
                    "source": "amazingdata",
                    "target": "ad_market_kline_daily",
                    "input_resolver": "market_kline_defaults",
                    "request_fields": ["name"],
                    "probe_fields": ["name"],
                }
            ],
        ):
            response = client.post("/api/jobs/run-task", json={"name": "amazingdata.daily_kline"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["job_id"], "job1")
        self.assertEqual(payload["task_metadata"]["name"], "amazingdata.daily_kline")

    def test_config_schedule_uses_config_scoped_endpoint(self) -> None:
        client = TestClient(app)

        fake_schedule = {
            "enabled": True,
            "frequency": "daily",
            "time": "18:00",
            "next_run_at": "2026-01-02T10:00:00+00:00",
        }
        with patch(
            "sync_data_system.service.api.SCHEDULE_MANAGER.get_schedule",
            return_value=fake_schedule,
        ) as get_schedule:
            response = client.get("/api/sync/configs/sync_config_daily/schedule")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), fake_schedule)
        get_schedule.assert_called_once_with("sync_config_daily")

    def test_schedule_switch_uses_dedicated_immediate_endpoint(self) -> None:
        client = TestClient(app)
        with patch(
            "sync_data_system.service.api.SCHEDULE_MANAGER.set_enabled",
            return_value={"enabled": True, "frequency": "daily"},
        ) as set_enabled:
            response = client.patch(
                "/api/sync/configs/sync_config_daily/schedule/enabled",
                json={"enabled": True},
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["schedule"]["enabled"])
        set_enabled.assert_called_once_with("sync_config_daily", True)

    def test_update_schedule_rejects_editing_while_disabled(self) -> None:
        client = TestClient(app)
        with patch(
            "sync_data_system.service.api.SCHEDULE_MANAGER.update_schedule",
            side_effect=ValueError("enable the schedule before editing its settings"),
        ):
            response = client.put(
                "/api/sync/configs/sync_config_daily/schedule",
                json={
                    "frequency": "daily",
                    "time": "18:00",
                    "weekdays": ["1", "2", "3", "4", "5"],
                    "interval_minutes": 60,
                },
            )

        self.assertEqual(response.status_code, 409)

    def test_removed_schedule_collection_routes_return_not_found(self) -> None:
        client = TestClient(app)
        for method, path in [
            ("get", "/api/sync/schedules"),
            ("post", "/api/sync/schedules"),
            ("get", "/api/sync/schedules/schedule_1"),
            ("delete", "/api/sync/schedules/schedule_1"),
        ]:
            self.assertEqual(getattr(client, method)(path).status_code, 404)

    def test_config_cannot_be_deleted_while_queued(self) -> None:
        client = TestClient(app)
        queued_job = JobRecord(
            job_id="queued-job",
            kind="sync_config",
            status="queued",
            created_at="2026-01-01T00:00:00+00:00",
            started_at=None,
            finished_at=None,
            cwd="/tmp",
            command=["python"],
            log_path="/tmp/queued-job.log",
            config_id="sync_config_daily",
        )
        with patch(
            "sync_data_system.service.api.CONFIG_MANAGER.get_config",
            return_value={"id": "sync_config_daily"},
        ), patch(
            "sync_data_system.service.api.JOB_MANAGER.find_active_config_job",
            return_value=queued_job,
        ), patch(
            "sync_data_system.service.api.CONFIG_MANAGER.delete_config",
        ) as delete_config:
            response = client.delete("/api/sync/configs/sync_config_daily")

        self.assertEqual(response.status_code, 409)
        delete_config.assert_not_called()


if __name__ == "__main__":
    unittest.main()
