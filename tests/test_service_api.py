#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
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

    def test_run_task_returns_409_when_another_job_running(self) -> None:
        client = TestClient(app)
        with patch(
            "sync_data_system.service.api.JOB_MANAGER.create_registered_task_job",
            side_effect=RuntimeError("another sync job is running job_id=job1 task=daily_kline; cancel it first"),
        ), patch(
            "sync_data_system.service.api.JOB_MANAGER.list_registered_tasks",
            return_value=[{"name": "amazingdata.daily_kline"}],
        ):
            response = client.post(
                "/api/jobs/run-task",
                json={"name": "amazingdata.daily_kline"},
            )
        self.assertEqual(response.status_code, 409)
        self.assertIn("another sync job is running", response.json()["detail"])

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

    def test_list_schedules_returns_schedule_records(self) -> None:
        client = TestClient(app)

        fake_schedule = SimpleNamespace(
            id="schedule_1",
            name="每日基础数据",
            enabled=True,
            target_type="config",
            target="run_sync.daily.toml",
            frequency="daily",
        )

        with patch("sync_data_system.service.api.SCHEDULE_MANAGER.list_schedules", return_value=[fake_schedule]):
            response = client.get("/api/sync/schedules")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["schedules"][0]["id"], "schedule_1")

    def test_create_schedule_returns_validation_error(self) -> None:
        client = TestClient(app)
        with patch(
            "sync_data_system.service.api.SCHEDULE_MANAGER.create_schedule",
            side_effect=ValueError("unknown sync config: missing.toml"),
        ):
            response = client.post(
                "/api/sync/schedules",
                json={
                    "name": "缺失配置",
                    "target_type": "config",
                    "target": "missing.toml",
                },
            )

        self.assertEqual(response.status_code, 400)
        self.assertIn("unknown sync config", response.json()["detail"])

    def test_run_schedule_now_returns_job(self) -> None:
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

        fake_schedule = SimpleNamespace(
            id="schedule_1",
            name="日线同步",
            enabled=True,
            target_type="task",
            target="amazingdata.daily_kline",
            last_job_id="job1",
        )

        with patch(
            "sync_data_system.service.api.SCHEDULE_MANAGER.run_schedule_now",
            return_value=(fake_schedule, fake_job),
        ):
            response = client.post("/api/sync/schedules/schedule_1/run-now")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["schedule"]["last_job_id"], "job1")
        self.assertEqual(payload["job"]["job_id"], "job1")


if __name__ == "__main__":
    unittest.main()
