#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import importlib
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

from program_bootstrap import install_sync_data_system_alias
from sync_data_system.providers.amazingdata.provider import AmazingDataSDKConfig
from sync_data_system.providers.qmt.provider import QmtConfig
from sync_data_system.runtime_config import load_runtime_config
from sync_data_system.scripts.validate_runtime_config import validate_runtime_config


class RuntimeConfigIntegrationTest(unittest.TestCase):
    def test_package_does_not_eagerly_import_provider_runner(self) -> None:
        sys.modules.pop("sync_data_system.providers.amazingdata.runner", None)
        sys.modules.pop("sync_data_system", None)
        install_sync_data_system_alias(Path(__file__).resolve().parents[1])

        pkg = importlib.import_module("sync_data_system")

        self.assertNotIn("sync_data_system.providers.amazingdata.runner", sys.modules)
        self.assertEqual(pkg.__all__, ["wide_table_sync"])

    def test_amazingdata_config_reports_all_missing_runtime_fields(self) -> None:
        runtime_yaml = textwrap.dedent(
            """
            llm:
              provider_name: deepseek
              base_url: ''
              api_key: ''
              model_name: deepseek-chat
              temperature: 0.1
              max_tokens: 4096
              enabled: true
              verify_ssl: true
            datasource:
              id: primary
              name: Primary Data Source
              db_type: clickhouse
              host: 127.0.0.1
              port: 8123
              database: starlight
              username: default
              password: TEST_PASSWORD
              secure: false
              extra_params: {}
            discovery:
              allow_databases: []
              allow_tables: []
              trading_calendar_table: ''
            """
        ).strip()

        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_path = Path(tmpdir) / "runtime.local.yaml"
            runtime_path.write_text(runtime_yaml, encoding="utf-8")

            with self.assertRaises(ValueError) as context:
                AmazingDataSDKConfig.from_env(runtime_path=runtime_path)

        message = str(context.exception)
        self.assertIn("sync.amazingdata.username", message)
        self.assertIn("sync.amazingdata.password", message)
        self.assertIn("sync.amazingdata.host", message)
        self.assertIn("sync.amazingdata.port", message)
        self.assertIn("sync:", message)
        self.assertIn("amazingdata:", message)

    def test_runtime_config_loads_qmt_defaults(self) -> None:
        runtime_yaml = textwrap.dedent(
            """
            datasource:
              host: 127.0.0.1
              port: 8123
              database: starlight
              username: default
              password: ''
            sync:
              qmt:
                base_url: http://qmt.example.internal:8000
                api_key: TEST_QMT_API_KEY
                timeout: 30
            """
        ).strip()

        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_path = Path(tmpdir) / "runtime.local.yaml"
            runtime_path.write_text(runtime_yaml, encoding="utf-8")

            runtime = load_runtime_config(runtime_path)

        self.assertEqual(runtime.sync.qmt.base_url, "http://qmt.example.internal:8000")
        self.assertEqual(runtime.sync.qmt.api_key, "TEST_QMT_API_KEY")
        self.assertEqual(runtime.sync.qmt.timeout, 30)

    def test_qmt_config_reports_missing_runtime_fields(self) -> None:
        runtime_yaml = textwrap.dedent(
            """
            datasource:
              host: 127.0.0.1
              port: 8123
              database: starlight
              username: default
              password: ''
            sync:
              qmt:
                timeout: 30
            """
        ).strip()

        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_path = Path(tmpdir) / "runtime.local.yaml"
            runtime_path.write_text(runtime_yaml, encoding="utf-8")

            with self.assertRaises(ValueError) as context:
                QmtConfig.from_env(runtime_path=runtime_path)

        message = str(context.exception)
        self.assertIn("sync.qmt.base_url", message)
        self.assertIn("sync.qmt.api_key", message)

    def test_validate_runtime_config_rejects_placeholders(self) -> None:
        runtime = load_runtime_config(Path(__file__).resolve().parents[1] / "config" / "runtime.example.yaml")

        errors = validate_runtime_config(runtime, providers=("qmt",), allow_placeholders=False)

        self.assertTrue(any("sync.qmt.base_url" in item for item in errors))
        self.assertTrue(any("sync.qmt.api_key" in item for item in errors))


if __name__ == "__main__":
    unittest.main()
