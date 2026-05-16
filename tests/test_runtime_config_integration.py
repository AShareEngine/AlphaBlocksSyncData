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
from sync_data_system.runtime_config import load_runtime_config


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
              password: secret
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
                base_url: http://172.16.2.89:8000
                api_key: dev-api-key-001
                timeout: 30
            """
        ).strip()

        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_path = Path(tmpdir) / "runtime.local.yaml"
            runtime_path.write_text(runtime_yaml, encoding="utf-8")

            runtime = load_runtime_config(runtime_path)

        self.assertEqual(runtime.sync.qmt.base_url, "http://172.16.2.89:8000")
        self.assertEqual(runtime.sync.qmt.api_key, "dev-api-key-001")
        self.assertEqual(runtime.sync.qmt.timeout, 30)


if __name__ == "__main__":
    unittest.main()
