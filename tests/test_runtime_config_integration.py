#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import importlib
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

from sync_data_system.amazingdata_sdk_provider import AmazingDataSDKConfig


class RuntimeConfigIntegrationTest(unittest.TestCase):
    def test_package_does_not_eagerly_import_run_sync(self) -> None:
        sys.modules.pop("sync_data_system.run_sync", None)
        sys.modules.pop("sync_data_system", None)

        pkg = importlib.import_module("sync_data_system")

        self.assertNotIn("sync_data_system.run_sync", sys.modules)
        module = pkg.run_sync
        self.assertEqual(module.__name__, "sync_data_system.run_sync")
        self.assertIn("sync_data_system.run_sync", sys.modules)

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


if __name__ == "__main__":
    unittest.main()
