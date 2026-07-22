#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path

from sync_data_system.core.providers import load_provider_manifest, load_provider_registry
from sync_data_system.config_paths import resolve_provider_root
from sync_data_system.providers.amazingdata.runner import detect_config_source, run_provider_config_file
from sync_data_system.scripts.validate_provider import validate_manifest


class ProviderRegistryTest(unittest.TestCase):
    def test_builtin_provider_manifests_are_loaded(self) -> None:
        registry = load_provider_registry()

        self.assertEqual(registry.names(), ["amazingdata", "baostock", "qmt"])
        self.assertGreater(len(registry.get("amazingdata").tasks), 10)
        self.assertIn("AmazingData", registry.get("amazingdata").import_modules)
        self.assertIn("codes", registry.get("qmt").plan_fields)
        self.assertIn("daily_kline", registry.get("amazingdata").task_names)
        self.assertIn("daily_kline", registry.get("baostock").task_names)
        self.assertIn("kline_history", registry.get("qmt").task_names)
        adjust_factor = next(task for task in registry.get("baostock").tasks if task.name == "adjust_factor")
        self.assertEqual(adjust_factor.freshness_mode, "event_driven")

    def test_provider_root_points_to_structured_provider_dir(self) -> None:
        root = resolve_provider_root()
        self.assertTrue((root / "qmt" / "provider.toml").is_file())
        self.assertTrue((root / "qmt" / "plans" / "sample.toml").is_file())

    def test_provider_paths_export_provider_objects(self) -> None:
        from sync_data_system.providers.qmt.provider import normalize_qmt_code
        from sync_data_system.providers.baostock.provider import normalize_baostock_code

        self.assertEqual(normalize_qmt_code("sh.600000"), "600000.SH")
        self.assertEqual(normalize_baostock_code("sh.600000"), "600000.SH")

    def test_provider_manifest_validator_requires_incremental_cursor(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "provider.toml"
            path.write_text(
                textwrap.dedent(
                    """
                    name = "bad"
                    module = "sync_data_system.providers.qmt"
                    plan_fields = ["codes"]

                    [entrypoints]
                    config_runner = "runner:run_config_file"

                    [[tasks]]
                    name = "daily"
                    target = "daily_table"
                    supports_incremental = true
                    """
                ),
                encoding="utf-8",
            )
            manifest = load_provider_manifest(path)
            with self.assertRaises(ValueError):
                validate_manifest(manifest, load_entrypoints=False)

    def test_registry_config_runner_can_dispatch_qmt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "qmt.toml"
            path.write_text(
                textwrap.dedent(
                    """
                    source = "qmt"
                    [[tasks]]
                    task = "kline_history"
                    codes = ["600000.SH"]
                    begin_date = 20240101
                    end_date = 20240131
                    """
                ),
                encoding="utf-8",
            )

            self.assertEqual(detect_config_source(str(path)), "qmt")

    def test_unknown_provider_config_fails_before_runner(self) -> None:
        with self.assertRaises(ValueError):
            run_provider_config_file(source="missing", config_path="missing.toml")


if __name__ == "__main__":
    unittest.main()
