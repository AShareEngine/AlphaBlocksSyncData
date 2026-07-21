#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from sync_data_system.service.sync_config_manager import SyncConfigManager


class SyncConfigManagerTest(unittest.TestCase):
    def test_cross_provider_tasks_are_derived_and_duplicates_are_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            manager = SyncConfigManager(root)
            config = manager.create_config(
                {
                    "name": "跨源日线",
                    "tasks": [
                        {
                            "id": "first",
                            "name": "amazingdata.daily_kline",
                            "provider": "amazingdata",
                            "date_mode": "incremental",
                        },
                        {"id": "second", "name": "baostock.daily_kline"},
                        {"id": "third", "name": "baostock.daily_kline"},
                    ],
                },
                config_id="sync_config_daily",
            )

            self.assertEqual([item["name"] for item in config["tasks"]].count("baostock.daily_kline"), 2)
            self.assertEqual(config["tasks"][0]["database"], "starlight")
            self.assertEqual(config["tasks"][1]["database"], "baostock")
            self.assertTrue((root / ".service_state" / "sync_configs" / "sync_config_daily.json").is_file())

            reloaded = SyncConfigManager(root)
            self.assertEqual(reloaded.get_config("sync_config_daily")["tasks"], config["tasks"])

    def test_names_are_unique_and_ids_are_immutable(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = SyncConfigManager(Path(tmpdir))
            payload = {"name": "每日同步", "tasks": [{"name": "baostock.daily_kline"}]}
            manager.create_config(payload, config_id="sync_config_one")
            with self.assertRaisesRegex(ValueError, "already exists"):
                manager.create_config(payload, config_id="sync_config_two")

            updated = manager.update_config(
                "sync_config_one",
                {"id": "attempted_change", "description": "已修改"},
            )
            self.assertEqual(updated["id"], "sync_config_one")
            self.assertEqual(updated["description"], "已修改")

    def test_unsupported_task_parameters_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = SyncConfigManager(Path(tmpdir))
            with self.assertRaisesRegex(ValueError, "unsupported parameters"):
                manager.create_config(
                    {
                        "name": "错误参数",
                        "tasks": [
                            {
                                "name": "baostock.daily_kline",
                                "parameters": {"made_up": 1},
                            }
                        ],
                    }
                )


if __name__ == "__main__":
    unittest.main()
