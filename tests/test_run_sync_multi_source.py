#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path

from sync_data_system.providers.amazingdata.runner import detect_config_source
from sync_data_system.providers.baostock.runner import load_execution_plan_from_toml
from sync_data_system.providers.qmt.runner import load_execution_plan_from_toml as load_qmt_execution_plan_from_toml


class RunSyncMultiSourceTest(unittest.TestCase):
    def test_detect_config_source_defaults_to_amazingdata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "cfg.toml"
            path.write_text("log_level = 'INFO'\n[[tasks]]\ntask = 'code_info'\n", encoding="utf-8")
            self.assertEqual(detect_config_source(str(path)), "amazingdata")

    def test_detect_config_source_baostock(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "cfg.toml"
            path.write_text("source = 'baostock'\n[[tasks]]\ntask = 'all_stock'\n", encoding="utf-8")
            self.assertEqual(detect_config_source(str(path)), "baostock")

    def test_detect_config_source_qmt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "cfg.toml"
            path.write_text("source = 'qmt'\n[[tasks]]\ntask = 'sectors'\n", encoding="utf-8")
            self.assertEqual(detect_config_source(str(path)), "qmt")

    def test_load_baostock_execution_plan_from_toml(self) -> None:
        content = textwrap.dedent(
            """
            source = "baostock"
            log_level = "INFO"
            continue_on_error = true
            database = "baostock"

            [defaults]
            force = false
            limit = 10
            continue_on_error = true

            [[tasks]]
            task = "all_stock"
            day = 20240110

            [[tasks]]
            task = "daily_kline"
            begin_date = 20240101
            end_date = 20240131
            enabled = true
            """
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "cfg.toml"
            path.write_text(content, encoding="utf-8")
            plan = load_execution_plan_from_toml(str(path))

        self.assertEqual(plan.database, "baostock")
        self.assertEqual(plan.log_level, "INFO")
        self.assertEqual(len(plan.tasks), 2)
        self.assertEqual(plan.tasks[0].task, "all_stock")
        self.assertEqual(plan.tasks[0].day, "20240110")
        self.assertEqual(plan.tasks[1].task, "daily_kline")
        self.assertEqual(plan.tasks[1].begin_date, "20240101")
        self.assertEqual(plan.tasks[1].end_date, "20240131")

    def test_load_qmt_execution_plan_from_toml(self) -> None:
        content = textwrap.dedent(
            """
            source = "qmt"
            log_level = "INFO"
            continue_on_error = true
            database = "qmt"

            [defaults]
            symbols = ["600000.SH"]

            [[tasks]]
            task = "kline_history"
            begin_date = 20240101
            end_date = 20240131
            period = "1d"
            """
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "cfg.toml"
            path.write_text(content, encoding="utf-8")
            plan = load_qmt_execution_plan_from_toml(str(path))

        self.assertEqual(plan.database, "qmt")
        self.assertEqual(plan.tasks[0].task, "kline_history")
        self.assertEqual(plan.tasks[0].symbols_raw, "600000.SH")


if __name__ == "__main__":
    unittest.main()
