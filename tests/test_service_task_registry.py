#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from sync_data_system.service.task_registry import TASK_REGISTRY, create_probe


class _FakeBaseData:
    def get_stock_universe(self, security_type: str, force: bool = False):
        return ["000001.SZ", "600000.SH"]

    def get_index_universe(self, security_type: str, force: bool = False):
        return ["000300.SH"]

    def get_etf_universe(self, security_type: str, force: bool = False):
        return ["510300.SH"]


class _FakeProviderSession:
    def get_latest_trade_date(self):
        from datetime import date

        return date(2024, 1, 31)


class _FakeProvider:
    session = _FakeProviderSession()


class ServiceTaskRegistryTest(unittest.TestCase):
    def test_registry_contains_market_tasks(self) -> None:
        tasks = {item.name: item for item in TASK_REGISTRY.list_tasks()}
        self.assertIn("daily_kline", tasks)
        self.assertIn("minute_kline", tasks)
        self.assertEqual(tasks["daily_kline"].input_resolver, "market_kline_defaults")

    def test_registry_metadata_contains_source_target(self) -> None:
        metadata = {item["name"]: item for item in TASK_REGISTRY.list_task_metadata()}
        self.assertIn("daily_kline", metadata)
        self.assertEqual(metadata["daily_kline"]["source"], "amazingdata")
        self.assertEqual(metadata["daily_kline"]["database"], "starlight")
        self.assertEqual(metadata["daily_kline"]["target"], "ad_market_kline_daily")
        self.assertIn("request_fields", metadata["daily_kline"])
        self.assertIn("probe_fields", metadata["daily_kline"])

    def test_registry_metadata_contains_baostock_tasks(self) -> None:
        metadata = {item["name"]: item for item in TASK_REGISTRY.list_task_metadata()}
        self.assertIn("baostock.daily_kline", metadata)
        self.assertEqual(metadata["baostock.daily_kline"]["source"], "baostock")
        self.assertEqual(metadata["baostock.daily_kline"]["database"], "baostock")
        self.assertEqual(metadata["baostock.daily_kline"]["target"], "bs_daily_kline")
        self.assertIn("frequency", metadata["baostock.daily_kline"]["request_fields"])

    def test_market_kline_defaults_resolver_populates_probe(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            probe = create_probe(
                task_name="daily_kline",
                job_id="job1",
                project_root=Path(tmpdir),
                log_path=Path(tmpdir) / "job1.log",
                begin_date=20240101,
                end_date=20240131,
            )
            probe.context = SimpleNamespace(
                base_data=_FakeBaseData(),
                provider=_FakeProvider(),
            )

            TASK_REGISTRY.resolve_inputs(probe)

        self.assertEqual(probe.begin_date, 20240101)
        self.assertEqual(probe.end_date, 20240131)
        self.assertEqual(probe.codes, ["000001.SZ", "600000.SH", "000300.SH", "510300.SH"])


if __name__ == "__main__":
    unittest.main()
