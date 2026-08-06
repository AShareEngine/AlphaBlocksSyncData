#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from sync_data_system.service.task_registry import TASK_REGISTRY, create_probe


class _FakeBaseData:
    def get_hist_code_list(self, security_type: str, start_date: int, end_date: int, local_path: str):
        return {
            "EXTRA_STOCK_A": ["000001.SZ", "600000.SH", "000005.SZ"],
            "EXTRA_INDEX_A": ["000300.SH"],
            "EXTRA_ETF": ["510300.SH"],
            "EXTRA_ETF_OP": ["10000001.SH"],
            "EXTRA_KZZ": ["110030.SH"],
        }[security_type]

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
        self.assertIn("amazingdata.daily_kline", tasks)
        self.assertIn("amazingdata.minute_kline", tasks)
        self.assertEqual(tasks["amazingdata.daily_kline"].input_resolver, "market_kline_defaults")

    def test_registry_metadata_contains_source_target(self) -> None:
        metadata = {item["name"]: item for item in TASK_REGISTRY.list_task_metadata()}
        self.assertIn("amazingdata.daily_kline", metadata)
        self.assertEqual(metadata["amazingdata.daily_kline"]["source"], "amazingdata")
        self.assertEqual(metadata["amazingdata.daily_kline"]["database"], "starlight")
        self.assertEqual(metadata["amazingdata.daily_kline"]["target"], "ad_market_kline_daily")
        self.assertEqual(metadata["amazingdata.daily_kline"]["incremental_scope"], "code")
        self.assertIn("universe_mode", metadata["amazingdata.daily_kline"]["request_fields"])
        self.assertIn("request_fields", metadata["amazingdata.daily_kline"])
        self.assertIn("probe_fields", metadata["amazingdata.daily_kline"])

    def test_registry_metadata_contains_baostock_tasks(self) -> None:
        metadata = {item["name"]: item for item in TASK_REGISTRY.list_task_metadata()}
        self.assertIn("baostock.daily_kline", metadata)
        self.assertEqual(metadata["baostock.daily_kline"]["source"], "baostock")
        self.assertEqual(metadata["baostock.daily_kline"]["database"], "baostock")
        self.assertEqual(metadata["baostock.daily_kline"]["target"], "bs_daily_kline")
        self.assertEqual(metadata["baostock.daily_kline"]["incremental_scope"], "code")
        self.assertIn("frequency", metadata["baostock.daily_kline"]["request_fields"])
        self.assertIn("universe_mode", metadata["baostock.daily_kline"]["request_fields"])
        self.assertIn("continue_on_error", metadata["baostock.daily_kline"]["request_fields"])

    def test_registry_metadata_contains_qmt_tasks(self) -> None:
        metadata = {item["name"]: item for item in TASK_REGISTRY.list_task_metadata()}
        self.assertIn("qmt.kline_history", metadata)
        self.assertEqual(metadata["qmt.kline_history"]["source"], "qmt")
        self.assertEqual(metadata["qmt.kline_history"]["database"], "qmt")
        self.assertEqual(metadata["qmt.kline_history"]["target"], "qmt_kline_history")
        self.assertEqual(metadata["qmt.kline_history"]["incremental_scope"], "code")
        self.assertIn("codes", metadata["qmt.kline_history"]["request_fields"])
        self.assertIn("period", metadata["qmt.kline_history"]["request_fields"])

    def test_registry_metadata_contains_akshare_ths_concept_tasks(self) -> None:
        metadata = {item["name"]: item for item in TASK_REGISTRY.list_task_metadata()}
        index = metadata["akshare.stock_board_concept_index_ths"]
        info = metadata["akshare.stock_board_concept_info_ths"]

        self.assertEqual(index["target"], "ak_stock_board_concept_index_ths")
        self.assertEqual(index["incremental_scope"], "code")
        self.assertEqual(index["cursor_field"], "trade_date")
        self.assertIn("codes", index["request_fields"])
        self.assertEqual(info["target"], "ak_stock_board_concept_info_ths")

    def test_manifest_provider_task_handler_uses_provider_runner(self) -> None:
        probe = create_probe(
            task_name="qmt.kline_history",
            job_id="job1",
            project_root=Path("."),
            log_path=Path("job1.log"),
            codes=["600000.SH"],
            begin_date=20240101,
            end_date=20240131,
        )
        probe.context = SimpleNamespace(provider=object(), repository=object())
        definition = TASK_REGISTRY.get_task("qmt.kline_history")

        calls = []

        def fake_runner(value):
            calls.append(value)
            value.set_row_count(7)
            return 7

        with patch("sync_data_system.core.providers.ProviderManifest.load_registered_task_runner", return_value=fake_runner):
            result = definition.handler(probe)

        self.assertEqual(result, 7)
        self.assertEqual(probe.row_count, 7)
        self.assertEqual(calls, [probe])

    def test_market_kline_defaults_resolver_populates_probe(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            probe = create_probe(
                task_name="amazingdata.daily_kline",
                job_id="job1",
                project_root=Path(tmpdir),
                log_path=Path(tmpdir) / "job1.log",
                begin_date=20240101,
                end_date=20240131,
            )
            probe.context = SimpleNamespace(
                base_data=_FakeBaseData(),
                provider=_FakeProvider(),
                sdk_config=SimpleNamespace(local_path="/tmp/amazing_data_cache"),
            )

            TASK_REGISTRY.resolve_inputs(probe)

        self.assertEqual(probe.begin_date, 20240101)
        self.assertEqual(probe.end_date, 20240131)
        self.assertEqual(
            probe.codes,
            [
                "000001.SZ",
                "600000.SH",
                "000005.SZ",
                "000300.SH",
                "510300.SH",
                "10000001.SH",
                "110030.SH",
            ],
        )

    def test_historical_market_resolver_defers_code_pool_to_provider_runner(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            probe = create_probe(
                task_name="amazingdata.daily_kline",
                job_id="job1",
                project_root=Path(tmpdir),
                log_path=Path(tmpdir) / "job1.log",
                begin_date=20100101,
                end_date=20240131,
                universe_mode="historical",
            )
            probe.context = SimpleNamespace(
                base_data=_FakeBaseData(),
                provider=_FakeProvider(),
            )

            TASK_REGISTRY.resolve_inputs(probe)

        self.assertEqual(probe.begin_date, 20100101)
        self.assertEqual(probe.end_date, 20240131)
        self.assertEqual(probe.codes, [])


if __name__ == "__main__":
    unittest.main()
