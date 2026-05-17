#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path

from sync_data_system.providers.qmt.provider import iter_qmt_rows, normalize_qmt_code, normalize_qmt_code_list
from sync_data_system.providers.qmt.repository import QmtRepository
from sync_data_system.providers.qmt.runner import (
    SyncArgs,
    build_fetch_kwargs,
    build_request_meta,
    load_execution_plan_from_toml,
    resolve_effective_request_meta,
    run_sync_args,
)
from sync_data_system.providers.qmt.specs import QMT_TASK_SPECS


class _FakeClickHouseClient:
    def __init__(self) -> None:
        self.insert_calls: list[tuple[str, list[str], list[tuple]]] = []
        self.query_value_calls: list[tuple[str, dict | None]] = []
        self.query_value_result = 0

    def command(self, sql: str, parameters=None):
        return None

    def insert_rows(self, table: str, column_names, rows):
        self.insert_calls.append((table, list(column_names), list(rows)))

    def query_value(self, sql: str, parameters=None):
        self.query_value_calls.append((sql, parameters))
        return self.query_value_result


class _FakeQmtProvider:
    def __init__(self, envelope) -> None:
        self.envelope = envelope
        self.fetch_calls: list[dict] = []

    def fetch_task(self, task: str, **kwargs):
        self.fetch_calls.append({"task": task, **kwargs})
        return self.envelope


class _FakeIncrementalRepository:
    def __init__(self, latest_cursor: str | None = None) -> None:
        self.latest_cursor = latest_cursor

    def load_latest_cursor(self, task: str, *, symbol: str | None = None):
        return self.latest_cursor


class QmtProviderHelperTest(unittest.TestCase):
    def test_normalize_qmt_code(self) -> None:
        self.assertEqual(normalize_qmt_code("sh.600000"), "600000.SH")
        self.assertEqual(normalize_qmt_code("000001.sz"), "000001.SZ")
        self.assertEqual(normalize_qmt_code("IF2406.CFFEX"), "IF2406.CFFEX")

    def test_normalize_qmt_code_list_deduplicates(self) -> None:
        self.assertEqual(
            normalize_qmt_code_list(["sh.600000", "600000.SH", "sz.000001"]),
            ["600000.SH", "000001.SZ"],
        )

    def test_iter_kline_history_rows_expands_bars(self) -> None:
        envelope = {
            "success": True,
            "data": {
                "items": [
                    {
                        "symbol": "600000.SH",
                        "bars": [
                            {"time_ms": 1704038400000, "open": 8.1, "close": 8.2},
                            {"time_ms": 1704124800000, "open": 8.2, "close": 8.3},
                        ],
                    }
                ]
            },
        }

        rows = iter_qmt_rows(
            QMT_TASK_SPECS["kline_history"],
            envelope,
            {"symbol": "600000.SH", "period": "1d", "start_time": "20240101", "end_time": "20240131"},
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["symbol"], "600000.SH")
        self.assertEqual(rows[0]["period"], "1d")
        self.assertEqual(rows[0]["time_ms"], 1704038400000)

    def test_iter_sector_rows_expands_symbols(self) -> None:
        envelope = {"success": True, "data": {"items": [{"sector_name": "沪深A股", "symbols": ["sh.600000", "sz.000001"]}]}}

        rows = iter_qmt_rows(QMT_TASK_SPECS["sectors"], envelope, {"sector_name": "沪深A股"})

        self.assertEqual([row["symbol"] for row in rows], ["600000.SH", "000001.SZ"])
        self.assertEqual(rows[0]["sector_name"], "沪深A股")


class QmtRunnerTest(unittest.TestCase):
    def _args(self, **overrides) -> SyncArgs:
        data = {
            "task": "kline_history",
            "symbols_raw": "sh.600000",
            "symbol": "",
            "market": "",
            "index_code": "",
            "stock_code": "",
            "table_names_raw": "",
            "sector_name": "",
            "code_market": "",
            "begin_time": "20240101",
            "end_time": "20240131",
            "period": "1d",
            "fields_raw": "",
            "adjust_type": "none",
            "fill_data": True,
            "count": -1,
            "incrementally": False,
            "complete": False,
            "limit": 0,
            "force": True,
            "continue_on_error": False,
            "runtime_path": None,
            "database": "qmt",
            "log_level": "INFO",
        }
        data.update(overrides)
        return SyncArgs(**data)

    def test_build_request_meta_normalizes_symbols(self) -> None:
        meta = build_request_meta(self._args(symbols_raw="sh.600000,600000.SH,sz.000001"))

        self.assertEqual(meta["symbols"], ["600000.SH", "000001.SZ"])
        self.assertEqual(meta["period"], "1d")
        self.assertEqual(meta["start_time"], "20240101")

    def test_build_fetch_kwargs_for_kline(self) -> None:
        args = self._args()
        meta = build_request_meta(args)

        kwargs = build_fetch_kwargs(args, meta)

        self.assertEqual(kwargs["symbols"], ["600000.SH"])
        self.assertEqual(kwargs["start_time"], "20240101")
        self.assertEqual(kwargs["end_time"], "20240131")
        self.assertEqual(kwargs["period"], "1d")
        self.assertTrue(kwargs["fill_data"])

    def test_run_sync_args_saves_response(self) -> None:
        provider = _FakeQmtProvider(
            {"success": True, "data": {"items": [{"symbol": "600000.SH", "bars": [{"time_ms": 1704038400000, "open": 8.1}]}]}}
        )
        client = _FakeClickHouseClient()
        repository = QmtRepository(client, database="qmt")

        inserted = run_sync_args(self._args(), provider, repository)

        self.assertEqual(inserted, 1)
        self.assertEqual(provider.fetch_calls[0]["symbols"], ["600000.SH"])
        table, columns, rows = client.insert_calls[0]
        self.assertEqual(table, "qmt.qmt_kline_history")
        self.assertIn("payload_json", columns)
        row = dict(zip(columns, rows[0]))
        self.assertEqual(row["symbol"], "600000.SH")
        self.assertEqual(row["time_ms"], 1704038400000)

    def test_tick_history_keeps_intraday_time_window(self) -> None:
        args = self._args(
            task="tick_history",
            begin_time="20240101093000",
            end_time="20240101150000",
        )
        meta = build_request_meta(args)

        effective = resolve_effective_request_meta(args, _FakeIncrementalRepository(), meta)

        self.assertIsNotNone(effective)
        self.assertEqual(effective["start_time"], "20240101093000")
        self.assertEqual(effective["end_time"], "20240101150000")

    def test_load_execution_plan_from_toml(self) -> None:
        content = textwrap.dedent(
            """
            source = "qmt"
            log_level = "INFO"
            continue_on_error = true
            database = "qmt"

            [defaults]
            codes = ["600000.SH"]
            begin_date = 20240101
            end_date = 20240131

            [[tasks]]
            task = "kline_history"
            period = "1d"

            [[tasks]]
            task = "sectors"
            sector_name = "沪深A股"
            """
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "cfg.toml"
            path.write_text(content, encoding="utf-8")
            plan = load_execution_plan_from_toml(str(path))

        self.assertEqual(plan.database, "qmt")
        self.assertEqual(len(plan.tasks), 2)
        self.assertEqual(plan.tasks[0].task, "kline_history")
        self.assertEqual(plan.tasks[0].symbols_raw, "600000.SH")
        self.assertEqual(plan.tasks[0].begin_time, "20240101")
        self.assertEqual(plan.tasks[1].sector_name, "沪深A股")


if __name__ == "__main__":
    unittest.main()
