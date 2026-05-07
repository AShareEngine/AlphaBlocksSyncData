#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

from sync_data_system.sources.baostock.provider import normalize_baostock_code, normalize_baostock_code_list, to_baostock_code
from sync_data_system.sources.baostock.repository import BaoStockRepository
from sync_data_system.sources.baostock.runner import SyncArgs, resolve_all_stock_request_meta, resolve_code_list, resolve_effective_request_meta, run_code_task, run_sync_args
from sync_data_system.sources.baostock.specs import BAOSTOCK_TASK_SPECS, camel_to_snake, table_columns_for_spec
from sync_data_system.sync_core.incremental import advance_cursor_value, normalize_request_value


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


class _FakeIncrementalRepository:
    def __init__(self, latest_cursor: str | None = None, has_request_data: bool = False) -> None:
        self.latest_cursor = latest_cursor
        self.has_request_data = has_request_data

    def load_latest_cursor(self, task: str, *, code: str | None = None):
        return self.latest_cursor

    def has_task_data_for_request(self, task: str, request_meta):
        return self.has_request_data


class _FakeBaoStockProvider:
    def __init__(self, codes_by_day: dict[str, list[str]] | None = None, latest_trading_day: str | None = None) -> None:
        self.codes_by_day = codes_by_day or {}
        self.latest_trading_day = latest_trading_day
        self.fetch_all_stock_calls: list[str | None] = []
        self.resolve_latest_trading_day_calls: list[str | None] = []
        self.fetch_latest_all_stock_calls: list[str | None] = []

    def fetch_all_stock_codes(self, day: str | None = None) -> list[str]:
        self.fetch_all_stock_calls.append(day)
        return list(self.codes_by_day.get(str(day), []))

    def resolve_latest_trading_day(self, day: str | None = None, *, lookback_days: int = 30) -> str | None:
        self.resolve_latest_trading_day_calls.append(day)
        return self.latest_trading_day

    def fetch_latest_all_stock_codes(self, day: str | None = None, *, lookback_days: int = 30):
        self.fetch_latest_all_stock_calls.append(day)
        candidate_days = [str(day)]
        if self.latest_trading_day and self.latest_trading_day not in candidate_days:
            candidate_days.append(self.latest_trading_day)
        for candidate_day in candidate_days:
            codes = self.fetch_all_stock_codes(candidate_day)
            if codes:
                return candidate_day, codes
        return None, []


class _FakeRunRepository:
    def __init__(self, latest_cursor: str | None = None) -> None:
        self.latest_cursor = latest_cursor
        self.saved_request_meta: list[dict] = []
        self.fetch_logs: list[object] = []

    def load_latest_cursor(self, task: str, *, code: str | None = None):
        return self.latest_cursor

    def has_task_data_for_request(self, task: str, request_meta):
        return False

    def has_successful_sync_today(self, task_name: str, scope_key: str, run_date) -> bool:
        return False

    def save_task_frame(self, task: str, frame, *, request_meta):
        self.saved_request_meta.append(dict(request_meta))
        return int(len(frame.index))

    def insert_sync_log(self, row) -> None:
        self.fetch_logs.append(row)


class _FakeRunProvider:
    def __init__(self, frame) -> None:
        self.frame = frame
        self.fetch_calls: list[dict] = []

    def fetch_dataframe(self, task: str, **kwargs):
        self.fetch_calls.append({"task": task, **kwargs})
        return self.frame


class BaoStockCodeNormalizeTest(unittest.TestCase):
    def test_normalize_baostock_code(self) -> None:
        self.assertEqual(normalize_baostock_code("sh.600000"), "600000.SH")
        self.assertEqual(normalize_baostock_code("sz.000001"), "000001.SZ")
        self.assertEqual(normalize_baostock_code("600000.SH"), "600000.SH")

    def test_to_baostock_code(self) -> None:
        self.assertEqual(to_baostock_code("600000.SH"), "sh.600000")
        self.assertEqual(to_baostock_code("000001.SZ"), "sz.000001")
        self.assertEqual(to_baostock_code("sh.600000"), "sh.600000")

    def test_normalize_baostock_code_list_deduplicates(self) -> None:
        self.assertEqual(
            normalize_baostock_code_list(["sh.600000", "600000.SH", "sz.000001"]),
            ["600000.SH", "000001.SZ"],
        )


class BaoStockSpecTest(unittest.TestCase):
    def test_camel_to_snake(self) -> None:
        self.assertEqual(camel_to_snake("dividOperateDate"), "divid_operate_date")
        self.assertEqual(camel_to_snake("isST"), "is_st")

    def test_code_task_columns_skip_source_code_and_ingested_at(self) -> None:
        columns = table_columns_for_spec(BAOSTOCK_TASK_SPECS["stock_basic"])
        self.assertIn("code", columns)
        self.assertNotIn("source_code", columns)
        self.assertNotIn("ingested_at", columns)

    def test_daily_kline_columns_skip_request_window_source_code_and_ingested_at(self) -> None:
        columns = table_columns_for_spec(BAOSTOCK_TASK_SPECS["daily_kline"])
        self.assertIn("date", columns)
        self.assertIn("code", columns)
        self.assertNotIn("source_code", columns)
        self.assertNotIn("request_start_date", columns)
        self.assertNotIn("request_end_date", columns)
        self.assertNotIn("ingested_at", columns)

    def test_quarterly_finance_columns_skip_request_year_quarter_source_code_and_ingested_at(self) -> None:
        columns = table_columns_for_spec(BAOSTOCK_TASK_SPECS["profit_data"])
        self.assertIn("code", columns)
        self.assertIn("pub_date", columns)
        self.assertIn("stat_date", columns)
        self.assertNotIn("source_code", columns)
        self.assertNotIn("request_year", columns)
        self.assertNotIn("request_quarter", columns)
        self.assertNotIn("ingested_at", columns)

    def test_all_business_tables_skip_request_window_source_code_and_ingested_at(self) -> None:
        forbidden_columns = {"request_start_date", "request_end_date", "source_code", "ingested_at"}
        for task, spec in BAOSTOCK_TASK_SPECS.items():
            with self.subTest(task=task):
                columns = set(table_columns_for_spec(spec))
                self.assertFalse(columns & forbidden_columns)


class BaoStockIncrementalHelperTest(unittest.TestCase):
    def test_normalize_request_value(self) -> None:
        self.assertEqual(normalize_request_value("2024-01-31", "day"), "20240131")
        self.assertEqual(normalize_request_value("2024-01", "month"), "202401")
        self.assertEqual(normalize_request_value("2024", "year"), "2024")

    def test_advance_cursor_value(self) -> None:
        self.assertEqual(advance_cursor_value("2024-01-31", "day"), "20240201")
        self.assertEqual(advance_cursor_value("2024-12", "month"), "202501")
        self.assertEqual(advance_cursor_value("2024", "year"), "2025")

    def test_resolve_effective_request_meta_pushes_begin_date_forward(self) -> None:
        args = SyncArgs(
            task="daily_kline",
            codes_raw="",
            begin_date="20240101",
            end_date="20240131",
            day="",
            year=None,
            quarter=None,
            year_type="",
            adjustflag="3",
            frequency="d",
            limit=0,
            force=False,
            continue_on_error=False,
            runtime_path=None,
            database="baostock",
            log_level="INFO",
        )
        request_meta = {
            "code": "600000.SH",
            "start_date": "20240101",
            "end_date": "20240131",
            "day": "",
            "year": None,
            "quarter": None,
            "year_type": "",
        }

        effective = resolve_effective_request_meta(
            args,
            _FakeIncrementalRepository(latest_cursor="2024-01-10"),
            request_meta,
        )

        self.assertIsNotNone(effective)
        self.assertEqual(effective["start_date"], "20240111")
        self.assertEqual(effective["end_date"], "20240131")

    def test_resolve_effective_request_meta_skips_when_request_already_exists(self) -> None:
        args = SyncArgs(
            task="stock_basic",
            codes_raw="",
            begin_date="",
            end_date="",
            day="",
            year=None,
            quarter=None,
            year_type="",
            adjustflag="3",
            frequency="d",
            limit=0,
            force=False,
            continue_on_error=False,
            runtime_path=None,
            database="baostock",
            log_level="INFO",
        )
        request_meta = {
            "code": "600000.SH",
            "start_date": "",
            "end_date": "",
            "day": "",
            "year": None,
            "quarter": None,
            "year_type": "",
        }

        effective = resolve_effective_request_meta(
            args,
            _FakeIncrementalRepository(latest_cursor=None, has_request_data=True),
            request_meta,
        )

        self.assertIsNone(effective)

    def test_resolve_code_list_falls_back_to_latest_trading_day(self) -> None:
        args = SyncArgs(
            task="daily_kline",
            codes_raw="",
            begin_date="20100101",
            end_date="20260426",
            day="",
            year=None,
            quarter=None,
            year_type="",
            adjustflag="3",
            frequency="d",
            limit=0,
            force=False,
            continue_on_error=False,
            runtime_path=None,
            database="baostock",
            log_level="INFO",
        )
        provider = _FakeBaoStockProvider(
            codes_by_day={
                "20260426": [],
                "20260424": ["600000.SH", "000001.SZ"],
            },
            latest_trading_day="20260424",
        )

        codes = resolve_code_list(provider, args)

        self.assertEqual(codes, ["600000.SH", "000001.SZ"])
        self.assertEqual(provider.fetch_all_stock_calls, ["20260426", "20260424"])
        self.assertEqual(provider.fetch_latest_all_stock_calls, ["20260426"])

    def test_resolve_all_stock_request_meta_falls_back_to_latest_populated_universe_day(self) -> None:
        args = SyncArgs(
            task="all_stock",
            codes_raw="",
            begin_date="",
            end_date="",
            day="",
            year=None,
            quarter=None,
            year_type="",
            adjustflag="3",
            frequency="d",
            limit=0,
            force=False,
            continue_on_error=False,
            runtime_path=None,
            database="baostock",
            log_level="INFO",
        )
        provider = _FakeBaoStockProvider(
            codes_by_day={
                "20260428": [],
                "20260424": ["600000.SH"],
            },
            latest_trading_day="20260424",
        )

        request_meta = resolve_all_stock_request_meta(args, provider, {"day": "20260428"})

        self.assertEqual(request_meta["day"], "20260424")
        self.assertEqual(provider.fetch_all_stock_calls, ["20260428", "20260424"])

    def test_run_sync_args_raises_when_code_task_has_no_codes(self) -> None:
        args = SyncArgs(
            task="daily_kline",
            codes_raw="",
            begin_date="20100101",
            end_date="",
            day="",
            year=None,
            quarter=None,
            year_type="",
            adjustflag="3",
            frequency="d",
            limit=0,
            force=False,
            continue_on_error=False,
            runtime_path=None,
            database="baostock",
            log_level="INFO",
        )
        provider = _FakeBaoStockProvider(codes_by_day={"20260426": []}, latest_trading_day=None)

        with self.assertRaises(ValueError) as context:
            run_sync_args(args, provider, _FakeIncrementalRepository())

        self.assertIn("未获取到可用股票代码", str(context.exception))


@unittest.skipIf(pd is None, "pandas is required")
class BaoStockRepositoryTest(unittest.TestCase):
    def test_save_task_frame_normalizes_code_column(self) -> None:
        client = _FakeClickHouseClient()
        repository = BaoStockRepository(client, database="baostock")
        frame = pd.DataFrame([{"code": "sh.600000", "tradeStatus": "1", "code_name": "浦发银行"}])

        inserted = repository.save_task_frame(
            "all_stock",
            frame,
            request_meta={"day": "20240110", "start_date": "", "end_date": "", "year": None, "quarter": None, "year_type": ""},
        )

        self.assertEqual(inserted, 1)
        table, columns, rows = client.insert_calls[0]
        self.assertEqual(table, "baostock.bs_all_stock")
        self.assertIn("code", columns)
        self.assertNotIn("source_code", columns)
        row = dict(zip(columns, rows[0]))
        self.assertEqual(row["code"], "600000.SH")
        self.assertEqual(row["query_date"], "20240110")
        self.assertNotIn("ingested_at", row)

    def test_save_daily_kline_skips_request_window_and_ingested_at_columns(self) -> None:
        client = _FakeClickHouseClient()
        repository = BaoStockRepository(client, database="baostock")
        frame = pd.DataFrame([{
            "date": "2024-01-10",
            "code": "sz.000001",
            "open": "10",
            "high": "11",
            "low": "9",
            "close": "10.5",
            "preclose": "9.8",
            "volume": "100",
            "amount": "1000",
            "adjustflag": "3",
            "turn": "1",
            "tradestatus": "1",
            "pctChg": "1.2",
            "peTTM": "8",
            "pbMRQ": "1",
            "psTTM": "2",
            "pcfNcfTTM": "3",
            "isST": "0",
        }])

        inserted = repository.save_task_frame(
            "daily_kline",
            frame,
            request_meta={"code": "000001.SZ", "start_date": "20240101", "end_date": "20240131"},
        )

        self.assertEqual(inserted, 1)
        table, columns, rows = client.insert_calls[0]
        self.assertEqual(table, "baostock.bs_daily_kline")
        self.assertNotIn("request_start_date", columns)
        self.assertNotIn("request_end_date", columns)
        self.assertNotIn("source_code", columns)
        self.assertNotIn("ingested_at", columns)
        row = dict(zip(columns, rows[0]))
        self.assertEqual(row["code"], "000001.SZ")

    def test_save_quarterly_finance_skips_request_year_quarter_source_code_and_ingested_at_columns(self) -> None:
        client = _FakeClickHouseClient()
        repository = BaoStockRepository(client, database="baostock")
        frame = pd.DataFrame([{
            "code": "sh.600000",
            "pubDate": "2024-04-30",
            "statDate": "2024-03-31",
            "roeAvg": "1",
            "npMargin": "2",
            "gpMargin": "3",
            "netProfit": "4",
            "epsTTM": "5",
            "MBRevenue": "6",
            "totalShare": "7",
            "liqaShare": "8",
        }])

        inserted = repository.save_task_frame(
            "profit_data",
            frame,
            request_meta={"code": "600000.SH", "year": 2024, "quarter": 1},
        )

        self.assertEqual(inserted, 1)
        table, columns, rows = client.insert_calls[0]
        self.assertEqual(table, "baostock.bs_profit_data")
        self.assertNotIn("request_year", columns)
        self.assertNotIn("request_quarter", columns)
        self.assertNotIn("source_code", columns)
        self.assertNotIn("ingested_at", columns)
        row = dict(zip(columns, rows[0]))
        self.assertEqual(row["code"], "600000.SH")


@unittest.skipIf(pd is None, "pandas is required")
class BaoStockRunnerExecutionTest(unittest.TestCase):
    def test_run_code_task_uses_effective_incremental_window_for_fetch(self) -> None:
        args = SyncArgs(
            task="daily_kline",
            codes_raw="",
            begin_date="20240101",
            end_date="20240131",
            day="",
            year=None,
            quarter=None,
            year_type="",
            adjustflag="3",
            frequency="d",
            limit=0,
            force=False,
            continue_on_error=False,
            runtime_path=None,
            database="baostock",
            log_level="INFO",
        )
        provider = _FakeRunProvider(pd.DataFrame([{"date": "2024-01-11", "code": "sh.600000"}]))
        repository = _FakeRunRepository(latest_cursor="2024-01-10")

        inserted = run_code_task(args, provider, repository, ["600000.SH"])

        self.assertEqual(inserted, 1)
        self.assertEqual(len(provider.fetch_calls), 1)
        self.assertEqual(provider.fetch_calls[0]["code"], "600000.SH")
        self.assertEqual(provider.fetch_calls[0]["start_date"], "20240111")
        self.assertEqual(provider.fetch_calls[0]["end_date"], "20240131")
        self.assertEqual(repository.saved_request_meta[0]["start_date"], "20240111")


if __name__ == "__main__":
    unittest.main()
