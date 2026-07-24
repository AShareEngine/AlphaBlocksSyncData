#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import tempfile
import textwrap
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from sync_data_system.providers.yfinance.provider import YFinanceConfig, YFinanceProvider
from sync_data_system.providers.yfinance.repository import YFinanceRepository
from sync_data_system.providers.yfinance.runner import SyncArgs, load_execution_plan_from_toml, run_sync_args
from sync_data_system.providers.yfinance.specs import CONCEPT_DEFINITIONS


class _FakeFinanceDatabase:
    class Equities:
        def select(self):
            return pd.DataFrame(
                [
                    {
                        "symbol": "AAPL",
                        "name": "Apple Inc.",
                        "currency": "USD",
                        "sector": "Technology",
                        "industry_group": "Technology Hardware",
                        "industry": "Consumer Electronics",
                        "exchange": "NMS",
                        "market": "NASDAQ Global Select",
                    },
                    {
                        "symbol": "IBM",
                        "name": "International Business Machines",
                        "currency": "USD",
                        "sector": "Technology",
                        "industry": "Information Technology Services",
                        "exchange": "NYQ",
                        "market": "New York Stock Exchange",
                    },
                    {
                        "symbol": "SHEL.L",
                        "name": "Shell plc",
                        "currency": "GBP",
                        "sector": "Energy",
                        "exchange": "LSE",
                        "market": "London Stock Exchange",
                    },
                ]
            ).set_index("symbol")


class _FakeFundsData:
    @property
    def top_holdings(self):
        return pd.DataFrame(
            {
                "Name": ["NVIDIA Corp", "Microsoft Corp"],
                "Holding Percent": [0.085, 0.074],
            },
            index=pd.Index(["NVDA", "MSFT"], name="Symbol"),
        )


class _FakeTicker:
    funds_data = _FakeFundsData()


class _FakeYFinance:
    def __init__(self) -> None:
        self.download_calls: list[dict] = []

    def download(self, **kwargs):
        self.download_calls.append(kwargs)
        symbols = kwargs["tickers"]
        index = pd.to_datetime(["2024-01-02", "2024-01-03"])
        columns = []
        values = {}
        for symbol in symbols:
            for field, data in {
                "Open": [100.0, 102.0],
                "High": [103.0, 104.0],
                "Low": [99.0, 101.0],
                "Close": [102.0, 103.0],
                "Adj Close": [101.5, 102.5],
                "Volume": [1000, 1100],
                "Dividends": [0.0, 0.25],
                "Stock Splits": [0.0, 0.0],
                "Capital Gains": [0.0, 0.0],
            }.items():
                key = (symbol, field)
                columns.append(key)
                values[key] = data
        return pd.DataFrame(values, index=index, columns=pd.MultiIndex.from_tuples(columns))

    def Ticker(self, symbol: str):
        return _FakeTicker()


class _FakeClickHouseClient:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.insert_calls: list[tuple[str, list[str], list[tuple]]] = []
        self.query_value_result = None

    def command(self, sql: str, parameters=None):
        self.commands.append(sql)

    def insert_rows(self, table: str, column_names, rows):
        self.insert_calls.append((table, list(column_names), list(rows)))

    def query_value(self, sql: str, parameters=None):
        return self.query_value_result

    def query_rows(self, sql: str, parameters=None):
        return []


class YFinanceProviderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.yf = _FakeYFinance()
        self.provider = YFinanceProvider(
            YFinanceConfig(batch_size=2),
            yfinance_module=self.yf,
            finance_database_module=_FakeFinanceDatabase,
        )

    def test_symbol_master_keeps_main_us_exchanges(self) -> None:
        frame = self.provider.fetch_symbol_master(snapshot_date=date(2024, 1, 5))

        self.assertEqual(frame["symbol"].tolist(), ["AAPL", "IBM"])
        self.assertEqual(frame.loc[frame["symbol"] == "AAPL", "name"].iloc[0], "Apple Inc.")
        self.assertTrue((frame["source"] == "financedatabase").all())

    def test_daily_download_normalizes_multi_index_and_inclusive_end(self) -> None:
        frame = self.provider.fetch_daily(
            ["AAPL", "MSFT"],
            start_date="20240102",
            end_date="20240103",
        )

        self.assertEqual(len(frame), 4)
        self.assertEqual(sorted(frame["symbol"].unique().tolist()), ["AAPL", "MSFT"])
        self.assertEqual(frame["trade_date"].min(), date(2024, 1, 2))
        self.assertEqual(self.yf.download_calls[0]["end"], "2024-01-04")
        self.assertFalse(self.yf.download_calls[0]["auto_adjust"])

    def test_corporate_actions_only_keeps_non_zero_events(self) -> None:
        frame = self.provider.fetch_corporate_actions(
            ["AAPL"],
            start_date="20240102",
            end_date="20240103",
        )

        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["dividend"], 0.25)
        self.assertEqual(frame.iloc[0]["event_date"], date(2024, 1, 3))
        self.assertEqual(frame.attrs["coverage_by_symbol"]["AAPL"], date(2024, 1, 3))

    def test_concept_membership_is_labeled_top_holdings(self) -> None:
        frame = self.provider.fetch_concept_membership(
            CONCEPT_DEFINITIONS[:1],
            snapshot_date=date(2024, 1, 5),
        )

        self.assertEqual(set(frame["symbol"]), {"NVDA", "MSFT"})
        self.assertEqual(set(frame["membership_scope"]), {"top_holdings"})
        self.assertEqual(set(frame["etf_symbol"]), {"AIQ", "BOTZ", "ROBO"})


class YFinanceRepositoryTest(unittest.TestCase):
    def test_save_daily_frame_uses_typed_table(self) -> None:
        client = _FakeClickHouseClient()
        repository = YFinanceRepository(client, database="yfinance")
        frame = pd.DataFrame(
            [
                {
                    "symbol": "AAPL",
                    "trade_date": date(2024, 1, 2),
                    "open": 100.0,
                    "high": 103.0,
                    "low": 99.0,
                    "close": 102.0,
                    "adj_close": 101.5,
                    "volume": 1000.0,
                    "dividends": 0.0,
                    "stock_splits": 0.0,
                    "capital_gains": 0.0,
                    "source": "yfinance",
                    "fetched_at": pd.Timestamp("2024-01-03T00:00:00"),
                }
            ]
        )

        inserted = repository.save_frame("daily_kline", frame)

        self.assertEqual(inserted, 1)
        table, columns, rows = client.insert_calls[0]
        self.assertEqual(table, "yfinance.yf_daily_kline")
        self.assertEqual(dict(zip(columns, rows[0]))["symbol"], "AAPL")

    def test_ensure_tables_creates_all_task_tables(self) -> None:
        client = _FakeClickHouseClient()
        repository = YFinanceRepository(client, database="yfinance")

        repository.ensure_tables()

        ddl = "\n".join(client.commands)
        self.assertIn("yf_symbol_master", ddl)
        self.assertIn("yf_daily_kline", ddl)
        self.assertIn("yf_concept_membership", ddl)


class YFinanceRunnerTest(unittest.TestCase):
    def test_daily_task_writes_data_cursor_and_sync_log(self) -> None:
        provider = YFinanceProvider(
            YFinanceConfig(batch_size=10),
            yfinance_module=_FakeYFinance(),
            finance_database_module=_FakeFinanceDatabase,
        )
        client = _FakeClickHouseClient()
        repository = YFinanceRepository(client, database="yfinance")
        args = SyncArgs(
            task="daily_kline",
            codes_raw="AAPL",
            begin_date="20240102",
            end_date="20240110",
            limit=0,
            force=True,
            continue_on_error=False,
            runtime_path=None,
            database="yfinance",
            log_level="INFO",
        )

        inserted = run_sync_args(args, provider, repository)

        self.assertEqual(inserted, 2)
        tables = [call[0] for call in client.insert_calls]
        self.assertIn("yfinance.yf_daily_kline", tables)
        self.assertIn("yfinance.yf_symbol_cursor", tables)
        self.assertIn("yfinance.yf_sync_task_log", tables)
        cursor_call = next(call for call in client.insert_calls if call[0].endswith("yf_symbol_cursor"))
        self.assertEqual(cursor_call[2][0][1], "AAPL")
        self.assertEqual(cursor_call[2][0][2], date(2024, 1, 3))

    def test_load_execution_plan(self) -> None:
        content = textwrap.dedent(
            """
            source = "yfinance"
            database = "us_market"

            [defaults]
            begin_date = 20240101
            limit = 10
            force = false
            continue_on_error = true

            [[tasks]]
            task = "symbol_master"

            [[tasks]]
            task = "daily_kline"
            codes = ["AAPL", "MSFT"]
            """
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "yfinance.toml"
            path.write_text(content, encoding="utf-8")
            plan = load_execution_plan_from_toml(str(path))

        self.assertEqual(plan.database, "us_market")
        self.assertEqual([task.task for task in plan.tasks], ["symbol_master", "daily_kline"])
        self.assertEqual(plan.tasks[1].codes_raw, "AAPL,MSFT")
        self.assertEqual(plan.tasks[1].limit, 10)


if __name__ == "__main__":
    unittest.main()
