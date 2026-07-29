#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import tempfile
import textwrap
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

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
                    {
                        "symbol": "AAA.ST",
                        "name": "Ambiguous Nordic Listing",
                        "currency": "SEK",
                        "sector": "Financials",
                        "exchange": "NGM",
                        "market": "Nordic Growth Market",
                    },
                    {
                        "symbol": "AAME",
                        "name": "Atlantic American Corporation",
                        "currency": "USD",
                        "sector": "Financials",
                        "industry": "Insurance",
                        "exchange": "NGM",
                        "market": "Nordic Growth Market",
                    },
                ]
            ).set_index("symbol")


class _FailingFinanceDatabase:
    select_calls = 0

    class Equities:
        def select(self):
            _FailingFinanceDatabase.select_calls += 1
            raise TimeoutError("raw.githubusercontent.com timed out")


class _ProxyAwareFinanceDatabase:
    proxy_snapshots: list[dict[str, str | None]] = []

    class Equities:
        def select(self):
            _ProxyAwareFinanceDatabase.proxy_snapshots.append(
                {
                    "HTTP_PROXY": os.environ.get("HTTP_PROXY"),
                    "HTTPS_PROXY": os.environ.get("HTTPS_PROXY"),
                }
            )
            return _FakeFinanceDatabase.Equities().select()


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
        self.download_failures = 0
        self.config = SimpleNamespace(
            network=SimpleNamespace(proxy=None, retries=0),
            debug=SimpleNamespace(hide_exceptions=True),
        )

    def download(self, **kwargs):
        self.download_calls.append(kwargs)
        if self.download_failures > 0:
            self.download_failures -= 1
            raise RuntimeError("HTTP Error 429: Too Many Requests")
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
        self.query_rows_calls: list[str] = []
        self.query_value_result = None

    def command(self, sql: str, parameters=None):
        self.commands.append(sql)

    def insert_rows(self, table: str, column_names, rows):
        self.insert_calls.append((table, list(column_names), list(rows)))

    def query_value(self, sql: str, parameters=None):
        return self.query_value_result

    def query_rows(self, sql: str, parameters=None):
        self.query_rows_calls.append(sql)
        return []


class _LegacyYFinanceSchemaClickHouseClient(_FakeClickHouseClient):
    def query_rows(self, sql: str, parameters=None):
        self.query_rows_calls.append(sql)
        table = str((parameters or {}).get("table") or "")
        if table == "yf_symbol_master":
            return [("source",)]
        if table == "yf_daily_kline":
            return [("source",), ("fetched_at",)]
        return []


class YFinanceProviderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.yf = _FakeYFinance()
        self.provider = YFinanceProvider(
            YFinanceConfig(
                batch_size=2,
                request_interval_seconds=0,
                active_symbols_only=False,
            ),
            yfinance_module=self.yf,
            finance_database_module=_FakeFinanceDatabase,
        )

    def test_symbol_master_keeps_main_us_exchanges(self) -> None:
        frame = self.provider.fetch_symbol_master(snapshot_date=date(2024, 1, 5))

        self.assertEqual(frame["symbol"].tolist(), ["AAPL", "IBM"])
        self.assertEqual(frame.loc[frame["symbol"] == "AAPL", "name"].iloc[0], "Apple Inc.")
        self.assertTrue({"source", "fetched_at"}.isdisjoint(frame.columns))

    def test_active_directory_excludes_non_common_and_adds_current_symbols(self) -> None:
        nasdaq_text = "\n".join(
            (
                "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares",
                "AAME|Atlantic American Corporation - Common Stock|G|N|N|100|N|N",
                "AAPL|Apple Inc. - Common Stock|Q|N|N|100|N|N",
                "ACACW|Acri Capital Acquisition Corporation - Warrants|S|N|N|100|N|N",
                "AACBU|Artius II Acquisition Inc. - Units|G|N|N|100|N|N",
                "BAD|Bad Filing Corp. - Common Stock|G|N|D|100|N|N",
                "QQQ|Invesco QQQ Trust|Q|N|N|100|Y|N",
                "File Creation Time: 0728202618:00|||||||",
            )
        )
        other_text = "\n".join(
            (
                "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol",
                "IBM|International Business Machines Corporation Common Stock|N|IBM|N|100|N|IBM",
                "BRK.B|Berkshire Hathaway Inc. Class B Common Stock|N|BRK.B|N|100|N|BRK.B",
                "BAC^A|Bank of America Preferred Stock|N|BAC^A|N|100|N|BAC^A",
                "AHLpE|Aspen Insurance Depositary Shares representing Preference Shares|N|AHLpE|N|100|N|AHLpE",
                "ATHpA|Athene Depositary Shares representing Preferred Stock|N|ATHpA|N|100|N|ATHpA",
                "SPY|SPDR S&P 500 ETF Trust|P|SPY|Y|100|N|SPY",
                "File Creation Time: 0728202618:00|||||||",
            )
        )
        texts = {
            "nasdaqlisted.txt": nasdaq_text,
            "otherlisted.txt": other_text,
        }
        provider = YFinanceProvider(
            YFinanceConfig(active_symbols_only=True),
            finance_database_module=_FakeFinanceDatabase,
            url_text_loader=lambda url: next(
                value for suffix, value in texts.items() if url.endswith(suffix)
            ),
        )

        frame = provider.fetch_symbol_master(snapshot_date=date(2026, 7, 28))

        self.assertEqual(frame["symbol"].tolist(), ["AAME", "AAPL", "BRK-B", "IBM"])
        self.assertEqual(frame.loc[frame["symbol"] == "AAME", "industry"].iloc[0], "Insurance")
        self.assertEqual(frame.loc[frame["symbol"] == "BRK-B", "currency"].iloc[0], "USD")
        self.assertTrue({"source", "fetched_at"}.isdisjoint(frame.columns))

    def test_finance_database_download_uses_runtime_proxy_and_restores_environment(self) -> None:
        _ProxyAwareFinanceDatabase.proxy_snapshots = []
        provider = YFinanceProvider(
            YFinanceConfig(
                proxy="http://127.0.0.1:7890",
                active_symbols_only=False,
            ),
            finance_database_module=_ProxyAwareFinanceDatabase,
        )

        with patch.dict(
            os.environ,
            {
                "HTTP_PROXY": "http://old-http.example",
                "HTTPS_PROXY": "http://old-https.example",
            },
            clear=False,
        ):
            frame = provider.fetch_symbol_master(snapshot_date=date(2026, 7, 28))
            self.assertEqual(os.environ["HTTP_PROXY"], "http://old-http.example")
            self.assertEqual(os.environ["HTTPS_PROXY"], "http://old-https.example")

        self.assertFalse(frame.empty)
        self.assertEqual(
            _ProxyAwareFinanceDatabase.proxy_snapshots,
            [
                {
                    "HTTP_PROXY": "http://127.0.0.1:7890",
                    "HTTPS_PROXY": "http://127.0.0.1:7890",
                }
            ],
        )

    def test_symbol_master_falls_back_to_nasdaq_directory_when_github_is_unavailable(self) -> None:
        _FailingFinanceDatabase.select_calls = 0
        texts = {
            "nasdaqlisted.txt": "\n".join(
                (
                    "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares",
                    "AAPL|Apple Inc. - Common Stock|Q|N|N|100|N|N",
                    "File Creation Time: 0728202618:00|||||||",
                )
            ),
            "otherlisted.txt": "\n".join(
                (
                    "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol",
                    "IBM|International Business Machines Corporation Common Stock|N|IBM|N|100|N|IBM",
                    "File Creation Time: 0728202618:00|||||||",
                )
            ),
        }
        provider = YFinanceProvider(
            YFinanceConfig(active_symbols_only=True),
            finance_database_module=_FailingFinanceDatabase,
            url_text_loader=lambda url: next(
                value for suffix, value in texts.items() if url.endswith(suffix)
            ),
        )

        frame = provider.fetch_symbol_master(snapshot_date=date(2026, 7, 28))
        industry = provider.fetch_industry_membership(symbol_master=frame)

        self.assertEqual(frame["symbol"].tolist(), ["AAPL", "IBM"])
        self.assertTrue({"source", "fetched_at"}.isdisjoint(frame.columns))
        self.assertTrue(industry.empty)
        self.assertEqual(_FailingFinanceDatabase.select_calls, 1)

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
        self.assertTrue({"source", "fetched_at"}.isdisjoint(frame.columns))

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
        self.assertTrue({"source", "fetched_at"}.isdisjoint(frame.columns))

    def test_concept_membership_is_labeled_top_holdings(self) -> None:
        frame = self.provider.fetch_concept_membership(
            CONCEPT_DEFINITIONS[:1],
            snapshot_date=date(2024, 1, 5),
        )

        self.assertEqual(set(frame["symbol"]), {"NVDA", "MSFT"})
        self.assertEqual(set(frame["membership_scope"]), {"top_holdings"})
        self.assertEqual(set(frame["etf_symbol"]), {"AIQ", "BOTZ", "ROBO"})
        self.assertTrue({"source", "fetched_at"}.isdisjoint(frame.columns))

    def test_proxy_config_and_rate_limit_retry_are_applied(self) -> None:
        self.yf.download_failures = 1
        provider = YFinanceProvider(
            YFinanceConfig(
                proxy="http://127.0.0.1:7890",
                network_retries=3,
                request_interval_seconds=0,
                rate_limit_retries=1,
                rate_limit_backoff_seconds=0,
                rate_limit_max_backoff_seconds=0,
                rate_limit_jitter_seconds=0,
            ),
            yfinance_module=self.yf,
        )

        frame = provider.fetch_daily(
            ["AAPL"],
            start_date="20240102",
            end_date="20240103",
        )

        self.assertEqual(len(frame), 2)
        self.assertEqual(len(self.yf.download_calls), 2)
        self.assertEqual(self.yf.config.network.proxy, "http://127.0.0.1:7890")
        self.assertEqual(self.yf.config.network.retries, 3)
        self.assertFalse(self.yf.config.debug.hide_exceptions)


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
                }
            ]
        )

        inserted = repository.save_frame("daily_kline", frame)

        self.assertEqual(inserted, 1)
        table, columns, rows = client.insert_calls[0]
        self.assertEqual(table, "yfinance.yf_daily_kline")
        self.assertEqual(dict(zip(columns, rows[0]))["symbol"], "AAPL")
        self.assertTrue({"source", "fetched_at"}.isdisjoint(columns))

    def test_ensure_tables_creates_all_task_tables(self) -> None:
        client = _FakeClickHouseClient()
        repository = YFinanceRepository(client, database="yfinance")

        repository.ensure_tables()

        ddl = "\n".join(client.commands)
        self.assertIn("yf_symbol_master", ddl)
        self.assertIn("yf_daily_kline", ddl)
        self.assertIn("yf_concept_membership", ddl)
        self.assertNotIn("source String", ddl)
        self.assertNotIn("fetched_at", ddl)

    def test_ensure_tables_migrates_legacy_metadata_columns(self) -> None:
        client = _LegacyYFinanceSchemaClickHouseClient()
        repository = YFinanceRepository(client, database="yfinance")

        repository.ensure_tables()

        commands = "\n".join(client.commands)
        self.assertIn(
            "ALTER TABLE yfinance.yf_symbol_master DROP COLUMN IF EXISTS source",
            commands,
        )
        self.assertIn(
            "CREATE TABLE IF NOT EXISTS yfinance.yf_daily_kline__without_metadata_v1",
            commands,
        )
        copy_sql = next(
            command
            for command in client.commands
            if command.startswith(
                "INSERT INTO yfinance.yf_daily_kline__without_metadata_v1"
            )
        )
        self.assertNotIn("source", copy_sql)
        self.assertNotIn("fetched_at", copy_sql)
        self.assertIn(
            "EXCHANGE TABLES yfinance.yf_daily_kline "
            "AND yfinance.yf_daily_kline__without_metadata_v1",
            commands,
        )

    def test_saved_symbol_universe_filters_non_common_security_names(self) -> None:
        client = _FakeClickHouseClient()
        repository = YFinanceRepository(client, database="yfinance")

        repository.load_symbols()

        sql = client.query_rows_calls[-1].lower()
        self.assertIn("positioncaseinsensitiveutf8(name, 'preference')", sql)
        self.assertIn("positioncaseinsensitiveutf8(name, 'preferred')", sql)
        self.assertIn("positioncaseinsensitiveutf8(name, 'warrant')", sql)


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

    def test_daily_task_reuses_stored_symbol_universe_without_finance_database(self) -> None:
        _FailingFinanceDatabase.select_calls = 0
        provider = YFinanceProvider(
            YFinanceConfig(batch_size=10, request_interval_seconds=0),
            yfinance_module=_FakeYFinance(),
            finance_database_module=_FailingFinanceDatabase,
        )
        repository = YFinanceRepository(_FakeClickHouseClient(), database="yfinance")
        args = SyncArgs(
            task="daily_kline",
            codes_raw="",
            begin_date="20240102",
            end_date="20240110",
            limit=0,
            force=True,
            continue_on_error=False,
            runtime_path=None,
            database="yfinance",
            log_level="INFO",
        )

        with patch.object(repository, "load_symbols", return_value=["AAPL"]):
            inserted = run_sync_args(args, provider, repository)

        self.assertEqual(inserted, 2)
        self.assertEqual(_FailingFinanceDatabase.select_calls, 0)

    def test_industry_task_reuses_stored_symbol_master_without_finance_database(self) -> None:
        _FailingFinanceDatabase.select_calls = 0
        provider = YFinanceProvider(
            YFinanceConfig(request_interval_seconds=0),
            finance_database_module=_FailingFinanceDatabase,
        )
        client = _FakeClickHouseClient()
        repository = YFinanceRepository(client, database="yfinance")
        stored_master = pd.DataFrame(
            [
                {
                    "snapshot_date": date(2026, 7, 28),
                    "symbol": "AAPL",
                    "sector": "Technology",
                    "industry_group": "Technology Hardware",
                    "industry": "Consumer Electronics",
                    "exchange": "NMS",
                }
            ]
        )
        args = SyncArgs(
            task="industry_membership",
            codes_raw="",
            begin_date="",
            end_date="",
            limit=0,
            force=True,
            continue_on_error=False,
            runtime_path=None,
            database="yfinance",
            log_level="INFO",
        )

        with patch.object(
            repository,
            "load_symbol_master",
            return_value=stored_master,
        ) as load_symbol_master:
            inserted = run_sync_args(args, provider, repository)

        self.assertEqual(inserted, 1)
        load_symbol_master.assert_called_once_with(limit=0, require_industry=True)
        self.assertEqual(_FailingFinanceDatabase.select_calls, 0)
        self.assertTrue(
            any(call[0].endswith("yf_industry_membership") for call in client.insert_calls)
        )

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
