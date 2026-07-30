#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import tempfile
import textwrap
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd

from sync_data_system.providers.yfinance.provider import YFinanceConfig, YFinanceProvider
from sync_data_system.providers.yfinance.repository import YFinanceRepository
from sync_data_system.providers.yfinance.runner import (
    SyncArgs,
    YFinancePartialSyncError,
    _request_meta,
    latest_completed_us_session_date,
    load_execution_plan_from_toml,
    run_registered_task,
    run_sync_args,
)
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

    def __init__(self, symbol: str = "") -> None:
        self.symbol = symbol

    def get_income_stmt(self, *, freq: str):
        report_date = "2023-12-31" if freq == "yearly" else "2024-03-31"
        return pd.DataFrame(
            {pd.Timestamp(report_date): [1000.0, 120.0]},
            index=["Total Revenue", "Net Income"],
        )

    def get_balance_sheet(self, *, freq: str):
        report_date = "2023-12-31" if freq == "yearly" else "2024-03-31"
        return pd.DataFrame(
            {pd.Timestamp(report_date): [5000.0, 1800.0]},
            index=["Total Assets", "Total Debt"],
        )

    def get_cash_flow(self, *, freq: str):
        report_date = "2023-12-31" if freq == "yearly" else "2024-03-31"
        return pd.DataFrame(
            {pd.Timestamp(report_date): [300.0, 220.0]},
            index=["Operating Cash Flow", "Free Cash Flow"],
        )

    def get_info(self):
        return {
            "currency": "USD",
            "financialCurrency": "USD",
            "quoteType": "EQUITY",
            "marketCap": 3_000_000_000_000,
            "enterpriseValue": 3_100_000_000_000,
            "trailingPE": 30.5,
            "forwardPE": 28.0,
            "priceToBook": 40.0,
            "returnOnEquity": 1.5,
            "totalRevenue": 400_000_000_000,
        }

    def get_earnings_dates(self, *, limit: int):
        return pd.DataFrame(
            {
                "EPS Estimate": [2.1, 2.2],
                "Reported EPS": [2.3, None],
                "Surprise(%)": [9.5, None],
            },
            index=pd.to_datetime(
                ["2024-01-25 16:00:00-05:00", "2024-04-25 16:00:00-04:00"],
                utc=True,
            ).rename("Earnings Date"),
        ).head(limit)

    def get_earnings_estimate(self):
        return pd.DataFrame(
            {"avg": [2.2], "low": [2.0], "high": [2.4]},
            index=pd.Index(["0q"], name="period"),
        )

    def get_revenue_estimate(self):
        return pd.DataFrame(
            {"avg": [100_000.0], "growth": [0.08]},
            index=pd.Index(["0q"], name="period"),
        )

    def get_eps_trend(self):
        return pd.DataFrame(
            {"current": [2.2], "30daysAgo": [2.1]},
            index=pd.Index(["0q"], name="period"),
        )

    def get_eps_revisions(self):
        return pd.DataFrame(
            {"upLast30days": [5], "downLast30days": [1]},
            index=pd.Index(["0q"], name="period"),
        )

    def get_growth_estimates(self):
        return pd.DataFrame(
            {"stock": [0.12], "industry": [0.08]},
            index=pd.Index(["+5y"], name="period"),
        )

    def get_recommendations(self):
        return pd.DataFrame(
            {"strongBuy": [10], "buy": [20], "hold": [5], "sell": [1]},
            index=pd.Index(["0m"], name="period"),
        )

    def get_analyst_price_targets(self):
        return {"current": 200.0, "low": 170.0, "high": 240.0, "mean": 210.0}

    def get_institutional_holders(self):
        return pd.DataFrame(
            {
                "Holder": ["Vanguard"],
                "Date Reported": [pd.Timestamp("2024-03-31")],
                "pctHeld": [0.08],
                "Shares": [1000],
                "Value": [200_000],
                "pctChange": [0.01],
            }
        )

    def get_mutualfund_holders(self):
        return pd.DataFrame(
            {
                "Holder": ["Vanguard 500 Index"],
                "Date Reported": [pd.Timestamp("2024-03-31")],
                "pctHeld": [0.02],
                "Shares": [250],
                "Value": [50_000],
                "pctChange": [0.005],
            }
        )

    def get_insider_transactions(self):
        return pd.DataFrame(
            {
                "Start Date": [pd.Timestamp("2024-04-01")],
                "Insider": ["Jane Doe"],
                "Position": ["Director"],
                "Transaction": ["Sale"],
                "Shares": [100],
                "Value": [20_000],
                "Ownership": ["Direct"],
                "Text": ["Sale at market"],
                "URL": ["https://example.test/filing"],
            }
        )


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
        return _FakeTicker(symbol)


class _FailingFundsYFinance(_FakeYFinance):
    def Ticker(self, symbol: str):
        raise RuntimeError("HTTP Error 401 token=secret")


class _PartialBatchYFinance(_FakeYFinance):
    def download(self, **kwargs):
        requested = list(kwargs["tickers"])
        if len(requested) > 1:
            kwargs = dict(kwargs)
            kwargs["tickers"] = requested[:1]
        return super().download(**kwargs)


class _MissingTickerYFinance(_FakeYFinance):
    def download(self, **kwargs):
        requested = list(kwargs["tickers"])
        if requested == ["KRG"]:
            self.download_calls.append(kwargs)
            return pd.DataFrame()
        if "KRG" in requested:
            kwargs = dict(kwargs)
            kwargs["tickers"] = [symbol for symbol in requested if symbol != "KRG"]
        return super().download(**kwargs)


class _EmptyYFinance(_FakeYFinance):
    def download(self, **kwargs):
        self.download_calls.append(kwargs)
        return pd.DataFrame()


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
                "DBRG$H|DigitalBridge Group, Inc. 7.125% Series H|N|DBRGpH|N|100|N|DBRG-H",
                "ADIG.V|ADI Global Distribution Inc. Common Stock When-Issued|N|ADIGw|N|100|N|ADIG#",
                "NEEPS|NextEra Energy, Inc. 7.299% Corporate Units|N|NEEPS|N|100|N|NEEPS",
                "TXO|TXO Partners, L.P. Common Units Representing Limited Partner Interests|N|TXO|N|100|N|TXO",
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

        self.assertEqual(frame["symbol"].tolist(), ["AAME", "AAPL", "BRK-B", "IBM", "TXO"])
        self.assertEqual(frame.loc[frame["symbol"] == "AAME", "industry"].iloc[0], "Insurance")
        self.assertEqual(frame.loc[frame["symbol"] == "AAME", "exchange"].iloc[0], "NASDAQ")
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

        self.assertEqual(frame["symbol"].tolist(), ["AAPL", "IBM"])
        self.assertTrue({"source", "fetched_at"}.isdisjoint(frame.columns))
        with self.assertRaisesRegex(RuntimeError, "未获取到行业分类"):
            provider.fetch_industry_membership(symbol_master=frame)
        diagnostics = provider.drain_diagnostics()
        self.assertTrue(any("FinanceDatabase equities unavailable" in item for item in diagnostics))
        self.assertTrue(any("sector、industry_group" in item for item in diagnostics))
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

    def test_financial_statements_are_normalized_as_period_metric_rows(self) -> None:
        income = self.provider.fetch_income_statement("AAPL")
        balance = self.provider.fetch_balance_sheet("AAPL")
        cash_flow = self.provider.fetch_cash_flow("AAPL")

        self.assertEqual(len(income), 4)
        self.assertEqual(set(income["period_type"]), {"annual", "quarterly"})
        self.assertEqual(set(income["metric"]), {"Total Revenue", "Net Income"})
        self.assertEqual(set(balance["metric"]), {"Total Assets", "Total Debt"})
        self.assertEqual(
            set(cash_flow["metric"]),
            {"Operating Cash Flow", "Free Cash Flow"},
        )
        self.assertEqual(set(income["symbol"]), {"AAPL"})
        self.assertTrue({"source", "fetched_at"}.isdisjoint(income.columns))

    def test_financial_metrics_earnings_and_analyst_data_are_normalized(self) -> None:
        metrics = self.provider.fetch_financial_metrics(
            "AAPL",
            snapshot_date=date(2024, 4, 5),
        )
        earnings = self.provider.fetch_earnings_calendar("AAPL")
        analyst = self.provider.fetch_analyst_estimates(
            "AAPL",
            snapshot_date=date(2024, 4, 5),
        )

        self.assertEqual(metrics.iloc[0]["market_cap"], 3_000_000_000_000)
        self.assertEqual(metrics.iloc[0]["quote_type"], "EQUITY")
        self.assertEqual(len(earnings), 2)
        self.assertEqual(earnings.iloc[0]["reported_eps"], 2.3)
        self.assertTrue(
            {
                "earnings_estimate",
                "revenue_estimate",
                "eps_trend",
                "eps_revisions",
                "growth_estimates",
                "recommendations",
                "price_targets",
            }.issubset(set(analyst["dataset"]))
        )
        self.assertTrue({"source", "fetched_at"}.isdisjoint(analyst.columns))

    def test_holder_and_insider_data_are_normalized(self) -> None:
        holders = self.provider.fetch_institutional_holders(
            "AAPL",
            snapshot_date=date(2024, 4, 5),
        )
        insiders = self.provider.fetch_insider_transactions("AAPL")

        self.assertEqual(set(holders["holder_type"]), {"institution", "mutual_fund"})
        self.assertEqual(set(holders["holder"]), {"Vanguard", "Vanguard 500 Index"})
        self.assertEqual(
            holders.loc[holders["holder"] == "Vanguard", "percent_held"].iloc[0],
            0.08,
        )
        self.assertEqual(insiders.iloc[0]["insider"], "Jane Doe")
        self.assertEqual(insiders.iloc[0]["transaction"], "Sale")
        self.assertEqual(insiders.iloc[0]["start_date"], date(2024, 4, 1))
        self.assertTrue({"source", "fetched_at"}.isdisjoint(insiders.columns))

    def test_concept_membership_is_labeled_top_holdings(self) -> None:
        frame = self.provider.fetch_concept_membership(
            CONCEPT_DEFINITIONS[:1],
            snapshot_date=date(2024, 1, 5),
        )

        self.assertEqual(set(frame["symbol"]), {"NVDA", "MSFT"})
        self.assertEqual(set(frame["membership_scope"]), {"top_holdings"})
        self.assertEqual(set(frame["etf_symbol"]), {"AIQ", "BOTZ", "ROBO"})
        self.assertTrue({"source", "fetched_at"}.isdisjoint(frame.columns))

    def test_concept_membership_fails_when_every_etf_request_fails(self) -> None:
        provider = YFinanceProvider(
            YFinanceConfig(
                request_interval_seconds=0,
                rate_limit_retries=0,
            ),
            yfinance_module=_FailingFundsYFinance(),
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "未获取到任何概念 ETF Top Holdings",
        ):
            provider.fetch_concept_membership(CONCEPT_DEFINITIONS)

        diagnostics = provider.drain_diagnostics()
        self.assertTrue(any("etf=AIQ" in item for item in diagnostics))
        self.assertTrue(any("failed_etfs=11/11" in item for item in diagnostics))

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
        self.assertIn("yf_income_statement", ddl)
        self.assertIn("yf_balance_sheet", ddl)
        self.assertIn("yf_cash_flow", ddl)
        self.assertIn("yf_financial_metrics", ddl)
        self.assertIn("yf_earnings_calendar", ddl)
        self.assertIn("yf_analyst_estimates", ddl)
        self.assertIn("yf_institutional_holders", ddl)
        self.assertIn("yf_insider_transactions", ddl)
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
        self.assertIn("positioncaseinsensitiveutf8(name, 'when-issued')", sql)
        self.assertIn("positioncaseinsensitiveutf8(name, ' dep shs')", sql)
        self.assertIn("positioncaseinsensitiveutf8(name, '% series')", sql)
        self.assertIn("american depositary shares", sql)


class YFinanceRunnerTest(unittest.TestCase):
    def test_latest_completed_session_waits_for_us_market_close(self) -> None:
        self.assertEqual(
            latest_completed_us_session_date(
                datetime(2026, 7, 30, 15, 0, tzinfo=timezone.utc)
            ),
            date(2026, 7, 29),
        )
        self.assertEqual(
            latest_completed_us_session_date(
                datetime(2026, 7, 30, 20, 30, tzinfo=timezone.utc)
            ),
            date(2026, 7, 30),
        )
        self.assertEqual(
            latest_completed_us_session_date(
                datetime(2026, 8, 3, 14, 0, tzinfo=timezone.utc)
            ),
            date(2026, 7, 31),
        )

    def test_request_meta_caps_future_end_to_completed_us_session(self) -> None:
        args = SyncArgs(
            task="daily_kline",
            codes_raw="AAPL",
            begin_date="20260701",
            end_date="20260730",
            limit=0,
            force=True,
            continue_on_error=False,
            runtime_path=None,
            database="yfinance",
            log_level="INFO",
        )

        with patch(
            "sync_data_system.providers.yfinance.runner.latest_completed_us_session_date",
            return_value=date(2026, 7, 29),
        ):
            request_meta = _request_meta(args, YFinanceConfig())

        self.assertEqual(request_meta["start_date"], "20260701")
        self.assertEqual(request_meta["end_date"], "20260729")
        self.assertEqual(request_meta["requested_end_date"], "20260730")

    def test_registered_task_flushes_provider_diagnostics_to_web_log(self) -> None:
        messages: list[str] = []
        provider = SimpleNamespace(
            drain_diagnostics=Mock(
                side_effect=[
                    (),
                    ("ETF Top Holdings 请求失败 token=secret",),
                ]
            )
        )
        probe = SimpleNamespace(
            name="yfinance.concept_membership",
            source="yfinance",
            input_codes=[],
            input_begin_date=None,
            input_end_date=None,
            limit=0,
            force=True,
            runtime_path=None,
            database="yfinance",
            log_level="INFO",
            context=SimpleNamespace(
                provider=provider,
                repository=SimpleNamespace(),
            ),
            log=messages.append,
            set_row_count=Mock(),
        )

        with (
            patch(
                "sync_data_system.providers.yfinance.runner.run_sync_args",
                side_effect=RuntimeError("upstream failed"),
            ),
            self.assertRaisesRegex(RuntimeError, "upstream failed"),
        ):
            run_registered_task(probe)

        self.assertEqual(
            messages,
            [
                "task=yfinance.concept_membership warning="
                "ETF Top Holdings 请求失败 token=[REDACTED]"
            ],
        )

    def test_empty_concept_membership_is_recorded_as_failed(self) -> None:
        provider = YFinanceProvider(
            YFinanceConfig(
                request_interval_seconds=0,
                rate_limit_retries=0,
            ),
            yfinance_module=_FailingFundsYFinance(),
        )
        client = _FakeClickHouseClient()
        repository = YFinanceRepository(client, database="yfinance")
        args = SyncArgs(
            task="concept_membership",
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

        with self.assertRaisesRegex(
            RuntimeError,
            "未获取到任何概念 ETF Top Holdings",
        ):
            run_sync_args(args, provider, repository)

        log_call = next(
            call for call in client.insert_calls if call[0].endswith("yf_sync_task_log")
        )
        saved_log = dict(zip(log_call[1], log_call[2][0]))
        self.assertEqual(saved_log["status"], "failed")
        self.assertEqual(saved_log["row_count"], 0)
        self.assertIn("failed_etfs=11/11", saved_log["message"])

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

    def test_fundamental_task_writes_rows_without_price_cursor(self) -> None:
        provider = YFinanceProvider(
            YFinanceConfig(request_interval_seconds=0),
            yfinance_module=_FakeYFinance(),
            finance_database_module=_FakeFinanceDatabase,
        )
        client = _FakeClickHouseClient()
        repository = YFinanceRepository(client, database="yfinance")
        args = SyncArgs(
            task="income_statement",
            codes_raw="AAPL",
            begin_date="",
            end_date="",
            limit=0,
            force=True,
            continue_on_error=True,
            runtime_path=None,
            database="yfinance",
            log_level="INFO",
        )

        inserted = run_sync_args(args, provider, repository)

        self.assertEqual(inserted, 4)
        tables = [call[0] for call in client.insert_calls]
        self.assertIn("yfinance.yf_income_statement", tables)
        self.assertIn("yfinance.yf_sync_task_log", tables)
        self.assertNotIn("yfinance.yf_symbol_cursor", tables)

    def test_fundamental_task_records_saved_rows_when_later_symbol_fails(self) -> None:
        provider = YFinanceProvider(
            YFinanceConfig(request_interval_seconds=0),
            yfinance_module=_FakeYFinance(),
            finance_database_module=_FakeFinanceDatabase,
        )
        client = _FakeClickHouseClient()
        repository = YFinanceRepository(client, database="yfinance")
        successful_frame = provider.fetch_income_statement("AAPL")
        args = SyncArgs(
            task="income_statement",
            codes_raw="AAPL,MSFT",
            begin_date="",
            end_date="",
            limit=0,
            force=True,
            continue_on_error=True,
            runtime_path=None,
            database="yfinance",
            log_level="INFO",
        )

        with (
            patch.object(
                provider,
                "fetch_income_statement",
                side_effect=[successful_frame, RuntimeError("upstream failed")],
            ),
            self.assertRaisesRegex(YFinancePartialSyncError, "symbols=MSFT") as caught,
        ):
            run_sync_args(args, provider, repository)

        self.assertEqual(caught.exception.row_count, 4)
        log_call = next(
            call for call in client.insert_calls if call[0].endswith("yf_sync_task_log")
        )
        saved_log = dict(zip(log_call[1], log_call[2][0]))
        self.assertEqual(saved_log["status"], "failed")
        self.assertEqual(saved_log["row_count"], 4)

    def test_daily_task_retries_symbols_missing_from_batch(self) -> None:
        fake_yfinance = _PartialBatchYFinance()
        provider = YFinanceProvider(
            YFinanceConfig(batch_size=2, request_interval_seconds=0),
            yfinance_module=fake_yfinance,
            finance_database_module=_FakeFinanceDatabase,
        )
        client = _FakeClickHouseClient()
        repository = YFinanceRepository(client, database="yfinance")
        args = SyncArgs(
            task="daily_kline",
            codes_raw="AAPL,KRG",
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

        self.assertEqual(inserted, 4)
        self.assertEqual(len(fake_yfinance.download_calls), 2)
        daily_calls = [
            (columns, rows)
            for table, columns, rows in client.insert_calls
            if table.endswith("yf_daily_kline")
        ]
        daily_symbols = {
            row[columns.index("symbol")]
            for columns, rows in daily_calls
            for row in rows
        }
        self.assertEqual(daily_symbols, {"AAPL", "KRG"})

    def test_daily_task_fails_with_saved_row_count_for_never_seen_missing_symbol(self) -> None:
        fake_yfinance = _MissingTickerYFinance()
        provider = YFinanceProvider(
            YFinanceConfig(batch_size=2, request_interval_seconds=0),
            yfinance_module=fake_yfinance,
            finance_database_module=_FakeFinanceDatabase,
        )
        client = _FakeClickHouseClient()
        repository = YFinanceRepository(client, database="yfinance")
        args = SyncArgs(
            task="daily_kline",
            codes_raw="AAPL,KRG",
            begin_date="20240102",
            end_date="20240110",
            limit=0,
            force=True,
            continue_on_error=False,
            runtime_path=None,
            database="yfinance",
            log_level="INFO",
        )

        with self.assertRaisesRegex(YFinancePartialSyncError, "symbols=KRG") as caught:
            run_sync_args(args, provider, repository)

        self.assertEqual(caught.exception.row_count, 2)
        log_call = next(
            call for call in client.insert_calls if call[0].endswith("yf_sync_task_log")
        )
        saved_log = dict(zip(log_call[1], log_call[2][0]))
        self.assertEqual(saved_log["status"], "failed")
        self.assertEqual(saved_log["row_count"], 2)
        self.assertTrue(
            any("symbol=KRG" in message for message in provider.drain_diagnostics())
        )

    def test_daily_task_does_not_retry_every_historical_symbol_on_empty_session(self) -> None:
        fake_yfinance = _EmptyYFinance()
        provider = YFinanceProvider(
            YFinanceConfig(batch_size=2, request_interval_seconds=0),
            yfinance_module=fake_yfinance,
            finance_database_module=_FakeFinanceDatabase,
        )
        client = _FakeClickHouseClient()
        repository = YFinanceRepository(client, database="yfinance")
        args = SyncArgs(
            task="daily_kline",
            codes_raw="AAPL,MSFT",
            begin_date="20240102",
            end_date="20240103",
            limit=0,
            force=False,
            continue_on_error=False,
            runtime_path=None,
            database="yfinance",
            log_level="INFO",
        )

        with (
            patch.object(repository, "has_successful_sync_today", return_value=False),
            patch.object(repository, "load_latest_cursor", return_value="20240101"),
        ):
            inserted = run_sync_args(args, provider, repository)

        self.assertEqual(inserted, 0)
        self.assertEqual(len(fake_yfinance.download_calls), 1)
        self.assertTrue(
            any(
                "已有历史游标的代码不逐个重试" in message
                for message in provider.drain_diagnostics()
            )
        )

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

    def test_fundamentals_plan_contains_all_new_tasks(self) -> None:
        plan_path = (
            Path(__file__).resolve().parents[1]
            / "providers"
            / "yfinance"
            / "plans"
            / "fundamentals.toml"
        )

        plan = load_execution_plan_from_toml(str(plan_path))

        self.assertEqual(
            [task.task for task in plan.tasks],
            [
                "income_statement",
                "balance_sheet",
                "cash_flow",
                "financial_metrics",
                "earnings_calendar",
                "analyst_estimates",
                "institutional_holders",
                "insider_transactions",
            ],
        )
        self.assertTrue(all(task.continue_on_error for task in plan.tasks))


if __name__ == "__main__":
    unittest.main()
