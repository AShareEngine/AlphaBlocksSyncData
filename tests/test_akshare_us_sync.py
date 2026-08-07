#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import tempfile
import textwrap
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import requests

from sync_data_system.providers.akshare.provider import AkshareUSConfig, AkshareUSProvider
from sync_data_system.providers.akshare.repository import AkshareUSRepository
from sync_data_system.providers.akshare.runner import (
    SyncArgs,
    load_execution_plan_from_toml,
    run_sync_args,
)


class _FakeAkshare:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.proxy_snapshots: list[dict[str, str | None]] = []

    def stock_us_spot_em(self):
        self.calls.append(("stock_us_spot_em", {}))
        self.proxy_snapshots.append(
            {
                "HTTP_PROXY": os.environ.get("HTTP_PROXY"),
                "HTTPS_PROXY": os.environ.get("HTTPS_PROXY"),
            }
        )
        return pd.DataFrame(
            [
                {
                    "代码": "105.AAPL",
                    "名称": "Apple Inc.",
                    "最新价": 225.0,
                    "涨跌额": 1.5,
                    "涨跌幅": 0.67,
                    "开盘价": 223.0,
                    "最高价": 226.0,
                    "最低价": 222.0,
                    "昨收价": 223.5,
                    "总市值": 3.4e12,
                    "市盈率": 35.0,
                    "成交量": 1_000_000,
                    "成交额": 225_000_000,
                    "振幅": 1.8,
                    "换手率": 0.7,
                },
                {"代码": "106.ACACW", "名称": "Acri Capital Acquisition Warrants"},
                {"代码": "153.ABCD", "名称": "Example OTC Common Stock"},
            ]
        )

    def stock_us_spot(self):
        self.calls.append(("stock_us_spot", {}))
        return pd.DataFrame(
            [
                {
                    "name": "Apple, Inc.",
                    "cname": "苹果公司",
                    "category": "计算机",
                    "symbol": "AAPL",
                    "price": 225.0,
                    "diff": 1.5,
                    "chg": 0.67,
                    "preclose": 223.5,
                    "open": 223.0,
                    "high": 226.0,
                    "low": 222.0,
                    "volume": 1_000_000,
                    "mktcap": 3.4e12,
                    "pe": 35.0,
                }
            ]
        )

    def get_us_stock_name(self):
        self.calls.append(("get_us_stock_name", {}))
        return pd.DataFrame([{"name": "Apple, Inc.", "cname": "苹果公司", "symbol": "AAPL"}])

    def stock_us_hist(self, **kwargs):
        self.calls.append(("stock_us_hist", kwargs))
        return pd.DataFrame(
            [
                {
                    "日期": "2024-01-02",
                    "开盘": 100.0,
                    "收盘": 102.0,
                    "最高": 103.0,
                    "最低": 99.0,
                    "成交量": 1000,
                    "成交额": 102000,
                    "振幅": 4.0,
                    "涨跌幅": 2.0,
                    "涨跌额": 2.0,
                    "换手率": 0.5,
                },
                {
                    "日期": "2024-01-03",
                    "开盘": 102.0,
                    "收盘": 103.0,
                    "最高": 104.0,
                    "最低": 101.0,
                    "成交量": 1100,
                    "成交额": 113300,
                    "振幅": 3.0,
                    "涨跌幅": 0.98,
                    "涨跌额": 1.0,
                    "换手率": 0.6,
                },
            ]
        )

    def stock_us_daily(self, **kwargs):
        self.calls.append(("stock_us_daily", kwargs))
        return pd.DataFrame(
            [
                {
                    "date": "2024-01-02",
                    "open": 100.0,
                    "close": 102.0,
                    "high": 103.0,
                    "low": 99.0,
                    "volume": 1000,
                },
                {
                    "date": "2024-01-03",
                    "open": 102.0,
                    "close": 103.0,
                    "high": 104.0,
                    "low": 101.0,
                    "volume": 1100,
                },
            ]
        )

    def stock_us_hist_min_em(self, **kwargs):
        self.calls.append(("stock_us_hist_min_em", kwargs))
        return pd.DataFrame(
            [
                {
                    "时间": "2024-01-03 09:30:00",
                    "开盘": 102.0,
                    "收盘": 102.5,
                    "最高": 103.0,
                    "最低": 101.5,
                    "成交量": 100,
                    "成交额": 10250,
                    "最新价": 102.5,
                }
            ]
        )

    def stock_individual_basic_info_us_xq(self, **kwargs):
        self.calls.append(("stock_individual_basic_info_us_xq", kwargs))
        return pd.DataFrame([{"item": "公司名称", "value": "Apple Inc."}])

    def stock_financial_us_report_em(self, **kwargs):
        self.calls.append(("stock_financial_us_report_em", kwargs))
        return pd.DataFrame(
            [
                {
                    "REPORT_DATE": 20241231,
                    "REPORT_TYPE": "FY",
                    "SECUCODE": "AAPL.O",
                    "SECURITY_NAME_ABBR": "Apple",
                    "STD_ITEM_CODE": "001",
                    "ITEM_NAME": "Cash",
                    "AMOUNT": 100.5,
                }
            ]
        )

    def stock_financial_us_analysis_indicator_em(self, **kwargs):
        self.calls.append(("stock_financial_us_analysis_indicator_em", kwargs))
        return pd.DataFrame(
            [
                {
                    "REPORT_DATE": 20241231,
                    "NOTICE_DATE": 20250130,
                    "CURRENCY": "USD",
                    "OPERATE_INCOME": 1000,
                    "PARENT_HOLDER_NETPROFIT": 200,
                    "BASIC_EPS": 2.5,
                    "ROE_AVG": 35.0,
                }
            ]
        )

    def stock_us_valuation_baidu(self, **kwargs):
        self.calls.append(("stock_us_valuation_baidu", kwargs))
        return pd.DataFrame([{"date": "2024-01-03", "value": 3.4e12}])

    def index_us_stock_sina(self, **kwargs):
        self.calls.append(("index_us_stock_sina", kwargs))
        return pd.DataFrame(
            [
                {
                    "date": "2024-01-03",
                    "open": 4700,
                    "high": 4750,
                    "low": 4680,
                    "close": 4730,
                    "volume": 100000,
                    "amount": 2.5e9,
                }
            ]
        )

    def stock_board_concept_name_ths(self):
        self.calls.append(("stock_board_concept_name_ths", {}))
        return pd.DataFrame(
            [
                {"name": "阿里巴巴概念", "code": "301558"},
                {"name": "机器人概念", "code": "301100"},
            ]
        )

    def stock_board_concept_index_ths(self, **kwargs):
        self.calls.append(("stock_board_concept_index_ths", kwargs))
        return pd.DataFrame(
            [
                {
                    "日期": "2024-01-02",
                    "开盘价": 1105.43,
                    "最高价": 1133.391,
                    "最低价": 1100.0,
                    "收盘价": 1130.28,
                    "成交量": 1_867_106_700,
                    "成交额": 2.270406e10,
                },
                {
                    "日期": "2024-01-03",
                    "开盘价": 1133.673,
                    "最高价": 1143.881,
                    "最低价": 1120.0,
                    "收盘价": 1140.087,
                    "成交量": 1_734_555_400,
                    "成交额": 2.049213e10,
                },
            ]
        )

    def stock_board_concept_info_ths(self, **kwargs):
        self.calls.append(("stock_board_concept_info_ths", kwargs))
        return pd.DataFrame(
            [
                {"项目": "今开", "值": 1825.71},
                {"项目": "板块涨幅", "值": "-4.96%"},
                {"项目": "涨幅排名", "值": "317/396"},
            ]
        )

    def stock_board_concept_name_em(self):
        self.calls.append(("stock_board_concept_name_em", {}))
        return pd.DataFrame(
            [
                {"板块名称": "融资融券", "板块代码": "BK0655"},
                {"板块名称": "绿色电力", "板块代码": "BK0715"},
            ]
        )

    def stock_board_concept_cons_em(self, **kwargs):
        self.calls.append(("stock_board_concept_cons_em", kwargs))
        return pd.DataFrame(
            [
                {
                    "序号": 1,
                    "代码": "000001",
                    "名称": "平安银行",
                    "最新价": 10.5,
                    "涨跌幅": 1.2,
                    "涨跌额": 0.12,
                    "成交量": 1000,
                    "成交额": 10500,
                    "振幅": 2.1,
                    "最高": 10.7,
                    "最低": 10.3,
                    "今开": 10.4,
                    "昨收": 10.38,
                    "换手率": 0.8,
                    "市盈率-动态": 6.5,
                    "市净率": 0.7,
                }
            ]
        )

    def stock_board_concept_hist_em(self, **kwargs):
        self.calls.append(("stock_board_concept_hist_em", kwargs))
        return pd.DataFrame(
            [
                {
                    "日期": "2024-01-02",
                    "开盘": 1100.0,
                    "收盘": 1110.0,
                    "最高": 1120.0,
                    "最低": 1090.0,
                    "涨跌幅": 0.9,
                    "涨跌额": 10.0,
                    "成交量": 100000,
                    "成交额": 2.5e9,
                    "振幅": 2.7,
                    "换手率": 1.1,
                },
                {
                    "日期": "2024-01-03",
                    "开盘": 1110.0,
                    "收盘": 1125.0,
                    "最高": 1130.0,
                    "最低": 1105.0,
                    "涨跌幅": 1.35,
                    "涨跌额": 15.0,
                    "成交量": 120000,
                    "成交额": 2.9e9,
                    "振幅": 2.25,
                    "换手率": 1.3,
                },
            ]
        )


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _FakeSession:
    def __init__(self, response: _FakeResponse) -> None:
        self.response = response
        self.trust_env = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return None

    def get(self, *args, **kwargs) -> _FakeResponse:
        return self.response


class _FakeClickHouseClient:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.insert_calls: list[tuple[str, list[str], list[tuple]]] = []

    def command(self, sql: str, parameters=None):
        self.commands.append(sql)

    def insert_rows(self, table: str, column_names, rows):
        self.insert_calls.append((table, list(column_names), list(rows)))

    def query_value(self, sql: str, parameters=None):
        return None

    def query_rows(self, sql: str, parameters=None):
        return []


class _FailingEastmoneyAkshare(_FakeAkshare):
    def stock_us_spot_em(self):
        self.calls.append(("stock_us_spot_em", {}))
        raise ConnectionError("Eastmoney unavailable")


class _MissingFinancialDataAkshare(_FakeAkshare):
    def stock_financial_us_report_em(self, **kwargs):
        self.calls.append(("stock_financial_us_report_em", kwargs))
        raise TypeError("'NoneType' object is not subscriptable")

    def stock_financial_us_analysis_indicator_em(self, **kwargs):
        self.calls.append(("stock_financial_us_analysis_indicator_em", kwargs))
        raise TypeError("'NoneType' object is not subscriptable")


class _IncompatibleFinancialAkshare(_FakeAkshare):
    def stock_financial_us_report_em(self, **kwargs):
        self.calls.append(("stock_financial_us_report_em", kwargs))
        raise TypeError("got an unexpected keyword argument 'stock'")


class _FailingValuationAkshare(_FakeAkshare):
    def stock_us_valuation_baidu(self, **kwargs):
        self.calls.append(("stock_us_valuation_baidu", kwargs))
        raise ValueError("upstream returned non-JSON response")


class AkshareUSProviderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.ak = _FakeAkshare()
        self.provider = AkshareUSProvider(
            AkshareUSConfig(
                request_interval_seconds=0,
                retries=0,
                common_stock_only=True,
                include_pink=False,
            ),
            akshare_module=self.ak,
        )

    def test_spot_keeps_common_us_stocks_and_normalizes_code(self) -> None:
        frame = self.provider.fetch_us_spot(snapshot_date=date(2024, 1, 5))

        self.assertEqual(frame["symbol"].tolist(), ["AAPL"])
        self.assertEqual(frame.iloc[0]["em_code"], "105.AAPL")
        self.assertEqual(frame.iloc[0]["instrument_type"], "common_stock")

    def test_spot_falls_back_to_sina_when_eastmoney_is_unavailable(self) -> None:
        akshare = _FailingEastmoneyAkshare()
        provider = AkshareUSProvider(
            AkshareUSConfig(request_interval_seconds=0, retries=0),
            akshare_module=akshare,
        )

        frame = provider.fetch_us_spot(snapshot_date=date(2024, 1, 5))

        self.assertEqual(frame["symbol"].tolist(), ["AAPL"])
        self.assertEqual(frame.iloc[0]["em_code"], "AAPL")
        self.assertEqual(frame.iloc[0]["source"], "akshare:stock_us_spot")
        self.assertEqual(frame.iloc[0]["last"], 225.0)
        self.assertEqual([call[0] for call in akshare.calls[:2]], ["stock_us_spot_em", "stock_us_spot"])

    def test_sina_spot_excludes_etfs_and_inactive_symbols_from_common_stock_pool(self) -> None:
        akshare = _FailingEastmoneyAkshare()
        akshare.stock_us_spot = lambda: pd.DataFrame(
            [
                {
                    "name": "Apple, Inc.",
                    "category": "Technology",
                    "symbol": "AAPL",
                    "price": 225.0,
                },
                {
                    "name": "Alternative Access First Priority CLO Bond ETF",
                    "category": "ETF",
                    "symbol": "AAA",
                    "price": 25.0,
                },
                {
                    "name": "Inactive Example Corp.",
                    "category": "Technology",
                    "symbol": "OLD",
                    "price": None,
                },
            ]
        )
        provider = AkshareUSProvider(
            AkshareUSConfig(
                request_interval_seconds=0,
                retries=0,
                common_stock_only=True,
            ),
            akshare_module=akshare,
        )

        frame = provider.fetch_us_spot(snapshot_date=date(2024, 1, 5))

        self.assertEqual(frame["symbol"].tolist(), ["AAPL"])

    def test_non_price_task_does_not_download_spot_for_explicit_symbols(self) -> None:
        symbols = self.provider.resolve_us_symbols(
            ["aapl", "BRK.B", "AAPL"],
            require_em_code=False,
        )

        self.assertEqual(
            symbols,
            [
                {"symbol": "AAPL", "em_code": ""},
                {"symbol": "BRK.B", "em_code": ""},
            ],
        )
        self.assertEqual(self.ak.calls, [])

    def test_eastmoney_codes_bypass_full_spot_download(self) -> None:
        symbols = self.provider.resolve_us_symbols(
            ["105.AAPL", "106.TTE"],
            require_em_code=True,
        )

        self.assertEqual(
            symbols,
            [
                {"symbol": "AAPL", "em_code": "105.AAPL"},
                {"symbol": "TTE", "em_code": "106.TTE"},
            ],
        )
        self.assertEqual(self.ak.calls, [])

    def test_runtime_proxy_is_scoped_to_akshare_request(self) -> None:
        provider = AkshareUSProvider(
            AkshareUSConfig(
                proxy="http://127.0.0.1:7890",
                request_interval_seconds=0,
                retries=0,
            ),
            akshare_module=self.ak,
        )
        with patch.dict(
            os.environ,
            {
                "HTTP_PROXY": "http://old-http.example",
                "HTTPS_PROXY": "http://old-https.example",
            },
            clear=False,
        ):
            provider.fetch_us_spot()
            self.assertEqual(os.environ["HTTP_PROXY"], "http://old-http.example")
            self.assertEqual(os.environ["HTTPS_PROXY"], "http://old-https.example")

        self.assertEqual(
            self.ak.proxy_snapshots[-1],
            {
                "HTTP_PROXY": "http://127.0.0.1:7890",
                "HTTPS_PROXY": "http://127.0.0.1:7890",
            },
        )

    def test_daily_uses_eastmoney_code_and_requested_date_range(self) -> None:
        frame = self.provider.fetch_us_daily(
            em_code="105.AAPL",
            symbol="AAPL",
            start_date="20240102",
            end_date="20240103",
        )

        self.assertEqual(len(frame), 2)
        self.assertEqual(frame["trade_date"].min(), date(2024, 1, 2))
        _, call = next(item for item in self.ak.calls if item[0] == "stock_us_hist")
        self.assertEqual(call["symbol"], "105.AAPL")
        self.assertEqual(call["start_date"], "20240102")
        self.assertEqual(call["end_date"], "20240103")

    def test_daily_uses_sina_when_eastmoney_code_is_unavailable(self) -> None:
        frame = self.provider.fetch_us_daily(
            em_code="",
            symbol="AAPL",
            start_date="20240102",
            end_date="20240103",
        )

        self.assertEqual(len(frame), 2)
        self.assertEqual(frame.iloc[0]["source"], "akshare:stock_us_daily")
        self.assertEqual(frame.iloc[0]["em_code"], "AAPL")
        daily_call = next(item for item in self.ak.calls if item[0] == "stock_us_daily")[1]
        self.assertEqual(daily_call["symbol"], "AAPL")
        self.assertEqual(daily_call["adjust"], "")

    def test_unmapped_symbols_are_kept_for_daily_sina_fallback(self) -> None:
        symbols = self.provider.resolve_us_symbols(
            ["AAPL", "AABA"],
            require_em_code=True,
        )

        self.assertEqual(
            symbols,
            [
                {"symbol": "AAPL", "em_code": "105.AAPL"},
                {"symbol": "AABA", "em_code": ""},
            ],
        )

    def test_financial_dates_accept_yyyymmdd_integers(self) -> None:
        statement = self.provider.fetch_us_financial_statement(
            "AAPL",
            statement_type="资产负债表",
            period_type="年报",
        )
        indicator = self.provider.fetch_us_financial_indicator("AAPL", period_type="年报")

        self.assertEqual(statement.iloc[0]["report_date"], date(2024, 12, 31))
        self.assertEqual(indicator.iloc[0]["report_date"], date(2024, 12, 31))
        self.assertEqual(indicator.iloc[0]["notice_date"], date(2025, 1, 30))
        self.assertEqual(indicator.iloc[0]["basic_eps"], 2.5)

    def test_missing_financial_data_is_skipped_without_retry(self) -> None:
        akshare = _MissingFinancialDataAkshare()
        provider = AkshareUSProvider(
            AkshareUSConfig(request_interval_seconds=0, retries=2),
            akshare_module=akshare,
        )

        statement = provider.fetch_us_financial_statement(
            "AAA",
            statement_type="资产负债表",
            period_type="年报",
        )
        indicator = provider.fetch_us_financial_indicator("AAA", period_type="年报")

        self.assertTrue(statement.empty)
        self.assertTrue(indicator.empty)
        call_names = [item[0] for item in akshare.calls]
        self.assertEqual(call_names.count("stock_financial_us_report_em"), 1)
        self.assertEqual(call_names.count("stock_financial_us_analysis_indicator_em"), 1)

    def test_financial_signature_error_is_reported_as_sdk_incompatibility(self) -> None:
        provider = AkshareUSProvider(
            AkshareUSConfig(request_interval_seconds=0, retries=2),
            akshare_module=_IncompatibleFinancialAkshare(),
        )

        with self.assertRaisesRegex(RuntimeError, "SDK 接口签名不兼容"):
            provider.fetch_us_financial_statement(
                "AAPL",
                statement_type="资产负债表",
                period_type="年报",
            )

    def test_profile_minute_valuation_and_index_are_normalized(self) -> None:
        profile = self.provider.fetch_us_company_profile("AAPL")
        minute = self.provider.fetch_us_minute(
            em_code="105.AAPL",
            symbol="AAPL",
            start_date="20240103",
            end_date="20240103",
        )
        valuation = self.provider.fetch_us_valuation(
            "AAPL",
            indicator="总市值",
            period="近一年",
        )
        index = self.provider.fetch_us_index_daily(
            ".INX",
            "S&P 500",
            start_date="20240101",
            end_date="20240131",
        )

        self.assertEqual(profile.iloc[0]["item"], "公司名称")
        self.assertEqual(minute.iloc[0]["close"], 102.5)
        minute_call = next(item for item in self.ak.calls if item[0] == "stock_us_hist_min_em")[1]
        self.assertEqual(minute_call["start_date"], "2024-01-03 00:00:00")
        self.assertEqual(minute_call["end_date"], "2024-01-03 23:59:59")
        self.assertEqual(valuation.iloc[0]["trade_date"], date(2024, 1, 3))
        self.assertEqual(index.iloc[0]["index_code"], ".INX")

    def test_ths_concept_directory_index_and_info_are_normalized(self) -> None:
        directory = self.provider.fetch_ths_concept_names(snapshot_date=date(2024, 1, 5))
        concepts = self.provider.resolve_ths_concepts(
            ["301558"],
            directory=directory,
        )
        index = self.provider.fetch_ths_concept_index(
            concepts[0]["concept_name"],
            concepts[0]["concept_code"],
            start_date="20240101",
            end_date="20240103",
        )
        info = self.provider.fetch_ths_concept_info(
            concepts[0]["concept_name"],
            concepts[0]["concept_code"],
            snapshot_date=date(2024, 1, 5),
        )

        self.assertEqual(directory["concept_name"].tolist(), ["机器人概念", "阿里巴巴概念"])
        self.assertEqual(concepts, [{"concept_code": "301558", "concept_name": "阿里巴巴概念"}])
        self.assertEqual(index["trade_date"].max(), date(2024, 1, 3))
        self.assertEqual(index.iloc[0]["close"], 1130.28)
        self.assertEqual(info.set_index("item").loc["板块涨幅", "value"], "-4.96%")
        self.assertEqual(info.set_index("item").loc["涨幅排名", "value"], "317/396")
        index_call = next(item for item in self.ak.calls if item[0] == "stock_board_concept_index_ths")[1]
        self.assertEqual(index_call["symbol"], "阿里巴巴概念")
        self.assertEqual(index_call["start_date"], "20240101")
        self.assertEqual(index_call["end_date"], "20240103")

    def test_em_concept_directory_constituents_and_history_are_normalized(self) -> None:
        directory = self.provider.fetch_em_concept_names(snapshot_date=date(2024, 1, 5))
        concepts = self.provider.resolve_em_concepts(["BK0655"], directory=directory)
        constituents = self.provider.fetch_em_concept_constituents(
            concepts[0]["concept_name"],
            concepts[0]["concept_code"],
            snapshot_date=date(2024, 1, 5),
        )
        history = self.provider.fetch_em_concept_history(
            concepts[0]["concept_name"],
            concepts[0]["concept_code"],
            period="daily",
            start_date="20240101",
            end_date="20240103",
            adjust="qfq",
        )

        self.assertEqual(directory["concept_code"].tolist(), ["BK0715", "BK0655"])
        self.assertEqual(concepts, [{"concept_code": "BK0655", "concept_name": "融资融券"}])
        self.assertEqual(constituents.iloc[0]["symbol"], "000001")
        self.assertEqual(constituents.iloc[0]["pe_dynamic"], 6.5)
        self.assertEqual(history["trade_date"].max(), date(2024, 1, 3))
        self.assertEqual(history.iloc[0]["period"], "daily")
        self.assertEqual(history.iloc[0]["adjust"], "qfq")
        cons_call = next(item for item in self.ak.calls if item[0] == "stock_board_concept_cons_em")[1]
        hist_call = next(item for item in self.ak.calls if item[0] == "stock_board_concept_hist_em")[1]
        self.assertEqual(cons_call["symbol"], "BK0655")
        self.assertEqual(hist_call["symbol"], "BK0655")
        self.assertEqual(hist_call["period"], "daily")
        self.assertEqual(hist_call["start_date"], "20240101")
        self.assertEqual(hist_call["end_date"], "20240103")
        self.assertEqual(hist_call["adjust"], "qfq")

    def test_em_concept_directory_uses_browser_header_fallback(self) -> None:
        response = _FakeResponse(
            {
                "data": {
                    "total": 2,
                    "diff": [
                        {"f12": "BK0655", "f14": "融资融券"},
                        {"f12": "BK0715", "f14": "绿色电力"},
                    ],
                }
            }
        )
        with patch.object(
            self.ak,
            "stock_board_concept_name_em",
            side_effect=requests.ConnectionError("connection aborted"),
        ), patch(
            "sync_data_system.providers.akshare.provider.requests.get",
            return_value=response,
        ) as request_get:
            directory = self.provider.fetch_em_concept_names()

        self.assertEqual(set(directory["concept_code"]), {"BK0655", "BK0715"})
        request_kwargs = request_get.call_args.kwargs
        self.assertIn("User-Agent", request_kwargs["headers"])
        self.assertEqual(request_kwargs["timeout"], 20)

    def test_em_concept_fallback_bypasses_broken_environment_proxy(self) -> None:
        response = _FakeResponse(
            {
                "data": {
                    "total": 1,
                    "diff": [{"f12": "BK0655", "f14": "融资融券"}],
                }
            }
        )
        session = _FakeSession(response)
        with patch.dict(
            os.environ,
            {"HTTP_PROXY": "http://broken.proxy", "HTTPS_PROXY": "http://broken.proxy"},
            clear=False,
        ), patch.object(
            self.ak,
            "stock_board_concept_name_em",
            side_effect=requests.ConnectionError("connection aborted"),
        ), patch(
            "sync_data_system.providers.akshare.provider.requests.get",
            side_effect=requests.ConnectionError("proxy disconnected"),
        ), patch(
            "sync_data_system.providers.akshare.provider.requests.Session",
            return_value=session,
        ), patch(
            "sync_data_system.providers.akshare.provider._EASTMONEY_DIRECT_URLS",
            set(),
        ):
            directory = self.provider.fetch_em_concept_names()

        self.assertEqual(directory.iloc[0]["concept_code"], "BK0655")
        self.assertFalse(session.trust_env)

    def test_em_concept_data_tasks_use_direct_fallback_after_sdk_failure(self) -> None:
        constituents_raw = self.ak.stock_board_concept_cons_em(symbol="BK0655")
        history_raw = self.ak.stock_board_concept_hist_em(symbol="BK0715")
        with patch.object(
            self.ak,
            "stock_board_concept_cons_em",
            side_effect=requests.ConnectionError("connection aborted"),
        ), patch.object(
            self.ak,
            "stock_board_concept_hist_em",
            side_effect=requests.ConnectionError("connection aborted"),
        ) as history_sdk, patch.object(
            self.provider,
            "_fetch_em_concept_constituents_fallback",
            return_value=constituents_raw,
        ) as constituents_fallback, patch.object(
            self.provider,
            "_fetch_em_concept_history_fallback",
            return_value=history_raw,
        ) as history_fallback:
            constituents = self.provider.fetch_em_concept_constituents(
                "融资融券",
                "BK0655",
            )
            history = self.provider.fetch_em_concept_history(
                "绿色电力",
                "BK0715",
                period="daily",
                start_date="20240101",
                end_date="20240103",
            )

        self.assertEqual(constituents.iloc[0]["symbol"], "000001")
        self.assertEqual(history["trade_date"].max(), date(2024, 1, 3))
        constituents_fallback.assert_called_once_with("BK0655")
        history_fallback.assert_called_once()
        history_sdk.assert_not_called()


class AkshareUSRepositoryTest(unittest.TestCase):
    def test_ensure_tables_creates_all_business_and_state_tables(self) -> None:
        client = _FakeClickHouseClient()
        repository = AkshareUSRepository(client, database="akshare")

        repository.ensure_tables()

        ddl = "\n".join(client.commands)
        self.assertIn("ak_us_spot", ddl)
        self.assertIn("ak_us_daily_kline", ddl)
        self.assertIn("ak_us_financial_statement", ddl)
        self.assertIn("ak_us_index_daily", ddl)
        self.assertIn("ak_stock_board_concept_name_ths", ddl)
        self.assertIn("ak_stock_board_concept_index_ths", ddl)
        self.assertIn("ak_stock_board_concept_info_ths", ddl)
        self.assertIn("ak_stock_board_concept_name_em", ddl)
        self.assertIn("ak_stock_board_concept_cons_em", ddl)
        self.assertIn("ak_stock_board_concept_hist_em", ddl)
        self.assertIn("ak_sync_task_log", ddl)
        self.assertIn("ak_symbol_cursor", ddl)


class AkshareUSRunnerTest(unittest.TestCase):
    def test_daily_task_writes_data_cursor_and_sync_log(self) -> None:
        provider = AkshareUSProvider(
            AkshareUSConfig(request_interval_seconds=0, retries=0),
            akshare_module=_FakeAkshare(),
        )
        client = _FakeClickHouseClient()
        repository = AkshareUSRepository(client, database="akshare")
        args = SyncArgs(
            task="us_daily_kline",
            codes_raw="AAPL",
            begin_date="20240102",
            end_date="20240110",
            index_code="",
            period="",
            fields="",
            limit=0,
            force=True,
            continue_on_error=False,
            runtime_path=None,
            database="akshare",
            log_level="INFO",
        )

        inserted = run_sync_args(args, provider, repository)

        self.assertEqual(inserted, 2)
        tables = [call[0] for call in client.insert_calls]
        self.assertIn("akshare.ak_us_daily_kline", tables)
        self.assertIn("akshare.ak_symbol_cursor", tables)
        self.assertIn("akshare.ak_sync_task_log", tables)
        cursor_call = next(call for call in client.insert_calls if call[0].endswith("ak_symbol_cursor"))
        self.assertEqual(cursor_call[2][0][1], "AAPL")
        self.assertEqual(cursor_call[2][0][2], date(2024, 1, 3))

    def test_profile_without_codes_uses_saved_symbol_universe(self) -> None:
        provider = AkshareUSProvider(
            AkshareUSConfig(request_interval_seconds=0, retries=0),
            akshare_module=_FakeAkshare(),
        )
        client = _FakeClickHouseClient()
        repository = AkshareUSRepository(client, database="akshare")
        args = SyncArgs(
            task="us_company_profile",
            codes_raw="",
            begin_date="",
            end_date="",
            index_code="",
            period="",
            fields="",
            limit=0,
            force=True,
            continue_on_error=False,
            runtime_path=None,
            database="akshare",
            log_level="INFO",
        )

        with patch.object(repository, "load_symbols", return_value=["AAPL"]):
            inserted = run_sync_args(args, provider, repository)

        self.assertEqual(inserted, 1)
        self.assertTrue(
            any(call[0] == "stock_individual_basic_info_us_xq" for call in provider._akshare_module.calls)
        )
        self.assertFalse(any(call[0] == "stock_us_spot_em" for call in provider._akshare_module.calls))
        self.assertTrue(any(call[0].endswith("ak_us_company_profile") for call in client.insert_calls))
        self.assertTrue(any(call[0].endswith("ak_sync_task_log") for call in client.insert_calls))

    def test_minute_task_skips_symbols_missing_eastmoney_market_code(self) -> None:
        provider = AkshareUSProvider(
            AkshareUSConfig(request_interval_seconds=0, retries=0),
            akshare_module=_FakeAkshare(),
        )
        client = _FakeClickHouseClient()
        repository = AkshareUSRepository(client, database="akshare")
        args = SyncArgs(
            task="us_minute_kline",
            codes_raw="AAPL,AABA",
            begin_date="20240103",
            end_date="20240103",
            index_code="",
            period="",
            fields="",
            limit=0,
            force=True,
            continue_on_error=False,
            runtime_path=None,
            database="akshare",
            log_level="INFO",
        )

        inserted = run_sync_args(args, provider, repository)

        self.assertEqual(inserted, 1)
        minute_calls = [
            item for item in provider._akshare_module.calls if item[0] == "stock_us_hist_min_em"
        ]
        self.assertEqual(len(minute_calls), 1)
        self.assertEqual(minute_calls[0][1]["symbol"], "105.AAPL")

    def test_per_symbol_task_stops_after_five_consecutive_upstream_failures(self) -> None:
        akshare = _FailingValuationAkshare()
        provider = AkshareUSProvider(
            AkshareUSConfig(request_interval_seconds=0, retries=2),
            akshare_module=akshare,
        )
        repository = AkshareUSRepository(_FakeClickHouseClient(), database="akshare")
        args = SyncArgs(
            task="us_valuation",
            codes_raw="A,B,C,D,E,F",
            begin_date="",
            end_date="",
            index_code="",
            period="近一年",
            fields="总市值",
            limit=0,
            force=True,
            continue_on_error=True,
            runtime_path=None,
            database="akshare",
            log_level="INFO",
        )

        with self.assertRaisesRegex(RuntimeError, "连续 5 个代码请求失败"):
            run_sync_args(args, provider, repository)

        valuation_calls = [
            item for item in akshare.calls if item[0] == "stock_us_valuation_baidu"
        ]
        self.assertEqual(len(valuation_calls), 5)

    def test_load_execution_plan(self) -> None:
        content = textwrap.dedent(
            """
            source = "akshare"
            database = "us_akshare"

            [defaults]
            codes = ["AAPL", "MSFT"]
            limit = 10
            continue_on_error = true

            [[tasks]]
            task = "us_daily_kline"
            begin_date = 20240101

            [[tasks]]
            task = "us_valuation"
            period = "近一年"
            fields = "总市值,市净率"
            """
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "akshare.toml"
            path.write_text(content, encoding="utf-8")
            plan = load_execution_plan_from_toml(str(path))

        self.assertEqual(plan.database, "us_akshare")
        self.assertEqual([task.task for task in plan.tasks], ["us_daily_kline", "us_valuation"])
        self.assertEqual(plan.tasks[0].codes_raw, "AAPL,MSFT")
        self.assertEqual(plan.tasks[1].fields, "总市值,市净率")

    def test_ths_concept_index_task_uses_directory_and_writes_per_concept_cursor(self) -> None:
        provider = AkshareUSProvider(
            AkshareUSConfig(request_interval_seconds=0, retries=0),
            akshare_module=_FakeAkshare(),
        )
        client = _FakeClickHouseClient()
        repository = AkshareUSRepository(client, database="akshare")
        args = SyncArgs(
            task="stock_board_concept_index_ths",
            codes_raw="阿里巴巴概念",
            begin_date="20240101",
            end_date="20240103",
            index_code="",
            period="",
            fields="",
            limit=0,
            force=True,
            continue_on_error=False,
            runtime_path=None,
            database="akshare",
            log_level="INFO",
        )

        inserted = run_sync_args(args, provider, repository)

        self.assertEqual(inserted, 2)
        tables = [call[0] for call in client.insert_calls]
        self.assertIn("akshare.ak_stock_board_concept_name_ths", tables)
        self.assertIn("akshare.ak_stock_board_concept_index_ths", tables)
        cursor_call = next(call for call in client.insert_calls if call[0].endswith("ak_symbol_cursor"))
        self.assertEqual(cursor_call[2][0][1], "301558")
        self.assertEqual(cursor_call[2][0][2], date(2024, 1, 3))

    def test_ths_concept_info_task_supports_all_concepts_with_limit(self) -> None:
        provider = AkshareUSProvider(
            AkshareUSConfig(request_interval_seconds=0, retries=0),
            akshare_module=_FakeAkshare(),
        )
        client = _FakeClickHouseClient()
        repository = AkshareUSRepository(client, database="akshare")
        args = SyncArgs(
            task="stock_board_concept_info_ths",
            codes_raw="",
            begin_date="",
            end_date="",
            index_code="",
            period="",
            fields="",
            limit=1,
            force=True,
            continue_on_error=False,
            runtime_path=None,
            database="akshare",
            log_level="INFO",
        )

        inserted = run_sync_args(args, provider, repository)

        self.assertEqual(inserted, 3)
        info_calls = [
            item for item in provider._akshare_module.calls
            if item[0] == "stock_board_concept_info_ths"
        ]
        self.assertEqual(len(info_calls), 1)
        self.assertEqual(info_calls[0][1]["symbol"], "机器人概念")

    def test_em_concept_constituents_task_writes_directory_and_snapshot(self) -> None:
        provider = AkshareUSProvider(
            AkshareUSConfig(request_interval_seconds=0, retries=0),
            akshare_module=_FakeAkshare(),
        )
        client = _FakeClickHouseClient()
        repository = AkshareUSRepository(client, database="akshare")
        args = SyncArgs(
            task="stock_board_concept_cons_em",
            codes_raw="融资融券",
            begin_date="",
            end_date="",
            index_code="",
            period="",
            fields="",
            limit=0,
            force=True,
            continue_on_error=False,
            runtime_path=None,
            database="akshare",
            log_level="INFO",
        )

        inserted = run_sync_args(args, provider, repository)

        self.assertEqual(inserted, 1)
        tables = [call[0] for call in client.insert_calls]
        self.assertIn("akshare.ak_stock_board_concept_name_em", tables)
        self.assertIn("akshare.ak_stock_board_concept_cons_em", tables)
        cons_call = next(
            item for item in provider._akshare_module.calls
            if item[0] == "stock_board_concept_cons_em"
        )
        self.assertEqual(cons_call[1]["symbol"], "BK0655")

    def test_em_concept_history_task_separates_period_and_adjust_cursor(self) -> None:
        provider = AkshareUSProvider(
            AkshareUSConfig(request_interval_seconds=0, retries=0, adjust="qfq"),
            akshare_module=_FakeAkshare(),
        )
        client = _FakeClickHouseClient()
        repository = AkshareUSRepository(client, database="akshare")
        args = SyncArgs(
            task="stock_board_concept_hist_em",
            codes_raw="BK0715",
            begin_date="20240101",
            end_date="20240103",
            index_code="",
            period="weekly",
            fields="",
            limit=0,
            force=True,
            continue_on_error=False,
            runtime_path=None,
            database="akshare",
            log_level="INFO",
        )

        inserted = run_sync_args(args, provider, repository)

        self.assertEqual(inserted, 2)
        tables = [call[0] for call in client.insert_calls]
        self.assertIn("akshare.ak_stock_board_concept_hist_em", tables)
        cursor_call = next(call for call in client.insert_calls if call[0].endswith("ak_symbol_cursor"))
        self.assertEqual(cursor_call[2][0][1], "BK0715|WEEKLY|QFQ")
        self.assertEqual(cursor_call[2][0][2], date(2024, 1, 3))
        hist_call = next(
            item for item in provider._akshare_module.calls
            if item[0] == "stock_board_concept_hist_em"
        )
        self.assertEqual(hist_call[1]["period"], "weekly")
        self.assertEqual(hist_call[1]["adjust"], "qfq")


if __name__ == "__main__":
    unittest.main()
