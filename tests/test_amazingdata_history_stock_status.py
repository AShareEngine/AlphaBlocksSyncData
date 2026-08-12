#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from datetime import date, datetime
from types import SimpleNamespace

import pandas as pd

from sync_data_system.providers.amazingdata.provider import (
    AmazingDataSDKConfig,
    AmazingDataSDKProvider,
)


def _config() -> AmazingDataSDKConfig:
    return AmazingDataSDKConfig(
        username="demo",
        password="secret",
        host="127.0.0.1",
        port=8600,
        local_path="/tmp/amazingdata-test",
    )


class AmazingDataHistoryStockStatusTest(unittest.TestCase):
    def test_empty_dedicated_result_falls_back_to_daily_kline(self) -> None:
        provider = AmazingDataSDKProvider(_config())
        info = SimpleNamespace(get_history_stock_status=lambda **kwargs: {"510300.SH": pd.DataFrame()})
        klines = pd.DataFrame(
            [
                {
                    "close": 3.90,
                },
                {
                    "close": 4.00,
                },
            ],
            index=[datetime(2026, 8, 7, 15, 0), datetime(2026, 8, 10, 15, 0)],
        )
        market = SimpleNamespace(
            query_kline=lambda codes, **kwargs: {"510300.SH": klines}
        )
        provider.session = SimpleNamespace(
            info=info,
            market=market,
            resolve_period_value=lambda period: 10008,
        )

        rows = list(
            provider.fetch_history_stock_status(
                ["510300.SH"],
                start_date=date(2026, 8, 10),
                end_date=date(2026, 8, 10),
            )
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].market_code, "510300.SH")
        self.assertEqual(rows[0].trade_date, date(2026, 8, 10))
        self.assertEqual(rows[0].preclose, 3.90)
        self.assertIsNone(rows[0].high_limited)
        self.assertIsNone(rows[0].price_high_lmt_rate)
        self.assertIsNone(rows[0].is_st_sec)

    def test_dedicated_result_does_not_request_snapshot_for_observed_code(self) -> None:
        provider = AmazingDataSDKProvider(_config())
        frame = pd.DataFrame(
            [{"MARKET_CODE": "000001.SZ", "TRADE_DATE": "20260810", "PRECLOSE": 10.0}]
        )
        info = SimpleNamespace(get_history_stock_status=lambda **kwargs: frame)

        class Market:
            def query_kline(self, *args, **kwargs):
                raise AssertionError("kline fallback should not run")

        provider.session = SimpleNamespace(info=info, market=Market())

        rows = list(
            provider.fetch_history_stock_status(
                ["000001.SZ"],
                start_date=date(2026, 8, 10),
                end_date=date(2026, 8, 10),
            )
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].market_code, "000001.SZ")


if __name__ == "__main__":
    unittest.main()
