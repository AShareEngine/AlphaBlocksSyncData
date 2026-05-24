#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from datetime import date, datetime

from sync_data_system.data_models import MarketKlineRow
from sync_data_system.providers.amazingdata.market import MarketData


class _FakeMarketRepository:
    def __init__(self, latest_date: date | None = None) -> None:
        self.latest_date = latest_date
        self.saved_batches: list[list[MarketKlineRow]] = []
        self.sync_logs = []

    def load_latest_kline_trade_date_map(self, code_list):
        return {code: self.latest_date for code in code_list}

    def load_latest_kline_minute_trade_date_map(self, code_list):
        return {code: self.latest_date for code in code_list}

    def save_market_kline_rows(self, rows) -> int:
        batch = list(rows)
        self.saved_batches.append(batch)
        return len(batch)

    def save_market_kline_minute_rows(self, rows) -> int:
        batch = list(rows)
        self.saved_batches.append(batch)
        return len(batch)

    def insert_sync_log(self, row) -> None:
        self.sync_logs.append(row)


class _FakeMarketProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[list[str], date, date, str]] = []

    def fetch_kline(self, code_list, begin_date, end_date, period, begin_time=None, end_time=None):
        self.calls.append((list(code_list), begin_date, end_date, str(period)))
        yield MarketKlineRow(
            trade_time=datetime.combine(begin_date, datetime.min.time()),
            code=code_list[0],
            open=1.0,
            high=1.0,
            low=1.0,
            close=1.0,
            volume=1.0,
            amount=1.0,
        )


class AmazingDataMarketSyncTest(unittest.TestCase):
    def test_minute_kline_force_uses_requested_begin_date_even_when_latest_exists(self) -> None:
        repository = _FakeMarketRepository(latest_date=date(2026, 5, 21))
        provider = _FakeMarketProvider()
        market_data = MarketData(repository=repository, sync_provider=provider)

        inserted = market_data.sync_kline_minute(
            code_list=["000001.SZ"],
            begin_date=20100101,
            end_date=20260525,
            force=True,
        )

        self.assertEqual(inserted, 1)
        self.assertEqual(provider.calls[0][1], date(2010, 1, 1))
        self.assertEqual(provider.calls[0][2], date(2026, 5, 25))

    def test_minute_kline_without_force_keeps_incremental_start(self) -> None:
        repository = _FakeMarketRepository(latest_date=date(2026, 5, 21))
        provider = _FakeMarketProvider()
        market_data = MarketData(repository=repository, sync_provider=provider)

        inserted = market_data.sync_kline_minute(
            code_list=["000001.SZ"],
            begin_date=20100101,
            end_date=20260525,
            force=False,
        )

        self.assertEqual(inserted, 1)
        self.assertEqual(provider.calls[0][1], date(2026, 5, 22))
        self.assertEqual(provider.calls[0][2], date(2026, 5, 25))


if __name__ == "__main__":
    unittest.main()
