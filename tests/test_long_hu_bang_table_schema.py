#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

from sync_data_system.clickhouse_tables import CREATE_AD_LONG_HU_BANG_TABLE


class LongHuBangTableSchemaTest(unittest.TestCase):
    def test_long_hu_bang_order_key_preserves_broker_rows(self) -> None:
        ddl = CREATE_AD_LONG_HU_BANG_TABLE

        self.assertIn("ENGINE = ReplacingMergeTree", ddl)
        self.assertIn("market_code", ddl)
        self.assertIn("trade_date", ddl)
        self.assertIn("ifNull(reason_type, '')", ddl)
        self.assertIn("ifNull(flow_mark, -1)", ddl)
        self.assertIn("ifNull(trader_name, '')", ddl)
        self.assertIn("ifNull(buy_amount, -1)", ddl)
        self.assertIn("ifNull(sell_amount, -1)", ddl)


if __name__ == "__main__":
    unittest.main()
