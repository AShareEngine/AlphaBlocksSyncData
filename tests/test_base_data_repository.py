#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

from sync_data_system.data_models import PriceFactorQuery
from sync_data_system.repositories.base_data_repository import BaseDataRepository


class _FakeClickHouseClient:
    def __init__(self) -> None:
        self.last_sql = ""
        self.last_params = None

    def query_df(self, sql, params=None):
        self.last_sql = sql
        self.last_params = params
        if pd is None:
            raise RuntimeError("pandas is required for this test")
        return pd.DataFrame(columns=["trade_date", "code", "factor_value"])


@unittest.skipIf(pd is None, "pandas is required")
class BaseDataRepositoryTest(unittest.TestCase):
    def test_load_price_factor_frame_reads_replacing_merge_tree_with_final(self) -> None:
        client = _FakeClickHouseClient()
        repository = BaseDataRepository(client)

        repository.load_price_factor_frame(
            table_name="ad_backward_factor",
            query=PriceFactorQuery(
                code_list=("600000.SH", "600004.SH"),
                local_path="/tmp/amazing_data_cache",
                is_local=False,
            ),
        )

        self.assertIn("FROM ad_backward_factor FINAL", client.last_sql)
        self.assertIn("factor_value", client.last_sql)
        self.assertNotIn("any(factor_value)", client.last_sql)
        self.assertNotIn("GROUP BY trade_date, code", client.last_sql)
        self.assertEqual(client.last_params, {"code_list": ["600000.SH", "600004.SH"]})


if __name__ == "__main__":
    unittest.main()
