#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

from sync_data_system.providers.amazingdata.base import BaseData


class _FailingCodeInfoProvider:
    def __init__(self) -> None:
        self.calls = 0

    def fetch_code_info(self, *, security_type, start_date=None):
        self.calls += 1
        raise TypeError("'NoneType' object is not subscriptable")


class _CachedCodeInfoRepository:
    def __init__(self) -> None:
        self.logs = []

    def has_successful_sync_today(self, task_name, scope_key, run_date):
        return False

    def load_sync_checkpoint_date(self, task_name, scope_key):
        return None

    def save_code_info_rows(self, rows):
        return len(list(rows))

    def load_code_info_frame(self, query):
        return pd.DataFrame(
            {"symbol": ["平安银行", "浦发银行"]},
            index=["000001.SZ", "600000.SH"],
        )

    def insert_sync_log(self, row):
        self.logs.append(row)


@unittest.skipIf(pd is None, "pandas is required")
class AmazingDataCodeUniverseTest(unittest.TestCase):
    def test_code_pool_falls_back_to_clickhouse_when_sdk_refresh_fails(self) -> None:
        provider = _FailingCodeInfoProvider()
        repository = _CachedCodeInfoRepository()
        base_data = BaseData(repository=repository, sync_provider=provider)

        codes = base_data.ensure_code_list(security_type="EXTRA_STOCK_A")

        self.assertEqual(codes, ["000001.SZ", "600000.SH"])
        self.assertEqual(provider.calls, 1)
        self.assertEqual(repository.logs[-1].status, "success")
        self.assertIn("fallback=clickhouse_cache", repository.logs[-1].message)

    def test_explicit_code_info_sync_still_fails_when_sdk_fails(self) -> None:
        base_data = BaseData(
            repository=_CachedCodeInfoRepository(),
            sync_provider=_FailingCodeInfoProvider(),
        )

        with self.assertRaisesRegex(TypeError, "NoneType"):
            base_data.sync_code_info(security_type="EXTRA_STOCK_A")


if __name__ == "__main__":
    unittest.main()
