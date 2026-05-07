#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from datetime import date

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

from sync_data_system.data_models import BjCodeMappingRow
from sync_data_system.info_data import InfoData


class _FakeRepository:
    def __init__(self) -> None:
        self.rows: list[BjCodeMappingRow] = []
        self.logs = []

    def load_sync_checkpoint_date(self, task_name: str, scope_key: str):
        return None

    def has_successful_sync_today(self, task_name: str, scope_key: str, run_date: date) -> bool:
        return False

    def save_bj_code_mapping_rows(self, rows) -> int:
        batch = list(rows)
        self.rows.extend(batch)
        return len(batch)

    def insert_sync_log(self, row) -> None:
        self.logs.append(row)

    def load_bj_code_mapping_frame(self):
        if pd is None:
            raise RuntimeError("pandas is required")
        return pd.DataFrame(
            [
                {
                    "old_code": row.old_code,
                    "new_code": row.new_code,
                    "security_name": row.security_name,
                    "listing_date": row.listing_date,
                }
                for row in self.rows
            ]
        )


class _FakeProvider:
    def fetch_bj_code_mapping(self):
        yield BjCodeMappingRow(
            old_code="430001.BJ",
            new_code="830001.BJ",
            security_name="示例证券",
            listing_date=20210101,
        )


@unittest.skipIf(pd is None, "pandas is required")
class BjCodeMappingTest(unittest.TestCase):
    def test_sync_and_get_bj_code_mapping(self) -> None:
        info_data = InfoData(repository=_FakeRepository(), sync_provider=_FakeProvider())

        inserted = info_data.sync_bj_code_mapping(force=True)
        frame = info_data.get_bj_code_mapping(local_path="/tmp/amazing_data_cache", is_local=True)

        self.assertEqual(inserted, 1)
        self.assertEqual(list(frame.columns), ["OLD_CODE", "NEW_CODE", "SECURITY_NAME", "LISTING_DATE"])
        self.assertEqual(frame.iloc[0]["OLD_CODE"], "430001.BJ")
        self.assertEqual(frame.iloc[0]["NEW_CODE"], "830001.BJ")


if __name__ == "__main__":
    unittest.main()
