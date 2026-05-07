#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from datetime import date, datetime
import unittest

from sync_data_system.sync_core.amazingdata import build_hashed_scope_key, resolve_incremental_start_date, safe_write_sync_log
from sync_data_system.sync_core.incremental import advance_cursor_value, compare_cursor_values, normalize_request_value
from sync_data_system.sync_core.scope import build_scope_key
from sync_data_system.sync_core.sync_models import SyncTaskLogRow


class SyncCoreIncrementalTest(unittest.TestCase):
    def test_normalize_request_value(self) -> None:
        self.assertEqual(normalize_request_value("2024-01-31", "day"), "20240131")
        self.assertEqual(normalize_request_value("2024-01", "month"), "202401")
        self.assertEqual(normalize_request_value("2024", "year"), "2024")

    def test_advance_cursor_value(self) -> None:
        self.assertEqual(advance_cursor_value("2024-01-31", "day"), "20240201")
        self.assertEqual(advance_cursor_value("2024-12", "month"), "202501")
        self.assertEqual(advance_cursor_value("2024", "year"), "2025")

    def test_compare_cursor_values(self) -> None:
        self.assertEqual(compare_cursor_values("20240101", "20240102"), -1)
        self.assertEqual(compare_cursor_values("20240102", "20240101"), 1)
        self.assertEqual(compare_cursor_values("20240101", "20240101"), 0)


class SyncCoreScopeTest(unittest.TestCase):
    def test_build_scope_key(self) -> None:
        scope_key = build_scope_key(
            "daily_kline",
            {
                "code": "600000.SH",
                "day": "",
                "start_date": "20240101",
                "end_date": "20240131",
                "year": None,
                "quarter": None,
                "year_type": "",
            },
        )
        self.assertEqual(scope_key, "task=daily_kline|code=600000.SH|begin=20240101|end=20240131")


class _FakeLogRepository:
    def __init__(self) -> None:
        self.rows: list[SyncTaskLogRow] = []

    def insert_sync_log(self, row: SyncTaskLogRow) -> None:
        self.rows.append(row)


class SyncCoreAmazingDataTest(unittest.TestCase):
    def test_build_hashed_scope_key(self) -> None:
        scope_key = build_hashed_scope_key("get_stock_basic", ["000001.SZ", "600000.SH"])
        self.assertTrue(scope_key.startswith("task=get_stock_basic|code_count=2|codes_sha1="))

    def test_resolve_incremental_start_date(self) -> None:
        self.assertEqual(
            resolve_incremental_start_date(date(2024, 1, 31), date(2024, 1, 1)),
            date(2024, 2, 1),
        )
        self.assertEqual(resolve_incremental_start_date(None, date(2024, 1, 1)), date(2024, 1, 1))

    def test_safe_write_sync_log(self) -> None:
        repository = _FakeLogRepository()
        safe_write_sync_log(
            repository=repository,
            task_name="demo",
            scope_key="task=demo",
            run_date=date(2024, 1, 31),
            status="success",
            target_table="demo_table",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
            row_count=12,
            message="ok",
            started_at=datetime(2024, 1, 31, 10, 0, 0),
            finished_at=datetime(2024, 1, 31, 10, 1, 0),
        )
        self.assertEqual(len(repository.rows), 1)
        self.assertEqual(repository.rows[0].task_name, "demo")
        self.assertEqual(repository.rows[0].row_count, 12)


if __name__ == "__main__":
    unittest.main()
