#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from sync_data_system.service.table_check_state import TableCheckStateStore


class TableCheckStateStoreTest(unittest.TestCase):
    def test_success_with_zero_rows_is_a_successful_check(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TableCheckStateStore(Path(tmpdir), state_dir=Path(tmpdir) / "state")

            record = store.record(
                provider="baostock",
                task="baostock.adjust_factor",
                database="baostock",
                table="bs_adjust_factor",
                status="success",
                job_id="job-1",
                attempted_at="2026-07-22T10:00:00+00:00",
                finished_at="2026-07-22T10:01:00+00:00",
                rows_written=0,
            )

            self.assertEqual(record["last_status"], "success")
            self.assertEqual(record["last_success_at"], "2026-07-22T10:01:00+00:00")
            self.assertEqual(record["rows_written"], 0)
            self.assertEqual(store.latest_for_tasks(["baostock.adjust_factor"]), record)
            self.assertEqual(list(store.checks_dir.glob("*.tmp")), [])

    def test_failure_preserves_previous_success_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TableCheckStateStore(Path(tmpdir), state_dir=Path(tmpdir) / "state")
            identity = {
                "provider": "baostock",
                "task": "baostock.adjust_factor",
                "database": "baostock",
                "table": "bs_adjust_factor",
            }
            store.record(
                **identity,
                status="success",
                job_id="job-1",
                finished_at="2026-07-21T10:00:00+00:00",
            )

            record = store.record(
                **identity,
                status="failed",
                job_id="job-2",
                attempted_at="2026-07-22T10:00:00+00:00",
                finished_at="2026-07-22T10:01:00+00:00",
                error="provider unavailable",
            )

            self.assertEqual(record["last_status"], "failed")
            self.assertEqual(record["last_success_at"], "2026-07-21T10:00:00+00:00")
            self.assertEqual(record["last_attempt_at"], "2026-07-22T10:00:00+00:00")
            self.assertEqual(record["last_error"], "provider unavailable")


if __name__ == "__main__":
    unittest.main()
