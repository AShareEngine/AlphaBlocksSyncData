#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path

from sync_data_system.core.sync_plan import validate_sync_plan


class SyncPlanValidationTest(unittest.TestCase):
    def _write_config(self, content: str) -> str:
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        path = Path(tmpdir.name) / "run_sync.test.toml"
        path.write_text(textwrap.dedent(content), encoding="utf-8")
        return str(path)

    def test_validates_qmt_canonical_codes_field(self) -> None:
        path = self._write_config(
            """
            source = "qmt"
            log_level = "INFO"
            continue_on_error = true
            database = "qmt"

            [defaults]
            codes = ["600000.SH"]
            begin_date = 20240101
            end_date = 20240131

            [[tasks]]
            task = "kline_history"
            period = "1d"
            """
        )

        result = validate_sync_plan(path)

        self.assertEqual(result.source, "qmt")
        self.assertEqual(result.total_tasks, 1)
        self.assertEqual(result.enabled_tasks, 1)

    def test_rejects_unknown_default_field(self) -> None:
        path = self._write_config(
            """
            source = "qmt"

            [defaults]
            symbols = ["600000.SH"]

            [[tasks]]
            task = "kline_history"
            """
        )

        with self.assertRaisesRegex(ValueError, "unknown field"):
            validate_sync_plan(path)

    def test_rejects_unknown_task_name(self) -> None:
        path = self._write_config(
            """
            source = "baostock"

            [[tasks]]
            task = "missing_task"
            """
        )

        with self.assertRaisesRegex(ValueError, "not declared"):
            validate_sync_plan(path)

    def test_rejects_wrong_field_type(self) -> None:
        path = self._write_config(
            """
            source = "amazingdata"

            [defaults]
            force = "yes"

            [[tasks]]
            task = "code_info"
            """
        )

        with self.assertRaisesRegex(ValueError, "force must be boolean"):
            validate_sync_plan(path)


if __name__ == "__main__":
    unittest.main()
