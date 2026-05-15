#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import io
import tempfile
import textwrap
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from sync_data_system.scripts import test_qmt_toml


class QmtTomlCliTest(unittest.TestCase):
    def _write_config(self, content: str) -> str:
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        path = Path(tmpdir.name) / "qmt.toml"
        path.write_text(textwrap.dedent(content), encoding="utf-8")
        return str(path)

    def test_dry_run_parses_toml_without_calling_provider(self) -> None:
        path = self._write_config(
            """
            source = "qmt"
            [[tasks]]
            task = "kline_history"
            symbols = ["600000.SH"]
            begin_date = 20240101
            end_date = 20240131
            """
        )

        output = io.StringIO()
        with redirect_stdout(output):
            code = test_qmt_toml.main_for_test(["--dry-run", path])

        self.assertEqual(code, 0)
        self.assertIn("[DRY] task=kline_history", output.getvalue())
        self.assertIn("[SUMMARY] passed=1 failed=0", output.getvalue())

    def test_skips_download_tasks_by_default(self) -> None:
        path = self._write_config(
            """
            source = "qmt"
            [[tasks]]
            task = "download_holiday"
            """
        )

        output = io.StringIO()
        with redirect_stdout(output):
            code = test_qmt_toml.main_for_test(["--dry-run", path])

        self.assertEqual(code, 0)
        self.assertIn("[SKIP] task=download_holiday reason=download_task", output.getvalue())

    def test_calls_provider_and_reports_failure(self) -> None:
        path = self._write_config(
            """
            source = "qmt"
            [[tasks]]
            task = "kline_history"
            symbols = ["600000.SH"]
            begin_date = 20240101
            end_date = 20240131
            [[tasks]]
            task = "full_tick"
            symbols = ["600000.SH"]
            """
        )
        calls = []

        def fake_call(config, request_payload):
            calls.append((config, request_payload))
            if request_payload["task"] == "full_tick":
                raise RuntimeError("boom")
            return {"success": True, "code": 200, "message": "ok", "data": {"items": [{"symbol": "600000.SH", "bars": [{"time_ms": 1}]}]}}

        output = io.StringIO()
        with patch.object(test_qmt_toml, "call_qmt", fake_call), redirect_stdout(output):
            code = test_qmt_toml.main_for_test(["--base-url", "http://qmt.local:8000", "--api-key", "test-key", path])

        self.assertEqual(code, 1)
        self.assertEqual(calls[0][1]["task"], "kline_history")
        self.assertIn("[OK] task=kline_history", output.getvalue())
        self.assertIn("[FAIL] task=full_tick", output.getvalue())


if __name__ == "__main__":
    unittest.main()
