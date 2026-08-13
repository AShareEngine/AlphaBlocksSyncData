#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

from scripts.backfill_amazingdata_missing_stocks import (
    KNOWN_COMPANY_CODE_ALIASES,
    audit_task,
    canonicalize_codes,
    normalize_mapped_code,
    resolve_requested_tasks,
)


class _AuditConnection:
    def __init__(self, existing_codes: list[str]) -> None:
        self.existing_codes = existing_codes

    def query_rows(self, sql, parameters=None):
        if "system.columns" in sql:
            return [("market_code",)]
        if "SELECT DISTINCT `market_code`" in sql:
            return [(code,) for code in self.existing_codes]
        raise AssertionError(sql)


class AmazingDataMissingStockBackfillTest(unittest.TestCase):
    def test_normalize_mapped_code_adds_market_suffix(self) -> None:
        self.assertEqual(normalize_mapped_code("430017", "BJ"), "430017.BJ")
        self.assertEqual(normalize_mapped_code("920017.BJ", "BJ"), "920017.BJ")

    def test_canonicalize_codes_deduplicates_legacy_bj_code(self) -> None:
        aliases = {"430017.BJ": "920017.BJ"}
        self.assertEqual(
            canonicalize_codes(["430017.BJ", "920017.BJ"], aliases),
            {"920017.BJ"},
        )

    def test_company_task_treats_successor_code_as_covered(self) -> None:
        result = audit_task(
            _AuditConnection(["601975.SH"]),
            task="income",
            database="starlight",
            historical_codes={"600087.SH"},
            current_codes={"601975.SH"},
            security_aliases={},
            company_aliases={"600087.SH": "601975.SH"},
        )

        self.assertEqual(result.missing_all, 0)
        self.assertEqual(result.aliases_satisfied, 1)

    def test_security_task_preserves_old_relisted_symbol(self) -> None:
        result = audit_task(
            _AuditConnection(["601975.SH"]),
            task="daily_kline",
            database="starlight",
            historical_codes={"600087.SH"},
            current_codes={"601975.SH"},
            security_aliases={},
            company_aliases={"600087.SH": "601975.SH"},
        )

        self.assertEqual(result.missing_codes, ("600087.SH",))

    def test_execute_defaults_to_safe_coverage_tasks(self) -> None:
        tasks = resolve_requested_tasks([], execute=True)
        self.assertIn("income", tasks)
        self.assertNotIn("long_hu_bang", tasks)

    def test_verified_company_migration_aliases_are_available(self) -> None:
        self.assertEqual(KNOWN_COMPANY_CODE_ALIASES["300114.SZ"], "302132.SZ")


if __name__ == "__main__":
    unittest.main()
