#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

from scripts.backfill_amazingdata_missing_stocks import (
    KNOWN_COMPANY_CODE_ALIASES,
    audit_task,
    canonicalize_codes,
    materialize_legacy_financial_rows,
    normalize_mapped_code,
    parse_code_filter,
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

    def test_financial_task_requires_old_code_for_backtest(self) -> None:
        result = audit_task(
            _AuditConnection(["601975.SH"]),
            task="income",
            database="starlight",
            historical_codes={"600087.SH"},
            current_codes={"601975.SH"},
            security_aliases={},
            company_aliases={"600087.SH": "601975.SH"},
        )

        self.assertEqual(result.category, "security_backtest")
        self.assertEqual(result.missing_codes, ("600087.SH",))
        self.assertEqual(result.aliases_satisfied, 0)

    def test_other_company_task_treats_successor_code_as_covered(self) -> None:
        result = audit_task(
            _AuditConnection(["601975.SH"]),
            task="share_holder",
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

    def test_parse_code_filter(self) -> None:
        self.assertEqual(
            parse_code_filter("300114.sz, 601313.SH,300114.SZ"),
            {"300114.SZ", "601313.SH"},
        )

    def test_materialize_financial_rows_is_pit_safe_and_idempotent(self) -> None:
        columns = [
            "market_code",
            "report_date",
            "statement_type",
            "report_type",
            "reporting_period",
            "ann_date",
            "actual_ann_date",
            "payload_json",
        ]
        source_rows = [
            (
                "302132.SZ",
                __import__("datetime").date(2024, 12, 31),
                "1",
                "4",
                __import__("datetime").date(2024, 12, 31),
                __import__("datetime").date(2025, 1, 30),
                __import__("datetime").date(2025, 1, 30),
                "{}",
            )
        ]

        class _MaterializeConnection:
            def __init__(self) -> None:
                self.inserted = []

            def query_rows(self, sql, parameters=None):
                if "system.columns" in sql:
                    return [(column,) for column in columns]
                if parameters and parameters.get("successor_code"):
                    return source_rows
                if parameters and parameters.get("old_code"):
                    return []
                raise AssertionError(sql)

            def insert_rows(self, table, column_names, rows):
                self.inserted.append((table, list(column_names), list(rows)))

        connection = _MaterializeConnection()
        inserted, processed = materialize_legacy_financial_rows(
            connection,
            database="starlight",
            task="income",
            old_codes=["300114.SZ"],
            company_aliases={"300114.SZ": "302132.SZ"},
            legacy_cutoffs={
                "300114.SZ": __import__("datetime").date(2025, 2, 14)
            },
        )

        self.assertEqual(inserted, 1)
        self.assertEqual(processed, ("300114.SZ",))
        self.assertEqual(connection.inserted[0][2][0][0], "300114.SZ")

    def test_materialize_equity_structure_uses_historical_symbol(self) -> None:
        columns = [
            "market_code",
            "ann_date",
            "change_date",
            "ex_change_date",
            "tot_share",
        ]
        source_rows = [
            (
                "302132.SZ",
                __import__("datetime").date(2025, 1, 17),
                __import__("datetime").date(2025, 1, 22),
                __import__("datetime").date(2025, 1, 21),
                267678.2376,
            )
        ]

        class _Connection:
            def __init__(self) -> None:
                self.inserted = []

            def query_rows(self, sql, parameters=None):
                if "system.columns" in sql:
                    return [(column,) for column in columns]
                if parameters and parameters.get("successor_code"):
                    self.assert_cutoff_sql = sql
                    return source_rows
                if parameters and parameters.get("old_code"):
                    return []
                raise AssertionError(sql)

            def insert_rows(self, table, column_names, rows):
                self.inserted.extend(rows)

        connection = _Connection()
        inserted, processed = materialize_legacy_financial_rows(
            connection,
            database="starlight",
            task="equity_structure",
            old_codes=["300114.SZ"],
            company_aliases={"300114.SZ": "302132.SZ"},
            legacy_cutoffs={
                "300114.SZ": __import__("datetime").date(2025, 2, 14)
            },
        )

        self.assertEqual(inserted, 1)
        self.assertEqual(processed, ("300114.SZ",))
        self.assertEqual(connection.inserted[0][0], "300114.SZ")
        self.assertIn("ex_change_date", connection.assert_cutoff_sql)


if __name__ == "__main__":
    unittest.main()
