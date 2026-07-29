#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from datetime import date, timedelta

from sync_data_system.clickhouse_tables import (
    CREATE_AD_BALANCE_SHEET_TABLE,
    CREATE_AD_CASH_FLOW_TABLE,
    CREATE_AD_INCOME_TABLE,
)
from sync_data_system.data_models import BalanceSheetRow, CashFlowRow, IncomeRow
from sync_data_system.providers.amazingdata.info import (
    HISTORICAL_REVISION_LOOKBACK_DAYS,
    InfoData,
)


class _FakeFinancialRepository:
    def __init__(self, latest_date: date | None) -> None:
        self.latest_date = latest_date
        self.saved_rows: list[object] = []
        self.sync_logs: list[object] = []

    def load_latest_date_by_codes(self, **_kwargs):
        return self.latest_date

    def save_balance_sheet_rows(self, rows) -> int:
        return self._save(rows)

    def save_cash_flow_rows(self, rows) -> int:
        return self._save(rows)

    def save_income_rows(self, rows) -> int:
        return self._save(rows)

    def insert_sync_log(self, row) -> None:
        self.sync_logs.append(row)

    def _save(self, rows) -> int:
        batch = list(rows)
        self.saved_rows.extend(batch)
        return len(batch)


class _FakeFinancialProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[str], date | None, date | None]] = []

    def fetch_balance_sheet(self, code_list, start_date=None, end_date=None):
        self.calls.append(("balance_sheet", list(code_list), start_date, end_date))
        yield BalanceSheetRow(
            market_code=code_list[0],
            statement_type="1",
            reporting_period=end_date,
            ann_date=end_date,
        )

    def fetch_cash_flow(self, code_list, start_date=None, end_date=None):
        self.calls.append(("cash_flow", list(code_list), start_date, end_date))
        yield CashFlowRow(
            market_code=code_list[0],
            statement_type="1",
            reporting_period=end_date,
            ann_date=end_date,
        )

    def fetch_income(self, code_list, start_date=None, end_date=None):
        self.calls.append(("income", list(code_list), start_date, end_date))
        yield IncomeRow(
            market_code=code_list[0],
            statement_type="1",
            reporting_period=end_date,
            ann_date=end_date,
        )


class AmazingDataFinancialStatementSchemaTest(unittest.TestCase):
    def test_sorting_key_preserves_statement_types_and_announcements(self) -> None:
        for ddl in (
            CREATE_AD_BALANCE_SHEET_TABLE,
            CREATE_AD_CASH_FLOW_TABLE,
            CREATE_AD_INCOME_TABLE,
        ):
            with self.subTest(ddl=ddl.splitlines()[1]):
                self.assertIn("statement_type Nullable(String)", ddl)
                self.assertIn("report_type Nullable(String)", ddl)
                self.assertIn("reporting_period Nullable(Date)", ddl)
                self.assertIn("ann_date Nullable(Date)", ddl)
                self.assertIn("actual_ann_date Nullable(Date)", ddl)
                self.assertIn("ifNull(statement_type, '')", ddl)
                self.assertIn("ifNull(report_type, '')", ddl)
                self.assertIn("ifNull(ann_date, toDate('1970-01-01'))", ddl)
                self.assertIn("ifNull(actual_ann_date, toDate('1970-01-01'))", ddl)


class AmazingDataFinancialStatementSyncTest(unittest.TestCase):
    def test_force_uses_requested_begin_date_for_all_financial_statements(self) -> None:
        for method_name in ("sync_balance_sheet", "sync_cash_flow", "sync_income"):
            with self.subTest(method_name=method_name):
                repository = _FakeFinancialRepository(latest_date=date(2026, 6, 30))
                provider = _FakeFinancialProvider()
                info_data = InfoData(repository=repository, sync_provider=provider)

                inserted = getattr(info_data, method_name)(
                    code_list=["002602.SZ"],
                    begin_date=20100101,
                    end_date=20261231,
                    force=True,
                )

                self.assertEqual(inserted, 1)
                self.assertEqual(provider.calls[0][2], date(2010, 1, 1))
                self.assertEqual(provider.calls[0][3], date(2026, 12, 31))

    def test_incremental_sync_refetches_adjusted_comparative_window(self) -> None:
        latest_date = date(2026, 6, 30)
        repository = _FakeFinancialRepository(latest_date=latest_date)
        provider = _FakeFinancialProvider()
        info_data = InfoData(repository=repository, sync_provider=provider)

        inserted = info_data.sync_income(
            code_list=["002602.SZ"],
            begin_date=20100101,
            end_date=20261231,
            force=False,
        )

        self.assertEqual(inserted, 1)
        self.assertEqual(
            provider.calls[0][2],
            latest_date - timedelta(days=HISTORICAL_REVISION_LOOKBACK_DAYS),
        )
        self.assertEqual(provider.calls[0][3], date(2026, 12, 31))


if __name__ == "__main__":
    unittest.main()
