#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from datetime import date
from types import SimpleNamespace

from sync_data_system.clickhouse_tables import (
    CREATE_AD_EQUITY_PLEDGE_FREEZE_TABLE,
    CREATE_AD_EQUITY_RESTRICTED_TABLE,
    CREATE_AD_FUND_SHARE_TABLE,
    CREATE_AD_HOLDER_NUM_TABLE,
    CREATE_AD_PROFIT_EXPRESS_TABLE,
    CREATE_AD_PROFIT_NOTICE_TABLE,
    CREATE_AD_SHARE_HOLDER_TABLE,
)
from sync_data_system.providers.amazingdata.info import InfoData
from sync_data_system.providers.amazingdata.runner import resolve_missing_historical_code_list


class _MissingHistoricalClient:
    def __init__(self) -> None:
        self.data_parameters: dict[str, object] | None = None

    def query_rows(self, sql, parameters):
        if "system.columns" in sql:
            return [("market_code",)]
        self.data_parameters = dict(parameters)
        return []


class AmazingDataReplacingMergeTreeKeyTest(unittest.TestCase):
    def test_profit_express_preserves_announcement_versions(self) -> None:
        ddl = CREATE_AD_PROFIT_EXPRESS_TABLE

        self.assertIn("reporting_period Nullable(Date)", ddl)
        self.assertIn("ann_date Nullable(Date)", ddl)
        self.assertIn("actual_ann_date Nullable(Date)", ddl)
        self.assertIn("ifNull(ann_date, toDate('1970-01-01'))", ddl)
        self.assertIn("ifNull(actual_ann_date, toDate('1970-01-01'))", ddl)

    def test_profit_notice_preserves_forecast_versions_and_types(self) -> None:
        ddl = CREATE_AD_PROFIT_NOTICE_TABLE

        for key_expression in (
            "ifNull(ann_date, toDate('1970-01-01'))",
            "ifNull(first_ann_date, toDate('1970-01-01'))",
            "ifNull(p_typecode, '')",
            "ifNull(report_type, '')",
            "ifNull(p_number, -1)",
        ):
            with self.subTest(key_expression=key_expression):
                self.assertIn(key_expression, ddl)

    def test_fund_share_preserves_consolidated_and_standalone_rows(self) -> None:
        ddl = CREATE_AD_FUND_SHARE_TABLE

        self.assertIn("ifNull(is_consolidated_data, -1)", ddl)
        self.assertIn("ifNull(change_reason, '')", ddl)

    def test_share_holder_preserves_announcement_and_holder_type(self) -> None:
        ddl = CREATE_AD_SHARE_HOLDER_TABLE

        self.assertIn("ifNull(ann_date, toDate('1970-01-01'))", ddl)
        self.assertIn("ifNull(holder_type, -1)", ddl)
        self.assertIn("ifNull(qty_num, -1)", ddl)

    def test_holder_num_preserves_announcement_versions(self) -> None:
        ddl = CREATE_AD_HOLDER_NUM_TABLE

        self.assertIn("ifNull(holder_enddate, toDate('1970-01-01'))", ddl)
        self.assertIn("ifNull(ann_dt, toDate('1970-01-01'))", ddl)

    def test_equity_pledge_preserves_distinct_holder_events(self) -> None:
        ddl = CREATE_AD_EQUITY_PLEDGE_FREEZE_TABLE

        for key_expression in (
            "ifNull(holder_type_code, -1)",
            "ifNull(begin_date, toDate('1970-01-01'))",
            "ifNull(frozen_institution, '')",
            "ifNull(shr_category_code, -1)",
            "ifNull(freeze_type, -1)",
            "ifNull(is_equity_pledge_repo, -1)",
        ):
            with self.subTest(key_expression=key_expression):
                self.assertIn(key_expression, ddl)

    def test_equity_restricted_preserves_distinct_unlock_types(self) -> None:
        ddl = CREATE_AD_EQUITY_RESTRICTED_TABLE

        self.assertIn("ifNull(share_lst_type_name, '')", ddl)
        self.assertIn("ifNull(share_lst_is_ann, -1)", ddl)

    def test_force_aware_start_uses_requested_history(self) -> None:
        self.assertEqual(
            InfoData._resolve_force_aware_sync_start_date(
                latest_date=date(2026, 6, 30),
                requested_begin_date=date(2010, 1, 1),
                force=True,
            ),
            date(2010, 1, 1),
        )
        self.assertEqual(
            InfoData._resolve_force_aware_sync_start_date(
                latest_date=date(2026, 6, 30),
                requested_begin_date=date(2010, 1, 1),
                force=False,
            ),
            date(2026, 7, 1),
        )

    def test_missing_historical_uses_etf_universe_for_fund_share(self) -> None:
        client = _MissingHistoricalClient()
        context = SimpleNamespace(
            base_data=SimpleNamespace(
                repository=SimpleNamespace(client=client),
            )
        )

        codes = resolve_missing_historical_code_list(
            context=context,
            task="fund_share",
            begin_date=20100101,
            end_date=20260729,
            limit=0,
        )

        self.assertEqual(codes, [])
        self.assertIsNotNone(client.data_parameters)
        self.assertEqual(client.data_parameters["security_type"], "EXTRA_ETF")


if __name__ == "__main__":
    unittest.main()
