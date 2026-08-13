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
from sync_data_system.providers.amazingdata.runner import (
    resolve_historical_code_list,
    resolve_missing_historical_code_list,
)


class _MissingHistoricalClient:
    def __init__(self) -> None:
        self.data_parameters: dict[str, object] | None = None

    def query_rows(self, sql, parameters=None):
        if "system.columns" in sql:
            return [("market_code",)]
        if "ad_bj_code_mapping" in sql:
            return []
        if "SELECT DISTINCT market_code FROM starlight.ad_fund_share" in " ".join(sql.split()):
            return []
        self.data_parameters = dict(parameters)
        return []


class AmazingDataReplacingMergeTreeKeyTest(unittest.TestCase):
    def test_historical_universe_merges_current_and_delisted_stocks(self) -> None:
        class _HistoricalClient:
            def query_rows(self, sql, parameters):
                if "ad_bj_code_mapping" in sql:
                    return []
                self.parameters = dict(parameters)
                return [("000005.SZ",), ("600000.SH",)]

        client = _HistoricalClient()
        base_data = SimpleNamespace(
            repository=SimpleNamespace(client=client),
            get_stock_universe=lambda security_type, force=False: ["000001.SZ", "600000.SH"],
        )
        context = SimpleNamespace(
            base_data=base_data,
            sdk_config=SimpleNamespace(local_path="/tmp"),
        )

        codes = resolve_historical_code_list(
            context=context,
            task="income",
            begin_date=20100101,
            end_date=20260729,
            limit=0,
        )

        self.assertEqual(codes, ["000001.SZ", "000005.SZ", "600000.SH"])
        self.assertEqual(client.parameters["security_type"], "EXTRA_STOCK_A")

    def test_historical_universe_maps_legacy_bj_codes(self) -> None:
        class _HistoricalClient:
            def query_rows(self, sql, parameters=None):
                if "ad_bj_code_mapping" in sql:
                    return [("430017", "920017")]
                return [("430017.BJ",), ("920017.BJ",)]

        context = SimpleNamespace(
            base_data=SimpleNamespace(
                repository=SimpleNamespace(client=_HistoricalClient()),
                get_stock_universe=lambda security_type, force=False: ["920017.BJ"],
            ),
            sdk_config=SimpleNamespace(local_path="/tmp"),
        )

        codes = resolve_historical_code_list(
            context=context,
            task="income",
            begin_date=20100101,
            end_date=20260729,
            limit=0,
        )

        self.assertEqual(codes, ["920017.BJ"])

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

    def test_missing_historical_ignores_legacy_bj_code_when_new_code_exists(self) -> None:
        class _BjMappedClient:
            def query_rows(self, sql, parameters=None):
                normalized = " ".join(sql.split())
                if "system.columns" in normalized:
                    return [("market_code",)]
                if "LEFT ANTI JOIN existing" in normalized:
                    return [("430017.BJ",)]
                if "ad_bj_code_mapping" in normalized:
                    return [("430017", "920017")]
                if "SELECT DISTINCT market_code FROM starlight.ad_income" in normalized:
                    self.candidate_codes = list(parameters["candidate_codes"])
                    return [("920017.BJ",)]
                raise AssertionError(normalized)

        client = _BjMappedClient()
        context = SimpleNamespace(
            base_data=SimpleNamespace(
                repository=SimpleNamespace(client=client),
            )
        )

        codes = resolve_missing_historical_code_list(
            context=context,
            task="income",
            begin_date=20100101,
            end_date=20260729,
            limit=0,
        )

        self.assertEqual(codes, [])
        self.assertEqual(client.candidate_codes, ["920017.BJ"])


if __name__ == "__main__":
    unittest.main()
