#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from types import SimpleNamespace

from scripts.test_tushare_enabled_tasks import (
    FRESHNESS_DEFAULT_LOCKED_TASKS,
    LimitedTushareProvider,
    PREFLIGHT_TASK_SAMPLES,
    PreflightResult,
    ReadOnlyPreflightRepository,
    check_table_layout,
    build_summary,
    selected_task_names,
)
from sync_data_system.providers.tushare.specs import TUSHARE_TASK_SPECS


class FakeClient:
    def __init__(self, table_rows=None, column_rows=None):
        self.table_rows = table_rows or []
        self.column_rows = column_rows or []

    def query_rows(self, sql, parameters=None):
        del parameters
        if "FROM system.tables" in sql:
            return list(self.table_rows)
        if "FROM system.columns" in sql:
            return list(self.column_rows)
        return []


def test_default_preflight_selection_excludes_freshness_locked_tasks():
    names = selected_task_names([], include_locked=False)

    assert names
    assert not (set(names) & FRESHNESS_DEFAULT_LOCKED_TASKS)
    assert "daily" in names
    assert "news" not in names
    assert "p_get" not in names


def test_explicit_preflight_task_can_probe_a_locked_task():
    assert selected_task_names(
        ["tushare.news", "news"],
        include_locked=False,
    ) == ["news"]


def test_limited_provider_preserves_params_and_caps_real_pagination():
    class Provider:
        def __init__(self):
            self.config = SimpleNamespace()
            self.request_count = 0
            self.calls = []

        def query_all(self, api_name, **kwargs):
            self.request_count += 1
            self.calls.append((api_name, kwargs))
            return [
                {"trade_date": "20240329"},
                {"trade_date": "20240328"},
            ]

    raw = Provider()
    provider = LimitedTushareProvider(raw)

    rows = provider.query_all("daily", params={"trade_date": "20240329"})

    assert rows == [{"trade_date": "20240329"}]
    assert provider.raw_row_count("daily") == 2
    assert "missing=ts_code" in provider.contract_error("daily")
    assert "row=1" in provider.contract_error("daily")
    assert raw.calls[0][1]["params"] == {"trade_date": "20240329"}
    assert raw.calls[0][1]["page_size"] == 1
    assert raw.calls[0][1]["max_pages"] == 1


def test_limited_provider_scans_more_rows_for_paginated_contracts():
    class Provider:
        def __init__(self):
            self.config = SimpleNamespace()
            self.request_count = 0
            self.calls = []

        def query_all(self, api_name, **kwargs):
            self.request_count += 1
            self.calls.append((api_name, kwargs))
            return [
                {"ts_code": f"00000{index}.SZ", "trade_date": "20240329"}
                for index in range(5)
            ]

    raw = Provider()
    provider = LimitedTushareProvider(raw, contract_row_limit=20)

    rows = provider.query_all(
        "daily",
        params={"trade_date": "20240329"},
        supports_pagination=True,
        page_size=1,
    )

    assert len(rows) == 1
    assert provider.raw_row_count("daily") == 5
    assert raw.calls[0][1]["page_size"] == 20


def test_contract_scan_accepts_documented_optional_member_dates():
    class Provider:
        def __init__(self):
            self.config = SimpleNamespace()
            self.request_count = 0

        def query_all(self, api_name, **kwargs):
            self.request_count += 1
            if api_name == "fund_manager":
                return [
                    {
                        "ts_code": "000001.OF",
                        "ann_date": "20260810",
                        "name": "测试经理",
                        "begin_date": "",
                    }
                ]
            if api_name == "ths_member":
                return [
                    {
                        "ts_code": "885001.TI",
                        "con_code": "000001.SZ",
                        "in_date": "",
                    }
                ]
            raise AssertionError(api_name)

    provider = LimitedTushareProvider(Provider(), contract_row_limit=100)

    provider.query_all("fund_manager", params={"ann_date": "20260810"})
    provider.query_all("ths_member", params={"ts_code": "885001.TI"})

    assert provider.contract_error("fund_manager") == ""
    assert provider.contract_error("ths_member") == ""


def test_fragile_tushare_tasks_use_known_documented_preflight_samples():
    assert PREFLIGHT_TASK_SAMPLES["bc_bestotcqt"].params is None
    assert PREFLIGHT_TASK_SAMPLES["bc_bestotcqt"].date == ""
    assert PREFLIGHT_TASK_SAMPLES["bc_bestotcqt"].expect_rows is True
    assert PREFLIGHT_TASK_SAMPLES["cb_rate"].codes == ("123046.SZ",)
    assert PREFLIGHT_TASK_SAMPLES["cb_rate"].expect_rows is True


def test_preflight_summary_separates_contract_permission_and_other_failures():
    results = [
        PreflightResult(
            task="bc_bestotcqt",
            table="ts_bc_bestotcqt",
            status="FAIL",
            rows=0,
            raw_rows=1,
            requests=1,
            elapsed_ms=1,
            table_status="ok",
            error_type="BUSINESS_KEY_CONTRACT",
        ),
        PreflightResult(
            task="news",
            table="ts_news",
            status="FAIL",
            rows=0,
            raw_rows=-1,
            requests=1,
            elapsed_ms=1,
            table_status="ok",
            error_type="NO_PERMISSION",
        ),
        PreflightResult(
            task="daily",
            table="ts_daily",
            status="FAIL",
            rows=0,
            raw_rows=0,
            requests=1,
            elapsed_ms=1,
            table_status="outdated:key",
            error_type="TABLE_LAYOUT",
        ),
    ]

    summary = build_summary(
        results,
        total_tasks=3,
        total_requests=3,
        elapsed_ms=3,
        probe_date="20260810",
    )

    assert summary["contract_failed"] == 1
    assert summary["no_permission"] == 1
    assert summary["other_failed"] == 1


def test_read_only_repository_keeps_rows_in_memory():
    repository = ReadOnlyPreflightRepository(FakeClient(), database="tushare")
    spec = TUSHARE_TASK_SPECS["stock_basic"]

    saved = repository.save_rows(
        spec,
        [{"ts_code": "000001.SZ"}],
        scope_key="ignored",
    )

    assert saved == 1
    assert repository.load_universe_codes(spec) == ["000001.SZ"]


def test_table_layout_check_detects_old_bc_otcqt_key():
    client = FakeClient(
        table_rows=[
            (
                "ReplacingMergeTree",
                "trade_date, qt_time, bank, ts_code",
                "trade_date, qt_time, bank, ts_code",
                "",
            )
        ]
    )

    status = check_table_layout(
        client,
        database="tushare",
        spec=TUSHARE_TASK_SPECS["bc_otcqt"],
    )

    assert status.startswith("outdated:sorting_key=")
