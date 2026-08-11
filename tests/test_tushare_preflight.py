#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from types import SimpleNamespace

from scripts.test_tushare_enabled_tasks import (
    FRESHNESS_DEFAULT_LOCKED_TASKS,
    LimitedTushareProvider,
    ReadOnlyPreflightRepository,
    check_table_layout,
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
    assert raw.calls[0][1]["params"] == {"trade_date": "20240329"}
    assert raw.calls[0][1]["page_size"] == 1
    assert raw.calls[0][1]["max_pages"] == 1


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
