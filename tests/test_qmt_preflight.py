#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from scripts.test_qmt_enabled_tasks import (
    FRESHNESS_DEFAULT_LOCKED_TASKS,
    build_sample_args,
    check_table_layout,
    classify_error,
    parse_args,
    response_is_empty,
    selected_task_names,
)
from sync_data_system.providers.qmt.repository import QmtRepository
from sync_data_system.providers.qmt.runner import (
    build_request_meta,
    validate_required_request,
)
from sync_data_system.providers.qmt.specs import (
    QMT_TASK_SPECS,
    order_by_columns_for_spec,
)


class FakeClient:
    def __init__(self, *, table_rows=None, legacy_rows=None, columns=None):
        self.table_rows = table_rows or []
        self.legacy_rows = legacy_rows or []
        self.columns = columns or []

    def query_rows(self, sql, parameters=None):
        del parameters
        if "FROM system.tables" in sql:
            return list(self.table_rows)
        if "name IN" in sql:
            return list(self.legacy_rows)
        if "FROM system.columns" in sql:
            return [(column,) for column in self.columns]
        return []


def test_default_qmt_preflight_selection_covers_freshness_tasks():
    names = selected_task_names([])

    assert names
    assert not (set(names) & FRESHNESS_DEFAULT_LOCKED_TASKS)
    assert "kline_history" in names
    assert "trading_calendar" not in names
    assert "download_holiday" not in names


def test_qmt_preflight_can_include_or_explicitly_probe_locked_tasks():
    assert selected_task_names([], include_locked=True) == list(QMT_TASK_SPECS)
    assert selected_task_names(["qmt.trading_calendar"]) == ["trading_calendar"]
    assert selected_task_names(["qmt.download_holiday"]) == ["download_holiday"]


def test_explicit_qmt_preflight_task_normalizes_provider_prefix():
    assert selected_task_names(["qmt.kline_history", "kline_history"]) == [
        "kline_history"
    ]


def test_every_qmt_preflight_sample_satisfies_request_validation():
    cli_args = parse_args([])

    for task in QMT_TASK_SPECS:
        sample = build_sample_args(task, cli_args, "20260810")
        request_meta = build_request_meta(sample)
        validate_required_request(sample, request_meta)


def test_qmt_table_layout_accepts_current_business_key_and_columns():
    spec = QMT_TASK_SPECS["kline_history"]
    key = ", ".join(order_by_columns_for_spec(spec))
    client = FakeClient(
        table_rows=[("ReplacingMergeTree", key, key, "")],
        columns=QmtRepository.table_columns_for_spec(spec),
    )

    assert check_table_layout(client, database="qmt", spec=spec) == "ok"


def test_qmt_table_layout_rejects_ingestion_metadata():
    spec = QMT_TASK_SPECS["kline_history"]
    key = ", ".join(order_by_columns_for_spec(spec))
    client = FakeClient(
        table_rows=[("ReplacingMergeTree", key, key, "")],
        legacy_rows=[("ingested_at",)],
        columns=(*QmtRepository.table_columns_for_spec(spec), "ingested_at"),
    )

    status = check_table_layout(client, database="qmt", spec=spec)

    assert status == "outdated:legacy_columns=ingested_at"


def test_qmt_table_layout_rejects_payload_json_schema():
    spec = QMT_TASK_SPECS["kline_history"]
    key = ", ".join(order_by_columns_for_spec(spec))
    client = FakeClient(
        table_rows=[("ReplacingMergeTree", key, key, "")],
        legacy_rows=[("payload_json",)],
        columns=("task", "payload_json"),
    )

    status = check_table_layout(client, database="qmt", spec=spec)

    assert status == "outdated:legacy_columns=payload_json"


def test_qmt_empty_response_detection_uses_nested_collections():
    assert response_is_empty({"data": {"items": []}})
    assert not response_is_empty({"data": {"items": [{"symbol": "600000.SH"}]}})
    assert not response_is_empty({"data": {"path": "/qmt/data"}})


def test_qmt_error_classification_prefers_service_status_over_generic_failure():
    assert classify_error(
        RuntimeError("QMT 请求失败 code=503 message=xtdata 未登录")
    ) == "QMT_UNAVAILABLE"
    assert classify_error(
        RuntimeError("QMT 请求失败 code=422 message=参数非法")
    ) == "PARAMETER"
