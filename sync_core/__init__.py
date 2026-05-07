#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""多数据源同步公共能力."""

from .amazingdata import build_hashed_scope_key, resolve_incremental_start_date, safe_write_sync_log
from .clickhouse import ClickHouseConfig, ClickHouseConnection, create_clickhouse_client
from .incremental import advance_cursor_value, compare_cursor_values, default_request_end, normalize_request_value
from .scope import build_scope_key
from .sync_models import SyncCheckpointRow, SyncTaskLogRow, to_ch_date
from .task_logging import to_sync_date, write_sync_result

__all__ = [
    "ClickHouseConfig",
    "ClickHouseConnection",
    "SyncCheckpointRow",
    "SyncTaskLogRow",
    "advance_cursor_value",
    "build_hashed_scope_key",
    "build_scope_key",
    "compare_cursor_values",
    "create_clickhouse_client",
    "default_request_end",
    "normalize_request_value",
    "resolve_incremental_start_date",
    "safe_write_sync_log",
    "to_sync_date",
    "to_ch_date",
    "write_sync_result",
]
