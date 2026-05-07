#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""同步日志公共工具."""

from __future__ import annotations

from datetime import date, datetime, timezone

from sync_data_system.sync_core.sync_models import SyncTaskLogRow, to_ch_date


def to_sync_date(value) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return to_ch_date(text)
    except Exception:
        return None


def write_sync_result(
    *,
    repository,
    task: str,
    scope_key: str,
    target_table: str,
    request_meta: dict[str, str | int | None],
    row_count: int,
    message: str | None,
    started_at: datetime,
    status: str,
) -> None:
    repository.insert_sync_log(
        SyncTaskLogRow(
            task_name=task,
            scope_key=scope_key,
            run_date=date.today(),
            status=status,
            target_table=target_table,
            start_date=to_sync_date(request_meta.get("start_date") or request_meta.get("day")),
            end_date=to_sync_date(request_meta.get("end_date") or request_meta.get("day")),
            row_count=row_count,
            message=message,
            started_at=started_at,
            finished_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )
    )


__all__ = [
    "to_sync_date",
    "write_sync_result",
]
