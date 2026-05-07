#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AmazingData 同步公共工具."""

from __future__ import annotations

import hashlib
import logging
from datetime import date, datetime, timedelta
from typing import Sequence

from sync_data_system.sync_core.sync_models import SyncTaskLogRow


logger = logging.getLogger(__name__)


def build_hashed_scope_key(
    task_name: str,
    code_list: Sequence[str],
    *,
    prefix: str = "task",
    begin_date: date | None = None,
    end_date: date | None = None,
) -> str:
    digest = hashlib.sha1(",".join(sorted(code_list)).encode("utf-8")).hexdigest()[:12]
    parts = [f"{prefix}={task_name}", f"code_count={len(code_list)}", f"codes_sha1={digest}"]
    if begin_date is not None or end_date is not None:
        parts.append(f"begin_date={begin_date.isoformat() if begin_date is not None else ''}")
        parts.append(f"end_date={end_date.isoformat() if end_date is not None else ''}")
    return "|".join(parts)


def resolve_incremental_start_date(
    latest_date: date | None,
    requested_begin_date: date | None,
) -> date | None:
    if latest_date is None:
        return requested_begin_date
    next_date = latest_date + timedelta(days=1)
    if requested_begin_date is None:
        return next_date
    return max(next_date, requested_begin_date)


def safe_write_sync_log(
    *,
    repository,
    task_name: str,
    scope_key: str,
    run_date: date,
    status: str,
    target_table: str,
    start_date: date | None,
    end_date: date | None,
    row_count: int,
    message: str,
    started_at: datetime,
    finished_at: datetime,
) -> None:
    try:
        repository.insert_sync_log(
            SyncTaskLogRow(
                task_name=task_name,
                scope_key=scope_key,
                run_date=run_date,
                status=status,
                target_table=target_table,
                start_date=start_date,
                end_date=end_date,
                row_count=max(0, int(row_count)),
                message=message,
                started_at=started_at,
                finished_at=finished_at,
            )
        )
    except Exception:
        logger.exception("写入同步日志失败 task=%s scope=%s status=%s", task_name, scope_key, status)


__all__ = [
    "build_hashed_scope_key",
    "resolve_incremental_start_date",
    "safe_write_sync_log",
]
