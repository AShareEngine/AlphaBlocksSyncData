#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Human-readable timestamps for execution logs."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DEFAULT_LOG_TIMEZONE = "Asia/Shanghai"


def log_timestamp(now: datetime | None = None) -> str:
    """Return a local timestamp without changing UTC persistence semantics."""

    zone_name = str(os.environ.get("SYNC_LOG_TIMEZONE") or DEFAULT_LOG_TIMEZONE).strip()
    try:
        zone = ZoneInfo(zone_name)
    except (ZoneInfoNotFoundError, ValueError):
        zone = ZoneInfo(DEFAULT_LOG_TIMEZONE)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(zone).replace(microsecond=0).isoformat(sep=" ")


__all__ = ["DEFAULT_LOG_TIMEZONE", "log_timestamp"]
