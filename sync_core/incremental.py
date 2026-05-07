#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""增量同步公共工具."""

from __future__ import annotations

from datetime import datetime, timedelta


def normalize_request_value(value: str | int | None, granularity: str) -> str:
    text = "".join(ch for ch in str(value or "").strip() if ch.isdigit())
    if not text:
        return ""
    if granularity == "day":
        if len(text) < 8:
            raise ValueError(f"日期必须是 YYYYMMDD / YYYY-MM-DD，当前值: {value!r}")
        return text[:8]
    if granularity == "month":
        if len(text) < 6:
            raise ValueError(f"月份必须是 YYYYMM / YYYY-MM，当前值: {value!r}")
        return text[:6]
    if granularity == "year":
        if len(text) < 4:
            raise ValueError(f"年份必须是 YYYY，当前值: {value!r}")
        return text[:4]
    return text


def default_request_end(granularity: str) -> str:
    now = datetime.now()
    if granularity == "day":
        return now.strftime("%Y%m%d")
    if granularity == "month":
        return now.strftime("%Y%m")
    if granularity == "year":
        return now.strftime("%Y")
    return ""


def advance_cursor_value(value: str, granularity: str) -> str:
    normalized = normalize_request_value(value, granularity)
    if granularity == "day":
        dt = datetime.strptime(normalized, "%Y%m%d").date() + timedelta(days=1)
        return dt.strftime("%Y%m%d")
    if granularity == "month":
        year = int(normalized[0:4])
        month = int(normalized[4:6]) + 1
        if month > 12:
            year += 1
            month = 1
        return f"{year:04d}{month:02d}"
    if granularity == "year":
        return f"{int(normalized) + 1:04d}"
    return normalized


def compare_cursor_values(left: str, right: str) -> int:
    if left < right:
        return -1
    if left > right:
        return 1
    return 0


__all__ = [
    "advance_cursor_value",
    "compare_cursor_values",
    "default_request_end",
    "normalize_request_value",
]
