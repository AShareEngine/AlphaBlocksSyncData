#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""同步 scope_key 公共工具."""

from __future__ import annotations


def build_scope_key(task: str, request_meta: dict[str, str | int | None]) -> str:
    parts = [f"task={task}"]
    code = str(request_meta.get("code") or "").strip()
    if code:
        parts.append(f"code={code}")
    day = str(request_meta.get("day") or "").strip()
    start_date = str(request_meta.get("start_date") or "").strip()
    end_date = str(request_meta.get("end_date") or "").strip()
    year = str(request_meta.get("year") or "").strip()
    quarter = str(request_meta.get("quarter") or "").strip()
    year_type = str(request_meta.get("year_type") or "").strip()
    if day:
        parts.append(f"day={day}")
    if start_date:
        parts.append(f"begin={start_date}")
    if end_date:
        parts.append(f"end={end_date}")
    if year:
        parts.append(f"year={year}")
    if quarter:
        parts.append(f"quarter={quarter}")
    if year_type:
        parts.append(f"year_type={year_type}")
    return "|".join(parts)


__all__ = ["build_scope_key"]
