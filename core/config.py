#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Core sync config helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from sync_data_system.config_paths import resolve_config_candidate
from sync_data_system.toml_compat import tomllib


def load_toml_config(path: str | Path, *, project_root: str | Path | None = None) -> tuple[Path, dict[str, Any]]:
    config_path = resolve_config_candidate(path, project_root=project_root)
    if not config_path.is_file():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    with config_path.open("rb") as fh:
        data = tomllib.load(fh)
    if not isinstance(data, dict):
        raise ValueError("配置文件格式错误：顶层必须是 TOML table。")
    return config_path, data


def detect_plan_source(
    path: str | Path,
    *,
    project_root: str | Path | None = None,
    default_source: str = "amazingdata",
) -> str:
    _, data = load_toml_config(path, project_root=project_root)
    return str(data.get("source") or default_source).strip() or default_source


__all__ = ["detect_plan_source", "load_toml_config"]
