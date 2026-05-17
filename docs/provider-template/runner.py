#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Demo provider runner skeleton."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class DemoExecutionContext:
    provider: Any
    repository: Any

    def close(self) -> None:
        close = getattr(self.provider, "close", None)
        if callable(close):
            close()


def run_config_file(path: str, *, log_level_override: str | None = None) -> int:
    del path, log_level_override
    raise NotImplementedError("请在 providers/<name>/runner.py 中实现 TOML 批量同步。")


def build_context(runtime_path: str | None = None, database: str = "demo") -> DemoExecutionContext:
    del runtime_path, database
    raise NotImplementedError("请创建 provider/repository 并返回执行上下文。")


def run_registered_task(probe: Any) -> int:
    del probe
    raise NotImplementedError("请实现 API 单任务执行。")
