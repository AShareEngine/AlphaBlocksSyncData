#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Core provider execution helpers."""

from __future__ import annotations

import logging
from pathlib import Path

from sync_data_system.core.providers import load_provider_registry


logger = logging.getLogger(__name__)


def run_provider_config(
    *,
    source: str,
    config_path: str,
    project_root: str | Path | None = None,
    log_level_override: str | None = None,
    resume: bool = False,
) -> int:
    registry = load_provider_registry(project_root)
    if not registry.has(source):
        raise ValueError(f"未知 provider source: {source!r}，已加载: {registry.names()}")
    if resume:
        logger.warning("%s 配置模式当前由 provider runner 处理 resume；如果 runner 未实现，该参数会被忽略。", source)
    manifest = registry.get(source)
    runner = manifest.load_config_runner()
    return runner(config_path, log_level_override=log_level_override)


__all__ = ["run_provider_config"]
