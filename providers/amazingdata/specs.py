#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AmazingData task metadata."""

from __future__ import annotations

from sync_data_system.providers.amazingdata import runner


AMAZINGDATA_TASK_SPECS = {
    task: {
        "task": task,
        "table_name": runner.TASK_TARGET_TABLE_MAP[task],
        "supports_incremental": task in runner.TASK_INPUT_RESOLVER_MAP or runner.task_requires_code_list(task),
    }
    for task in runner.TASK_CHOICES
}

AMAZINGDATA_TASK_CHOICES = tuple(AMAZINGDATA_TASK_SPECS.keys())

__all__ = ["AMAZINGDATA_TASK_CHOICES", "AMAZINGDATA_TASK_SPECS"]
