#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Execute a registered sync task in a standalone process."""

from __future__ import annotations

import argparse
from pathlib import Path

from sync_data_system.service.task_registry import TASK_REGISTRY, build_amazingdata_context, build_baostock_context, create_probe


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run registered sync task")
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--task", required=True)
    parser.add_argument("--log-path", required=True)
    parser.add_argument("--runtime-path")
    parser.add_argument("--codes", default="")
    parser.add_argument("--day", type=int)
    parser.add_argument("--begin-date", type=int)
    parser.add_argument("--end-date", type=int)
    parser.add_argument("--year", type=int)
    parser.add_argument("--quarter", type=int)
    parser.add_argument("--year-type")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--adjustflag", default="3")
    parser.add_argument("--frequency", default="d")
    parser.add_argument("--log-level")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    probe = create_probe(
        task_name=args.task,
        job_id=args.job_id,
        project_root=PROJECT_ROOT,
        log_path=Path(args.log_path),
        runtime_path=args.runtime_path,
        codes=[item.strip() for item in args.codes.split(",") if item.strip()],
        day=args.day,
        begin_date=args.begin_date,
        end_date=args.end_date,
        year=args.year,
        quarter=args.quarter,
        year_type=args.year_type,
        limit=args.limit,
        force=args.force,
        resume=args.resume,
        adjustflag=args.adjustflag,
        frequency=args.frequency,
        log_level=args.log_level,
    )
    definition = TASK_REGISTRY.get_task(probe.name)
    probe.log(f"task={probe.name} source={definition.source} target={definition.target} status=preparing")
    if definition.source == "baostock":
        context = build_baostock_context(runtime_path=probe.runtime_path, database=definition.database or "baostock")
    else:
        context = build_amazingdata_context(runtime_path=probe.runtime_path)
    probe.context = context
    try:
        TASK_REGISTRY.resolve_inputs(probe)
        probe.log(
            f"task={probe.name} status=resolved code_count={len(probe.codes)} "
            f"begin_date={probe.begin_date} end_date={probe.end_date}"
        )
        result = definition.handler(probe)
        row_count = probe.row_count or int(result or 0)
        probe.set_row_count(row_count)
        probe.log(f"task={probe.name} status=success row_count={probe.row_count}")
        return 0
    finally:
        context.close()


if __name__ == "__main__":
    raise SystemExit(main())
