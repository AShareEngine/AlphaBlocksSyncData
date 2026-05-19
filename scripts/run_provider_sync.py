#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run provider sync configs and registered provider tasks."""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import replace
from pathlib import Path
from typing import Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from program_bootstrap import install_sync_data_system_alias

install_sync_data_system_alias(PROJECT_ROOT)

from sync_data_system.core.config import detect_plan_source
from sync_data_system.core.engine import run_provider_config
from sync_data_system.providers.amazingdata import runner as amazingdata_runner
from sync_data_system.service.task_registry import TASK_REGISTRY, build_provider_context, create_probe

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run sync provider tasks")
    task_choices = tuple(task.name for task in TASK_REGISTRY.list_tasks())
    parser.add_argument(
        "task_name",
        nargs="?",
        choices=task_choices,
        help="Registered provider task name, for example amazingdata.daily_kline.",
    )
    parser.add_argument("--config", action="append", default=[], help="TOML sync plan path. Repeat to run multiple configs in one job.")
    parser.add_argument("--task", dest="task_option", choices=task_choices, help=argparse.SUPPRESS)
    parser.add_argument("--job-id", default="cli", help=argparse.SUPPRESS)
    parser.add_argument("--log-path", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--runtime-path", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--codes", default="", help="Comma-separated code list")
    parser.add_argument("--day", type=int)
    parser.add_argument("--begin-date", type=int, help="Begin date YYYYMMDD")
    parser.add_argument("--end-date", type=int, help="End date YYYYMMDD")
    parser.add_argument("--year", type=int)
    parser.add_argument("--quarter", type=int)
    parser.add_argument("--year-type")
    parser.add_argument("--market")
    parser.add_argument("--index-code")
    parser.add_argument("--table-names")
    parser.add_argument("--sector-name")
    parser.add_argument("--code-market")
    parser.add_argument("--period")
    parser.add_argument("--fields")
    parser.add_argument("--adjust-type")
    parser.add_argument("--qmt-adjust-type")
    parser.add_argument("--fill-data", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--count", type=int, default=-1)
    parser.add_argument("--incrementally", action="store_true")
    parser.add_argument("--complete", action="store_true")
    parser.add_argument("--limit", type=int, default=0, help="Debug item limit")
    parser.add_argument("--force", action="store_true", help="Force rerun")
    parser.add_argument("--resume", action="store_true", help="Resume from successful checkpoints")
    parser.add_argument("--adjustflag", default="3")
    parser.add_argument("--frequency", default="d")
    parser.add_argument("--log-level", default=None)
    args = parser.parse_args()
    if args.task_name and args.task_option:
        parser.error("use either positional task or --task, not both.")
    args.task = args.task_option or args.task_name
    if args.task and args.config:
        parser.error("task and --config cannot be used together.")
    if args.config and (
        args.codes.strip()
        or args.day is not None
        or args.begin_date is not None
        or args.end_date is not None
        or args.year is not None
        or args.quarter is not None
        or args.year_type
        or args.market
        or args.index_code
        or args.table_names
        or args.sector_name
        or args.code_market
        or args.period
        or args.fields
        or args.adjust_type
        or args.qmt_adjust_type
        or args.fill_data is not True
        or args.count != -1
        or args.incrementally
        or args.complete
        or args.limit != 0
        or args.force
        or args.adjustflag != "3"
        or args.frequency != "d"
    ):
        parser.error("--config mode cannot be mixed with task request options.")
    return args


def main() -> int:
    args = parse_args()
    if args.config:
        return run_config_sequence(args.config, args)

    if args.task is None:
        default_config = str(PROJECT_ROOT / "config" / "sync" / "plans" / amazingdata_runner.DEFAULT_PLAN_CONFIG)
        return run_config_sequence([default_config], args)

    return run_registered_task(args)


def run_config_sequence(config_paths: Sequence[str], args: argparse.Namespace) -> int:
    if len(config_paths) == 1:
        return run_single_config(config_paths[0], args)

    final_code = 0
    for index, config_path in enumerate(config_paths, start=1):
        logger.info("config batch start progress=%s/%s config=%s", index, len(config_paths), config_path)
        try:
            return_code = run_single_config(config_path, args)
        except Exception:
            logger.exception("config batch failed progress=%s/%s config=%s", index, len(config_paths), config_path)
            final_code = 1
            continue
        if return_code:
            logger.error(
                "config batch returned non-zero progress=%s/%s config=%s return_code=%s",
                index,
                len(config_paths),
                config_path,
                return_code,
            )
            final_code = return_code
        else:
            logger.info("config batch finished progress=%s/%s config=%s", index, len(config_paths), config_path)
    return final_code


def run_single_config(config_path: str, args: argparse.Namespace) -> int:
    source = detect_plan_source(config_path, project_root=PROJECT_ROOT, default_source="amazingdata")
    if source == "amazingdata" and args.resume:
        plan = amazingdata_runner.load_execution_plan_from_toml(config_path)
        plan = _apply_amazingdata_overrides(plan, runtime_path=args.runtime_path, log_level=args.log_level, resume=True)
        return amazingdata_runner.execute_execution_plan(plan)
    return run_provider_config(
        source=source,
        config_path=config_path,
        project_root=PROJECT_ROOT,
        log_level_override=args.log_level,
        resume=args.resume,
    )


def run_registered_task(args: argparse.Namespace) -> int:
    log_path = Path(args.log_path) if args.log_path else PROJECT_ROOT / ".service_state" / "logs" / "cli.log"
    probe = create_probe(
        task_name=args.task,
        job_id=args.job_id,
        project_root=PROJECT_ROOT,
        log_path=log_path,
        runtime_path=args.runtime_path,
        codes=[item.strip() for item in args.codes.split(",") if item.strip()],
        day=args.day,
        begin_date=args.begin_date,
        end_date=args.end_date,
        year=args.year,
        quarter=args.quarter,
        year_type=args.year_type,
        market=args.market,
        index_code=args.index_code,
        table_names=args.table_names,
        sector_name=args.sector_name,
        code_market=args.code_market,
        period=args.period,
        fields=args.fields,
        qmt_adjust_type=args.qmt_adjust_type or args.adjust_type,
        fill_data=args.fill_data,
        count=args.count,
        incrementally=args.incrementally,
        complete=args.complete,
        limit=args.limit,
        force=args.force,
        resume=args.resume,
        adjustflag=args.adjustflag,
        frequency=args.frequency,
        log_level=args.log_level,
    )
    definition = TASK_REGISTRY.get_task(probe.name)
    probe.log(f"task={probe.name} source={definition.source} target={definition.target} status=preparing")
    context = build_provider_context(
        definition.source,
        runtime_path=probe.runtime_path,
        database=definition.database,
    )
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


def _apply_amazingdata_overrides(
    plan: amazingdata_runner.ExecutionPlan,
    *,
    runtime_path: str | None,
    log_level: str | None,
    resume: bool,
) -> amazingdata_runner.ExecutionPlan:
    tasks = plan.tasks
    if resume:
        tasks = tuple(replace(task, resume=True) for task in tasks)
    return amazingdata_runner.ExecutionPlan(
        runtime_path=runtime_path or plan.runtime_path,
        log_level=log_level or plan.log_level,
        continue_on_error=plan.continue_on_error,
        tasks=tasks,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())
