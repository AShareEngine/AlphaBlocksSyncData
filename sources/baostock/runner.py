#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""BaoStock -> ClickHouse 同步入口."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from sync_data_system.config_paths import resolve_config_candidate
from sync_data_system.sources.baostock.provider import BaoStockConfig, BaoStockProvider, normalize_baostock_code_list
from sync_data_system.sources.baostock.repository import BaoStockRepository
from sync_data_system.sources.baostock.specs import BAOSTOCK_TASK_CHOICES, BAOSTOCK_TASK_SPECS
from sync_data_system.sync_core.clickhouse import ClickHouseConfig, create_clickhouse_client
from sync_data_system.sync_core.incremental import (
    advance_cursor_value,
    compare_cursor_values,
    default_request_end,
    normalize_request_value,
)
from sync_data_system.sync_core.scope import build_scope_key
from sync_data_system.sync_core.task_logging import write_sync_result
from sync_data_system.toml_compat import tomllib


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SyncArgs:
    task: str
    codes_raw: str
    begin_date: str
    end_date: str
    day: str
    year: int | None
    quarter: int | None
    year_type: str
    adjustflag: str
    frequency: str
    limit: int
    force: bool
    continue_on_error: bool
    runtime_path: str | None
    database: str
    log_level: str


@dataclass(frozen=True)
class BaoStockExecutionPlan:
    runtime_path: str | None
    log_level: str
    continue_on_error: bool
    database: str
    tasks: tuple[SyncArgs, ...]


def parse_args() -> SyncArgs:
    parser = argparse.ArgumentParser(description="BaoStock 正式同步入口")
    parser.add_argument("task", choices=BAOSTOCK_TASK_CHOICES)
    parser.add_argument("--codes", default="", help="逗号分隔代码，支持 `600000.SH` 或 `sh.600000`")
    parser.add_argument("--begin-date", default="", help="开始日期，支持 YYYYMMDD / YYYY-MM-DD / YYYYMM / YYYY / YYYY-MM")
    parser.add_argument("--end-date", default="", help="结束日期，支持 YYYYMMDD / YYYY-MM-DD / YYYYMM / YYYY / YYYY-MM")
    parser.add_argument("--day", default="", help="按日接口使用，支持 YYYYMMDD / YYYY-MM-DD")
    parser.add_argument("--year", type=int, help="年频/季频接口使用")
    parser.add_argument("--quarter", type=int, choices=(1, 2, 3, 4), help="季频接口使用")
    parser.add_argument("--year-type", default="", help="分红/准备金等接口附加参数")
    parser.add_argument("--adjustflag", default="3", help="K 线复权参数，默认 3=不复权")
    parser.add_argument("--frequency", default="d", help="K 线周期，当前默认 d")
    parser.add_argument("--limit", type=int, default=0, help="仅同步前 N 个 code，0 表示不限制")
    parser.add_argument("--force", action="store_true", help="忽略当天成功日志，强制重跑")
    parser.add_argument("--continue-on-error", action="store_true", help="单 code 失败后继续后续 code")
    parser.add_argument("--runtime-path", default=None, help="可选 runtime.local.yaml 路径")
    parser.add_argument("--database", default="baostock", help="ClickHouse 目标 database，默认 baostock")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    return SyncArgs(
        task=args.task,
        codes_raw=args.codes,
        begin_date=str(args.begin_date or "").strip(),
        end_date=str(args.end_date or "").strip(),
        day=str(args.day or "").strip(),
        year=args.year,
        quarter=args.quarter,
        year_type=str(args.year_type or "").strip(),
        adjustflag=str(args.adjustflag or "3").strip() or "3",
        frequency=str(args.frequency or "d").strip() or "d",
        limit=max(0, int(args.limit or 0)),
        force=bool(args.force),
        continue_on_error=bool(args.continue_on_error),
        runtime_path=args.runtime_path,
        database=str(args.database or "baostock").strip() or "baostock",
        log_level=str(args.log_level or "INFO").strip() or "INFO",
    )


def run_config_file(path: str, *, log_level_override: str | None = None) -> int:
    plan = load_execution_plan_from_toml(path, log_level_override=log_level_override)
    logging.basicConfig(
        level=getattr(logging, plan.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    clickhouse_config = ClickHouseConfig.from_env(runtime_path=plan.runtime_path)
    provider = BaoStockProvider(BaoStockConfig.from_env(runtime_path=plan.runtime_path))
    connection = create_clickhouse_client(clickhouse_config)
    repository = BaoStockRepository(connection, database=plan.database)

    try:
        repository.ensure_tables()
        failed_tasks: list[str] = []
        for index, task_args in enumerate(plan.tasks, start=1):
            logger.info("batch task start progress=%s/%s task=%s", index, len(plan.tasks), task_args.task)
            try:
                run_sync_args(task_args, provider, repository)
            except Exception:
                failed_tasks.append(task_args.task)
                logger.exception("batch task failed progress=%s/%s task=%s", index, len(plan.tasks), task_args.task)
                if not plan.continue_on_error:
                    raise
            else:
                logger.info("batch task finished progress=%s/%s task=%s", index, len(plan.tasks), task_args.task)
        return 1 if failed_tasks else 0
    finally:
        try:
            connection.close()
        except Exception:
            pass
        provider.close()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    clickhouse_config = ClickHouseConfig.from_env(runtime_path=args.runtime_path)
    provider = BaoStockProvider(BaoStockConfig.from_env(runtime_path=args.runtime_path))
    connection = create_clickhouse_client(clickhouse_config)
    repository = BaoStockRepository(connection, database=args.database)

    try:
        repository.ensure_tables()
        return run_sync_args(args, provider, repository)
    finally:
        try:
            connection.close()
        except Exception:
            pass
        provider.close()


def run_sync_args(args: SyncArgs, provider: BaoStockProvider, repository: BaoStockRepository) -> int:
    spec = BAOSTOCK_TASK_SPECS[args.task]
    codes = resolve_code_list(provider, args)
    if codes:
        return run_code_task(args, provider, repository, codes)
    if spec.uses_code:
        if args.codes_raw.strip():
            raise ValueError(f"BaoStock 任务 {args.task} 未解析出有效股票代码，请检查 codes 参数。")
        raise ValueError(
            f"BaoStock 任务 {args.task} 未获取到可用股票代码。"
            "如果今天是非交易日，请改在交易日执行，或显式传 --codes。"
        )
    return run_single_task(args, provider, repository)


def resolve_code_list(provider: BaoStockProvider, args: SyncArgs) -> list[str]:
    spec = BAOSTOCK_TASK_SPECS[args.task]
    if not spec.uses_code:
        return []

    if args.codes_raw.strip():
        codes = normalize_baostock_code_list([part.strip() for part in args.codes_raw.split(",") if part.strip()])
    elif spec.auto_code_universe:
        snapshot_day = args.day or args.end_date or datetime.now().strftime("%Y%m%d")
        resolved_day, codes = provider.fetch_latest_all_stock_codes(snapshot_day)
        if resolved_day and resolved_day != snapshot_day:
            logger.warning(
                "BaoStock code universe empty task=%s snapshot_day=%s; fallback to latest all_stock day=%s",
                args.task,
                snapshot_day,
                resolved_day,
            )
    else:
        codes = []

    if args.limit > 0:
        codes = codes[: args.limit]
    return codes


def run_single_task(args: SyncArgs, provider: BaoStockProvider, repository: BaoStockRepository) -> int:
    request_meta = build_request_meta(args)
    request_meta = resolve_effective_request_meta(args, repository, request_meta)
    if request_meta is None:
        logger.info("skip task=%s reason=no_incremental_window", args.task)
        return 0
    request_meta = resolve_all_stock_request_meta(args, provider, request_meta)
    if args.task == "all_stock" and not args.force and request_identity_is_complete(BAOSTOCK_TASK_SPECS[args.task], request_meta):
        if repository.has_task_data_for_request(args.task, request_meta):
            logger.info("skip task=%s day=%s reason=request_already_exists", args.task, request_meta.get("day") or "")
            return 0
    scope_key = build_scope_key(args.task, request_meta)
    run_date = date.today()
    target_table = f"{args.database}.{BAOSTOCK_TASK_SPECS[args.task].table_name}"
    if not args.force and repository.has_successful_sync_today(args.task, scope_key, run_date):
        logger.info("skip task=%s scope=%s reason=already_success_today", args.task, scope_key)
        return 0

    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    try:
        frame = provider.fetch_dataframe(args.task, **build_fetch_kwargs(args, request_meta))
        inserted = repository.save_task_frame(args.task, frame, request_meta=request_meta)
        write_sync_result(
            repository=repository,
            task=args.task,
            scope_key=scope_key,
            target_table=target_table,
            request_meta=request_meta,
            row_count=inserted,
            message=None,
            started_at=started_at,
            status="success",
        )
        logger.info("task=%s inserted_rows=%s", args.task, inserted)
        return inserted
    except Exception as exc:
        write_sync_result(
            repository=repository,
            task=args.task,
            scope_key=scope_key,
            target_table=target_table,
            request_meta=request_meta,
            row_count=0,
            message=str(exc),
            started_at=started_at,
            status="failed",
        )
        raise


def resolve_all_stock_request_meta(
    args: SyncArgs,
    provider: BaoStockProvider,
    request_meta: dict[str, str | int | None],
) -> dict[str, str | int | None]:
    if args.task != "all_stock" or args.day:
        return request_meta
    snapshot_day = str(request_meta.get("day") or "").strip()
    resolved_day, codes = provider.fetch_latest_all_stock_codes(snapshot_day)
    if resolved_day and codes and resolved_day != snapshot_day:
        logger.warning(
            "BaoStock all_stock empty snapshot_day=%s; fallback to latest all_stock day=%s",
            snapshot_day,
            resolved_day,
        )
        effective = dict(request_meta)
        effective["day"] = resolved_day
        return effective
    return request_meta


def run_code_task(args: SyncArgs, provider: BaoStockProvider, repository: BaoStockRepository, codes: list[str]) -> int:
    total = 0
    target_table = f"{args.database}.{BAOSTOCK_TASK_SPECS[args.task].table_name}"
    run_date = date.today()
    for index, code in enumerate(codes, start=1):
        request_meta = build_request_meta(args, code=code)
        request_meta = resolve_effective_request_meta(args, repository, request_meta)
        if request_meta is None:
            logger.info("skip task=%s progress=%s/%s code=%s reason=no_incremental_window", args.task, index, len(codes), code)
            continue
        scope_key = build_scope_key(args.task, request_meta)
        if not args.force and repository.has_successful_sync_today(args.task, scope_key, run_date):
            logger.info("skip task=%s progress=%s/%s code=%s reason=already_success_today", args.task, index, len(codes), code)
            continue

        started_at = datetime.now(timezone.utc).replace(tzinfo=None)
        try:
            frame = provider.fetch_dataframe(args.task, **build_fetch_kwargs(args, request_meta))
            inserted = repository.save_task_frame(args.task, frame, request_meta=request_meta)
            total += inserted
            write_sync_result(
                repository=repository,
                task=args.task,
                scope_key=scope_key,
                target_table=target_table,
                request_meta=request_meta,
                row_count=inserted,
                message=None,
                started_at=started_at,
                status="success",
            )
            logger.info("task=%s progress=%s/%s code=%s inserted_rows=%s", args.task, index, len(codes), code, inserted)
        except Exception as exc:
            write_sync_result(
                repository=repository,
                task=args.task,
                scope_key=scope_key,
                target_table=target_table,
                request_meta=request_meta,
                row_count=0,
                message=str(exc),
                started_at=started_at,
                status="failed",
            )
            logger.warning("task=%s progress=%s/%s code=%s failed: %s", args.task, index, len(codes), code, exc)
            if not args.continue_on_error:
                raise
    return total


def build_request_meta(args: SyncArgs, *, code: str | None = None) -> dict[str, str | int | None]:
    spec = BAOSTOCK_TASK_SPECS[args.task]
    return {
        "code": code,
        "start_date": normalize_request_value(args.begin_date, spec.cursor_granularity) if args.begin_date else "",
        "end_date": normalize_request_value(args.end_date, spec.cursor_granularity) if args.end_date else "",
        "day": normalize_request_value(args.day or args.end_date, "day") if (args.day or args.end_date) else "",
        "year": args.year,
        "quarter": args.quarter,
        "year_type": resolve_year_type(args),
    }


def build_fetch_kwargs(args: SyncArgs, request_meta: dict[str, str | int | None]) -> dict[str, Any]:
    return {
        "code": str(request_meta.get("code") or "").strip() or None,
        "start_date": str(request_meta.get("start_date") or "").strip() or None,
        "end_date": str(request_meta.get("end_date") or "").strip() or None,
        "day": str(request_meta.get("day") or "").strip() or None,
        "year": request_meta.get("year"),
        "quarter": request_meta.get("quarter"),
        "year_type": str(request_meta.get("year_type") or "").strip() or None,
        "adjustflag": args.adjustflag,
        "frequency": args.frequency,
    }


def resolve_effective_request_meta(
    args: SyncArgs,
    repository: BaoStockRepository,
    request_meta: dict[str, str | int | None],
) -> dict[str, str | int | None] | None:
    spec = BAOSTOCK_TASK_SPECS[args.task]

    if spec.uses_begin_end and spec.cursor_granularity:
        effective = dict(request_meta)
        end_value = normalize_request_value(
            effective.get("end_date") or default_request_end(spec.cursor_granularity),
            spec.cursor_granularity,
        )
        start_value = normalize_request_value(effective.get("start_date"), spec.cursor_granularity)

        latest_cursor = None
        if not args.force:
            latest_cursor = repository.load_latest_cursor(args.task, code=str(effective.get("code") or "") or None)
        if latest_cursor:
            next_value = advance_cursor_value(latest_cursor, spec.cursor_granularity)
            if not start_value or compare_cursor_values(next_value, start_value) > 0:
                start_value = next_value

        if start_value and end_value and compare_cursor_values(start_value, end_value) > 0:
            return None

        effective["start_date"] = start_value
        effective["end_date"] = end_value
        return effective

    if spec.uses_day:
        effective = dict(request_meta)
        day_value = normalize_request_value(effective.get("day") or default_request_end("day"), "day")
        effective["day"] = day_value
        if not args.force and request_identity_is_complete(spec, effective):
            if repository.has_task_data_for_request(args.task, effective):
                return None
        return effective

    if not args.force and request_identity_is_complete(spec, request_meta):
        if repository.has_task_data_for_request(args.task, request_meta):
            return None
    return request_meta


def resolve_year_type(args: SyncArgs) -> str:
    if args.year_type:
        return args.year_type
    if args.task == "dividend_data":
        return "report"
    if args.task == "required_reserve_ratio_data":
        return "0"
    return ""


def request_identity_is_complete(spec, request_meta: dict[str, str | int | None]) -> bool:
    if spec.uses_day and not request_meta.get("day"):
        return False
    if spec.uses_year and request_meta.get("year") in (None, ""):
        return False
    if spec.uses_quarter and request_meta.get("quarter") in (None, ""):
        return False
    if spec.uses_year_type and spec.task == "required_reserve_ratio_data" and not request_meta.get("year_type"):
        return False
    return True


CONFIG_TOP_LEVEL_KEYS = frozenset(
    {"source", "runtime_path", "log_level", "continue_on_error", "database", "defaults", "tasks"}
)
CONFIG_DEFAULT_KEYS = frozenset(
    {"codes", "begin_date", "end_date", "day", "year", "quarter", "year_type", "adjustflag", "frequency", "limit", "force", "continue_on_error"}
)
CONFIG_TASK_KEYS = frozenset({"task", "enabled"} | CONFIG_DEFAULT_KEYS)


def load_execution_plan_from_toml(path: str, *, log_level_override: str | None = None) -> BaoStockExecutionPlan:
    config_path = resolve_config_candidate(path)
    if not config_path.is_file():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")

    with config_path.open("rb") as fh:
        data = tomllib.load(fh)
    if not isinstance(data, dict):
        raise ValueError("配置文件格式错误：顶层必须是 TOML table。")

    unexpected_top_level_keys = set(data.keys()) - CONFIG_TOP_LEVEL_KEYS
    if unexpected_top_level_keys:
        raise ValueError(f"配置文件存在未知顶层字段: {sorted(unexpected_top_level_keys)}")

    source = str(data.get("source") or "baostock").strip() or "baostock"
    if source != "baostock":
        raise ValueError(f"BaoStock 配置文件 source 必须是 'baostock'，当前值: {source!r}")

    defaults = data.get("defaults", {})
    if defaults is None:
        defaults = {}
    if not isinstance(defaults, dict):
        raise ValueError("[defaults] 必须是 TOML table。")
    unexpected_default_keys = set(defaults.keys()) - CONFIG_DEFAULT_KEYS
    if unexpected_default_keys:
        raise ValueError(f"[defaults] 存在未知字段: {sorted(unexpected_default_keys)}")

    raw_tasks = data.get("tasks")
    if not isinstance(raw_tasks, list) or not raw_tasks:
        raise ValueError("配置文件至少需要一个 [[tasks]]。")

    task_specs: list[SyncArgs] = []
    for index, raw_task in enumerate(raw_tasks, start=1):
        if not isinstance(raw_task, dict):
            raise ValueError(f"tasks[{index}] 必须是 TOML table。")
        unexpected_task_keys = set(raw_task.keys()) - CONFIG_TASK_KEYS
        if unexpected_task_keys:
            raise ValueError(f"tasks[{index}] 存在未知字段: {sorted(unexpected_task_keys)}")
        if not _as_bool(raw_task.get("enabled", True), field_name=f"tasks[{index}].enabled"):
            continue

        merged = dict(defaults)
        merged.update(raw_task)
        task_name = str(merged.get("task") or "").strip()
        if task_name not in BAOSTOCK_TASK_CHOICES:
            raise ValueError(f"tasks[{index}].task 非法: {task_name!r}")

        task_specs.append(
            SyncArgs(
                task=task_name,
                codes_raw=_normalize_config_codes(merged.get("codes"), field_name=f"tasks[{index}].codes"),
                begin_date=str(merged.get("begin_date") or "").strip(),
                end_date=str(merged.get("end_date") or "").strip(),
                day=str(merged.get("day") or "").strip(),
                year=_as_optional_int(merged.get("year"), field_name=f"tasks[{index}].year"),
                quarter=_as_optional_int(merged.get("quarter"), field_name=f"tasks[{index}].quarter"),
                year_type=str(merged.get("year_type") or "").strip(),
                adjustflag=str(merged.get("adjustflag") or "3").strip() or "3",
                frequency=str(merged.get("frequency") or "d").strip() or "d",
                limit=_as_non_negative_int(merged.get("limit", 0), field_name=f"tasks[{index}].limit"),
                force=_as_bool(merged.get("force", False), field_name=f"tasks[{index}].force"),
                continue_on_error=_as_bool(merged.get("continue_on_error", False), field_name=f"tasks[{index}].continue_on_error"),
                runtime_path=str(data.get("runtime_path") or "").strip() or None,
                database=str(data.get("database") or "baostock").strip() or "baostock",
                log_level=str(log_level_override or data.get("log_level") or "INFO").strip() or "INFO",
            )
        )

    if not task_specs:
        raise ValueError("配置文件中的 [[tasks]] 全部被禁用，无法执行。")

    return BaoStockExecutionPlan(
        runtime_path=str(data.get("runtime_path") or "").strip() or None,
        log_level=str(log_level_override or data.get("log_level") or "INFO").strip() or "INFO",
        continue_on_error=_as_bool(data.get("continue_on_error", False), field_name="continue_on_error"),
        database=str(data.get("database") or "baostock").strip() or "baostock",
        tasks=tuple(task_specs),
    )


def _normalize_config_codes(value: Any, field_name: str) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (list, tuple)):
        codes = [str(item).strip() for item in value if str(item).strip()]
        return ",".join(codes)
    raise ValueError(f"{field_name} 必须是字符串或字符串数组。")


def _as_optional_int(value: Any, field_name: str) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception as exc:
        raise ValueError(f"{field_name} 必须是整数。") from exc


def _as_non_negative_int(value: Any, field_name: str) -> int:
    try:
        result = int(value)
    except Exception as exc:
        raise ValueError(f"{field_name} 必须是整数。") from exc
    if result < 0:
        raise ValueError(f"{field_name} 不能小于 0。")
    return result


def _as_bool(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ValueError(f"{field_name} 必须是布尔值。")


if __name__ == "__main__":
    raise SystemExit(main())
