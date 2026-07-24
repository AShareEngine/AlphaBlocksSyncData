#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""yfinance / FinanceDatabase -> ClickHouse sync runner."""

from __future__ import annotations

import argparse
import hashlib
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

from sync_data_system.config_paths import resolve_config_candidate
from sync_data_system.providers.yfinance.provider import (
    YFinanceConfig,
    YFinanceProvider,
    normalize_us_symbol_list,
)
from sync_data_system.providers.yfinance.repository import YFinanceRepository
from sync_data_system.providers.yfinance.specs import (
    CONCEPT_DEFINITIONS,
    SECTOR_DEFINITIONS,
    YFINANCE_TASK_CHOICES,
    YFINANCE_TASK_SPECS,
    MarketGroupDefinition,
)
from sync_data_system.sync_core.clickhouse import ClickHouseConfig, create_clickhouse_client
from sync_data_system.sync_core.incremental import advance_cursor_value, normalize_request_value
from sync_data_system.sync_core.task_logging import write_sync_result
from sync_data_system.toml_compat import tomllib


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SyncArgs:
    task: str
    codes_raw: str
    begin_date: str
    end_date: str
    limit: int
    force: bool
    continue_on_error: bool
    runtime_path: str | None
    database: str
    log_level: str


@dataclass(frozen=True)
class YFinanceExecutionPlan:
    runtime_path: str | None
    log_level: str
    continue_on_error: bool
    database: str
    tasks: tuple[SyncArgs, ...]


@dataclass
class YFinanceExecutionContext:
    provider: YFinanceProvider
    repository: YFinanceRepository
    connection: Any

    def close(self) -> None:
        try:
            self.connection.close()
        except Exception:
            pass
        self.provider.close()


def parse_args() -> SyncArgs:
    parser = argparse.ArgumentParser(description="免费美股数据同步入口")
    parser.add_argument("task", choices=YFINANCE_TASK_CHOICES)
    parser.add_argument("--codes", default="", help="逗号分隔的 Yahoo Finance symbol，例如 AAPL,MSFT,BRK-B")
    parser.add_argument("--begin-date", default="", help="开始日期，支持 YYYYMMDD / YYYY-MM-DD")
    parser.add_argument("--end-date", default="", help="结束日期，支持 YYYYMMDD / YYYY-MM-DD")
    parser.add_argument("--limit", type=int, default=0, help="自动股票池仅取前 N 个 symbol，0 表示不限制")
    parser.add_argument("--force", action="store_true", help="忽略已有游标和当天成功日志，强制重跑")
    parser.add_argument("--continue-on-error", action="store_true", help="单批或单 symbol 失败后继续")
    parser.add_argument("--runtime-path", default=None, help="可选 runtime.local.yaml 路径")
    parser.add_argument("--database", default="yfinance", help="ClickHouse 目标 database")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    return SyncArgs(
        task=args.task,
        codes_raw=str(args.codes or "").strip(),
        begin_date=str(args.begin_date or "").strip(),
        end_date=str(args.end_date or "").strip(),
        limit=max(0, int(args.limit or 0)),
        force=bool(args.force),
        continue_on_error=bool(args.continue_on_error),
        runtime_path=args.runtime_path,
        database=str(args.database or "yfinance").strip() or "yfinance",
        log_level=str(args.log_level or "INFO").strip() or "INFO",
    )


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    context = build_context(args.runtime_path, args.database)
    try:
        run_sync_args(args, context.provider, context.repository)
        return 0
    finally:
        context.close()


def run_config_file(path: str, *, log_level_override: str | None = None) -> int:
    plan = load_execution_plan_from_toml(path, log_level_override=log_level_override)
    logging.basicConfig(
        level=getattr(logging, plan.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    context = build_context(plan.runtime_path, plan.database)
    failed_tasks: list[str] = []
    try:
        for index, task_args in enumerate(plan.tasks, start=1):
            logger.info("batch task start progress=%s/%s task=%s", index, len(plan.tasks), task_args.task)
            try:
                run_sync_args(task_args, context.provider, context.repository)
            except Exception:
                failed_tasks.append(task_args.task)
                logger.exception("batch task failed progress=%s/%s task=%s", index, len(plan.tasks), task_args.task)
                if not plan.continue_on_error:
                    raise
        return 1 if failed_tasks else 0
    finally:
        context.close()


def build_context(runtime_path: str | None = None, database: str = "yfinance") -> YFinanceExecutionContext:
    clickhouse_config = ClickHouseConfig.from_env(runtime_path=runtime_path)
    provider = YFinanceProvider(YFinanceConfig.from_env(runtime_path=runtime_path))
    connection = create_clickhouse_client(clickhouse_config)
    repository = YFinanceRepository(connection, database=database)
    repository.ensure_tables()
    return YFinanceExecutionContext(provider=provider, repository=repository, connection=connection)


def run_registered_task(probe: Any) -> int:
    args = SyncArgs(
        task=_provider_task_name(probe.name, probe.source),
        codes_raw=",".join(probe.input_codes),
        begin_date="" if probe.input_begin_date is None else str(probe.input_begin_date),
        end_date="" if probe.input_end_date is None else str(probe.input_end_date),
        limit=max(0, int(probe.limit or 0)),
        force=bool(probe.force),
        continue_on_error=False,
        runtime_path=probe.runtime_path,
        database=str(probe.database or "yfinance"),
        log_level=str(probe.log_level or "INFO"),
    )
    inserted = run_sync_args(args, probe.context.provider, probe.context.repository)
    probe.set_row_count(inserted)
    return inserted


def run_sync_args(
    args: SyncArgs,
    provider: YFinanceProvider,
    repository: YFinanceRepository,
) -> int:
    if args.task not in YFINANCE_TASK_SPECS:
        raise ValueError(f"未知 yfinance 任务: {args.task}")
    request_meta = _request_meta(args, provider.config)
    scope_key = _scope_key(args, request_meta)
    if not args.force and repository.has_successful_sync_today(args.task, scope_key, date.today()):
        logger.info("skip task=%s reason=successful_sync_today scope=%s", args.task, scope_key)
        return 0

    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    row_count = 0
    try:
        row_count = _execute_task(args, provider, repository, request_meta)
    except Exception as exc:
        write_sync_result(
            repository=repository,
            task=args.task,
            scope_key=scope_key,
            target_table=YFINANCE_TASK_SPECS[args.task].table_name,
            request_meta=request_meta,
            row_count=row_count,
            message=str(exc),
            started_at=started_at,
            status="failed",
        )
        raise
    write_sync_result(
        repository=repository,
        task=args.task,
        scope_key=scope_key,
        target_table=YFINANCE_TASK_SPECS[args.task].table_name,
        request_meta=request_meta,
        row_count=row_count,
        message=None,
        started_at=started_at,
        status="success",
    )
    logger.info("sync finished task=%s rows=%s", args.task, row_count)
    return row_count


def _execute_task(
    args: SyncArgs,
    provider: YFinanceProvider,
    repository: YFinanceRepository,
    request_meta: dict[str, str | int | None],
) -> int:
    if args.task == "symbol_master":
        return repository.save_frame("symbol_master", provider.fetch_symbol_master(limit=args.limit))
    if args.task == "industry_membership":
        master = provider.fetch_symbol_master(limit=args.limit)
        return repository.save_frame(
            "industry_membership",
            provider.fetch_industry_membership(symbol_master=master),
        )
    if args.task == "concept_membership":
        return repository.save_frame(
            "concept_membership",
            provider.fetch_concept_membership(CONCEPT_DEFINITIONS),
        )
    if args.task == "sector_daily":
        return _run_group_price_task(
            args,
            provider,
            repository,
            SECTOR_DEFINITIONS,
            request_meta,
        )
    if args.task == "concept_daily":
        return _run_group_price_task(
            args,
            provider,
            repository,
            CONCEPT_DEFINITIONS,
            request_meta,
        )
    if args.task in {"daily_kline", "corporate_actions"}:
        symbols = resolve_symbol_list(args, provider, repository)
        if not symbols:
            raise ValueError("未获取到可用的美股 symbol；请先同步 symbol_master 或显式传 --codes。")
        return _run_symbol_task(args, provider, repository, symbols, request_meta)
    raise KeyError(args.task)


def resolve_symbol_list(
    args: SyncArgs,
    provider: YFinanceProvider,
    repository: YFinanceRepository,
) -> list[str]:
    if args.codes_raw.strip():
        symbols = normalize_us_symbol_list(args.codes_raw.split(","))
    else:
        symbols = repository.load_symbols(limit=args.limit)
        if not symbols:
            master = provider.fetch_symbol_master(limit=args.limit)
            repository.save_frame("symbol_master", master)
            symbols = normalize_us_symbol_list(master["symbol"].tolist()) if not master.empty else []
    if args.limit > 0:
        symbols = symbols[: args.limit]
    return symbols


def _run_symbol_task(
    args: SyncArgs,
    provider: YFinanceProvider,
    repository: YFinanceRepository,
    symbols: Sequence[str],
    request_meta: dict[str, str | int | None],
) -> int:
    start = str(request_meta["start_date"])
    end = str(request_meta["end_date"])
    windows: dict[str, list[str]] = {}
    for symbol in symbols:
        effective_start = _effective_start(
            start,
            repository.load_latest_cursor(args.task, symbol=symbol),
            force=args.force,
        )
        if effective_start > end:
            continue
        windows.setdefault(effective_start, []).append(symbol)

    total = 0
    fetcher: Callable[..., Any]
    fetcher = provider.fetch_daily if args.task == "daily_kline" else provider.fetch_corporate_actions
    for effective_start, window_symbols in sorted(windows.items()):
        for batch in _chunks(window_symbols, provider.config.batch_size):
            try:
                frame = fetcher(batch, start_date=effective_start, end_date=end)
                total += repository.save_frame(args.task, frame)
                _update_task_cursors(repository, args.task, batch, frame)
            except Exception:
                if not args.continue_on_error:
                    raise
                logger.exception(
                    "batch failed task=%s symbols=%s; retrying individually",
                    args.task,
                    ",".join(batch),
                )
                for symbol in batch:
                    try:
                        frame = fetcher([symbol], start_date=effective_start, end_date=end)
                        total += repository.save_frame(args.task, frame)
                        _update_task_cursors(repository, args.task, [symbol], frame)
                    except Exception:
                        logger.exception("symbol failed task=%s symbol=%s", args.task, symbol)
    return total


def _run_group_price_task(
    args: SyncArgs,
    provider: YFinanceProvider,
    repository: YFinanceRepository,
    definitions: Sequence[MarketGroupDefinition],
    request_meta: dict[str, str | int | None],
) -> int:
    start = str(request_meta["start_date"])
    end = str(request_meta["end_date"])
    total = 0
    for definition in definitions:
        effective_start = _effective_start(
            start,
            repository.load_latest_cursor(args.task, symbol=definition.benchmark_symbol),
            force=args.force,
        )
        if effective_start > end:
            continue
        try:
            frame = provider.fetch_group_daily(
                (definition,),
                start_date=effective_start,
                end_date=end,
            )
            total += repository.save_frame(args.task, frame)
            _update_task_cursors(
                repository,
                args.task,
                [definition.benchmark_symbol],
                frame,
            )
        except Exception:
            if not args.continue_on_error:
                raise
            logger.exception(
                "group failed task=%s group=%s benchmark=%s",
                args.task,
                definition.code,
                definition.benchmark_symbol,
            )
    return total


def _request_meta(args: SyncArgs, config: YFinanceConfig) -> dict[str, str | int | None]:
    spec = YFINANCE_TASK_SPECS[args.task]
    if not spec.supports_incremental:
        return {"start_date": None, "end_date": None}
    start = normalize_request_value(args.begin_date or config.default_start_date, "day")
    end = normalize_request_value(args.end_date or date.today().strftime("%Y%m%d"), "day")
    if start > end:
        raise ValueError(f"开始日期不能晚于结束日期: {start} > {end}")
    return {"start_date": start, "end_date": end}


def _effective_start(requested_start: str, latest_cursor: str | None, *, force: bool) -> str:
    if force or not latest_cursor:
        return requested_start
    return max(requested_start, advance_cursor_value(latest_cursor, "day"))


def _scope_key(args: SyncArgs, request_meta: dict[str, str | int | None]) -> str:
    parts = [f"task={args.task}"]
    if request_meta.get("start_date"):
        parts.append(f"begin={request_meta['start_date']}")
    if request_meta.get("end_date"):
        parts.append(f"end={request_meta['end_date']}")
    if args.codes_raw.strip():
        normalized = ",".join(normalize_us_symbol_list(args.codes_raw.split(",")))
        digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]
        parts.append(f"codes={digest}")
    else:
        parts.append("universe=us")
    if args.limit > 0:
        parts.append(f"limit={args.limit}")
    return "|".join(parts)


def _chunks(values: Sequence[str], size: int) -> list[list[str]]:
    return [list(values[index : index + size]) for index in range(0, len(values), max(1, size))]


def _update_task_cursors(
    repository: YFinanceRepository,
    task: str,
    requested_symbols: Sequence[str],
    frame: Any,
) -> None:
    coverage = dict(getattr(frame, "attrs", {}).get("coverage_by_symbol", {}))
    for symbol in requested_symbols:
        normalized = str(symbol or "").strip().upper()
        cursor_date = coverage.get(normalized)
        if cursor_date is not None:
            repository.upsert_task_cursor(task, normalized, cursor_date)


def _provider_task_name(registry_name: str, source: str) -> str:
    prefix = f"{source}."
    return registry_name[len(prefix) :] if str(registry_name).startswith(prefix) else registry_name


CONFIG_TOP_LEVEL_KEYS = frozenset(
    {"source", "runtime_path", "log_level", "continue_on_error", "database", "defaults", "tasks"}
)
CONFIG_DEFAULT_KEYS = frozenset(
    {"codes", "begin_date", "end_date", "limit", "force", "continue_on_error"}
)
CONFIG_TASK_KEYS = frozenset({"task", "enabled"} | CONFIG_DEFAULT_KEYS)


def load_execution_plan_from_toml(
    path: str,
    *,
    log_level_override: str | None = None,
) -> YFinanceExecutionPlan:
    config_path = resolve_config_candidate(path)
    if not config_path.is_file():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    with config_path.open("rb") as fh:
        data = tomllib.load(fh)
    if not isinstance(data, dict):
        raise ValueError("配置文件格式错误：顶层必须是 TOML table。")
    unexpected = set(data) - CONFIG_TOP_LEVEL_KEYS
    if unexpected:
        raise ValueError(f"配置文件存在未知顶层字段: {sorted(unexpected)}")
    source = str(data.get("source") or "yfinance").strip() or "yfinance"
    if source != "yfinance":
        raise ValueError(f"yfinance 配置文件 source 必须是 'yfinance'，当前值: {source!r}")
    defaults = data.get("defaults", {}) or {}
    if not isinstance(defaults, dict):
        raise ValueError("[defaults] 必须是 TOML table。")
    unexpected = set(defaults) - CONFIG_DEFAULT_KEYS
    if unexpected:
        raise ValueError(f"[defaults] 存在未知字段: {sorted(unexpected)}")
    raw_tasks = data.get("tasks")
    if not isinstance(raw_tasks, list) or not raw_tasks:
        raise ValueError("配置文件至少需要一个 [[tasks]]。")

    runtime_path = str(data.get("runtime_path") or "").strip() or None
    database = str(data.get("database") or "yfinance").strip() or "yfinance"
    log_level = str(log_level_override or data.get("log_level") or "INFO").strip() or "INFO"
    tasks: list[SyncArgs] = []
    for index, raw_task in enumerate(raw_tasks, start=1):
        if not isinstance(raw_task, dict):
            raise ValueError(f"tasks[{index}] 必须是 TOML table。")
        unexpected = set(raw_task) - CONFIG_TASK_KEYS
        if unexpected:
            raise ValueError(f"tasks[{index}] 存在未知字段: {sorted(unexpected)}")
        if not _as_bool(raw_task.get("enabled", True), f"tasks[{index}].enabled"):
            continue
        merged = dict(defaults)
        merged.update(raw_task)
        task_name = str(merged.get("task") or "").strip()
        if task_name not in YFINANCE_TASK_SPECS:
            raise ValueError(f"tasks[{index}].task 非法: {task_name!r}")
        tasks.append(
            SyncArgs(
                task=task_name,
                codes_raw=_normalize_codes(merged.get("codes"), f"tasks[{index}].codes"),
                begin_date=str(merged.get("begin_date") or "").strip(),
                end_date=str(merged.get("end_date") or "").strip(),
                limit=_as_non_negative_int(merged.get("limit", 0), f"tasks[{index}].limit"),
                force=_as_bool(merged.get("force", False), f"tasks[{index}].force"),
                continue_on_error=_as_bool(
                    merged.get("continue_on_error", False),
                    f"tasks[{index}].continue_on_error",
                ),
                runtime_path=runtime_path,
                database=database,
                log_level=log_level,
            )
        )
    if not tasks:
        raise ValueError("配置文件中的 [[tasks]] 全部被禁用，无法执行。")
    return YFinanceExecutionPlan(
        runtime_path=runtime_path,
        log_level=log_level,
        continue_on_error=_as_bool(data.get("continue_on_error", False), "continue_on_error"),
        database=database,
        tasks=tuple(tasks),
    )


def _normalize_codes(value: Any, field_name: str) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (list, tuple)):
        return ",".join(str(item).strip() for item in value if str(item).strip())
    raise ValueError(f"{field_name} 必须是字符串或字符串数组。")


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
