#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AKShare -> ClickHouse sync runner."""

from __future__ import annotations

import argparse
import hashlib
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from sync_data_system.config_paths import resolve_config_candidate
from sync_data_system.providers.akshare.provider import (
    AkshareUSConfig,
    AkshareUSProvider,
    normalize_ths_concept_list,
    normalize_us_symbol_list,
)
from sync_data_system.providers.akshare.repository import AkshareUSRepository
from sync_data_system.providers.akshare.specs import (
    AKSHARE_TASK_CHOICES,
    AKSHARE_TASK_SPECS,
    FINANCIAL_PERIOD_TYPES,
    FINANCIAL_STATEMENT_TYPES,
    US_INDEX_NAMES,
    VALUATION_INDICATORS,
)
from sync_data_system.sync_core.clickhouse import ClickHouseConfig, create_clickhouse_client
from sync_data_system.sync_core.incremental import advance_cursor_value, normalize_request_value
from sync_data_system.sync_core.task_logging import write_sync_result
from sync_data_system.toml_compat import tomllib


logger = logging.getLogger(__name__)

MAX_CONSECUTIVE_SYMBOL_FAILURES = 5


@dataclass(frozen=True)
class SyncArgs:
    task: str
    codes_raw: str
    begin_date: str
    end_date: str
    index_code: str
    period: str
    fields: str
    limit: int
    force: bool
    continue_on_error: bool
    runtime_path: str | None
    database: str
    log_level: str


@dataclass(frozen=True)
class AkshareExecutionPlan:
    runtime_path: str | None
    log_level: str
    continue_on_error: bool
    database: str
    tasks: tuple[SyncArgs, ...]


@dataclass
class AkshareExecutionContext:
    provider: AkshareUSProvider
    repository: AkshareUSRepository
    connection: Any

    def close(self) -> None:
        try:
            self.connection.close()
        except Exception:
            pass
        self.provider.close()


def parse_args() -> SyncArgs:
    parser = argparse.ArgumentParser(description="AKShare 免费美股数据同步入口")
    parser.add_argument("task", choices=AKSHARE_TASK_CHOICES)
    parser.add_argument("--codes", default="", help="逗号分隔的美股代码，例如 AAPL,MSFT")
    parser.add_argument("--begin-date", default="", help="开始日期，支持 YYYYMMDD / YYYY-MM-DD")
    parser.add_argument("--end-date", default="", help="结束日期，支持 YYYYMMDD / YYYY-MM-DD")
    parser.add_argument("--index-code", default="", help="美股指数代码，例如 .INX,.IXIC")
    parser.add_argument("--period", default="", help="报告期或估值区间")
    parser.add_argument("--fields", default="", help="财务报表类型或估值指标，逗号分隔")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--runtime-path", default=None)
    parser.add_argument("--database", default="akshare")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    return SyncArgs(
        task=args.task,
        codes_raw=str(args.codes or "").strip(),
        begin_date=str(args.begin_date or "").strip(),
        end_date=str(args.end_date or "").strip(),
        index_code=str(args.index_code or "").strip(),
        period=str(args.period or "").strip(),
        fields=str(args.fields or "").strip(),
        limit=max(0, int(args.limit or 0)),
        force=bool(args.force),
        continue_on_error=bool(args.continue_on_error),
        runtime_path=args.runtime_path,
        database=str(args.database or "akshare").strip() or "akshare",
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


def build_context(runtime_path: str | None = None, database: str = "akshare") -> AkshareExecutionContext:
    clickhouse_config = ClickHouseConfig.from_env(runtime_path=runtime_path)
    provider = AkshareUSProvider(AkshareUSConfig.from_env(runtime_path=runtime_path))
    connection = create_clickhouse_client(clickhouse_config)
    repository = AkshareUSRepository(connection, database=database)
    repository.ensure_tables()
    return AkshareExecutionContext(
        provider=provider,
        repository=repository,
        connection=connection,
    )


def run_registered_task(probe: Any) -> int:
    args = SyncArgs(
        task=_provider_task_name(probe.name, probe.source),
        codes_raw=",".join(probe.input_codes),
        begin_date="" if probe.input_begin_date is None else str(probe.input_begin_date),
        end_date="" if probe.input_end_date is None else str(probe.input_end_date),
        index_code=str(probe.input_index_code or "").strip(),
        period=str(probe.input_period or "").strip(),
        fields=str(probe.input_fields or "").strip(),
        limit=max(0, int(probe.limit or 0)),
        force=bool(probe.force),
        continue_on_error=True,
        runtime_path=probe.runtime_path,
        database=str(probe.database or "akshare"),
        log_level=str(probe.log_level or "INFO"),
    )
    inserted = run_sync_args(args, probe.context.provider, probe.context.repository)
    probe.set_row_count(inserted)
    return inserted


def run_sync_args(
    args: SyncArgs,
    provider: AkshareUSProvider,
    repository: AkshareUSRepository,
) -> int:
    if args.task not in AKSHARE_TASK_SPECS:
        raise ValueError(f"未知 akshare 任务: {args.task}")
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
            target_table=AKSHARE_TASK_SPECS[args.task].table_name,
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
        target_table=AKSHARE_TASK_SPECS[args.task].table_name,
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
    provider: AkshareUSProvider,
    repository: AkshareUSRepository,
    request_meta: dict[str, str | None],
) -> int:
    if args.task == "us_spot":
        return repository.save_frame("us_spot", provider.fetch_us_spot(limit=args.limit))
    if args.task == "us_index_daily":
        return _run_index_task(args, provider, repository, request_meta)
    if args.task == "stock_board_concept_name_ths":
        return repository.save_frame(
            args.task,
            provider.fetch_ths_concept_names(limit=args.limit),
        )
    if args.task == "stock_board_concept_name_em":
        return repository.save_frame(
            args.task,
            provider.fetch_em_concept_names(limit=args.limit),
        )
    if args.task in {
        "stock_board_concept_index_ths",
        "stock_board_concept_info_ths",
    }:
        concepts = _resolve_ths_concepts(args, provider, repository)
        if not concepts:
            raise ValueError("未获取到同花顺概念板块目录。")
        if args.task == "stock_board_concept_index_ths":
            return _run_ths_concept_index_task(
                args,
                provider,
                repository,
                concepts,
                request_meta,
            )
        return _run_per_concept(
            args,
            repository,
            concepts,
            lambda item: provider.fetch_ths_concept_info(
                item["concept_name"],
                item["concept_code"],
            ),
        )
    if args.task in {
        "stock_board_concept_cons_em",
        "stock_board_concept_hist_em",
    }:
        concepts = _resolve_em_concepts(args, provider, repository)
        if not concepts:
            raise ValueError("未获取到东方财富概念板块目录。")
        if args.task == "stock_board_concept_hist_em":
            return _run_em_concept_history_task(
                args,
                provider,
                repository,
                concepts,
                request_meta,
            )
        return _run_per_concept(
            args,
            repository,
            concepts,
            lambda item: provider.fetch_em_concept_constituents(
                item["concept_name"],
                item["concept_code"],
            ),
        )

    require_em_code = args.task in {"us_daily_kline", "us_minute_kline"}
    symbols = _resolve_symbols(args, provider, repository, require_em_code=require_em_code)
    if not symbols:
        raise ValueError("未获取到可用的美股代码；请先同步 us_spot 或显式传 --codes。")
    if args.task == "us_minute_kline":
        unresolved = [
            item["symbol"]
            for item in symbols
            if not re.match(r"^\d+\..+$", str(item.get("em_code") or ""))
        ]
        if unresolved:
            preview = ",".join(unresolved[:10])
            logger.warning(
                "AKShare minute symbols without Eastmoney market code will be skipped "
                "count=%s preview=%s",
                len(unresolved),
                preview,
            )
            symbols = [
                item
                for item in symbols
                if re.match(r"^\d+\..+$", str(item.get("em_code") or ""))
            ]
        if not symbols:
            raise ValueError(
                "AKShare 美股分钟线没有可用的东方财富市场代码；"
                "请先同步最新 us_spot，或显式传入类似 105.AAPL 的代码。"
            )

    if args.task == "us_daily_kline":
        return _run_daily_task(args, provider, repository, symbols, request_meta)
    if args.task == "us_minute_kline":
        return _run_per_symbol(
            args,
            repository,
            symbols,
            lambda item: provider.fetch_us_minute(
                em_code=item["em_code"],
                symbol=item["symbol"],
                start_date=args.begin_date or None,
                end_date=args.end_date or None,
            ),
        )
    if args.task == "us_company_profile":
        return _run_per_symbol(
            args,
            repository,
            symbols,
            lambda item: provider.fetch_us_company_profile(item["symbol"]),
        )
    if args.task == "us_financial_statement":
        statement_types = _selection(
            args.fields,
            FINANCIAL_STATEMENT_TYPES,
            default=FINANCIAL_STATEMENT_TYPES,
            label="财务报表类型",
        )
        period_types = _selection(
            args.period,
            FINANCIAL_PERIOD_TYPES,
            default=("年报",),
            label="报告期",
        )
        total = 0
        for statement_type in statement_types:
            for period_type in period_types:
                total += _run_per_symbol(
                    args,
                    repository,
                    symbols,
                    lambda item, statement_type=statement_type, period_type=period_type: (
                        provider.fetch_us_financial_statement(
                            item["symbol"],
                            statement_type=statement_type,
                            period_type=period_type,
                        )
                    ),
                )
        return total
    if args.task == "us_financial_indicator":
        period_types = _selection(
            args.period,
            FINANCIAL_PERIOD_TYPES,
            default=("年报",),
            label="报告期",
        )
        total = 0
        for period_type in period_types:
            total += _run_per_symbol(
                args,
                repository,
                symbols,
                lambda item, period_type=period_type: provider.fetch_us_financial_indicator(
                    item["symbol"],
                    period_type=period_type,
                ),
            )
        return total
    if args.task == "us_valuation":
        indicators = _selection(
            args.fields,
            VALUATION_INDICATORS,
            default=("总市值",),
            label="估值指标",
        )
        period = args.period or "近一年"
        if period not in {"近一年", "近三年", "全部"}:
            raise ValueError(f"估值区间非法: {period!r}")
        total = 0
        for indicator in indicators:
            total += _run_per_symbol(
                args,
                repository,
                symbols,
                lambda item, indicator=indicator: provider.fetch_us_valuation(
                    item["symbol"],
                    indicator=indicator,
                    period=period,
                ),
            )
        return total
    raise KeyError(args.task)


def _resolve_symbols(
    args: SyncArgs,
    provider: AkshareUSProvider,
    repository: AkshareUSRepository,
    *,
    require_em_code: bool,
) -> list[dict[str, str]]:
    requested = normalize_us_symbol_list(args.codes_raw.split(",")) if args.codes_raw else []
    if not requested:
        saved = repository.load_symbols(limit=args.limit)
        requested = normalize_us_symbol_list(saved)
    if not requested:
        spot = provider.fetch_us_spot(limit=args.limit)
        repository.save_frame("us_spot", spot)
        requested = normalize_us_symbol_list(spot["symbol"].tolist())
    return provider.resolve_us_symbols(
        requested,
        limit=args.limit,
        require_em_code=require_em_code,
    )


def _resolve_ths_concepts(
    args: SyncArgs,
    provider: AkshareUSProvider,
    repository: AkshareUSRepository,
) -> list[dict[str, str]]:
    requested = (
        normalize_ths_concept_list(args.codes_raw.split(","))
        if args.codes_raw
        else []
    )
    saved = repository.load_ths_concepts()
    if saved:
        try:
            return provider.resolve_ths_concepts(
                requested,
                limit=args.limit,
                directory=saved,
            )
        except ValueError:
            if not requested:
                raise
            logger.info("refreshing THS concept directory for explicitly requested concepts")
    directory = provider.fetch_ths_concept_names()
    repository.save_frame("stock_board_concept_name_ths", directory)
    return provider.resolve_ths_concepts(
        requested,
        limit=args.limit,
        directory=directory,
    )


def _resolve_em_concepts(
    args: SyncArgs,
    provider: AkshareUSProvider,
    repository: AkshareUSRepository,
) -> list[dict[str, str]]:
    requested = (
        normalize_ths_concept_list(args.codes_raw.split(","))
        if args.codes_raw
        else []
    )
    saved = repository.load_em_concepts()
    if saved:
        try:
            return provider.resolve_em_concepts(
                requested,
                limit=args.limit,
                directory=saved,
            )
        except ValueError:
            if not requested:
                raise
            logger.info("refreshing Eastmoney concept directory for explicitly requested concepts")
    directory = provider.fetch_em_concept_names()
    repository.save_frame("stock_board_concept_name_em", directory)
    return provider.resolve_em_concepts(
        requested,
        limit=args.limit,
        directory=directory,
    )


def _run_daily_task(
    args: SyncArgs,
    provider: AkshareUSProvider,
    repository: AkshareUSRepository,
    symbols: list[dict[str, str]],
    request_meta: dict[str, str | None],
) -> int:
    start = str(request_meta["start_date"])
    end = str(request_meta["end_date"])
    total = 0
    succeeded = 0
    failures: list[str] = []
    consecutive_failures = 0
    for item in symbols:
        symbol = item["symbol"]
        effective_start = _effective_start(
            start,
            repository.load_latest_cursor(args.task, symbol=symbol),
            force=args.force,
        )
        if effective_start > end:
            continue
        try:
            frame = provider.fetch_us_daily(
                em_code=item["em_code"],
                symbol=symbol,
                start_date=effective_start,
                end_date=end,
            )
            total += repository.save_frame(args.task, frame)
            cursor = dict(frame.attrs.get("coverage_by_symbol", {})).get(symbol)
            if cursor is not None:
                repository.upsert_task_cursor(args.task, symbol, cursor)
            succeeded += 1
            consecutive_failures = 0
        except Exception as exc:
            if not args.continue_on_error:
                raise
            failures.append(f"{symbol}: {exc}")
            consecutive_failures += 1
            logger.exception("AKShare symbol failed task=%s symbol=%s", args.task, symbol)
            _raise_if_failure_circuit_open(
                args.task,
                consecutive_failures=consecutive_failures,
                failures=failures,
            )
    _raise_if_all_failed(args.task, succeeded=succeeded, failures=failures)
    return total


def _run_per_symbol(
    args: SyncArgs,
    repository: AkshareUSRepository,
    symbols: Iterable[dict[str, str]],
    fetcher: Callable[[dict[str, str]], Any],
) -> int:
    total = 0
    succeeded = 0
    failures: list[str] = []
    consecutive_failures = 0
    for item in symbols:
        try:
            total += repository.save_frame(args.task, fetcher(item))
            succeeded += 1
            consecutive_failures = 0
        except Exception as exc:
            if not args.continue_on_error:
                raise
            failures.append(f"{item['symbol']}: {exc}")
            consecutive_failures += 1
            logger.exception("AKShare symbol failed task=%s symbol=%s", args.task, item["symbol"])
            _raise_if_failure_circuit_open(
                args.task,
                consecutive_failures=consecutive_failures,
                failures=failures,
            )
    _raise_if_all_failed(args.task, succeeded=succeeded, failures=failures)
    return total


def _run_per_concept(
    args: SyncArgs,
    repository: AkshareUSRepository,
    concepts: Iterable[dict[str, str]],
    fetcher: Callable[[dict[str, str]], Any],
) -> int:
    total = 0
    succeeded = 0
    failures: list[str] = []
    consecutive_failures = 0
    for item in concepts:
        concept_name = item["concept_name"]
        try:
            total += repository.save_frame(args.task, fetcher(item))
            succeeded += 1
            consecutive_failures = 0
        except Exception as exc:
            if not args.continue_on_error:
                raise
            failures.append(f"{concept_name}: {exc}")
            consecutive_failures += 1
            logger.exception(
                "AKShare concept failed task=%s concept=%s",
                args.task,
                concept_name,
            )
            _raise_if_failure_circuit_open(
                args.task,
                consecutive_failures=consecutive_failures,
                failures=failures,
            )
    _raise_if_all_failed(args.task, succeeded=succeeded, failures=failures)
    return total


def _raise_if_failure_circuit_open(
    task: str,
    *,
    consecutive_failures: int,
    failures: list[str],
) -> None:
    if consecutive_failures < MAX_CONSECUTIVE_SYMBOL_FAILURES:
        return
    preview = "; ".join(failures[-MAX_CONSECUTIVE_SYMBOL_FAILURES:])
    raise RuntimeError(
        f"AKShare 任务 {task} 连续 {consecutive_failures} 个代码请求失败，"
        f"已停止后续全市场请求: {preview}"
    )


def _run_index_task(
    args: SyncArgs,
    provider: AkshareUSProvider,
    repository: AkshareUSRepository,
    request_meta: dict[str, str | None],
) -> int:
    index_codes = _selection(
        args.index_code,
        tuple(US_INDEX_NAMES),
        default=tuple(US_INDEX_NAMES),
        label="美股指数",
    )
    start = str(request_meta["start_date"])
    end = str(request_meta["end_date"])
    total = 0
    succeeded = 0
    failures: list[str] = []
    for index_code in index_codes:
        effective_start = _effective_start(
            start,
            repository.load_latest_cursor(args.task, symbol=index_code),
            force=args.force,
        )
        if effective_start > end:
            continue
        try:
            frame = provider.fetch_us_index_daily(
                index_code,
                US_INDEX_NAMES[index_code],
                start_date=effective_start,
                end_date=end,
            )
            total += repository.save_frame(args.task, frame)
            cursor = dict(frame.attrs.get("coverage_by_symbol", {})).get(index_code)
            if cursor is not None:
                repository.upsert_task_cursor(args.task, index_code, cursor)
            succeeded += 1
        except Exception as exc:
            if not args.continue_on_error:
                raise
            failures.append(f"{index_code}: {exc}")
            logger.exception("AKShare index failed index=%s", index_code)
    _raise_if_all_failed(args.task, succeeded=succeeded, failures=failures)
    return total


def _run_ths_concept_index_task(
    args: SyncArgs,
    provider: AkshareUSProvider,
    repository: AkshareUSRepository,
    concepts: Iterable[dict[str, str]],
    request_meta: dict[str, str | None],
) -> int:
    start = str(request_meta["start_date"])
    end = str(request_meta["end_date"])
    total = 0
    succeeded = 0
    failures: list[str] = []
    consecutive_failures = 0
    for item in concepts:
        concept_code = item["concept_code"]
        concept_name = item["concept_name"]
        cursor_key = concept_code or concept_name
        effective_start = _effective_start(
            start,
            repository.load_latest_cursor(args.task, symbol=cursor_key),
            force=args.force,
        )
        if effective_start > end:
            continue
        try:
            frame = provider.fetch_ths_concept_index(
                concept_name,
                concept_code,
                start_date=effective_start,
                end_date=end,
            )
            total += repository.save_frame(args.task, frame)
            cursor = dict(frame.attrs.get("coverage_by_symbol", {})).get(cursor_key)
            if cursor is not None:
                repository.upsert_task_cursor(args.task, cursor_key, cursor)
            succeeded += 1
            consecutive_failures = 0
        except Exception as exc:
            if not args.continue_on_error:
                raise
            failures.append(f"{concept_name}: {exc}")
            consecutive_failures += 1
            logger.exception(
                "AKShare THS concept index failed concept=%s code=%s",
                concept_name,
                concept_code,
            )
            _raise_if_failure_circuit_open(
                args.task,
                consecutive_failures=consecutive_failures,
                failures=failures,
            )
    _raise_if_all_failed(args.task, succeeded=succeeded, failures=failures)
    return total


def _run_em_concept_history_task(
    args: SyncArgs,
    provider: AkshareUSProvider,
    repository: AkshareUSRepository,
    concepts: Iterable[dict[str, str]],
    request_meta: dict[str, str | None],
) -> int:
    period = str(args.period or "daily").strip().lower()
    if period not in {"daily", "weekly", "monthly"}:
        raise ValueError("东方财富概念历史周期只能是 daily、weekly 或 monthly。")
    adjust = provider.config.adjust
    start = str(request_meta["start_date"])
    end = str(request_meta["end_date"])
    total = 0
    succeeded = 0
    failures: list[str] = []
    consecutive_failures = 0
    for item in concepts:
        concept_code = item["concept_code"]
        concept_name = item["concept_name"]
        cursor_key = f"{concept_code or concept_name}|{period}|{adjust or 'none'}"
        effective_start = _effective_start(
            start,
            repository.load_latest_cursor(args.task, symbol=cursor_key),
            force=args.force,
        )
        if effective_start > end:
            continue
        try:
            frame = provider.fetch_em_concept_history(
                concept_name,
                concept_code,
                period=period,
                start_date=effective_start,
                end_date=end,
                adjust=adjust,
            )
            total += repository.save_frame(args.task, frame)
            if not frame.empty:
                repository.upsert_task_cursor(
                    args.task,
                    cursor_key,
                    frame["trade_date"].max(),
                )
            succeeded += 1
            consecutive_failures = 0
        except Exception as exc:
            if not args.continue_on_error:
                raise
            failures.append(f"{concept_name}: {exc}")
            consecutive_failures += 1
            logger.exception(
                "AKShare Eastmoney concept history failed concept=%s code=%s period=%s adjust=%s",
                concept_name,
                concept_code,
                period,
                adjust or "none",
            )
            _raise_if_failure_circuit_open(
                args.task,
                consecutive_failures=consecutive_failures,
                failures=failures,
            )
    _raise_if_all_failed(args.task, succeeded=succeeded, failures=failures)
    return total


def _raise_if_all_failed(
    task: str,
    *,
    succeeded: int,
    failures: list[str],
) -> None:
    if not failures:
        return
    if succeeded <= 0:
        preview = "; ".join(failures[:5])
        raise RuntimeError(f"AKShare 任务 {task} 的全部请求均失败: {preview}")
    logger.warning(
        "AKShare task completed with partial failures task=%s succeeded=%s failed=%s",
        task,
        succeeded,
        len(failures),
    )


def _request_meta(args: SyncArgs, config: AkshareUSConfig) -> dict[str, str | None]:
    spec = AKSHARE_TASK_SPECS[args.task]
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


def _scope_key(args: SyncArgs, request_meta: dict[str, str | None]) -> str:
    parts = [f"task={args.task}"]
    if request_meta.get("start_date"):
        parts.append(f"begin={request_meta['start_date']}")
    if request_meta.get("end_date"):
        parts.append(f"end={request_meta['end_date']}")
    if args.codes_raw:
        normalized = ",".join(normalize_us_symbol_list(args.codes_raw.split(",")))
        digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]
        parts.append(f"codes={digest}")
    else:
        if args.task.endswith("_em") and args.task.startswith("stock_board_concept_"):
            universe = "em_concept"
        elif args.task.startswith("stock_board_concept_"):
            universe = "ths_concept"
        else:
            universe = "us"
        parts.append(f"universe={universe}")
    if args.index_code:
        parts.append(f"index={args.index_code}")
    if args.period:
        parts.append(f"period={args.period}")
    if args.fields:
        parts.append(f"fields={args.fields}")
    if args.limit > 0:
        parts.append(f"limit={args.limit}")
    return "|".join(parts)


def _selection(
    raw: str,
    allowed: tuple[str, ...],
    *,
    default: tuple[str, ...],
    label: str,
) -> tuple[str, ...]:
    values = tuple(item.strip() for item in str(raw or "").split(",") if item.strip()) or default
    invalid = [item for item in values if item not in allowed]
    if invalid:
        raise ValueError(f"{label}非法: {invalid}; allowed={list(allowed)}")
    return values


def _provider_task_name(registry_name: str, source: str) -> str:
    prefix = f"{source}."
    return registry_name[len(prefix) :] if str(registry_name).startswith(prefix) else registry_name


CONFIG_TOP_LEVEL_KEYS = frozenset(
    {"source", "runtime_path", "log_level", "continue_on_error", "database", "defaults", "tasks"}
)
CONFIG_DEFAULT_KEYS = frozenset(
    {
        "codes",
        "begin_date",
        "end_date",
        "index_code",
        "period",
        "fields",
        "limit",
        "force",
        "continue_on_error",
    }
)
CONFIG_TASK_KEYS = frozenset({"task", "enabled"} | CONFIG_DEFAULT_KEYS)


def load_execution_plan_from_toml(
    path: str,
    *,
    log_level_override: str | None = None,
) -> AkshareExecutionPlan:
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
    source = str(data.get("source") or "akshare").strip() or "akshare"
    if source != "akshare":
        raise ValueError(f"AKShare 配置文件 source 必须是 'akshare'，当前值: {source!r}")
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
    database = str(data.get("database") or "akshare").strip() or "akshare"
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
        if task_name not in AKSHARE_TASK_SPECS:
            raise ValueError(f"tasks[{index}].task 非法: {task_name!r}")
        tasks.append(
            SyncArgs(
                task=task_name,
                codes_raw=_normalize_codes(merged.get("codes"), f"tasks[{index}].codes"),
                begin_date=str(merged.get("begin_date") or "").strip(),
                end_date=str(merged.get("end_date") or "").strip(),
                index_code=str(merged.get("index_code") or "").strip(),
                period=str(merged.get("period") or "").strip(),
                fields=str(merged.get("fields") or "").strip(),
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
    return AkshareExecutionPlan(
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
        parsed = int(value or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} 必须是非负整数。") from exc
    if parsed < 0:
        raise ValueError(f"{field_name} 必须是非负整数。")
    return parsed


def _as_bool(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off", ""}:
        return False
    raise ValueError(f"{field_name} 必须是布尔值。")


__all__ = [
    "AkshareExecutionContext",
    "AkshareExecutionPlan",
    "SyncArgs",
    "build_context",
    "load_execution_plan_from_toml",
    "main",
    "run_config_file",
    "run_registered_task",
    "run_sync_args",
]


if __name__ == "__main__":
    raise SystemExit(main())
