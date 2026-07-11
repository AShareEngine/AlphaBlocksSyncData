#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""QMT REST -> ClickHouse sync runner."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from sync_data_system.config_paths import resolve_config_candidate
from sync_data_system.providers.qmt.provider import QmtConfig, QmtProvider, iter_qmt_rows, normalize_qmt_code_list
from sync_data_system.providers.qmt.repository import QmtRepository
from sync_data_system.providers.qmt.specs import QMT_TASK_CHOICES, QMT_TASK_SPECS
from sync_data_system.sync_core.clickhouse import ClickHouseConfig, create_clickhouse_client
from sync_data_system.sync_core.incremental import (
    advance_cursor_value,
    compare_cursor_values,
    default_request_end,
)
from sync_data_system.sync_core.scope import build_scope_key
from sync_data_system.sync_core.task_logging import write_sync_result
from sync_data_system.toml_compat import tomllib


logger = logging.getLogger(__name__)

QMT_DEFAULT_SYMBOL_UNIVERSE_SECTOR = "沪深A股"


@dataclass(frozen=True)
class SyncArgs:
    task: str
    symbols_raw: str
    symbol: str
    market: str
    index_code: str
    stock_code: str
    table_names_raw: str
    sector_name: str
    code_market: str
    begin_time: str
    end_time: str
    period: str
    fields_raw: str
    adjust_type: str
    fill_data: bool
    count: int
    incrementally: bool
    complete: bool
    limit: int
    force: bool
    continue_on_error: bool
    runtime_path: str | None
    database: str
    log_level: str


@dataclass(frozen=True)
class QmtExecutionPlan:
    runtime_path: str | None
    log_level: str
    continue_on_error: bool
    database: str
    tasks: tuple[SyncArgs, ...]


@dataclass
class QmtExecutionContext:
    provider: QmtProvider
    repository: QmtRepository
    connection: Any

    def close(self) -> None:
        try:
            self.connection.close()
        except Exception:
            pass
        self.provider.close()


def parse_args() -> SyncArgs:
    parser = argparse.ArgumentParser(description="QMT REST 同步入口")
    parser.add_argument("task", choices=QMT_TASK_CHOICES)
    parser.add_argument("--symbols", default="", help="逗号分隔代码，支持 `600000.SH`")
    parser.add_argument("--symbol", default="", help="单代码 GET 接口使用")
    parser.add_argument("--market", default="", help="市场，例如 SH / SZ")
    parser.add_argument("--index-code", default="", help="指数代码，例如 000300.SH")
    parser.add_argument("--stock-code", default="", help="单只下载/除权接口使用")
    parser.add_argument("--table-names", default="", help="逗号分隔财务表名，例如 Balance,Income")
    parser.add_argument("--sector-name", default="", help="板块名称，例如 沪深A股")
    parser.add_argument("--code-market", default="", help="主力合约接口 code_market")
    parser.add_argument("--begin-time", default="", help="开始时间，YYYYMMDD 或 YYYYMMDDHHMMSS")
    parser.add_argument("--end-time", default="", help="结束时间，YYYYMMDD 或 YYYYMMDDHHMMSS")
    parser.add_argument("--period", default="", help="周期，例如 1d / 1m / 5m")
    parser.add_argument("--fields", default="", help="逗号分隔字段列表")
    parser.add_argument("--adjust-type", default="none", help="复权类型")
    parser.add_argument("--fill-data", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--count", type=int, default=-1)
    parser.add_argument("--incrementally", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--complete", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--runtime-path", default=None)
    parser.add_argument("--database", default="qmt")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    return SyncArgs(
        task=args.task,
        symbols_raw=str(args.symbols or "").strip(),
        symbol=str(args.symbol or "").strip(),
        market=str(args.market or "").strip(),
        index_code=str(args.index_code or "").strip(),
        stock_code=str(args.stock_code or "").strip(),
        table_names_raw=str(args.table_names or "").strip(),
        sector_name=str(args.sector_name or "").strip(),
        code_market=str(args.code_market or "").strip(),
        begin_time=str(args.begin_time or "").strip(),
        end_time=str(args.end_time or "").strip(),
        period=str(args.period or "").strip(),
        fields_raw=str(args.fields or "").strip(),
        adjust_type=str(args.adjust_type or "none").strip() or "none",
        fill_data=bool(args.fill_data),
        count=int(args.count),
        incrementally=bool(args.incrementally),
        complete=bool(args.complete),
        limit=max(0, int(args.limit or 0)),
        force=bool(args.force),
        continue_on_error=bool(args.continue_on_error),
        runtime_path=args.runtime_path,
        database=str(args.database or "qmt").strip() or "qmt",
        log_level=str(args.log_level or "INFO").strip() or "INFO",
    )


def run_config_file(path: str, *, log_level_override: str | None = None) -> int:
    plan = load_execution_plan_from_toml(path, log_level_override=log_level_override)
    logging.basicConfig(
        level=getattr(logging, plan.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    clickhouse_config = ClickHouseConfig.from_env(runtime_path=plan.runtime_path)
    provider = QmtProvider(QmtConfig.from_env(runtime_path=plan.runtime_path))
    connection = create_clickhouse_client(clickhouse_config)
    repository = QmtRepository(connection, database=plan.database)

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


def build_context(runtime_path: str | None = None, database: str = "qmt") -> QmtExecutionContext:
    clickhouse_config = ClickHouseConfig.from_env(runtime_path=runtime_path)
    provider = QmtProvider(QmtConfig.from_env(runtime_path=runtime_path))
    connection = create_clickhouse_client(clickhouse_config)
    repository = QmtRepository(connection, database=database)
    repository.ensure_tables()
    return QmtExecutionContext(provider=provider, repository=repository, connection=connection)


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    clickhouse_config = ClickHouseConfig.from_env(runtime_path=args.runtime_path)
    provider = QmtProvider(QmtConfig.from_env(runtime_path=args.runtime_path))
    connection = create_clickhouse_client(clickhouse_config)
    repository = QmtRepository(connection, database=args.database)

    try:
        repository.ensure_tables()
        return run_sync_args(args, provider, repository)
    finally:
        try:
            connection.close()
        except Exception:
            pass
        provider.close()


def run_sync_args(args: SyncArgs, provider: QmtProvider, repository: QmtRepository) -> int:
    args = resolve_auto_symbol_universe(args, provider)
    specs = expand_task_args(args)
    if not specs:
        raise ValueError(f"QMT 任务 {args.task} 未解析出可执行请求，请检查参数。")

    total = 0
    for index, task_args in enumerate(specs, start=1):
        request_meta = build_request_meta(task_args)
        validate_required_request(task_args, request_meta)
        request_meta = resolve_effective_request_meta(task_args, repository, request_meta)
        if request_meta is None:
            logger.info("skip task=%s progress=%s/%s reason=no_incremental_window", task_args.task, index, len(specs))
            continue
        inserted = run_single_request(task_args, provider, repository, request_meta)
        total += inserted
    return total


def run_registered_task(probe: Any) -> int:
    codes = ",".join(probe.input_codes)
    args = SyncArgs(
        task=_provider_task_name(probe.name, probe.source),
        symbols_raw=codes,
        symbol=codes.split(",", 1)[0] if codes else "",
        market=str(getattr(probe, "input_market", "") or ""),
        index_code=str(getattr(probe, "input_index_code", "") or ""),
        stock_code=codes.split(",", 1)[0] if codes else "",
        table_names_raw=str(getattr(probe, "input_table_names", "") or ""),
        sector_name=str(getattr(probe, "input_sector_name", "") or ""),
        code_market=str(getattr(probe, "input_code_market", "") or ""),
        begin_time=_format_optional_int(probe.input_begin_date),
        end_time=_format_optional_int(probe.input_end_date),
        period=str(getattr(probe, "input_period", "") or ""),
        fields_raw=str(getattr(probe, "input_fields", "") or ""),
        adjust_type=str(getattr(probe, "input_adjust_type", "none") or "none"),
        fill_data=bool(getattr(probe, "input_fill_data", True)),
        count=int(getattr(probe, "input_count", -1) or -1),
        incrementally=bool(getattr(probe, "input_incrementally", False)),
        complete=bool(getattr(probe, "input_complete", False)),
        limit=probe.limit,
        force=probe.force,
        continue_on_error=False,
        runtime_path=probe.runtime_path,
        database=str(probe.database or "qmt"),
        log_level=str(probe.log_level or "INFO"),
    )
    inserted = run_sync_args(args, probe.context.provider, probe.context.repository)
    probe.set_row_count(inserted)
    return inserted


def _provider_task_name(registry_name: str, source: str) -> str:
    prefix = f"{source}."
    return registry_name[len(prefix):] if str(registry_name).startswith(prefix) else registry_name


def _format_optional_int(value: Any) -> str:
    return "" if value is None else str(value)


def resolve_auto_symbol_universe(args: SyncArgs, provider: QmtProvider) -> SyncArgs:
    spec = QMT_TASK_SPECS[args.task]
    if not spec.uses_symbols or parse_symbol_list(args.symbols_raw):
        return args
    if not spec.auto_symbol_universe:
        return args

    sector_name = args.sector_name.strip() or QMT_DEFAULT_SYMBOL_UNIVERSE_SECTOR
    envelope = provider.fetch_task("sectors", sector_name=sector_name)
    rows = iter_qmt_rows(QMT_TASK_SPECS["sectors"], envelope, {"sector_name": sector_name})
    symbols = normalize_qmt_code_list([str(row.get("symbol") or "") for row in rows])
    if not symbols:
        raise ValueError(
            f"QMT 任务 {args.task} 未传 codes，且无法从板块 {sector_name!r} 获取 symbols；"
            "请在请求或 TOML 配置中填写 codes，或确认 QMT sectors 接口可用。"
        )
    logger.info(
        "resolved QMT symbol universe task=%s sector=%s count=%s",
        args.task,
        sector_name,
        len(symbols),
    )
    return SyncArgs(
        **{
            **args.__dict__,
            "symbols_raw": ",".join(symbols),
            "sector_name": sector_name,
        }
    )


def expand_task_args(args: SyncArgs) -> list[SyncArgs]:
    spec = QMT_TASK_SPECS[args.task]
    if not spec.uses_symbols:
        return [args]

    symbols = parse_symbol_list(args.symbols_raw)
    if args.limit > 0:
        symbols = symbols[: args.limit]
    if not symbols:
        raise ValueError(f"QMT 任务 {args.task} 需要 codes 参数（会映射为 QMT REST symbols）。")
    if spec.task in {"download_history_batch"}:
        return [
            SyncArgs(
                **{
                    **args.__dict__,
                    "symbols_raw": ",".join(symbols),
                }
            )
        ]
    return [
        SyncArgs(
            **{
                **args.__dict__,
                "symbols_raw": symbol,
                "symbol": symbol,
                "stock_code": args.stock_code or symbol,
            }
        )
        for symbol in symbols
    ]


def run_single_request(
    args: SyncArgs,
    provider: QmtProvider,
    repository: QmtRepository,
    request_meta: dict[str, Any],
) -> int:
    spec = QMT_TASK_SPECS[args.task]
    scope_key = build_scope_key(args.task, _scope_meta(request_meta))
    run_date = date.today()
    target_table = f"{args.database}.{spec.table_name}"
    if not args.force and repository.has_successful_sync_today(args.task, scope_key, run_date):
        logger.info("skip task=%s scope=%s reason=already_success_today", args.task, scope_key)
        return 0
    if not args.force and request_identity_is_complete(args, request_meta) and repository.has_task_data_for_request(args.task, request_meta):
        logger.info("skip task=%s scope=%s reason=request_already_exists", args.task, scope_key)
        return 0

    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    try:
        envelope = provider.fetch_task(args.task, **build_fetch_kwargs(args, request_meta))
        inserted = repository.save_task_response(args.task, envelope, request_meta=request_meta)
        write_sync_result(
            repository=repository,
            task=args.task,
            scope_key=scope_key,
            target_table=target_table,
            request_meta=_sync_log_meta(request_meta),
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
            request_meta=_sync_log_meta(request_meta),
            row_count=0,
            message=str(exc),
            started_at=started_at,
            status="failed",
        )
        if args.continue_on_error:
            logger.warning("task=%s scope=%s failed: %s", args.task, scope_key, exc)
            return 0
        raise


def build_request_meta(args: SyncArgs) -> dict[str, Any]:
    spec = QMT_TASK_SPECS[args.task]
    symbols = parse_symbol_list(args.symbols_raw)
    symbol = normalize_qmt_code_list([args.symbol])[0] if args.symbol.strip() else (symbols[0] if len(symbols) == 1 else "")
    table_names = parse_csv(args.table_names_raw)
    return {
        "symbols": symbols,
        "symbol": symbol,
        "market": args.market.strip().upper(),
        "index_code": args.index_code.strip().upper(),
        "stock_code": normalize_qmt_code_list([args.stock_code])[0] if args.stock_code.strip() else "",
        "table_names": table_names,
        "table_name": ",".join(table_names),
        "sector_name": args.sector_name.strip(),
        "code_market": args.code_market.strip(),
        "start_time": normalize_qmt_time(args.begin_time) if args.begin_time else "",
        "end_time": normalize_qmt_time(args.end_time) if args.end_time else "",
        "period": args.period or spec.default_period,
        "fields": parse_csv(args.fields_raw),
        "adjust_type": args.adjust_type or spec.default_adjust_type,
        "fill_data": args.fill_data,
        "count": args.count if spec.uses_count else None,
        "incrementally": args.incrementally,
        "complete": args.complete,
    }


def build_fetch_kwargs(args: SyncArgs, request_meta: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbols": list(request_meta.get("symbols") or []),
        "symbol": request_meta.get("symbol") or None,
        "market": request_meta.get("market") or None,
        "index_code": request_meta.get("index_code") or None,
        "stock_code": request_meta.get("stock_code") or None,
        "table_names": list(request_meta.get("table_names") or []),
        "sector_name": request_meta.get("sector_name") or None,
        "code_market": request_meta.get("code_market") or None,
        "start_time": request_meta.get("start_time") or None,
        "end_time": request_meta.get("end_time") or None,
        "period": request_meta.get("period") or None,
        "fields": list(request_meta.get("fields") or []),
        "adjust_type": request_meta.get("adjust_type") or None,
        "fill_data": request_meta.get("fill_data"),
        "count": request_meta.get("count"),
        "incrementally": request_meta.get("incrementally"),
        "complete": request_meta.get("complete"),
    }


def resolve_effective_request_meta(
    args: SyncArgs,
    repository: QmtRepository,
    request_meta: dict[str, Any],
) -> dict[str, Any] | None:
    spec = QMT_TASK_SPECS[args.task]
    if not spec.supports_incremental:
        return request_meta

    effective = dict(request_meta)
    end_value = normalize_qmt_time(effective.get("end_time") or (_default_qmt_end_time(args) if effective.get("start_time") else ""))
    start_value = normalize_qmt_time(effective.get("start_time"))
    latest_cursor = None
    if not args.force:
        latest_cursor = repository.load_latest_cursor(args.task, symbol=str(effective.get("symbol") or "") or None)
    if latest_cursor and spec.cursor_path != ("time_ms",):
        next_value = advance_cursor_value(latest_cursor, spec.cursor_granularity)
        if not start_value or _compare_qmt_time_values(next_value, start_value) > 0:
            start_value = next_value

    if start_value and end_value and _compare_qmt_time_values(start_value, end_value) > 0:
        return None

    effective["start_time"] = start_value
    effective["end_time"] = end_value
    return effective


def validate_required_request(args: SyncArgs, request_meta: dict[str, Any]) -> None:
    if request_identity_is_complete(args, request_meta):
        return
    spec = QMT_TASK_SPECS[args.task]
    missing: list[str] = []
    if spec.uses_symbols and not request_meta.get("symbols"):
        missing.append("symbols")
    if spec.uses_symbol and not request_meta.get("symbol"):
        missing.append("symbol")
    if spec.uses_market and not request_meta.get("market"):
        missing.append("market")
    if spec.uses_index_code and not request_meta.get("index_code") and args.task != "download_index_weight":
        missing.append("index_code")
    if spec.uses_stock_code and not request_meta.get("stock_code"):
        missing.append("stock_code")
    if spec.uses_table_names and not request_meta.get("table_names"):
        missing.append("table_names")
    if spec.uses_code_market and not request_meta.get("code_market"):
        missing.append("code_market")
    raise ValueError(f"QMT 任务 {args.task} 缺少必填参数: {', '.join(missing)}")


def request_identity_is_complete(args: SyncArgs, request_meta: dict[str, Any]) -> bool:
    spec = QMT_TASK_SPECS[args.task]
    if spec.uses_symbols and not request_meta.get("symbols"):
        return False
    if spec.uses_symbol and not request_meta.get("symbol"):
        return False
    if spec.uses_market and not request_meta.get("market"):
        return False
    if spec.uses_index_code and not request_meta.get("index_code") and args.task != "download_index_weight":
        return False
    if spec.uses_stock_code and not request_meta.get("stock_code"):
        return False
    if spec.uses_table_names and not request_meta.get("table_names"):
        return False
    if spec.uses_code_market and not request_meta.get("code_market"):
        return False
    return True


CONFIG_TOP_LEVEL_KEYS = frozenset(
    {"source", "runtime_path", "log_level", "continue_on_error", "database", "defaults", "tasks"}
)
CONFIG_DEFAULT_KEYS = frozenset(
    {
        "codes",
        "symbol",
        "market",
        "index_code",
        "stock_code",
        "table_names",
        "sector_name",
        "code_market",
        "begin_time",
        "end_time",
        "begin_date",
        "end_date",
        "period",
        "fields",
        "adjust_type",
        "fill_data",
        "count",
        "incrementally",
        "complete",
        "limit",
        "force",
        "continue_on_error",
    }
)
CONFIG_TASK_KEYS = frozenset({"task", "enabled"} | CONFIG_DEFAULT_KEYS)


def load_execution_plan_from_toml(path: str, *, log_level_override: str | None = None) -> QmtExecutionPlan:
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

    source = str(data.get("source") or "qmt").strip() or "qmt"
    if source != "qmt":
        raise ValueError(f"QMT 配置文件 source 必须是 'qmt'，当前值: {source!r}")

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
        if task_name not in QMT_TASK_CHOICES:
            raise ValueError(f"tasks[{index}].task 非法: {task_name!r}")

        task_specs.append(
            SyncArgs(
                task=task_name,
                symbols_raw=_normalize_config_list(merged.get("codes"), field_name=f"tasks[{index}].codes"),
                symbol=str(merged.get("symbol") or "").strip(),
                market=str(merged.get("market") or "").strip(),
                index_code=str(merged.get("index_code") or "").strip(),
                stock_code=str(merged.get("stock_code") or "").strip(),
                table_names_raw=_normalize_config_list(merged.get("table_names"), field_name=f"tasks[{index}].table_names"),
                sector_name=str(merged.get("sector_name") or "").strip(),
                code_market=str(merged.get("code_market") or "").strip(),
                begin_time=str(merged.get("begin_time") or merged.get("begin_date") or "").strip(),
                end_time=str(merged.get("end_time") or merged.get("end_date") or "").strip(),
                period=str(merged.get("period") or "").strip(),
                fields_raw=_normalize_config_list(merged.get("fields"), field_name=f"tasks[{index}].fields"),
                adjust_type=str(merged.get("adjust_type") or "none").strip() or "none",
                fill_data=_as_bool(merged.get("fill_data", True), field_name=f"tasks[{index}].fill_data"),
                count=_as_int(merged.get("count", -1), field_name=f"tasks[{index}].count"),
                incrementally=_as_bool(merged.get("incrementally", False), field_name=f"tasks[{index}].incrementally"),
                complete=_as_bool(merged.get("complete", False), field_name=f"tasks[{index}].complete"),
                limit=_as_non_negative_int(merged.get("limit", 0), field_name=f"tasks[{index}].limit"),
                force=_as_bool(merged.get("force", False), field_name=f"tasks[{index}].force"),
                continue_on_error=_as_bool(merged.get("continue_on_error", False), field_name=f"tasks[{index}].continue_on_error"),
                runtime_path=str(data.get("runtime_path") or "").strip() or None,
                database=str(data.get("database") or "qmt").strip() or "qmt",
                log_level=str(log_level_override or data.get("log_level") or "INFO").strip() or "INFO",
            )
        )

    if not task_specs:
        raise ValueError("配置文件中的 [[tasks]] 全部被禁用，无法执行。")

    return QmtExecutionPlan(
        runtime_path=str(data.get("runtime_path") or "").strip() or None,
        log_level=str(log_level_override or data.get("log_level") or "INFO").strip() or "INFO",
        continue_on_error=_as_bool(data.get("continue_on_error", False), field_name="continue_on_error"),
        database=str(data.get("database") or "qmt").strip() or "qmt",
        tasks=tuple(task_specs),
    )


def parse_symbol_list(value: str) -> list[str]:
    return normalize_qmt_code_list(parse_csv(value))


def parse_csv(value: str) -> list[str]:
    return [part.strip() for part in str(value or "").split(",") if part.strip()]


def normalize_qmt_time(value: Any) -> str:
    text = "".join(ch for ch in str(value or "").strip() if ch.isdigit())
    if not text:
        return ""
    if len(text) not in {4, 6, 8, 14}:
        raise ValueError(f"QMT 时间必须是 YYYY / YYYYMM / YYYYMMDD / YYYYMMDDHHMMSS，当前值: {value!r}")
    return text


def _default_qmt_end_time(args: SyncArgs) -> str:
    if str(args.begin_time or "").strip():
        return default_request_end("day")
    return ""


def _scope_meta(request_meta: dict[str, Any]) -> dict[str, str | int | None]:
    return {
        "code": request_meta.get("symbol") or request_meta.get("stock_code") or request_meta.get("index_code") or request_meta.get("market") or request_meta.get("sector_name"),
        "start_date": request_meta.get("start_time"),
        "end_date": request_meta.get("end_time"),
        "day": "",
        "year": "",
        "quarter": "",
        "year_type": request_meta.get("period") or request_meta.get("table_name"),
    }


def _sync_log_meta(request_meta: dict[str, Any]) -> dict[str, str | int | None]:
    start_time = str(request_meta.get("start_time") or "")
    end_time = str(request_meta.get("end_time") or "")
    return {
        "start_date": start_time[:8],
        "end_date": end_time[:8],
        "day": "",
        "year": "",
        "quarter": "",
        "year_type": "",
    }


def _normalize_config_list(value: Any, field_name: str) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (list, tuple)):
        items = [str(item).strip() for item in value if str(item).strip()]
        return ",".join(items)
    raise ValueError(f"{field_name} 必须是字符串或字符串数组。")


def _as_int(value: Any, field_name: str) -> int:
    try:
        return int(value)
    except Exception as exc:
        raise ValueError(f"{field_name} 必须是整数。") from exc


def _as_non_negative_int(value: Any, field_name: str) -> int:
    result = _as_int(value, field_name)
    if result < 0:
        raise ValueError(f"{field_name} 不能小于 0。")
    return result


def _as_bool(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ValueError(f"{field_name} 必须是布尔值。")


def _compare_qmt_time_values(left: str, right: str) -> int:
    left_text = str(left or "")
    right_text = str(right or "")
    if len(left_text) == len(right_text):
        return compare_cursor_values(left_text, right_text)
    left_day = left_text[:8]
    right_day = right_text[:8]
    if left_day and right_day and left_day != right_day:
        return compare_cursor_values(left_day, right_day)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
