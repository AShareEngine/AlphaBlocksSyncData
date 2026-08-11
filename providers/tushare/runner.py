#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tushare -> ClickHouse sync runner."""

from __future__ import annotations

import argparse
import itertools
import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from sync_data_system.config_paths import resolve_config_candidate
from sync_data_system.providers.tushare.provider import (
    TushareAPIError,
    TushareConfig,
    TushareProvider,
    TushareRequestBudgetExceeded,
    normalize_tushare_date,
)
from sync_data_system.providers.tushare.repository import TushareRepository
from sync_data_system.providers.tushare.specs import (
    TUSHARE_TASK_CHOICES,
    TUSHARE_TASK_SPECS,
    TushareTaskSpec,
)
from sync_data_system.sync_core.clickhouse import ClickHouseConfig, create_clickhouse_client
from sync_data_system.sync_core.task_logging import write_sync_result
from sync_data_system.toml_compat import tomllib


logger = logging.getLogger(__name__)
DATE_PARAMS = frozenset(
    {
        "trade_date",
        "cal_date",
        "ann_date",
        "end_date",
        "start_date",
        "date",
        "month",
        "quarter",
        "period",
        "time",
        "trade_time",
        "datetime",
    }
)


@dataclass(frozen=True)
class UniverseSourceDefinition:
    task: str
    param_variants: tuple[dict[str, Any], ...]
    historical: bool = False
    begin_date: str = ""


UNIVERSE_DEFINITIONS: dict[str, tuple[UniverseSourceDefinition, ...]] = {
    "股票数据": (
        UniverseSourceDefinition(
            "stock_basic",
            tuple({"list_status": status} for status in ("L", "D", "P", "G")),
        ),
        # bak_basic is the daily historical security list. Once populated, its
        # distinct codes are the primary historical A-share universe; the
        # current stock_basic table only complements newly listed securities.
        UniverseSourceDefinition(
            "bak_basic",
            ({},),
            historical=True,
            begin_date="20160101",
        ),
    ),
    "ETF专题": (
        UniverseSourceDefinition(
            "etf_basic",
            tuple({"list_status": status} for status in ("L", "D", "P")),
        ),
    ),
    "公募基金": (
        UniverseSourceDefinition(
            "fund_basic",
            tuple({"status": status} for status in ("L", "D", "I")),
        ),
    ),
    "指数专题": (
        UniverseSourceDefinition(
            "index_basic",
            tuple(
                {"market": market}
                for market in ("MSCI", "CSI", "SSE", "SZSE", "CICC", "SW", "OTH")
            ),
        ),
    ),
    "期货数据": (
        UniverseSourceDefinition(
            "fut_basic",
            tuple(
                {"exchange": exchange}
                for exchange in ("CFFEX", "DCE", "CZCE", "SHFE", "INE", "GFEX")
            ),
        ),
    ),
    "期权数据": (UniverseSourceDefinition("opt_basic", ({},)),),
    "债券专题": (UniverseSourceDefinition("cb_basic", ({},)),),
    "外汇数据": (UniverseSourceDefinition("fx_obasic", ({},)),),
    "港股数据": (
        UniverseSourceDefinition(
            "hk_basic",
            tuple({"list_status": status} for status in ("L", "D", "P")),
        ),
    ),
    "美股数据": (UniverseSourceDefinition("us_basic", ({},)),),
    "现货数据": (UniverseSourceDefinition("sge_basic", ({},)),),
}
TASK_UNIVERSE_DEFINITIONS: dict[str, tuple[UniverseSourceDefinition, ...]] = {
    # ths_member requires either a THS index code or a constituent code even
    # though the official parameter table marks both fields as optional. Use
    # the matching THS index list instead of the broad A-share stock universe.
    "ths_member": (UniverseSourceDefinition("ths_index", ({},)),),
}
TASK_DEFAULT_PARAMS: dict[str, dict[str, Any]] = {
    # These dimensions are required by the upstream APIs. Plans already carry
    # the same values, but registered/UI tasks do not have task-specific params.
    "fut_basic": {
        "exchange": ["CFFEX", "DCE", "CZCE", "SHFE", "INE", "GFEX"],
    },
    "fut_weekly_monthly": {"freq": ["week", "month"]},
    "stk_week_month_adj": {"freq": ["week", "month"]},
    "stk_weekly_monthly": {"freq": ["week", "month"]},
    "stock_hsgt": {"type": ["HK_SZ", "SZ_HK", "HK_SH", "SH_HK"]},
    "dc_index": {"idx_type": ["行业板块", "概念板块", "地域板块"]},
}
TASK_CURSOR_REQUEST_FIELDS = {
    # cn_pmi documents `month` as the output cursor but accepts `m` as the
    # single-month request parameter. Compatible gateways may omit month from
    # the response, so the request dimension also supplies the business key.
    "cn_pmi": "m",
}
MULTI_CODE_SNAPSHOT_BATCH_SIZES = {
    # The API explicitly accepts comma-separated convertible-bond codes. Keep
    # batches conservative because a response is capped at 2,000 event rows.
    "cb_price_chg": 20,
}
MULTI_CODE_SNAPSHOT_ROW_LIMITS = {
    "cb_price_chg": 2000,
}


@dataclass(frozen=True)
class SyncArgs:
    task: str
    codes_raw: str = ""
    begin_date: str = ""
    end_date: str = ""
    fields: str = ""
    params: Mapping[str, Any] = field(default_factory=dict)
    limit: int = 0
    page_size: int = 0
    max_pages: int = 0
    window_days: int = 0
    force: bool = False
    resume: bool = False
    continue_on_error: bool = False
    runtime_path: str | None = None
    database: str = "tushare"
    log_level: str = "INFO"


@dataclass(frozen=True)
class TushareExecutionPlan:
    runtime_path: str | None
    log_level: str
    continue_on_error: bool
    database: str
    tasks: tuple[SyncArgs, ...]


@dataclass
class TushareExecutionContext:
    provider: TushareProvider
    repository: TushareRepository
    connection: Any
    universe_cache: dict[str, list[str]] = field(default_factory=dict)

    def close(self) -> None:
        try:
            self.connection.close()
        except Exception:
            pass
        self.provider.close()


def parse_args() -> SyncArgs:
    parser = argparse.ArgumentParser(description="Tushare Pro 通用同步入口")
    parser.add_argument("task", choices=TUSHARE_TASK_CHOICES)
    parser.add_argument("--codes", default="")
    parser.add_argument("--begin-date", default="")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--fields", default="")
    parser.add_argument("--params", default="{}", help="额外 API 参数 JSON object")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--page-size", type=int, default=0)
    parser.add_argument("--max-pages", type=int, default=0)
    parser.add_argument("--window-days", type=int, default=0)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--runtime-path", default=None)
    parser.add_argument("--database", default="tushare")
    parser.add_argument("--log-level", default="INFO")
    raw = parser.parse_args()
    return SyncArgs(
        task=raw.task,
        codes_raw=str(raw.codes or "").strip(),
        begin_date=str(raw.begin_date or "").strip(),
        end_date=str(raw.end_date or "").strip(),
        fields=str(raw.fields or "").strip(),
        params=_parse_json_params(raw.params),
        limit=max(0, int(raw.limit or 0)),
        page_size=max(0, int(raw.page_size or 0)),
        max_pages=max(0, int(raw.max_pages or 0)),
        window_days=max(0, int(raw.window_days or 0)),
        force=bool(raw.force),
        resume=bool(raw.resume),
        continue_on_error=bool(raw.continue_on_error),
        runtime_path=raw.runtime_path,
        database=str(raw.database or "tushare").strip() or "tushare",
        log_level=str(raw.log_level or "INFO").strip() or "INFO",
    )


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    context = build_context(args.runtime_path, args.database)
    try:
        run_sync_args(args, context.provider, context.repository, context=context)
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
    failures: list[str] = []
    try:
        for index, args in enumerate(plan.tasks, start=1):
            logger.info("batch task start progress=%s/%s task=%s", index, len(plan.tasks), args.task)
            try:
                run_sync_args(args, context.provider, context.repository, context=context)
            except TushareRequestBudgetExceeded:
                logger.warning(
                    "Tushare request budget exhausted after %s requests; "
                    "persisted rows are resumable on the next run.",
                    context.provider.request_count,
                )
                break
            except Exception:
                failures.append(args.task)
                logger.exception("batch task failed progress=%s/%s task=%s", index, len(plan.tasks), args.task)
                if not plan.continue_on_error:
                    raise
        return 1 if failures else 0
    finally:
        context.close()


def build_context(
    runtime_path: str | None = None,
    database: str = "tushare",
) -> TushareExecutionContext:
    clickhouse_config = ClickHouseConfig.from_env(runtime_path=runtime_path)
    provider = TushareProvider(TushareConfig.from_env(runtime_path=runtime_path))
    connection = create_clickhouse_client(clickhouse_config)
    repository = TushareRepository(connection, database=database)
    repository.ensure_tables()
    return TushareExecutionContext(provider=provider, repository=repository, connection=connection)


def run_registered_task(probe: Any) -> int:
    args = SyncArgs(
        task=_provider_task_name(probe.name, probe.source),
        codes_raw=",".join(probe.input_codes),
        begin_date="" if probe.input_begin_date is None else str(probe.input_begin_date),
        end_date="" if probe.input_end_date is None else str(probe.input_end_date),
        fields=str(probe.input_fields or ""),
        params=dict(getattr(probe, "input_params", None) or {}),
        limit=max(0, int(probe.limit or 0)),
        force=bool(probe.force),
        resume=bool(probe.resume),
        continue_on_error=bool(probe.continue_on_error),
        runtime_path=probe.runtime_path,
        database=str(probe.database or "tushare"),
        log_level=str(probe.log_level or "INFO"),
    )
    inserted = run_sync_args(
        args,
        probe.context.provider,
        probe.context.repository,
        context=probe.context,
    )
    probe.set_row_count(inserted)
    return inserted


def run_sync_args(
    args: SyncArgs,
    provider: TushareProvider,
    repository: TushareRepository,
    *,
    context: TushareExecutionContext | None = None,
) -> int:
    spec = TUSHARE_TASK_SPECS[args.task]
    if spec.stopped and not args.force:
        logger.warning("skip stopped Tushare API task=%s doc=%s; use force=true to run", spec.task, spec.doc_url)
        return 0
    repository.ensure_task_table(spec)
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    total = 0
    scope_key = _task_scope_key(args)
    request_meta = {
        "start_date": normalize_tushare_date(args.begin_date) if args.begin_date else None,
        "end_date": normalize_tushare_date(args.end_date) if args.end_date else None,
    }
    try:
        total = _execute_task(args, spec, provider, repository, context=context)
        _invalidate_universe_cache(context, spec.task)
    except Exception as exc:
        write_sync_result(
            repository=repository,
            task=spec.task,
            scope_key=scope_key,
            target_table=spec.table_name,
            request_meta=request_meta,
            row_count=total,
            message=str(exc),
            started_at=started_at,
            status="failed",
        )
        raise
    write_sync_result(
        repository=repository,
        task=spec.task,
        scope_key=scope_key,
        target_table=spec.table_name,
        request_meta=request_meta,
        row_count=total,
        message=None,
        started_at=started_at,
        status="success",
    )
    logger.info(
        "Tushare sync finished task=%s rows=%s api_requests=%s",
        spec.task,
        total,
        provider.request_count,
    )
    return total


def _execute_task(
    args: SyncArgs,
    spec: TushareTaskSpec,
    provider: TushareProvider,
    repository: TushareRepository,
    *,
    context: TushareExecutionContext | None,
) -> int:
    if spec.request_mode == "code_range":
        return _run_code_range(args, spec, provider, repository, context=context)
    if spec.request_mode == "date_range":
        return _run_date_range(args, spec, provider, repository)
    if spec.request_mode == "date_slice":
        return _run_date_slice(args, spec, provider, repository)
    return _run_snapshot(args, spec, provider, repository, context=context)


def _run_code_range(
    args: SyncArgs,
    spec: TushareTaskSpec,
    provider: TushareProvider,
    repository: TushareRepository,
    *,
    context: TushareExecutionContext | None,
) -> int:
    codes = _normalize_codes(args.codes_raw)
    if not codes:
        codes = _resolve_universe(
            spec,
            provider,
            repository=repository,
            context=context,
        )
    if args.limit > 0:
        codes = codes[: args.limit]
    if not codes:
        if spec.code_field not in spec.required_input_names:
            logger.warning(
                "Tushare task=%s code universe is empty; falling back to a global date request",
                spec.task,
            )
            if (
                spec.cursor_field
                and spec.cursor_field in spec.input_names
                and spec.cursor_field not in {"start_date", "end_date"}
            ):
                return _run_date_slice(args, spec, provider, repository)
            return _run_date_range(args, spec, provider, repository)
        raise ValueError(
            f"Tushare task={spec.task} 无可用代码池；请通过 codes 显式传入。文档：{spec.doc_url}"
        )

    base_begin, end_date = _request_window(args, provider.config)
    latest = {} if args.force else repository.load_latest_cursors(spec, codes)
    total = 0
    failures: list[str] = []
    for index, code in enumerate(codes, start=1):
        begin_date = base_begin
        if latest.get(code):
            cursor_date = _cursor_to_date(latest[code])
            if cursor_date and cursor_date > end_date:
                logger.info(
                    "skip Tushare code task=%s code=%s reason=cursor_after_requested_end",
                    spec.task,
                    code,
                )
                continue
            if cursor_date and cursor_date > begin_date:
                begin_date = cursor_date
        logger.info(
            "Tushare code progress=%s/%s task=%s code=%s begin=%s end=%s",
            index,
            len(codes),
            spec.task,
            code,
            begin_date,
            end_date,
        )
        try:
            for params in _task_param_variants(spec, args.params):
                if spec.code_field:
                    params[spec.code_field] = code
                if "start_date" in spec.input_names:
                    params["start_date"] = begin_date
                if "end_date" in spec.input_names:
                    params["end_date"] = end_date
                _validate_required_params(spec, params, date_range_filled=True)
                for window_params in _date_window_params(params, args.window_days):
                    rows = _fetch_rows(args, spec, provider, window_params)
                    total += repository.save_rows(
                        spec,
                        rows,
                        scope_key=_params_scope_key(
                            spec.task,
                            {**window_params, spec.code_field or "code": code},
                        ),
                    )
        except TushareRequestBudgetExceeded:
            raise
        except TushareAPIError:
            # Permission and parameter errors apply to the whole API, not one
            # security. Continuing would burn the quota on every code.
            raise
        except Exception:
            failures.append(code)
            logger.exception("Tushare code failed task=%s code=%s", spec.task, code)
            if not args.continue_on_error:
                raise
    if failures:
        raise RuntimeError(
            f"Tushare task={spec.task} failed codes={len(failures)} samples={failures[:10]}"
        )
    return total


def _run_date_range(
    args: SyncArgs,
    spec: TushareTaskSpec,
    provider: TushareProvider,
    repository: TushareRepository,
) -> int:
    begin_date, end_date = _request_window(args, provider.config)
    if not args.force:
        latest = repository.load_latest_cursor(spec)
        cursor_date = _cursor_to_date(latest or "")
        if cursor_date and cursor_date > end_date:
            return 0
        if cursor_date and cursor_date > begin_date:
            begin_date = cursor_date
    total = 0
    for params in _task_param_variants(spec, args.params):
        if "start_date" in spec.input_names:
            params["start_date"] = _format_range_boundary(spec, "start_date", begin_date)
        if "end_date" in spec.input_names:
            params["end_date"] = _format_range_boundary(spec, "end_date", end_date)
        _validate_required_params(spec, params, date_range_filled=True)
        for window_params in _date_window_params(params, args.window_days):
            rows = _fetch_rows(args, spec, provider, window_params)
            total += repository.save_rows(spec, rows, scope_key=_params_scope_key(spec.task, params))
    return total


def _run_date_slice(
    args: SyncArgs,
    spec: TushareTaskSpec,
    provider: TushareProvider,
    repository: TushareRepository,
) -> int:
    begin_date, end_date = _request_window(args, provider.config)
    if not args.force:
        latest = repository.load_latest_cursor(spec)
        cursor_date = _cursor_to_date(latest or "")
        if cursor_date and cursor_date > end_date:
            return 0
        if cursor_date and cursor_date > begin_date:
            begin_date = cursor_date
    total = 0
    for cursor_value in _cursor_values(spec.cursor_field, begin_date, end_date):
        for params in _task_param_variants(spec, args.params):
            cursor_request_field = TASK_CURSOR_REQUEST_FIELDS.get(
                spec.task,
                spec.cursor_field,
            )
            params[cursor_request_field] = cursor_value
            _validate_required_params(spec, params)
            rows = _fetch_rows(args, spec, provider, params)
            total += repository.save_rows(
                spec,
                rows,
                scope_key=f"task={spec.task}|{spec.cursor_field}={cursor_value}",
            )
    return total


def _run_snapshot(
    args: SyncArgs,
    spec: TushareTaskSpec,
    provider: TushareProvider,
    repository: TushareRepository,
    *,
    context: TushareExecutionContext | None,
) -> int:
    params_variants = _task_param_variants(spec, args.params)
    required_code = bool(spec.code_field and spec.code_field in spec.required_input_names)
    task_universe = spec.task in TASK_UNIVERSE_DEFINITIONS
    needs_code_universe = bool(spec.code_field and (required_code or task_universe))
    if needs_code_universe and not any(spec.code_field in params for params in params_variants):
        codes = _normalize_codes(args.codes_raw) or _resolve_universe(
            spec,
            provider,
            repository=repository,
            context=context,
        )
        if args.limit > 0:
            codes = codes[: args.limit]
        if not codes:
            source_tables = ", ".join(
                TUSHARE_TASK_SPECS[source.task].table_name
                for source in TASK_UNIVERSE_DEFINITIONS.get(spec.task, ())
            )
            source_hint = f"；请先同步基础表 {source_tables}" if source_tables else ""
            raise ValueError(
                f"Tushare task={spec.task} 无可用代码池{source_hint}。"
            )
        batch_size = MULTI_CODE_SNAPSHOT_BATCH_SIZES.get(spec.task, 1)
        code_batches = [
            codes[index : index + batch_size]
            for index in range(0, len(codes), batch_size)
        ]
        params_variants = [
            {**params, spec.code_field: ",".join(code_batch)}
            for params in params_variants
            for code_batch in code_batches
        ]
        logger.info(
            "Tushare snapshot code batches task=%s codes=%s batches=%s batch_size=%s",
            spec.task,
            len(codes),
            len(code_batches),
            batch_size,
        )
    total = 0
    for index, params in enumerate(params_variants, start=1):
        _validate_required_params(spec, params)
        rows = _fetch_rows(args, spec, provider, params)
        code_batch = _normalize_codes(str(params.get(spec.code_field) or ""))
        row_limit = MULTI_CODE_SNAPSHOT_ROW_LIMITS.get(spec.task, 0)
        if len(code_batch) > 1 and row_limit > 0 and len(rows) >= row_limit:
            logger.warning(
                "Tushare snapshot batch reached row limit; retrying per code "
                "task=%s progress=%s/%s codes=%s rows=%s limit=%s",
                spec.task,
                index,
                len(params_variants),
                len(code_batch),
                len(rows),
                row_limit,
            )
            for code in code_batch:
                single_params = {**params, spec.code_field: code}
                single_rows = _fetch_rows(args, spec, provider, single_params)
                total += repository.save_rows(
                    spec,
                    single_rows,
                    scope_key=_params_scope_key(spec.task, single_params),
                )
            continue
        total += repository.save_rows(
            spec,
            rows,
            scope_key=_params_scope_key(spec.task, params),
        )
    return total


def _fetch_rows(
    args: SyncArgs,
    spec: TushareTaskSpec,
    provider: TushareProvider,
    params: Mapping[str, Any],
) -> list[dict[str, Any]]:
    fields = list(spec.output_provider_names)
    for requested_field in args.fields.split(","):
        requested_field = requested_field.strip()
        provider_field = spec.provider_field_name(requested_field)
        if provider_field and provider_field not in fields:
            fields.append(provider_field)
    for required_field in (spec.code_field, spec.cursor_field):
        if required_field and required_field in spec.output_names:
            provider_field = spec.provider_field_name(required_field)
            if provider_field not in fields:
                fields.append(provider_field)
    rows = provider.query_all(
        spec.task,
        params=params,
        fields=fields,
        supports_pagination=spec.supports_pagination,
        page_size=args.page_size,
        max_pages=args.max_pages,
    )
    normalized_rows = [spec.normalize_output_row(row) for row in rows]
    for row in normalized_rows:
        for field in spec.business_key_fields:
            if field in row:
                continue
            request_field = (
                TASK_CURSOR_REQUEST_FIELDS.get(spec.task, field)
                if field == spec.cursor_field
                else field
            )
            if request_field in params:
                row[field] = params[request_field]
                continue
            if field in spec.business_key_defaults:
                row[field] = spec.business_key_defaults[field]
                continue
            raise ValueError(
                f"Tushare task={spec.task} 返回数据缺少业务键字段 {field!r}，"
                f"且请求参数未提供该维度；实际返回字段={sorted(row)}。"
            )
    return normalized_rows


def _resolve_universe(
    spec: TushareTaskSpec,
    provider: TushareProvider,
    *,
    repository: TushareRepository,
    context: TushareExecutionContext | None,
) -> list[str]:
    sources = TASK_UNIVERSE_DEFINITIONS.get(spec.task)
    universe_scope = f"task:{spec.task}"
    if sources is None:
        sources = UNIVERSE_DEFINITIONS.get(spec.category_root)
        universe_scope = f"category:{spec.category_root}"
    if sources is None:
        return []
    cache_key = _universe_cache_key(universe_scope, sources)
    if context is not None and cache_key in context.universe_cache:
        cached_codes = list(context.universe_cache[cache_key])
        if cached_codes:
            return cached_codes

    local_loader = getattr(repository, "load_universe_codes", None)
    source_codes: dict[str, list[str]] = {}
    sync_errors: dict[str, Exception] = {}
    for source in sources:
        source_spec = TUSHARE_TASK_SPECS[source.task]
        codes = local_loader(source_spec) if callable(local_loader) else []
        if not codes:
            logger.info(
                "Tushare local universe is empty; synchronizing source first "
                "category=%s source=%s table=%s historical=%s",
                spec.category_root,
                source.task,
                source_spec.table_name,
                source.historical,
            )
            try:
                _sync_universe_source(
                    source,
                    provider=provider,
                    repository=repository,
                    context=context,
                )
            except Exception as exc:
                sync_errors[source.task] = exc
                logger.warning(
                    "Tushare universe source sync failed; re-reading any rows already persisted "
                    "category=%s source=%s table=%s",
                    spec.category_root,
                    source.task,
                    source_spec.table_name,
                    exc_info=True,
                )
            codes = local_loader(source_spec) if callable(local_loader) else []
        source_codes[source.task] = sorted(set(_normalize_codes(codes)))

    historical_codes = {
        code
        for source in sources
        if source.historical
        for code in source_codes[source.task]
    }
    current_codes = {
        code
        for source in sources
        if not source.historical
        for code in source_codes[source.task]
    }
    # Historical lists are authoritative for previously listed/delisted
    # securities. Current basic tables only complement codes not yet present in
    # the historical list, such as a newly listed security before the next
    # historical-list refresh.
    codes = sorted(historical_codes | current_codes)
    if (
        not codes
        and sync_errors
        and (
            spec.code_field in spec.required_input_names
            or spec.task in TASK_UNIVERSE_DEFINITIONS
        )
    ):
        source_tables = ", ".join(
            TUSHARE_TASK_SPECS[source.task].table_name for source in sources
        )
        failed_sources = ", ".join(sorted(sync_errors))
        raise ValueError(
            f"Tushare task={spec.task} 本地类目代码池为空；基础/历史列表同步失败 "
            f"sources={failed_sources} tables={source_tables}。请先同步这些基础表。"
        ) from next(iter(sync_errors.values()))
    if not codes and sync_errors:
        logger.warning(
            "Tushare optional code universe remains empty after source sync failures; "
            "task=%s failed_sources=%s",
            spec.task,
            ",".join(sorted(sync_errors)),
        )
    if context is not None:
        context.universe_cache[cache_key] = list(codes)
    logger.info(
        "Tushare local universe resolved category=%s sources=%s "
        "historical_codes=%s current_codes=%s codes=%s",
        spec.category_root,
        ",".join(source.task for source in sources),
        len(historical_codes),
        len(current_codes),
        len(codes),
    )
    return codes


def _universe_cache_key(
    scope: str,
    sources: Sequence[UniverseSourceDefinition],
) -> str:
    return f"{scope}:{','.join(source.task for source in sources)}"


def _invalidate_universe_cache(
    context: TushareExecutionContext | None,
    completed_task: str,
) -> None:
    if context is None:
        return
    for category, sources in UNIVERSE_DEFINITIONS.items():
        if any(source.task == completed_task for source in sources):
            context.universe_cache.pop(
                _universe_cache_key(f"category:{category}", sources),
                None,
            )
    for task, sources in TASK_UNIVERSE_DEFINITIONS.items():
        if any(source.task == completed_task for source in sources):
            context.universe_cache.pop(
                _universe_cache_key(f"task:{task}", sources),
                None,
            )


def _sync_universe_source(
    source: UniverseSourceDefinition,
    *,
    provider: TushareProvider,
    repository: TushareRepository,
    context: TushareExecutionContext | None,
) -> int:
    source_spec = TUSHARE_TASK_SPECS[source.task]
    ensure_table = getattr(repository, "ensure_task_table", None)
    if callable(ensure_table):
        ensure_table(source_spec)
    total = 0
    for params in source.param_variants:
        source_args = SyncArgs(
            task=source.task,
            begin_date=source.begin_date,
            end_date=date.today().strftime("%Y%m%d") if source.begin_date else "",
            params=params,
            resume=True,
            database=getattr(repository, "database", "tushare"),
        )
        total += _execute_task(
            source_args,
            source_spec,
            provider,
            repository,
            context=context,
        )
    logger.info(
        "Tushare universe source synchronized source=%s table=%s historical=%s rows=%s",
        source.task,
        source_spec.table_name,
        source.historical,
        total,
    )
    return total


def _request_window(args: SyncArgs, config: TushareConfig) -> tuple[str, str]:
    begin_date = normalize_tushare_date(args.begin_date or config.default_start_date)
    end_date = normalize_tushare_date(args.end_date or date.today().strftime("%Y%m%d"))
    if begin_date > end_date:
        raise ValueError(f"begin_date {begin_date} 晚于 end_date {end_date}")
    return begin_date, end_date


def _date_window_params(
    params: Mapping[str, Any],
    window_days: int,
) -> list[dict[str, Any]]:
    if window_days <= 0 or "start_date" not in params or "end_date" not in params:
        return [dict(params)]
    start = datetime.strptime(_cursor_to_date(str(params["start_date"])), "%Y%m%d").date()
    end = datetime.strptime(_cursor_to_date(str(params["end_date"])), "%Y%m%d").date()
    result: list[dict[str, Any]] = []
    cursor = start
    while cursor <= end:
        window_end = min(end, cursor + timedelta(days=window_days - 1))
        result.append(
            {
                **params,
                "start_date": _replace_date_prefix(str(params["start_date"]), cursor),
                "end_date": _replace_date_prefix(str(params["end_date"]), window_end),
            }
        )
        cursor = window_end + timedelta(days=1)
    return result


def _format_range_boundary(spec: TushareTaskSpec, field_name: str, value: str) -> str:
    field = next((item for item in spec.input_fields if item.name == field_name), None)
    if field is not None and "datetime" in field.data_type.lower():
        parsed = datetime.strptime(value, "%Y%m%d")
        suffix = "00:00:00" if field_name == "start_date" else "23:59:59"
        return f"{parsed:%Y-%m-%d} {suffix}"
    return value


def _replace_date_prefix(original: str, value: date) -> str:
    if ":" in original:
        suffix = original.split(" ", 1)[1] if " " in original else "00:00:00"
        return f"{value:%Y-%m-%d} {suffix}"
    return value.strftime("%Y%m%d")


def _cursor_values(cursor_field: str, begin_date: str, end_date: str) -> list[str]:
    start = datetime.strptime(begin_date, "%Y%m%d").date()
    end = datetime.strptime(end_date, "%Y%m%d").date()
    if cursor_field == "month":
        values: list[str] = []
        year, month = start.year, start.month
        while (year, month) <= (end.year, end.month):
            values.append(f"{year:04d}{month:02d}")
            month += 1
            if month > 12:
                year += 1
                month = 1
        return values
    values = []
    current = start
    while current <= end:
        if cursor_field != "trade_date" or current.weekday() < 5:
            values.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return values


def _validate_required_params(
    spec: TushareTaskSpec,
    params: Mapping[str, Any],
    *,
    date_range_filled: bool = False,
) -> None:
    missing: list[str] = []
    for field_name in spec.required_input_names:
        if field_name in params and str(params[field_name]).strip():
            continue
        if date_range_filled and field_name in DATE_PARAMS:
            continue
        missing.append(field_name)
    if missing:
        raise ValueError(
            f"Tushare task={spec.task} 缺少必填参数 {missing}；"
            f"请在任务 params 中提供。文档：{spec.doc_url}"
        )


def _expand_params(params: Mapping[str, Any]) -> list[dict[str, Any]]:
    keys = list(params)
    value_sets = [
        list(value) if isinstance(value, (list, tuple)) else [value]
        for value in params.values()
    ]
    if not keys:
        return [{}]
    return [
        {key: value for key, value in zip(keys, combination)}
        for combination in itertools.product(*value_sets)
    ]


def _task_param_variants(
    spec: TushareTaskSpec,
    params: Mapping[str, Any],
) -> list[dict[str, Any]]:
    return _expand_params({**TASK_DEFAULT_PARAMS.get(spec.task, {}), **dict(params)})


def _cursor_to_date(value: str) -> str:
    digits = "".join(character for character in str(value or "") if character.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    if len(digits) == 6:
        return f"{digits}01"
    return ""


def _normalize_codes(raw: str | Sequence[str]) -> list[str]:
    values = raw if not isinstance(raw, str) else raw.replace(";", ",").split(",")
    return list(dict.fromkeys(str(item).strip().upper() for item in values if str(item).strip()))


def _task_scope_key(args: SyncArgs) -> str:
    parts = [f"task={args.task}"]
    codes = _normalize_codes(args.codes_raw)
    if codes:
        parts.append(f"codes={','.join(codes)}")
    if args.begin_date:
        parts.append(f"begin={args.begin_date}")
    if args.end_date:
        parts.append(f"end={args.end_date}")
    if args.params:
        parts.append(
            "params="
            + json.dumps(args.params, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        )
    return "|".join(parts)


def _params_scope_key(task: str, params: Mapping[str, Any]) -> str:
    if not params:
        return f"task={task}"
    return (
        f"task={task}|params="
        + json.dumps(params, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    )


def _parse_json_params(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    text = str(value or "").strip()
    if not text:
        return {}
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("params 必须是 JSON object")
    return payload


def _provider_task_name(registry_name: str, source: str) -> str:
    prefix = f"{source}."
    return registry_name[len(prefix) :] if str(registry_name).startswith(prefix) else registry_name


def load_execution_plan_from_toml(
    path: str,
    *,
    log_level_override: str | None = None,
) -> TushareExecutionPlan:
    resolved = resolve_config_candidate(path, project_root=Path(__file__).resolve().parents[2])
    with resolved.open("rb") as handle:
        payload = tomllib.load(handle)
    if str(payload.get("source") or "").strip() != "tushare":
        raise ValueError(f"{resolved}: source 必须为 tushare")
    defaults = dict(payload.get("defaults") or {})
    runtime_path = payload.get("runtime_path")
    database = str(payload.get("database") or "tushare").strip() or "tushare"
    log_level = str(log_level_override or payload.get("log_level") or "INFO").strip() or "INFO"
    continue_on_error = bool(payload.get("continue_on_error", True))
    tasks: list[SyncArgs] = []
    for raw in payload.get("tasks") or []:
        if not bool(raw.get("enabled", True)):
            continue
        merged = {**defaults, **raw}
        task = str(merged.get("task") or "").strip()
        if task not in TUSHARE_TASK_SPECS:
            raise ValueError(f"{resolved}: 未知 Tushare task={task!r}")
        raw_params = merged.get("params") or {}
        if not isinstance(raw_params, dict):
            raise ValueError(f"{resolved}: task={task} params 必须是 TOML inline table/table")
        tasks.append(
            SyncArgs(
                task=task,
                codes_raw=_join_values(merged.get("codes")),
                begin_date=str(merged.get("begin_date") or "").strip(),
                end_date=str(merged.get("end_date") or "").strip(),
                fields=_join_values(merged.get("fields")),
                params=dict(raw_params),
                limit=max(0, int(merged.get("limit") or 0)),
                page_size=max(0, int(merged.get("page_size") or 0)),
                max_pages=max(0, int(merged.get("max_pages") or 0)),
                window_days=max(0, int(merged.get("window_days") or 0)),
                force=bool(merged.get("force", False)),
                resume=bool(merged.get("resume", False)),
                continue_on_error=bool(merged.get("continue_on_error", continue_on_error)),
                runtime_path=str(runtime_path).strip() if runtime_path else None,
                database=database,
                log_level=log_level,
            )
        )
    if not tasks:
        raise ValueError(f"{resolved}: 没有启用的 Tushare 任务")
    return TushareExecutionPlan(
        runtime_path=str(runtime_path).strip() if runtime_path else None,
        log_level=log_level,
        continue_on_error=continue_on_error,
        database=database,
        tasks=tuple(tasks),
    )


def _join_values(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return ",".join(str(item).strip() for item in value if str(item).strip())
    return str(value).strip()


__all__ = [
    "SyncArgs",
    "TushareExecutionContext",
    "TushareExecutionPlan",
    "build_context",
    "load_execution_plan_from_toml",
    "run_config_file",
    "run_registered_task",
    "run_sync_args",
]
