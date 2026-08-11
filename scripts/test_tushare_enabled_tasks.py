#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Preflight the Tushare tasks selectable on the freshness page.

The probe deliberately does not call ``run_sync_args`` and never inserts rows,
task logs, or checkpoints. It performs at most one limited provider request per
selected task (plus limited universe-source requests when a local code pool is
empty), validates returned business keys, and checks existing ClickHouse table
layouts with read-only system-table queries.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from program_bootstrap import install_sync_data_system_alias

install_sync_data_system_alias(PROJECT_ROOT)

from sync_data_system.providers.tushare.provider import (
    TushareAPIError,
    TushareConfig,
    TushareProvider,
)
from sync_data_system.providers.tushare.repository import (
    LEGACY_META_COLUMNS,
    _normalize_key_expression,
    _normalized_key_expression,
    _quote_identifier,
)
from sync_data_system.providers.tushare.runner import (
    SyncArgs,
    TushareExecutionContext,
    _execute_task,
)
from sync_data_system.providers.tushare.specs import (
    TUSHARE_TASK_SPECS,
    TushareTaskSpec,
)
from sync_data_system.sync_core.clickhouse import (
    ClickHouseConfig,
    create_clickhouse_client,
)


# Keep this account-specific list aligned with
# AlphaBlocks/studio/pages/sync/freshness.vue. Tasks in this set are disabled
# by default because the current account has no separately purchased access.
# ``--task`` can still probe one explicitly.
FRESHNESS_DEFAULT_LOCKED_TASKS = frozenset(
    {
        "anns_d",
        "cb_price_chg",
        "cctv_news",
        "etf_mins",
        "ft_mins",
        "hk_adjfactor",
        "hk_balancesheet",
        "hk_cashflow",
        "hk_daily",
        "hk_daily_adj",
        "hk_fina_indicator",
        "hk_income",
        "hk_mins",
        "idx_mins",
        "irm_qa_sh",
        "irm_qa_sz",
        "major_news",
        "monetary_policy",
        "news",
        "npr",
        "opt_mins",
        "p_get",
        "research_report",
        "rt_etf_k",
        "rt_etf_min",
        "rt_etf_min_daily",
        "rt_etf_sz_iopv",
        "rt_fut_min",
        "rt_fut_min_daily",
        "rt_hk_k",
        "rt_idx_k",
        "rt_idx_min",
        "rt_idx_min_daily",
        "rt_k",
        "rt_min",
        "rt_min_daily",
        "rt_sw_k",
        "stk_auction",
        "stk_auction_c",
        "stk_auction_o",
        "stk_mins",
        "stk_premarket",
        "sw_mins",
        "us_adjfactor",
        "us_balancesheet",
        "us_cashflow",
        "us_daily",
        "us_daily_adj",
        "us_fina_indicator",
        "us_income",
        "yc_cb",
    }
)


@dataclass(frozen=True)
class PreflightResult:
    task: str
    table: str
    status: str
    rows: int
    requests: int
    elapsed_ms: int
    table_status: str
    error_type: str = ""
    error: str = ""


class LimitedTushareProvider:
    """Cap real pagination while preserving task-specific query parameters."""

    def __init__(self, provider: TushareProvider, row_limit: int = 1) -> None:
        self.provider = provider
        self.config = provider.config
        self.row_limit = max(1, int(row_limit))

    @property
    def request_count(self) -> int:
        return self.provider.request_count

    def query_all(
        self,
        api_name: str,
        *,
        params: Mapping[str, Any] | None = None,
        fields: Sequence[str] | str = (),
        supports_pagination: bool = False,
        page_size: int = 0,
        max_pages: int = 0,
    ) -> list[dict[str, Any]]:
        rows = self.provider.query_all(
            api_name,
            params=dict(params or {}),
            fields=fields,
            supports_pagination=supports_pagination,
            page_size=min(max(1, int(page_size or self.row_limit)), self.row_limit),
            max_pages=1,
        )
        return rows[: self.row_limit]


class ReadOnlyPreflightRepository:
    """Repository facade that reads universes but keeps fetched rows in memory."""

    def __init__(self, client: Any, *, database: str) -> None:
        self.client = client
        self.database = database
        self.memory_rows: dict[str, list[dict[str, Any]]] = {}

    def ensure_task_table(
        self,
        spec: TushareTaskSpec,
        *,
        observed_fields: Sequence[str] = (),
    ) -> None:
        del spec, observed_fields
        # Production creates/migrates here. Preflight must not mutate ClickHouse.

    def load_latest_cursor(self, spec: TushareTaskSpec, *, code: str = "") -> None:
        del spec, code
        return None

    def load_latest_cursors(
        self,
        spec: TushareTaskSpec,
        codes: Sequence[str],
    ) -> dict[str, str]:
        del spec, codes
        return {}

    def save_rows(
        self,
        spec: TushareTaskSpec,
        rows: Sequence[Mapping[str, Any]],
        *,
        scope_key: str,
    ) -> int:
        del scope_key
        copied = [dict(row) for row in rows]
        self.memory_rows.setdefault(spec.table_name, []).extend(copied)
        return len(copied)

    def load_universe_codes(self, spec: TushareTaskSpec) -> list[str]:
        code_field = spec.code_field or ("ts_code" if "ts_code" in spec.output_names else "")
        if not code_field:
            return []
        codes = {
            str(row.get(code_field) or "").strip().upper()
            for row in self.memory_rows.get(spec.table_name, [])
            if str(row.get(code_field) or "").strip()
        }
        if not _table_exists(self.client, self.database, spec.table_name):
            return sorted(codes)
        if not _column_exists(self.client, self.database, spec.table_name, code_field):
            return sorted(codes)
        table_ref = (
            f"{_quote_identifier(self.database)}."
            f"{_quote_identifier(spec.table_name)}"
        )
        column = _quote_identifier(code_field)
        rows = self.client.query_rows(
            f"SELECT DISTINCT {column} FROM {table_ref} "
            f"WHERE {column} != '' ORDER BY {column} LIMIT 10"
        )
        codes.update(
            str(row[0]).strip().upper()
            for row in rows
            if row and str(row[0]).strip()
        )
        return sorted(codes)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Small, read-only Tushare preflight for tasks selectable on /sync/freshness."
        )
    )
    parser.add_argument("--runtime-path", default=None)
    parser.add_argument("--database", default="tushare")
    parser.add_argument(
        "--task",
        action="append",
        default=[],
        help="Only probe this task; repeatable. Accepts task or tushare.task.",
    )
    parser.add_argument(
        "--include-locked",
        action="store_true",
        help="Also probe tasks disabled by default on the freshness page.",
    )
    parser.add_argument(
        "--date",
        default="",
        help="Probe date in YYYYMMDD. Default: previous weekday.",
    )
    parser.add_argument("--max-tasks", type=int, default=0)
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--skip-table-check", action="store_true")
    parser.add_argument("--json", action="store_true", help="Print JSON lines.")
    parser.add_argument("--json-report", default="", help="Write the final report to this path.")
    return parser.parse_args(argv)


def selected_task_names(
    requested: Sequence[str],
    *,
    include_locked: bool,
    max_tasks: int = 0,
) -> list[str]:
    explicit = [_normalize_task_name(item) for item in requested if str(item).strip()]
    if explicit:
        unknown = sorted(set(explicit) - set(TUSHARE_TASK_SPECS))
        if unknown:
            raise ValueError(f"未知 Tushare 任务: {','.join(unknown)}")
        names = list(dict.fromkeys(explicit))
    else:
        names = [
            name
            for name, spec in TUSHARE_TASK_SPECS.items()
            if spec.default_enabled
            and not spec.stopped
            and (include_locked or name not in FRESHNESS_DEFAULT_LOCKED_TASKS)
        ]
    if max_tasks > 0:
        names = names[:max_tasks]
    return names


def run_preflight(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    task_names = selected_task_names(
        args.task,
        include_locked=bool(args.include_locked),
        max_tasks=max(0, int(args.max_tasks or 0)),
    )
    probe_date = _normalize_probe_date(args.date)
    clickhouse_config = ClickHouseConfig.from_env(runtime_path=args.runtime_path)
    connection = create_clickhouse_client(clickhouse_config)
    base_provider = TushareProvider(TushareConfig.from_env(runtime_path=args.runtime_path))
    provider = LimitedTushareProvider(base_provider)
    repository = ReadOnlyPreflightRepository(connection, database=str(args.database or "tushare"))
    context = TushareExecutionContext(
        provider=provider,
        repository=repository,
        connection=connection,
    )
    results: list[PreflightResult] = []
    started = time.monotonic()
    try:
        for index, task_name in enumerate(task_names, start=1):
            spec = TUSHARE_TASK_SPECS[task_name]
            before_requests = provider.request_count
            task_started = time.monotonic()
            table_status = "skipped" if args.skip_table_check else check_table_layout(
                connection,
                database=repository.database,
                spec=spec,
            )
            try:
                rows = _execute_task(
                    SyncArgs(
                        task=task_name,
                        begin_date=probe_date,
                        end_date=probe_date,
                        limit=1,
                        page_size=1,
                        max_pages=1,
                        force=True,
                        database=repository.database,
                    ),
                    spec,
                    provider,
                    repository,
                    context=context,
                )
                status = "EMPTY" if rows == 0 else "OK"
                error_type = ""
                error = ""
                if table_status.startswith("outdated:"):
                    status = "FAIL"
                    error_type = "TABLE_LAYOUT"
                    error = table_status.removeprefix("outdated:")
            except Exception as exc:
                rows = 0
                status = "FAIL"
                error_type = classify_error(exc)
                error = _compact_error(exc)
            result = PreflightResult(
                task=task_name,
                table=spec.table_name,
                status=status,
                rows=int(rows),
                requests=provider.request_count - before_requests,
                elapsed_ms=int((time.monotonic() - task_started) * 1000),
                table_status=table_status,
                error_type=error_type,
                error=error,
            )
            results.append(result)
            emit_result(args, index, len(task_names), result)
            if args.fail_fast and result.status == "FAIL":
                break
    finally:
        try:
            base_provider.close()
        finally:
            connection.close()

    summary = build_summary(
        results,
        total_tasks=len(task_names),
        total_requests=provider.request_count,
        elapsed_ms=int((time.monotonic() - started) * 1000),
        probe_date=probe_date,
    )
    emit(args, summary)
    if args.json_report:
        report_path = Path(args.json_report).expanduser().resolve()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(
                {"summary": summary, "results": [asdict(item) for item in results]},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    return 1 if summary["failed"] else 0


def check_table_layout(client: Any, *, database: str, spec: TushareTaskSpec) -> str:
    rows = client.query_rows(
        """
        SELECT engine, sorting_key, primary_key, partition_key
        FROM system.tables
        WHERE database = {database:String}
          AND name = {table:String}
        """,
        {"database": database, "table": spec.table_name},
    )
    if not rows:
        return "missing"
    engine, sorting_key, primary_key, partition_key = (
        str(value) for value in rows[0][:4]
    )
    expected_key = _normalized_key_expression(spec.business_key_fields)
    if engine != "ReplacingMergeTree":
        return f"outdated:engine={engine}, expected=ReplacingMergeTree"
    if _normalize_key_expression(sorting_key) != expected_key:
        return f"outdated:sorting_key={sorting_key!r}, expected={expected_key!r}"
    if _normalize_key_expression(primary_key) != expected_key:
        return f"outdated:primary_key={primary_key!r}, expected={expected_key!r}"
    if _normalize_key_expression(partition_key):
        return f"outdated:partition_key={partition_key!r}, expected=''"
    legacy_rows = client.query_rows(
        """
        SELECT name
        FROM system.columns
        WHERE database = {database:String}
          AND table = {table:String}
          AND name IN {columns:Array(String)}
        """,
        {
            "database": database,
            "table": spec.table_name,
            "columns": list(LEGACY_META_COLUMNS),
        },
    )
    if legacy_rows:
        columns = ",".join(str(row[0]) for row in legacy_rows if row)
        return f"outdated:legacy_columns={columns}"
    return "ok"


def classify_error(exc: Exception) -> str:
    text = str(exc).lower()
    if isinstance(exc, TushareAPIError):
        if any(token in text for token in ("权限", "积分", "permission", "privilege")):
            return "NO_PERMISSION"
        if any(token in text for token in ("timeout", "timed out", "connection", "连接")):
            return "NETWORK"
        return "API"
    if "缺少业务键字段" in str(exc):
        return "BUSINESS_KEY"
    if "缺少必填参数" in str(exc) or "required" in text:
        return "PARAMETER"
    if "代码池" in str(exc) or "无可用代码" in str(exc):
        return "CODE_POOL"
    return type(exc).__name__.upper()


def build_summary(
    results: Sequence[PreflightResult],
    *,
    total_tasks: int,
    total_requests: int,
    elapsed_ms: int,
    probe_date: str,
) -> dict[str, Any]:
    return {
        "status": "SUMMARY",
        "probe_date": probe_date,
        "tasks": total_tasks,
        "checked": len(results),
        "passed": sum(item.status == "OK" for item in results),
        "empty": sum(item.status == "EMPTY" for item in results),
        "failed": sum(item.status == "FAIL" for item in results),
        "requests": total_requests,
        "elapsed_ms": elapsed_ms,
    }


def emit_result(
    args: argparse.Namespace,
    index: int,
    total: int,
    result: PreflightResult,
) -> None:
    payload = {"index": index, "total": total, **asdict(result)}
    if args.json:
        emit(args, payload)
        return
    suffix = f" error_type={result.error_type} error={result.error}" if result.error else ""
    print(
        f"[{index:03d}/{total:03d}] {result.status:<5} "
        f"task={result.task} rows={result.rows} requests={result.requests} "
        f"table={result.table_status} elapsed_ms={result.elapsed_ms}{suffix}",
        flush=True,
    )


def emit(args: argparse.Namespace, payload: Mapping[str, Any]) -> None:
    if args.json:
        print(json.dumps(dict(payload), ensure_ascii=False), flush=True)
        return
    if payload.get("status") == "SUMMARY":
        print(
            "[SUMMARY] "
            f"date={payload['probe_date']} tasks={payload['tasks']} "
            f"checked={payload['checked']} passed={payload['passed']} "
            f"empty={payload['empty']} failed={payload['failed']} "
            f"requests={payload['requests']} elapsed_ms={payload['elapsed_ms']}",
            flush=True,
        )


def _table_exists(client: Any, database: str, table: str) -> bool:
    rows = client.query_rows(
        """
        SELECT 1
        FROM system.tables
        WHERE database = {database:String}
          AND name = {table:String}
        LIMIT 1
        """,
        {"database": database, "table": table},
    )
    return bool(rows)


def _column_exists(client: Any, database: str, table: str, column: str) -> bool:
    rows = client.query_rows(
        """
        SELECT 1
        FROM system.columns
        WHERE database = {database:String}
          AND table = {table:String}
          AND name = {column:String}
        LIMIT 1
        """,
        {"database": database, "table": table, "column": column},
    )
    return bool(rows)


def _normalize_task_name(value: str) -> str:
    text = str(value or "").strip()
    return text.removeprefix("tushare.")


def _normalize_probe_date(value: str) -> str:
    text = str(value or "").strip()
    if text:
        if len(text) != 8 or not text.isdigit():
            raise ValueError("--date 必须是 YYYYMMDD")
        return text
    candidate = date.today() - timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate.strftime("%Y%m%d")


def _compact_error(exc: Exception) -> str:
    return " ".join(str(exc).split())[:1000]


def main() -> int:
    try:
        return run_preflight()
    except KeyboardInterrupt:
        print("\n[STOP] 用户中断", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"[FATAL] {type(exc).__name__}: {_compact_error(exc)}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
