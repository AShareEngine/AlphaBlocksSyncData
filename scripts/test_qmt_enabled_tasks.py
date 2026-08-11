#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only preflight for QMT tasks shown on the freshness page.

Query endpoints receive one representative symbol and a one-day window. QMT
``download_*`` endpoints are skipped by default because probing them mutates
QMT's local data cache. No ClickHouse business rows, logs, or checkpoints are
written.
"""

from __future__ import annotations

import argparse
import json
import re
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

from sync_data_system.providers.qmt.provider import (
    QmtConfig,
    QmtProvider,
    iter_qmt_rows,
)
from sync_data_system.providers.qmt.repository import QmtRepository
from sync_data_system.providers.qmt.runner import (
    SyncArgs,
    build_fetch_kwargs,
    build_request_meta,
    validate_required_request,
)
from sync_data_system.providers.qmt.specs import (
    QMT_TASK_SPECS,
    QmtTaskSpec,
    order_by_columns_for_spec,
)
from sync_data_system.sync_core.clickhouse import (
    ClickHouseConfig,
    create_clickhouse_client,
)


DOWNLOAD_TASK_PREFIX = "download_"
LEGACY_QMT_COLUMNS = ("source", "fetched_at", "ingested_at", "payload_json")
FRESHNESS_DEFAULT_LOCKED_TASKS = frozenset(
    {
        # The deployed QMT REST service reports these xtdata methods as
        # unsupported (HTTP 501). Keep them available for explicit probing so
        # they can be re-enabled after the QMT client/server is upgraded.
        "trading_calendar",
        "full_kline",
        "trade_times",
        "holidays",
        "periods",
        "cb_info",
        "ipo_info",
        "etf_info",
        "download_holiday",
        "download_cb",
        "download_etf",
    }
)


@dataclass(frozen=True)
class PreflightResult:
    task: str
    table: str
    status: str
    rows: int
    elapsed_ms: int
    table_status: str
    error_type: str = ""
    error: str = ""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Small QMT preflight for tasks selectable on /sync/freshness."
    )
    parser.add_argument("--runtime-path", default=None)
    parser.add_argument("--database", default="qmt")
    parser.add_argument(
        "--task",
        action="append",
        default=[],
        help="Only probe this task; repeatable. Accepts task or qmt.task.",
    )
    parser.add_argument(
        "--date",
        default="",
        help="Probe date in YYYYMMDD. Default: previous weekday.",
    )
    parser.add_argument("--symbol", default="600000.SH")
    parser.add_argument("--cb-symbol", default="113001.SH")
    parser.add_argument("--etf-symbol", default="510300.SH")
    parser.add_argument("--index-code", default="000300.SH")
    parser.add_argument("--market", default="SH")
    parser.add_argument("--sector-name", default="沪深A股")
    parser.add_argument("--financial-table", default="Balance")
    parser.add_argument("--code-market", default="IF.CFFEX")
    parser.add_argument("--include-downloads", action="store_true")
    parser.add_argument(
        "--include-locked",
        action="store_true",
        help="Also probe QMT tasks disabled by default on the freshness page.",
    )
    parser.add_argument("--skip-table-check", action="store_true")
    parser.add_argument("--max-tasks", type=int, default=0)
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--json-report", default="")
    return parser.parse_args(argv)


def selected_task_names(
    requested: Sequence[str],
    *,
    include_locked: bool = False,
    max_tasks: int = 0,
) -> list[str]:
    explicit = [_normalize_task_name(item) for item in requested if str(item).strip()]
    if explicit:
        unknown = sorted(set(explicit) - set(QMT_TASK_SPECS))
        if unknown:
            raise ValueError(f"未知 QMT 任务: {','.join(unknown)}")
        names = list(dict.fromkeys(explicit))
    else:
        names = [
            name
            for name in QMT_TASK_SPECS
            if include_locked or name not in FRESHNESS_DEFAULT_LOCKED_TASKS
        ]
    if max_tasks > 0:
        names = names[:max_tasks]
    return names


def build_sample_args(task: str, args: argparse.Namespace, probe_date: str) -> SyncArgs:
    spec = QMT_TASK_SPECS[task]
    symbol = str(args.symbol).strip().upper()
    if task == "cb_info":
        symbol = str(args.cb_symbol).strip().upper()
    elif task == "etf_info":
        symbol = str(args.etf_symbol).strip().upper()
    return SyncArgs(
        task=task,
        symbols_raw=symbol if spec.uses_symbols else "",
        symbol=symbol if spec.uses_symbol else "",
        market=str(args.market).strip().upper() if spec.uses_market else "",
        index_code=str(args.index_code).strip().upper() if spec.uses_index_code else "",
        stock_code=symbol if spec.uses_stock_code else "",
        table_names_raw=str(args.financial_table).strip() if spec.uses_table_names else "",
        sector_name=str(args.sector_name).strip() if spec.uses_sector_name else "",
        code_market=str(args.code_market).strip() if spec.uses_code_market else "",
        begin_time=probe_date if spec.uses_begin_end else "",
        end_time=probe_date if spec.uses_begin_end else "",
        period=spec.default_period or ("1d" if spec.uses_period else ""),
        fields_raw="",
        adjust_type=spec.default_adjust_type or "none",
        fill_data=spec.default_fill_data,
        count=1 if spec.uses_count else -1,
        incrementally=False,
        complete=False,
        limit=1,
        force=True,
        continue_on_error=False,
        runtime_path=args.runtime_path,
        database=str(args.database or "qmt"),
        log_level="INFO",
    )


def run_preflight(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    task_names = selected_task_names(
        args.task,
        include_locked=bool(args.include_locked),
        max_tasks=max(0, int(args.max_tasks or 0)),
    )
    probe_date = _normalize_probe_date(args.date)
    provider = QmtProvider(QmtConfig.from_env(runtime_path=args.runtime_path))
    connection = None
    if not args.skip_table_check:
        connection = create_clickhouse_client(
            ClickHouseConfig.from_env(runtime_path=args.runtime_path)
        )
    results: list[PreflightResult] = []
    started = time.monotonic()
    requests = 0
    try:
        for index, task in enumerate(task_names, start=1):
            spec = QMT_TASK_SPECS[task]
            task_started = time.monotonic()
            table_status = (
                "skipped"
                if connection is None
                else check_table_layout(
                    connection,
                    database=str(args.database or "qmt"),
                    spec=spec,
                )
            )
            if task.startswith(DOWNLOAD_TASK_PREFIX) and not args.include_downloads:
                table_outdated = table_status.startswith("outdated:")
                result = PreflightResult(
                    task=task,
                    table=spec.table_name,
                    status="FAIL" if table_outdated else "SKIP",
                    rows=0,
                    elapsed_ms=int((time.monotonic() - task_started) * 1000),
                    table_status=table_status,
                    error_type="TABLE_LAYOUT" if table_outdated else "MUTATING_ENDPOINT",
                    error=(
                        table_status.removeprefix("outdated:")
                        if table_outdated
                        else "download_* 会修改 QMT 本地缓存，默认不在只读预检中调用"
                    ),
                )
            else:
                try:
                    sample_args = build_sample_args(task, args, probe_date)
                    request_meta = build_request_meta(sample_args)
                    validate_required_request(sample_args, request_meta)
                    requests += 1
                    envelope = provider.fetch_task(
                        task,
                        **build_fetch_kwargs(sample_args, request_meta),
                    )
                    rows = iter_qmt_rows(spec, envelope, request_meta)
                    status = "EMPTY" if response_is_empty(envelope) else "OK"
                    error_type = ""
                    error = ""
                    if table_status.startswith("outdated:"):
                        status = "FAIL"
                        error_type = "TABLE_LAYOUT"
                        error = table_status.removeprefix("outdated:")
                    result = PreflightResult(
                        task=task,
                        table=spec.table_name,
                        status=status,
                        rows=0 if status == "EMPTY" else len(rows),
                        elapsed_ms=int((time.monotonic() - task_started) * 1000),
                        table_status=table_status,
                        error_type=error_type,
                        error=error,
                    )
                except Exception as exc:
                    result = PreflightResult(
                        task=task,
                        table=spec.table_name,
                        status="FAIL",
                        rows=0,
                        elapsed_ms=int((time.monotonic() - task_started) * 1000),
                        table_status=table_status,
                        error_type=classify_error(exc),
                        error=_compact_error(exc),
                    )
            results.append(result)
            emit_result(args, index, len(task_names), result)
            if args.fail_fast and result.status == "FAIL":
                break
    finally:
        provider.close()
        if connection is not None:
            connection.close()

    summary = build_summary(
        results,
        total_tasks=len(task_names),
        requests=requests,
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


def check_table_layout(client: Any, *, database: str, spec: QmtTaskSpec) -> str:
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
    expected = ",".join(order_by_columns_for_spec(spec))
    if engine != "ReplacingMergeTree":
        return f"outdated:engine={engine}, expected=ReplacingMergeTree"
    if _normalize_key(sorting_key) != expected:
        return f"outdated:sorting_key={sorting_key!r}, expected={expected!r}"
    if _normalize_key(primary_key) != expected:
        return f"outdated:primary_key={primary_key!r}, expected={expected!r}"
    if _normalize_key(partition_key):
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
            "columns": list(LEGACY_QMT_COLUMNS),
        },
    )
    if legacy_rows:
        columns = ",".join(str(row[0]) for row in legacy_rows if row)
        return f"outdated:legacy_columns={columns}"
    actual_rows = client.query_rows(
        """
        SELECT name
        FROM system.columns
        WHERE database = {database:String}
          AND table = {table:String}
        """,
        {"database": database, "table": spec.table_name},
    )
    actual_columns = {str(row[0]) for row in actual_rows if row}
    expected_columns = set(QmtRepository.table_columns_for_spec(spec))
    missing = sorted(expected_columns - actual_columns)
    if missing:
        return f"outdated:missing_columns={','.join(missing)}"
    legacy = sorted(actual_columns & QmtRepository.legacy_columns_for_spec(spec))
    if legacy:
        return f"outdated:legacy_columns={','.join(legacy)}"
    if not QmtRepository.table_layout_is_current(spec, actual_columns):
        unexpected = sorted(actual_columns - expected_columns)
        return f"outdated:unexpected_columns={','.join(unexpected)}"
    return "ok"


def response_is_empty(envelope: Mapping[str, Any]) -> bool:
    data = envelope.get("data")
    if data in (None, "", [], {}):
        return True
    if isinstance(data, Mapping):
        collection_keys = (
            "items",
            "dates",
            "components",
            "periods",
            "holidays",
            "rows",
            "bars",
            "ticks",
            "orders",
            "transactions",
        )
        present = [data.get(key) for key in collection_keys if key in data]
        if present and all(value in (None, [], {}) for value in present):
            return True
    return False


def classify_error(exc: Exception) -> str:
    text = str(exc).lower()
    if any(token in text for token in ("http 401", "http 403", "api 密钥", "authentication")):
        return "AUTH"
    if "缺少必填参数" in str(exc) or "422" in text:
        return "PARAMETER"
    if "503" in text or "xtdata" in text or "未登录" in str(exc):
        return "QMT_UNAVAILABLE"
    if "501" in text or "不支持" in str(exc):
        return "UNSUPPORTED"
    if any(token in text for token in ("请求失败", "connection", "timed out", "timeout", "no route")):
        return "NETWORK"
    if "非 json" in text or "返回结构" in str(exc):
        return "RESPONSE"
    return type(exc).__name__.upper()


def build_summary(
    results: Sequence[PreflightResult],
    *,
    total_tasks: int,
    requests: int,
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
        "skipped": sum(item.status == "SKIP" for item in results),
        "failed": sum(item.status == "FAIL" for item in results),
        "requests": requests,
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
        f"[{index:02d}/{total:02d}] {result.status:<5} task={result.task} "
        f"rows={result.rows} table={result.table_status} "
        f"elapsed_ms={result.elapsed_ms}{suffix}",
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
            f"empty={payload['empty']} skipped={payload['skipped']} "
            f"failed={payload['failed']} requests={payload['requests']} "
            f"elapsed_ms={payload['elapsed_ms']}",
            flush=True,
        )


def _normalize_key(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("`", "").replace('"', "")
    text = re.sub(r"\btuple\s*\((.*)\)$", r"\1", text)
    text = re.sub(r"[()\s]", "", text)
    return text


def _normalize_task_name(value: str) -> str:
    return str(value or "").strip().removeprefix("qmt.")


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
