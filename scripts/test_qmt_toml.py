#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Smoke test a QMT TOML plan through REST without ClickHouse writes.

This script is intentionally lightweight: it does not import the sync runner,
runtime config dataclasses, or ClickHouse modules. That keeps the command usable
in the deployment shell even when only the REST client test is needed.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from toml_compat import tomllib


DEFAULT_CONFIG = "run_sync.qmt.sample.toml"
DEFAULT_BASE_URL = "http://YOUR_QMT_HOST:8000"
DEFAULT_API_KEY = "YOUR_QMT_API_KEY"
DEFAULT_TIMEOUT = 60


@dataclass(frozen=True)
class QmtRestConfig:
    base_url: str = DEFAULT_BASE_URL
    api_key: str = DEFAULT_API_KEY
    timeout: int = DEFAULT_TIMEOUT


@dataclass(frozen=True)
class QmtTomlTask:
    task: str
    symbols_raw: str = ""
    symbol: str = ""
    market: str = ""
    index_code: str = ""
    stock_code: str = ""
    table_names_raw: str = ""
    sector_name: str = ""
    code_market: str = ""
    begin_time: str = ""
    end_time: str = ""
    period: str = ""
    fields_raw: str = ""
    adjust_type: str = "none"
    fill_data: bool = True
    count: int = -1
    incrementally: bool = False
    complete: bool = False
    limit: int = 0


@dataclass(frozen=True)
class QmtTaskSpec:
    method: str
    path: str
    uses_symbols: bool = False
    uses_symbol: bool = False
    uses_market: bool = False
    uses_index_code: bool = False
    uses_stock_code: bool = False
    uses_table_names: bool = False
    uses_sector_name: bool = False
    uses_code_market: bool = False
    uses_begin_end: bool = False
    uses_period: bool = False
    uses_fields: bool = False
    uses_adjust_type: bool = False
    uses_fill_data: bool = False
    uses_count: bool = False
    uses_incrementally: bool = False
    uses_complete: bool = False
    default_period: str = ""
    default_adjust_type: str = "none"
    default_fill_data: bool = True
    default_count: int = -1
    default_incrementally: bool = False


QMT_SPECS: dict[str, QmtTaskSpec] = {
    "kline_history": QmtTaskSpec("POST", "/kline-history", uses_symbols=True, uses_begin_end=True, uses_period=True, uses_fields=True, uses_adjust_type=True, uses_fill_data=True, default_period="1d"),
    "tick_history": QmtTaskSpec("POST", "/tick-history", uses_symbols=True, uses_begin_end=True, uses_fields=True, uses_adjust_type=True),
    "full_tick": QmtTaskSpec("POST", "/full-tick", uses_symbols=True),
    "financial": QmtTaskSpec("POST", "/financial", uses_symbols=True, uses_table_names=True, uses_begin_end=True),
    "instrument": QmtTaskSpec("GET", "/instrument/{symbol}", uses_symbol=True, uses_complete=True),
    "trading_calendar": QmtTaskSpec("POST", "/trading-calendar", uses_market=True, uses_begin_end=True),
    "index_weight": QmtTaskSpec("POST", "/index-weight", uses_index_code=True),
    "sectors": QmtTaskSpec("GET", "/sectors", uses_sector_name=True),
    "l2_quote": QmtTaskSpec("POST", "/l2/quote", uses_symbols=True, uses_begin_end=True),
    "l2_order": QmtTaskSpec("POST", "/l2/order", uses_symbols=True, uses_begin_end=True),
    "l2_transaction": QmtTaskSpec("POST", "/l2/transaction", uses_symbols=True, uses_begin_end=True),
    "market_data_ex": QmtTaskSpec("POST", "/market-data-ex", uses_symbols=True, uses_begin_end=True, uses_period=True, uses_count=True, uses_fields=True, uses_adjust_type=True, uses_fill_data=True, default_period="1d"),
    "local_data": QmtTaskSpec("POST", "/local-data", uses_symbols=True, uses_begin_end=True, uses_period=True, uses_count=True, uses_fields=True, uses_adjust_type=True, uses_fill_data=True, default_period="1d"),
    "full_kline": QmtTaskSpec("POST", "/full-kline", uses_symbols=True, uses_period=True, uses_count=True, uses_fields=True, uses_adjust_type=True, uses_fill_data=True, default_period="1d"),
    "instrument_type": QmtTaskSpec("GET", "/instrument-type/{symbol}", uses_symbol=True),
    "trade_times": QmtTaskSpec("GET", "/trade-times/{symbol}", uses_symbol=True),
    "main_contract": QmtTaskSpec("GET", "/main-contract/{code_market}", uses_code_market=True),
    "trading_dates": QmtTaskSpec("POST", "/trading-dates", uses_market=True, uses_begin_end=True, uses_count=True),
    "holidays": QmtTaskSpec("GET", "/holidays"),
    "periods": QmtTaskSpec("GET", "/periods"),
    "data_dir": QmtTaskSpec("GET", "/data-dir"),
    "divid_factors": QmtTaskSpec("POST", "/divid-factors", uses_stock_code=True, uses_begin_end=True),
    "cb_info": QmtTaskSpec("GET", "/cb-info/{symbol}", uses_symbol=True),
    "ipo_info": QmtTaskSpec("GET", "/ipo-info"),
    "etf_info": QmtTaskSpec("GET", "/etf-info/{symbol}", uses_symbol=True),
    "download_history": QmtTaskSpec("POST", "/download/history", uses_stock_code=True, uses_begin_end=True, uses_period=True, uses_incrementally=True, default_period="1d"),
    "download_history_batch": QmtTaskSpec("POST", "/download/history/batch", uses_symbols=True, uses_begin_end=True, uses_period=True, uses_incrementally=True, default_period="1d"),
    "download_financial": QmtTaskSpec("POST", "/download/financial", uses_symbols=True, uses_table_names=True, uses_begin_end=True),
    "download_index_weight": QmtTaskSpec("POST", "/download/index-weight", uses_index_code=True),
    "download_history_contracts": QmtTaskSpec("POST", "/download/history-contracts", uses_market=True),
    "download_sector": QmtTaskSpec("POST", "/download/sector", uses_sector_name=True),
    "download_holiday": QmtTaskSpec("POST", "/download/holiday"),
    "download_cb": QmtTaskSpec("POST", "/download/cb"),
    "download_etf": QmtTaskSpec("POST", "/download/etf"),
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test a QMT run_sync TOML by calling QMT REST endpoints.")
    parser.add_argument("config", nargs="?", default=DEFAULT_CONFIG, help="QMT TOML path or config/sync/plans filename.")
    parser.add_argument("--runtime-path", help="runtime.local.yaml path. Overrides runtime_path in TOML.")
    parser.add_argument("--base-url", help="Override sync.qmt.base_url for this test.")
    parser.add_argument("--api-key", help="Override sync.qmt.api_key for this test.")
    parser.add_argument("--timeout", type=int, help="Override QMT HTTP timeout seconds.")
    parser.add_argument("--task", action="append", default=[], help="Only test this task. Can be repeated. Accepts qmt.task or task.")
    parser.add_argument("--max-requests", type=int, default=0, help="Stop after N REST requests. 0 means no cap.")
    parser.add_argument("--include-downloads", action="store_true", help="Also run download_* endpoints. Default skips them.")
    parser.add_argument("--dry-run", action="store_true", help="Only parse and print requests; do not call QMT.")
    parser.add_argument("--fail-fast", action="store_true", help="Stop on first failed request.")
    parser.add_argument("--json", action="store_true", help="Emit JSON lines instead of human-readable output.")
    parser.add_argument("--sample-bytes", type=int, default=500, help="Bytes of response summary data to show in human output.")
    return parser.parse_args(argv)


def main() -> int:
    return main_for_test(None)


def main_for_test(argv: list[str] | None) -> int:
    args = parse_args(argv)
    tasks, plan_runtime_path = load_qmt_toml_tasks(args.config)
    runtime_path = args.runtime_path or plan_runtime_path
    rest_config = build_rest_config(args, runtime_path)
    selected_tasks = {_normalize_task_name(item) for item in args.task}

    passed = 0
    failed = 0
    skipped = 0
    request_count = 0

    for task_index, task in enumerate(tasks, start=1):
        if selected_tasks and task.task not in selected_tasks:
            skipped += 1
            emit(args, {"status": "SKIP", "task": task.task, "reason": "task_filter"})
            continue
        if task.task.startswith("download_") and not args.include_downloads:
            skipped += 1
            emit(args, {"status": "SKIP", "task": task.task, "reason": "download_task"})
            continue

        expanded_tasks = expand_task(task)
        for request_index, single_task in enumerate(expanded_tasks, start=1):
            if args.max_requests and request_count >= args.max_requests:
                emit(args, {"status": "STOP", "reason": "max_requests", "max_requests": args.max_requests})
                return 1 if failed else 0

            request_count += 1
            try:
                request = build_request(single_task)
                if args.dry_run:
                    passed += 1
                    emit_request(args, "DRY", task_index, len(tasks), request_index, len(expanded_tasks), single_task.task, request)
                    continue

                started = time.monotonic()
                envelope = call_qmt(rest_config, request)
                elapsed_ms = int((time.monotonic() - started) * 1000)
                passed += 1
                emit_response(
                    args,
                    "OK",
                    task_index,
                    len(tasks),
                    request_index,
                    len(expanded_tasks),
                    single_task.task,
                    request,
                    envelope,
                    elapsed_ms,
                )
            except Exception as exc:
                failed += 1
                emit(args, {"status": "FAIL", "task": single_task.task, "request_no": request_count, "error": str(exc)})
                if args.fail_fast:
                    return 1

    emit(args, {"status": "SUMMARY", "passed": passed, "failed": failed, "skipped": skipped, "requests": request_count})
    return 1 if failed else 0


def load_qmt_toml_tasks(path_like: str) -> tuple[list[QmtTomlTask], str | None]:
    path = resolve_config_candidate(path_like)
    if not path.is_file():
        raise FileNotFoundError(f"QMT TOML 不存在: {path}")
    with path.open("rb") as fh:
        data = tomllib.load(fh)
    if not isinstance(data, dict):
        raise ValueError("QMT TOML 顶层必须是 table。")
    source = str(data.get("source") or "qmt").strip()
    if source != "qmt":
        raise ValueError(f"只支持 source = 'qmt' 的配置，当前: {source!r}")
    defaults = data.get("defaults", {}) or {}
    if not isinstance(defaults, dict):
        raise ValueError("[defaults] 必须是 table。")
    raw_tasks = data.get("tasks")
    if not isinstance(raw_tasks, list) or not raw_tasks:
        raise ValueError("QMT TOML 至少需要一个 [[tasks]]。")

    tasks: list[QmtTomlTask] = []
    for index, raw_task in enumerate(raw_tasks, start=1):
        if not isinstance(raw_task, dict):
            raise ValueError(f"tasks[{index}] 必须是 table。")
        if not bool(raw_task.get("enabled", True)):
            continue
        merged = dict(defaults)
        merged.update(raw_task)
        task_name = str(merged.get("task") or "").strip()
        if task_name not in QMT_SPECS:
            raise ValueError(f"tasks[{index}].task 不支持: {task_name!r}")
        tasks.append(
            QmtTomlTask(
                task=task_name,
                symbols_raw=normalize_config_list(merged.get("codes")),
                symbol=str(merged.get("symbol") or "").strip(),
                market=str(merged.get("market") or "").strip(),
                index_code=str(merged.get("index_code") or "").strip(),
                stock_code=str(merged.get("stock_code") or "").strip(),
                table_names_raw=normalize_config_list(merged.get("table_names")),
                sector_name=str(merged.get("sector_name") or "").strip(),
                code_market=str(merged.get("code_market") or "").strip(),
                begin_time=str(merged.get("begin_time") or merged.get("begin_date") or "").strip(),
                end_time=str(merged.get("end_time") or merged.get("end_date") or "").strip(),
                period=str(merged.get("period") or "").strip(),
                fields_raw=normalize_config_list(merged.get("fields")),
                adjust_type=str(merged.get("adjust_type") or "none").strip() or "none",
                fill_data=bool(merged.get("fill_data", True)),
                count=int(merged.get("count", -1) or -1),
                incrementally=bool(merged.get("incrementally", False)),
                complete=bool(merged.get("complete", False)),
                limit=max(0, int(merged.get("limit", 0) or 0)),
            )
        )
    if not tasks:
        raise ValueError("QMT TOML 中所有任务都被禁用。")
    runtime_path = str(data.get("runtime_path") or "").strip() or None
    return tasks, runtime_path


def build_rest_config(args: argparse.Namespace, runtime_path: str | None) -> QmtRestConfig:
    runtime_config = load_runtime_qmt_config(runtime_path)
    return QmtRestConfig(
        base_url=str(args.base_url or runtime_config.base_url).strip().rstrip("/"),
        api_key=str(args.api_key or runtime_config.api_key).strip(),
        timeout=max(1, int(args.timeout or runtime_config.timeout or DEFAULT_TIMEOUT)),
    )


def load_runtime_qmt_config(runtime_path: str | None) -> QmtRestConfig:
    path = resolve_runtime_path(runtime_path)
    if not path.is_file():
        return QmtRestConfig()
    try:
        with path.open("rb") as fh:
            data = tomllib.load(fh)
    except Exception:
        return QmtRestConfig()
    sync = data.get("sync", {}) if isinstance(data, dict) else {}
    qmt = sync.get("qmt", {}) if isinstance(sync, dict) else {}
    if not isinstance(qmt, dict):
        return QmtRestConfig()
    return QmtRestConfig(
        base_url=str(qmt.get("base_url") or DEFAULT_BASE_URL).strip().rstrip("/"),
        api_key=str(qmt.get("api_key") or DEFAULT_API_KEY).strip(),
        timeout=max(1, int(qmt.get("timeout") or DEFAULT_TIMEOUT)),
    )


def expand_task(task: QmtTomlTask) -> list[QmtTomlTask]:
    spec = QMT_SPECS[task.task]
    if not spec.uses_symbols:
        return [task]
    symbols = parse_symbol_list(task.symbols_raw)
    if task.limit > 0:
        symbols = symbols[: task.limit]
    if not symbols:
        raise ValueError(f"QMT 任务 {task.task} 需要 codes 参数。")
    if task.task == "download_history_batch":
        return [replace_task(task, symbols_raw=",".join(symbols))]
    return [
        replace_task(
            task,
            symbols_raw=symbol,
            symbol=symbol,
            stock_code=task.stock_code or symbol,
        )
        for symbol in symbols
    ]


def build_request(task: QmtTomlTask) -> dict[str, Any]:
    spec = QMT_SPECS[task.task]
    symbols = parse_symbol_list(task.symbols_raw)
    symbol = normalize_qmt_code(task.symbol) if task.symbol else (symbols[0] if len(symbols) == 1 else "")
    stock_code = normalize_qmt_code(task.stock_code) if task.stock_code else ""
    meta = {
        "symbols": symbols,
        "symbol": symbol,
        "market": task.market.strip().upper(),
        "index_code": task.index_code.strip().upper(),
        "stock_code": stock_code,
        "table_names": parse_csv(task.table_names_raw),
        "sector_name": task.sector_name.strip(),
        "code_market": task.code_market.strip(),
        "start_time": normalize_qmt_time(task.begin_time),
        "end_time": normalize_qmt_time(task.end_time),
        "period": task.period or spec.default_period,
        "fields": parse_csv(task.fields_raw),
        "adjust_type": task.adjust_type or spec.default_adjust_type,
        "fill_data": task.fill_data,
        "count": task.count,
        "incrementally": task.incrementally,
        "complete": task.complete,
    }
    validate_required_request(task.task, spec, meta)

    path = spec.path
    query: dict[str, Any] = {}
    body: dict[str, Any] = {}

    if "{symbol}" in path:
        path = path.replace("{symbol}", quote(str(meta["symbol"]), safe=""))
    if "{code_market}" in path:
        path = path.replace("{code_market}", quote(str(meta["code_market"]), safe=""))
    if spec.uses_symbols:
        body["symbols"] = meta["symbols"]
    if spec.uses_symbol and spec.uses_complete:
        query["complete"] = bool(meta["complete"])
    if spec.uses_market:
        body["market"] = meta["market"]
    if spec.uses_index_code:
        body["index_code"] = meta["index_code"]
    if spec.uses_stock_code:
        body["stock_code"] = meta["stock_code"]
    if spec.uses_table_names:
        body["table_names"] = meta["table_names"]
    if spec.uses_sector_name and meta["sector_name"]:
        if spec.method == "GET":
            query["sector_name"] = meta["sector_name"]
        else:
            body["sector_name"] = meta["sector_name"]
    if spec.uses_begin_end:
        body["start_time"] = meta["start_time"]
        body["end_time"] = meta["end_time"]
    if spec.uses_period:
        body["period"] = meta["period"]
    if spec.uses_fields:
        body["fields"] = meta["fields"]
    if spec.uses_adjust_type:
        body["adjust_type"] = meta["adjust_type"]
    if spec.uses_fill_data:
        body["fill_data"] = bool(spec.default_fill_data if meta["fill_data"] is None else meta["fill_data"])
    if spec.uses_count:
        body["count"] = spec.default_count if meta["count"] is None else int(meta["count"])
    if spec.uses_incrementally:
        body["incrementally"] = bool(spec.default_incrementally if meta["incrementally"] is None else meta["incrementally"])

    return {
        "task": task.task,
        "method": spec.method,
        "path": path,
        "query": {key: value for key, value in query.items() if value not in (None, "", [], {})},
        "body": body if spec.method == "POST" else None,
        "compact": compact_request(meta),
    }


def call_qmt(config: QmtRestConfig, request_payload: dict[str, Any]) -> dict[str, Any]:
    url = build_url(config.base_url, request_payload["path"], request_payload.get("query") or {})
    body = request_payload.get("body")
    data = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {config.api_key}",
        "Content-Type": "application/json",
    }
    request = Request(url, data=data, method=str(request_payload["method"]).upper(), headers=headers)
    try:
        with urlopen(request, timeout=config.timeout) as response:
            raw = response.read().decode("utf-8")
    except HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"QMT HTTP {exc.code}: {extract_error_message(raw) or exc.reason}") from exc
    except URLError as exc:
        raise RuntimeError(f"QMT 请求失败: {exc.reason}") from exc
    try:
        envelope = json.loads(raw or "{}")
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"QMT 返回非 JSON 响应: {raw[:200]}") from exc
    if not isinstance(envelope, dict):
        raise RuntimeError(f"QMT 返回结构必须是对象，当前类型: {type(envelope).__name__}")
    if envelope.get("success") is False:
        raise RuntimeError(f"QMT 请求失败 code={envelope.get('code')} message={envelope.get('message')}")
    return envelope


def emit_request(
    args: argparse.Namespace,
    status: str,
    task_index: int,
    task_total: int,
    request_index: int,
    request_total: int,
    task: str,
    request: dict[str, Any],
) -> None:
    emit(
        args,
        {
            "status": status,
            "task": task,
            "method": request["method"],
            "path": request["path"],
            "task_progress": f"{task_index}/{task_total}",
            "request_progress": f"{request_index}/{request_total}",
            "request": request["compact"],
        },
    )


def emit_response(
    args: argparse.Namespace,
    status: str,
    task_index: int,
    task_total: int,
    request_index: int,
    request_total: int,
    task: str,
    request: dict[str, Any],
    envelope: dict[str, Any],
    elapsed_ms: int,
) -> None:
    emit(
        args,
        {
            "status": status,
            "task": task,
            "method": request["method"],
            "path": request["path"],
            "task_progress": f"{task_index}/{task_total}",
            "request_progress": f"{request_index}/{request_total}",
            "elapsed_ms": elapsed_ms,
            "request": request["compact"],
            "response": summarize_envelope(envelope, sample_bytes=args.sample_bytes),
        },
    )


def emit(args: argparse.Namespace, payload: dict[str, Any]) -> None:
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str))
        return

    status = payload.get("status", "")
    if status in {"OK", "DRY"}:
        print(
            f"[{status}] task={payload.get('task')} task_progress={payload.get('task_progress')} "
            f"request_progress={payload.get('request_progress')} method={payload.get('method')} path={payload.get('path')}"
        )
        print(f"      request={json.dumps(payload.get('request'), ensure_ascii=False, default=str)}")
        if status == "OK":
            print(f"      elapsed_ms={payload.get('elapsed_ms')} response={json.dumps(payload.get('response'), ensure_ascii=False, default=str)}")
        return
    if status == "FAIL":
        print(f"[FAIL] task={payload.get('task')} request_no={payload.get('request_no')} error={payload.get('error')}")
        return
    if status == "SKIP":
        print(f"[SKIP] task={payload.get('task')} reason={payload.get('reason')}")
        return
    if status == "SUMMARY":
        print(f"[SUMMARY] passed={payload.get('passed')} failed={payload.get('failed')} skipped={payload.get('skipped')} requests={payload.get('requests')}")
        return
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str))


def summarize_envelope(envelope: dict[str, Any], *, sample_bytes: int) -> dict[str, Any]:
    data = envelope.get("data")
    summary: dict[str, Any] = {
        "success": envelope.get("success"),
        "code": envelope.get("code"),
        "message": envelope.get("message"),
    }
    if isinstance(data, dict):
        for key in ("items", "dates", "components", "periods", "holidays"):
            value = data.get(key)
            if isinstance(value, list):
                summary[f"{key}_count"] = len(value)
        items = data.get("items")
        if isinstance(items, list):
            for nested_key in ("bars", "ticks", "orders", "transactions", "symbols", "rows"):
                total = sum(len(item[nested_key]) for item in items if isinstance(item, dict) and isinstance(item.get(nested_key), list))
                if total:
                    summary[f"{nested_key}_count"] = total
        summary["data_keys"] = sorted(str(key) for key in data.keys())
    elif isinstance(data, list):
        summary["data_count"] = len(data)
    elif data is not None:
        summary["data_type"] = type(data).__name__
    if sample_bytes > 0:
        sample = json.dumps(data, ensure_ascii=False, default=str)
        summary["data_sample"] = sample[:sample_bytes]
    return summary


def validate_required_request(task: str, spec: QmtTaskSpec, meta: dict[str, Any]) -> None:
    missing: list[str] = []
    if spec.uses_symbols and not meta["symbols"]:
        missing.append("symbols")
    if spec.uses_symbol and not meta["symbol"]:
        missing.append("symbol")
    if spec.uses_market and not meta["market"]:
        missing.append("market")
    if spec.uses_index_code and not meta["index_code"] and task != "download_index_weight":
        missing.append("index_code")
    if spec.uses_stock_code and not meta["stock_code"]:
        missing.append("stock_code")
    if spec.uses_table_names and not meta["table_names"]:
        missing.append("table_names")
    if spec.uses_code_market and not meta["code_market"]:
        missing.append("code_market")
    if missing:
        raise ValueError(f"QMT 任务 {task} 缺少必填参数: {', '.join(missing)}")


def compact_request(meta: dict[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key in (
        "symbols",
        "symbol",
        "market",
        "index_code",
        "stock_code",
        "table_names",
        "sector_name",
        "code_market",
        "start_time",
        "end_time",
        "period",
        "fields",
        "adjust_type",
        "fill_data",
        "count",
        "incrementally",
        "complete",
    ):
        value = meta.get(key)
        if value not in (None, "", [], {}):
            compact[key] = value
    return compact


def resolve_config_candidate(path_like: str) -> Path:
    candidate = Path(path_like).expanduser()
    if candidate.is_absolute():
        return candidate
    roots = [
        Path.cwd(),
        PROJECT_ROOT,
        PROJECT_ROOT.parent,
        PROJECT_ROOT / "config" / "sync" / "plans",
        PROJECT_ROOT / "config" / "sync",
    ]
    for root in roots:
        resolved = (root / candidate).resolve()
        if resolved.exists():
            return resolved
    return (Path.cwd() / candidate).resolve()


def resolve_runtime_path(path_like: str | None) -> Path:
    env_path = (
        os.environ.get("SYNC_DATA_RUNTIME_CONFIG")
        or os.environ.get("ALPHABLOCKS_SYNC_DATA_RUNTIME_CONFIG")
        or os.environ.get("ALPHABLOCKS_RUNTIME_CONFIG")
        or os.environ.get("RUNTIME_CONFIG_PATH")
    )
    if path_like:
        candidate = Path(path_like).expanduser()
        return candidate.resolve() if candidate.is_absolute() else (Path.cwd() / candidate).resolve()
    if env_path:
        return Path(env_path).expanduser().resolve()
    return PROJECT_ROOT / "config" / "runtime.local.yaml"


def normalize_qmt_code(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = re.fullmatch(r"(?i)(sh|sz|bj)\.(\d{6})", text)
    if match:
        market, code = match.groups()
        return f"{code}.{market.upper()}"
    match = re.fullmatch(r"(\d{6})\.(SH|SZ|BJ)", text, flags=re.IGNORECASE)
    if match:
        code, market = match.groups()
        return f"{code}.{market.upper()}"
    return text.upper()


def parse_symbol_list(value: str) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in parse_csv(value):
        code = normalize_qmt_code(item)
        if not code or code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return normalized


def parse_csv(value: str) -> list[str]:
    return [part.strip() for part in str(value or "").split(",") if part.strip()]


def normalize_config_list(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (list, tuple)):
        return ",".join(str(item).strip() for item in value if str(item).strip())
    raise ValueError("配置列表字段必须是字符串或字符串数组。")


def normalize_qmt_time(value: Any) -> str:
    text = "".join(ch for ch in str(value or "").strip() if ch.isdigit())
    if not text:
        return ""
    if len(text) not in {4, 6, 8, 14}:
        raise ValueError(f"QMT 时间必须是 YYYY / YYYYMM / YYYYMMDD / YYYYMMDDHHMMSS，当前值: {value!r}")
    return text


def replace_task(task: QmtTomlTask, **updates: Any) -> QmtTomlTask:
    data = task.__dict__.copy()
    data.update(updates)
    return QmtTomlTask(**data)


def build_url(base_url: str, path: str, query: Mapping[str, Any]) -> str:
    url = str(base_url).rstrip("/") + "/api/v1/data/" + str(path).lstrip("/")
    query_items = {key: value for key, value in query.items() if value not in (None, "", [], {})}
    if query_items:
        url = f"{url}?{urlencode(query_items)}"
    return url


def extract_error_message(raw: str) -> str:
    try:
        payload = json.loads(raw or "{}")
    except Exception:
        return raw[:200]
    if isinstance(payload, dict):
        return str(payload.get("message") or payload.get("detail") or "")[:200]
    return raw[:200]


def _normalize_task_name(value: str) -> str:
    text = str(value or "").strip()
    if text.startswith("qmt."):
        return text.split(".", 1)[1]
    return text


if __name__ == "__main__":
    raise SystemExit(main())
