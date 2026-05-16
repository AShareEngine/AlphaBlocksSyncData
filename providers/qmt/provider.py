#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""QMT REST API provider."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from sync_data_system.config_paths import resolve_runtime_config_path
from sync_data_system.runtime_config import load_runtime_config
from sync_data_system.providers.qmt.specs import QMT_TASK_SPECS, QmtTaskSpec


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class QmtConfig:
    base_url: str = "http://172.16.2.89:8000"
    api_key: str = "dev-api-key-001"
    timeout: int = 60

    @classmethod
    def from_env(cls, runtime_path: Optional[str | Path] = None) -> "QmtConfig":
        resolved_runtime_path = resolve_runtime_config_path(runtime_path)
        runtime = load_runtime_config(resolved_runtime_path)
        sync_config = runtime.sync.qmt
        return cls(
            base_url=str(sync_config.base_url or "http://172.16.2.89:8000").strip().rstrip("/"),
            api_key=str(sync_config.api_key or "dev-api-key-001").strip(),
            timeout=max(1, int(sync_config.timeout or 60)),
        )


class QmtProvider:
    def __init__(self, config: QmtConfig) -> None:
        self.config = config

    def close(self) -> None:
        return None

    def fetch_task(
        self,
        task: str,
        *,
        symbols: list[str] | None = None,
        symbol: str | None = None,
        market: str | None = None,
        index_code: str | None = None,
        stock_code: str | None = None,
        table_names: list[str] | None = None,
        sector_name: str | None = None,
        code_market: str | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        period: str | None = None,
        fields: list[str] | None = None,
        adjust_type: str | None = None,
        fill_data: bool | None = None,
        count: int | None = None,
        incrementally: bool | None = None,
        complete: bool | None = None,
    ) -> dict[str, Any]:
        spec = QMT_TASK_SPECS[task]
        path = _format_path(
            spec.path,
            symbol=symbol,
            code_market=code_market,
        )
        query: dict[str, Any] = {}
        body: dict[str, Any] = {}

        if spec.uses_symbols:
            body["symbols"] = normalize_qmt_code_list(symbols or [])
        if spec.uses_symbol and spec.uses_complete:
            query["complete"] = bool(complete) if complete is not None else False
        if spec.uses_market:
            if spec.method == "GET":
                query["market"] = str(market or "").strip()
            else:
                body["market"] = str(market or "").strip()
        if spec.uses_index_code:
            body["index_code"] = str(index_code or "").strip()
        if spec.uses_stock_code:
            body["stock_code"] = normalize_qmt_code(stock_code)
        if spec.uses_table_names:
            body["table_names"] = [str(item).strip() for item in (table_names or []) if str(item).strip()]
        if spec.uses_sector_name and sector_name:
            if spec.method == "GET":
                query["sector_name"] = str(sector_name).strip()
            else:
                body["sector_name"] = str(sector_name).strip()
        if spec.uses_begin_end:
            body["start_time"] = str(start_time or "").strip()
            body["end_time"] = str(end_time or "").strip()
        if spec.uses_period:
            body["period"] = str(period or spec.default_period or "1d").strip()
        if spec.uses_fields:
            body["fields"] = [str(item).strip() for item in (fields or []) if str(item).strip()]
        if spec.uses_adjust_type:
            body["adjust_type"] = str(adjust_type or spec.default_adjust_type or "none").strip() or "none"
        if spec.uses_fill_data:
            body["fill_data"] = bool(spec.default_fill_data if fill_data is None else fill_data)
        if spec.uses_count:
            body["count"] = spec.default_count if count is None else int(count)
        if spec.uses_incrementally:
            body["incrementally"] = bool(spec.default_incrementally if incrementally is None else incrementally)

        return self.request(spec.method, path, query=query, body=body if spec.method == "POST" else None)

    def request(
        self,
        method: str,
        path: str,
        *,
        query: Mapping[str, Any] | None = None,
        body: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = self._build_url(path, query=query)
        payload = None
        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
        }
        if body is not None:
            payload = json.dumps(dict(body), ensure_ascii=False).encode("utf-8")
        request = Request(url, data=payload, method=method.upper(), headers=headers)
        logger.debug("QMT request method=%s url=%s body=%s", method.upper(), url, body or {})
        try:
            with urlopen(request, timeout=self.config.timeout) as response:
                raw = response.read().decode("utf-8")
        except HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            message = _extract_error_message(raw) or exc.reason
            raise RuntimeError(f"QMT HTTP {exc.code}: {message}") from exc
        except URLError as exc:
            raise RuntimeError(f"QMT 请求失败: {exc.reason}") from exc

        try:
            envelope = json.loads(raw or "{}")
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"QMT 返回非 JSON 响应: {raw[:200]}") from exc

        if not isinstance(envelope, dict):
            raise RuntimeError(f"QMT 返回结构必须是对象，当前类型: {type(envelope).__name__}")
        if envelope.get("success") is False:
            message = str(envelope.get("message") or "QMT 请求失败")
            code = envelope.get("code", "")
            raise RuntimeError(f"QMT 请求失败 code={code} message={message}")
        return envelope

    def _build_url(self, path: str, *, query: Mapping[str, Any] | None = None) -> str:
        clean_base = self.config.base_url.rstrip("/")
        clean_path = "/api/v1/data/" + str(path or "").strip().lstrip("/")
        url = clean_base + clean_path
        query_items = {key: value for key, value in (query or {}).items() if value not in (None, "")}
        if query_items:
            url = f"{url}?{urlencode(query_items)}"
        return url


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


def normalize_qmt_code_list(code_list: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for code in code_list:
        text = normalize_qmt_code(code)
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def iter_qmt_rows(spec: QmtTaskSpec, envelope: Mapping[str, Any], request_meta: Mapping[str, Any]) -> list[dict[str, Any]]:
    data = envelope.get("data")
    if data is None:
        data = {}
    rows: list[dict[str, Any]] = []

    if spec.row_kind == "bar":
        for item in _iter_items(data, spec.item_collection_key):
            symbol = normalize_qmt_code(item.get("symbol") or request_meta.get("symbol"))
            for bar in _as_list(item.get("bars")):
                rows.append(_build_row(spec, request_meta, symbol=symbol, payload=bar))
    elif spec.row_kind == "tick":
        for item in _iter_items(data, spec.item_collection_key):
            symbol = normalize_qmt_code(item.get("symbol") or request_meta.get("symbol"))
            if isinstance(item.get("tick"), Mapping):
                rows.append(_build_row(spec, request_meta, symbol=symbol, payload=item.get("tick")))
            for tick in _as_list(item.get("ticks")):
                rows.append(_build_row(spec, request_meta, symbol=symbol, payload=tick))
    elif spec.row_kind == "financial_row":
        for item in _iter_items(data, spec.item_collection_key):
            symbol = normalize_qmt_code(item.get("symbol") or request_meta.get("symbol"))
            table_name = str(item.get("table_name") or request_meta.get("table_name") or "").strip()
            for row in _as_list(item.get("rows")):
                rows.append(_build_row(spec, request_meta, symbol=symbol, table_name=table_name, payload=row))
    elif spec.row_kind == "calendar_date":
        for day in _extract_dates(data):
            rows.append(_build_row(spec, request_meta, date=str(day), payload={"date": str(day)}))
    elif spec.row_kind == "component":
        payload = data if isinstance(data, Mapping) else {}
        index_code = str(payload.get("index_code") or request_meta.get("index_code") or "").strip()
        for component in _as_list(payload.get("components")):
            rows.append(_build_row(spec, request_meta, index_code=index_code, symbol=normalize_qmt_code(component.get("symbol")), payload=component))
    elif spec.row_kind == "sector_symbol":
        for item in _iter_items(data, spec.item_collection_key):
            sector_name = str(item.get("sector_name") or request_meta.get("sector_name") or "").strip()
            symbols = _as_list(item.get("symbols"))
            if not symbols:
                rows.append(_build_row(spec, request_meta, sector_name=sector_name, payload=item))
            for symbol in symbols:
                rows.append(_build_row(spec, request_meta, sector_name=sector_name, symbol=normalize_qmt_code(symbol), payload={"symbol": normalize_qmt_code(symbol)}))
    elif spec.row_kind == "quote":
        for item in _iter_items(data, spec.item_collection_key):
            rows.append(_build_row(spec, request_meta, symbol=normalize_qmt_code(item.get("symbol")), payload=item.get("quote") or item))
    elif spec.row_kind == "order":
        for item in _iter_items(data, spec.item_collection_key):
            symbol = normalize_qmt_code(item.get("symbol") or request_meta.get("symbol"))
            for order in _as_list(item.get("orders")):
                rows.append(_build_row(spec, request_meta, symbol=symbol, payload=order))
    elif spec.row_kind == "transaction":
        for item in _iter_items(data, spec.item_collection_key):
            symbol = normalize_qmt_code(item.get("symbol") or request_meta.get("symbol"))
            for transaction in _as_list(item.get("transactions")):
                rows.append(_build_row(spec, request_meta, symbol=symbol, payload=transaction))
    elif spec.row_kind == "period":
        for period in _extract_sequence(data):
            rows.append(_build_row(spec, request_meta, period=str(period), payload={"period": period}))
    elif spec.row_kind in {"download_result", "factor", "item"}:
        items = _iter_items(data, spec.item_collection_key)
        if items:
            for item in items:
                rows.append(_build_row(spec, request_meta, symbol=normalize_qmt_code(item.get("symbol")), payload=item))
        elif isinstance(data, list):
            for item in data:
                rows.append(_build_row(spec, request_meta, payload=item))
        else:
            rows.append(_build_row(spec, request_meta, payload=data))
    else:
        rows.append(_build_row(spec, request_meta, payload=data))

    if not rows:
        rows.append(_build_row(spec, request_meta, payload=data))
    return rows


def _build_row(
    spec: QmtTaskSpec,
    request_meta: Mapping[str, Any],
    *,
    symbol: str = "",
    table_name: str = "",
    index_code: str = "",
    sector_name: str = "",
    date: str = "",
    period: str = "",
    payload: Any,
) -> dict[str, Any]:
    payload_map = payload if isinstance(payload, Mapping) else {}
    return {
        "task": spec.task,
        "symbol": normalize_qmt_code(symbol or payload_map.get("symbol") or request_meta.get("symbol")),
        "stock_code": normalize_qmt_code(request_meta.get("stock_code")),
        "index_code": str(index_code or request_meta.get("index_code") or "").strip(),
        "market": str(request_meta.get("market") or "").strip(),
        "sector_name": str(sector_name or request_meta.get("sector_name") or "").strip(),
        "table_name": str(table_name or request_meta.get("table_name") or "").strip(),
        "period": str(period or request_meta.get("period") or "").strip(),
        "date": str(date or payload_map.get("date") or "").strip(),
        "time_ms": _as_optional_int(payload_map.get("time_ms")),
        "request_start_time": str(request_meta.get("start_time") or "").strip(),
        "request_end_time": str(request_meta.get("end_time") or "").strip(),
        "payload": payload,
    }


def _format_path(path: str, *, symbol: str | None = None, code_market: str | None = None) -> str:
    result = path
    if "{symbol}" in result:
        result = result.replace("{symbol}", normalize_qmt_code(symbol))
    if "{code_market}" in result:
        result = result.replace("{code_market}", str(code_market or "").strip())
    return result


def _iter_items(data: Any, key: str) -> list[Mapping[str, Any]]:
    value = data.get(key) if isinstance(data, Mapping) and key else data
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def _extract_dates(data: Any) -> list[Any]:
    if isinstance(data, Mapping):
        for key in ("dates", "items", "holidays"):
            if isinstance(data.get(key), list):
                return list(data.get(key) or [])
    if isinstance(data, list):
        return list(data)
    return []


def _extract_sequence(data: Any) -> list[Any]:
    if isinstance(data, Mapping):
        for key in ("periods", "items", "data"):
            if isinstance(data.get(key), list):
                return list(data.get(key) or [])
    if isinstance(data, list):
        return list(data)
    return []


def _as_optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _extract_error_message(raw: str) -> str:
    try:
        payload = json.loads(raw or "{}")
    except Exception:
        return raw[:200]
    if isinstance(payload, Mapping):
        return str(payload.get("message") or payload.get("detail") or "")[:200]
    return raw[:200]


__all__ = [
    "QmtConfig",
    "QmtProvider",
    "iter_qmt_rows",
    "normalize_qmt_code",
    "normalize_qmt_code_list",
]
