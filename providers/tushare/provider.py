#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tushare Pro HTTP client.

The official API is language-neutral JSON-over-HTTP. Using it directly keeps
the provider independent from SDK release cadence and exposes every api_name.
"""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import httpx

from sync_data_system.config_paths import resolve_runtime_config_path
from sync_data_system.runtime_config import load_runtime_config


logger = logging.getLogger(__name__)


class TushareAPIError(RuntimeError):
    def __init__(self, api_name: str, code: int, message: str) -> None:
        super().__init__(f"Tushare api={api_name} code={code}: {message}")
        self.api_name = api_name
        self.code = int(code)
        self.message = str(message)


class TushareRequestBudgetExceeded(RuntimeError):
    pass


@dataclass(frozen=True)
class TushareResponse:
    fields: tuple[str, ...]
    items: tuple[tuple[Any, ...], ...]

    def rows(self) -> list[dict[str, Any]]:
        return [
            {field: value for field, value in zip(self.fields, item)}
            for item in self.items
        ]


@dataclass(frozen=True)
class TushareConfig:
    token: str
    base_url: str = "https://api.tushare.pro"
    timeout: int = 60
    retries: int = 2
    retry_backoff_seconds: float = 2.0
    request_interval_seconds: float = 0.2
    default_start_date: str = "20100101"
    page_size: int = 5000
    max_requests_per_run: int = 0

    @classmethod
    def from_env(cls, runtime_path: str | Path | None = None) -> "TushareConfig":
        runtime = load_runtime_config(resolve_runtime_config_path(runtime_path))
        config = runtime.sync.tushare
        token = str(os.environ.get("TUSHARE_TOKEN") or config.token or "").strip()
        if not token:
            raise ValueError(
                "Tushare token 未配置：请设置环境变量 TUSHARE_TOKEN，"
                "或在 runtime.local.yaml 的 sync.tushare.token 中填写。"
            )
        return cls(
            token=token,
            base_url=str(config.base_url or "https://api.tushare.pro").strip(),
            timeout=max(1, int(config.timeout or 60)),
            retries=max(0, int(config.retries or 0)),
            retry_backoff_seconds=max(0.0, float(config.retry_backoff_seconds or 0.0)),
            request_interval_seconds=max(0.0, float(config.request_interval_seconds or 0.0)),
            default_start_date=normalize_tushare_date(config.default_start_date) or "20100101",
            page_size=max(1, int(config.page_size or 5000)),
            max_requests_per_run=max(0, int(config.max_requests_per_run or 0)),
        )


class TushareProvider:
    def __init__(
        self,
        config: TushareConfig,
        *,
        client: httpx.Client | None = None,
        sleep: Any = time.sleep,
        tushare_module: Any | None = None,
    ) -> None:
        self.config = config
        self._owns_client = client is None
        self._client = client or httpx.Client(
            timeout=config.timeout,
            follow_redirects=True,
            headers={"User-Agent": "AlphaBlocksSyncData/tushare"},
        )
        self._sleep = sleep
        self._tushare_module = tushare_module
        self._request_lock = threading.Lock()
        self._last_request_at = 0.0
        self._request_count = 0

    @property
    def request_count(self) -> int:
        return self._request_count

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def query(
        self,
        api_name: str,
        *,
        params: Mapping[str, Any] | None = None,
        fields: Iterable[str] | str = (),
    ) -> TushareResponse:
        field_text = fields if isinstance(fields, str) else ",".join(str(item) for item in fields)
        payload = {
            "api_name": str(api_name).strip(),
            "token": self.config.token,
            "params": _clean_params(params or {}),
            "fields": str(field_text or "").strip(),
        }
        last_error: Exception | None = None
        for attempt in range(self.config.retries + 1):
            try:
                self._before_request()
                response = self._client.post(self.config.base_url, json=payload)
                response.raise_for_status()
                body = response.json()
                return _parse_response(api_name, body)
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
                if attempt >= self.config.retries:
                    raise
                delay = self.config.retry_backoff_seconds * (2**attempt) + random.uniform(0.0, 0.25)
                logger.warning(
                    "Tushare request retry api=%s attempt=%s/%s delay=%.2fs error=%s",
                    api_name,
                    attempt + 1,
                    self.config.retries,
                    delay,
                    exc,
                )
                self._sleep(delay)
        assert last_error is not None
        raise last_error

    def query_all(
        self,
        api_name: str,
        *,
        params: Mapping[str, Any] | None = None,
        fields: Iterable[str] | str = (),
        supports_pagination: bool = False,
        page_size: int = 0,
        max_pages: int = 0,
    ) -> list[dict[str, Any]]:
        base_params = dict(params or {})
        if api_name == "pro_bar":
            return self._query_pro_bar(base_params)
        size = max(1, int(page_size or self.config.page_size))
        if not supports_pagination:
            return self.query(api_name, params=base_params, fields=fields).rows()

        rows: list[dict[str, Any]] = []
        page = 0
        while True:
            request_params = {**base_params, "offset": page * size, "limit": size}
            response = self.query(api_name, params=request_params, fields=fields)
            page_rows = response.rows()
            rows.extend(page_rows)
            page += 1
            if len(page_rows) < size or (max_pages > 0 and page >= max_pages):
                break
        return rows

    def _query_pro_bar(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        self._before_request()
        module = self._tushare_module
        if module is None:
            try:
                import tushare as module  # type: ignore[no-redef]
            except ImportError as exc:
                raise ImportError(
                    "pro_bar 是 Tushare SDK 专用接口，请先执行 "
                    "`python3 scripts/install_provider_deps.py tushare --install`。"
                ) from exc
            self._tushare_module = module
        pro_api = module.pro_api(self.config.token)
        frame = module.pro_bar(api=pro_api, **_clean_params(params))
        if frame is None:
            return []
        if hasattr(frame, "reset_index"):
            index_names = [
                str(name)
                for name in getattr(frame.index, "names", ())
                if name is not None
            ]
            existing = {str(column) for column in getattr(frame, "columns", ())}
            if not any(name in existing for name in index_names):
                frame = frame.reset_index()
        if not hasattr(frame, "to_dict"):
            raise ValueError("Tushare pro_bar returned a non-DataFrame result")
        return [
            {str(key): value for key, value in row.items()}
            for row in frame.to_dict(orient="records")
        ]

    def _before_request(self) -> None:
        with self._request_lock:
            if (
                self.config.max_requests_per_run > 0
                and self._request_count >= self.config.max_requests_per_run
            ):
                raise TushareRequestBudgetExceeded(
                    f"Tushare 本次运行请求预算已用完：{self._request_count}/"
                    f"{self.config.max_requests_per_run}"
                )
            elapsed = time.monotonic() - self._last_request_at
            remaining = self.config.request_interval_seconds - elapsed
            if remaining > 0:
                self._sleep(remaining)
            self._last_request_at = time.monotonic()
            self._request_count += 1


def _parse_response(api_name: str, payload: Any) -> TushareResponse:
    if not isinstance(payload, dict):
        raise ValueError(f"Tushare api={api_name} returned non-object JSON")
    code = int(payload.get("code") or 0)
    if code != 0:
        raise TushareAPIError(api_name, code, str(payload.get("msg") or "unknown error"))
    data = payload.get("data") or {}
    fields = data.get("fields") or []
    items = data.get("items") or []
    if not isinstance(fields, list) or not isinstance(items, list):
        raise ValueError(f"Tushare api={api_name} returned invalid data.fields/items")
    normalized_items: list[tuple[Any, ...]] = []
    for item in items:
        if not isinstance(item, (list, tuple)):
            raise ValueError(f"Tushare api={api_name} returned a non-array item")
        normalized_items.append(tuple(item))
    return TushareResponse(
        fields=tuple(str(field).strip() for field in fields),
        items=tuple(normalized_items),
    )


def _clean_params(params: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): value
        for key, value in params.items()
        if value is not None and str(value).strip() != ""
    }


def normalize_tushare_date(value: Any) -> str:
    text = str(value or "").strip().replace("-", "").replace("/", "")
    if not text:
        return ""
    if len(text) == 8 and text.isdigit():
        return text
    raise ValueError(f"无效 Tushare 日期 {value!r}，应为 YYYYMMDD 或 YYYY-MM-DD")


__all__ = [
    "TushareAPIError",
    "TushareConfig",
    "TushareProvider",
    "TushareRequestBudgetExceeded",
    "TushareResponse",
    "normalize_tushare_date",
]
