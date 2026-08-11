#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tushare Pro SDK client with configurable token and API base URL."""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from sync_data_system.config_paths import resolve_runtime_config_path
from sync_data_system.network_proxy import scoped_proxy
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
        base_url = str(
            os.environ.get("TUSHARE_BASE_URL")
            or config.base_url
            or "https://api.tushare.pro"
        ).strip()
        if not token:
            raise ValueError(
                "Tushare token 未配置：请设置环境变量 TUSHARE_TOKEN，"
                "或在 runtime.local.yaml 的 sync.tushare.token 中填写。"
            )
        if not base_url:
            raise ValueError(
                "Tushare API 地址未配置：请设置环境变量 TUSHARE_BASE_URL，"
                "或在 runtime.local.yaml 的 sync.tushare.base_url 中填写。"
            )
        return cls(
            token=token,
            base_url=base_url.rstrip("/"),
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
        sleep: Any = time.sleep,
        tushare_module: Any | None = None,
    ) -> None:
        self.config = config
        self._sleep = sleep
        self._tushare_module = tushare_module or _load_tushare_module()
        self._sdk_api = self._tushare_module.pro_api(config.token)
        # Tushare 1.4.29+ appends /<api_name> to this configurable root URL.
        # Explicitly set both private attributes to support compatible gateways.
        self._sdk_api._DataApi__token = config.token
        self._sdk_api._DataApi__http_url = str(config.base_url).rstrip("/")
        self._sdk_api._DataApi__timeout = config.timeout
        self._request_lock = threading.Lock()
        self._last_request_at = 0.0
        self._request_count = 0

    @property
    def request_count(self) -> int:
        return self._request_count

    def close(self) -> None:
        return None

    def query(
        self,
        api_name: str,
        *,
        params: Mapping[str, Any] | None = None,
        fields: Iterable[str] | str = (),
    ) -> TushareResponse:
        normalized_api_name = str(api_name).strip()
        if not normalized_api_name:
            raise ValueError("Tushare api_name 不能为空")
        field_text = fields if isinstance(fields, str) else ",".join(str(item) for item in fields)
        request_params = _clean_params(params or {})
        last_error: Exception | None = None
        for attempt in range(self.config.retries + 1):
            self._before_request()
            try:
                sdk_method = getattr(self._sdk_api, normalized_api_name)
                with scoped_proxy():
                    frame = sdk_method(
                        fields=str(field_text or "").strip(),
                        **request_params,
                    )
                return _frame_to_response(normalized_api_name, frame)
            except TushareRequestBudgetExceeded:
                raise
            except Exception as exc:
                last_error = exc
                if attempt >= self.config.retries:
                    break
                delay = self.config.retry_backoff_seconds * (2**attempt) + random.uniform(0.0, 0.25)
                logger.warning(
                    "Tushare SDK request retry api=%s attempt=%s/%s delay=%.2fs error=%s",
                    normalized_api_name,
                    attempt + 1,
                    self.config.retries,
                    delay,
                    exc,
                )
                self._sleep(delay)
        assert last_error is not None
        if isinstance(last_error, TushareAPIError):
            raise last_error
        raise TushareAPIError(
            normalized_api_name,
            -1,
            str(last_error) or type(last_error).__name__,
        ) from last_error

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
        with scoped_proxy():
            frame = self._tushare_module.pro_bar(
                api=self._sdk_api,
                **_clean_params(params),
            )
        if frame is None:
            return []
        frame = _restore_named_index_columns(frame)
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


def _load_tushare_module() -> Any:
    try:
        import tushare
    except ImportError as exc:
        raise ImportError(
            "Tushare Provider 需要 tushare>=1.4.29，请执行 "
            "`python3 scripts/install_provider_deps.py tushare --install --upgrade`。"
        ) from exc
    version = str(getattr(tushare, "__version__", "") or "").strip()
    if version and _numeric_version(version) < (1, 4, 29):
        raise ImportError(
            f"Tushare SDK 版本过低：当前 {version}，需要 >=1.4.29。请执行 "
            "`python3 -m pip install --upgrade 'tushare>=1.4.29'`。"
        )
    return tushare


def _numeric_version(value: str) -> tuple[int, ...]:
    parts = [int(part) for part in str(value).split(".") if part.isdigit()]
    return tuple(parts or [0])


def _frame_to_response(api_name: str, frame: Any) -> TushareResponse:
    if frame is None:
        return TushareResponse(fields=(), items=())
    if not hasattr(frame, "to_dict"):
        raise ValueError(f"Tushare api={api_name} returned a non-DataFrame result")
    frame = _restore_named_index_columns(frame)
    records = frame.to_dict(orient="records")
    if not isinstance(records, list):
        raise ValueError(f"Tushare api={api_name} returned invalid DataFrame records")
    fields = tuple(str(column) for column in getattr(frame, "columns", ()))
    if not fields and records:
        fields = tuple(str(field) for field in records[0])
    items = tuple(
        tuple(row.get(field) for field in fields)
        for row in records
        if isinstance(row, Mapping)
    )
    return TushareResponse(
        fields=fields,
        items=items,
    )


def _restore_named_index_columns(frame: Any) -> Any:
    """Preserve business dimensions placed in a compatible gateway's index."""

    if not hasattr(frame, "reset_index"):
        return frame
    index_names = [
        str(name)
        for name in getattr(getattr(frame, "index", None), "names", ())
        if name is not None
    ]
    if not index_names:
        return frame
    existing = {str(column) for column in getattr(frame, "columns", ())}
    if all(name in existing for name in index_names):
        return frame
    return frame.reset_index()


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
