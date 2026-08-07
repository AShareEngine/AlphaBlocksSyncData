#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AKShare market data normalization."""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

import pandas as pd

from sync_data_system.config_paths import resolve_runtime_config_path
from sync_data_system.runtime_config import load_runtime_config


logger = logging.getLogger(__name__)
_PROXY_ENV_LOCK = threading.RLock()
_PROXY_ENV_KEYS = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy")

SPOT_COLUMNS = (
    "snapshot_date",
    "snapshot_at",
    "em_code",
    "market_id",
    "symbol",
    "name",
    "instrument_type",
    "last",
    "change_amount",
    "change_percent",
    "open",
    "high",
    "low",
    "previous_close",
    "market_cap",
    "pe",
    "volume",
    "turnover",
    "amplitude",
    "turnover_rate",
    "source",
    "fetched_at",
)

DAILY_COLUMNS = (
    "em_code",
    "symbol",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "turnover",
    "amplitude",
    "change_percent",
    "change_amount",
    "turnover_rate",
    "adjust",
    "source",
    "fetched_at",
)

MINUTE_COLUMNS = (
    "em_code",
    "symbol",
    "trade_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "turnover",
    "latest",
    "source",
    "fetched_at",
)

PROFILE_COLUMNS = (
    "snapshot_date",
    "symbol",
    "item",
    "value",
    "source",
    "fetched_at",
)

FINANCIAL_STATEMENT_COLUMNS = (
    "symbol",
    "statement_type",
    "period_type",
    "report_date",
    "report_type",
    "secu_code",
    "security_name",
    "item_code",
    "item_name",
    "amount",
    "raw_json",
    "source",
    "fetched_at",
)

FINANCIAL_INDICATOR_COLUMNS = (
    "symbol",
    "period_type",
    "report_date",
    "notice_date",
    "currency",
    "operate_income",
    "operate_income_yoy",
    "gross_profit",
    "gross_profit_yoy",
    "net_profit",
    "net_profit_yoy",
    "basic_eps",
    "diluted_eps",
    "gross_profit_ratio",
    "net_profit_ratio",
    "roe",
    "roa",
    "current_ratio",
    "quick_ratio",
    "debt_asset_ratio",
    "raw_json",
    "source",
    "fetched_at",
)

VALUATION_COLUMNS = (
    "symbol",
    "indicator",
    "period",
    "trade_date",
    "value",
    "source",
    "fetched_at",
)

INDEX_COLUMNS = (
    "index_code",
    "index_name",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "source",
    "fetched_at",
)

THS_CONCEPT_NAME_COLUMNS = (
    "snapshot_date",
    "concept_code",
    "concept_name",
    "source",
    "fetched_at",
)

THS_CONCEPT_INDEX_COLUMNS = (
    "concept_code",
    "concept_name",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "source",
    "fetched_at",
)

THS_CONCEPT_INFO_COLUMNS = (
    "snapshot_date",
    "concept_code",
    "concept_name",
    "item",
    "value",
    "source",
    "fetched_at",
)

EM_CONCEPT_NAME_COLUMNS = (
    "snapshot_date",
    "concept_code",
    "concept_name",
    "source",
    "fetched_at",
)

EM_CONCEPT_CONS_COLUMNS = (
    "snapshot_date",
    "concept_code",
    "concept_name",
    "rank",
    "symbol",
    "name",
    "last",
    "change_percent",
    "change_amount",
    "volume",
    "amount",
    "amplitude",
    "high",
    "low",
    "open",
    "previous_close",
    "turnover_rate",
    "pe_dynamic",
    "pb",
    "source",
    "fetched_at",
)

EM_CONCEPT_HIST_COLUMNS = (
    "concept_code",
    "concept_name",
    "period",
    "adjust",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "change_percent",
    "change_amount",
    "volume",
    "amount",
    "amplitude",
    "turnover_rate",
    "source",
    "fetched_at",
)


@dataclass(frozen=True)
class AkshareUSConfig:
    proxy: str = ""
    request_interval_seconds: float = 1.0
    retries: int = 2
    retry_backoff_seconds: float = 2.0
    default_start_date: str = "2010-01-01"
    adjust: str = ""
    common_stock_only: bool = True
    include_pink: bool = False

    @classmethod
    def from_env(cls, runtime_path: str | Path | None = None) -> "AkshareUSConfig":
        runtime = load_runtime_config(resolve_runtime_config_path(runtime_path))
        source = runtime.sync.akshare
        adjust = str(source.adjust or "").strip().lower()
        if adjust not in {"", "qfq", "hfq"}:
            raise ValueError("sync.akshare.adjust 只能是空字符串、qfq 或 hfq。")
        return cls(
            proxy=str(source.proxy or "").strip(),
            request_interval_seconds=max(0.0, float(source.request_interval_seconds)),
            retries=max(0, int(source.retries)),
            retry_backoff_seconds=max(0.0, float(source.retry_backoff_seconds)),
            default_start_date=str(source.default_start_date or "2010-01-01").strip() or "2010-01-01",
            adjust=adjust,
            common_stock_only=bool(source.common_stock_only),
            include_pink=bool(source.include_pink),
        )


class AkshareUSProvider:
    def __init__(
        self,
        config: AkshareUSConfig,
        *,
        akshare_module: Any | None = None,
    ) -> None:
        self.config = config
        self._akshare_module = akshare_module
        self._request_lock = threading.Lock()
        self._last_request_at = 0.0
        self._spot_cache: pd.DataFrame | None = None
        self._ths_concept_cache: pd.DataFrame | None = None
        self._em_concept_cache: pd.DataFrame | None = None

    def close(self) -> None:
        self._spot_cache = None
        self._ths_concept_cache = None
        self._em_concept_cache = None

    def fetch_em_concept_names(
        self,
        *,
        limit: int = 0,
        snapshot_date: date | None = None,
    ) -> pd.DataFrame:
        raw = self._run_call(
            "stock_board_concept_name_em",
            self._akshare.stock_board_concept_name_em,
        )
        frame = _as_dataframe(raw)
        if frame.empty:
            return _empty_frame(EM_CONCEPT_NAME_COLUMNS)
        result = pd.DataFrame(index=frame.index)
        result["snapshot_date"] = snapshot_date or date.today()
        result["concept_code"] = _text_series(frame, "板块代码", "代码", "code")
        result["concept_name"] = _text_series(frame, "板块名称", "名称", "name")
        result["source"] = "akshare:stock_board_concept_name_em"
        result["fetched_at"] = _utcnow()
        result = result[(result["concept_code"] != "") & (result["concept_name"] != "")]
        result = result.drop_duplicates(subset=["concept_code"], keep="first")
        result = result.sort_values(["concept_name", "concept_code"])
        if limit > 0:
            result = result.head(limit)
        normalized = result.loc[:, list(EM_CONCEPT_NAME_COLUMNS)].reset_index(drop=True)
        if limit <= 0:
            self._em_concept_cache = normalized.copy()
        return normalized

    def resolve_em_concepts(
        self,
        values: Iterable[Any] = (),
        *,
        limit: int = 0,
        directory: pd.DataFrame | Sequence[dict[str, str]] | None = None,
    ) -> list[dict[str, str]]:
        requested = normalize_ths_concept_list(values)
        if directory is None:
            directory = (
                self._em_concept_cache.copy()
                if self._em_concept_cache is not None
                else self.fetch_em_concept_names()
            )
        frame = _as_dataframe(directory)
        concepts = [
            {
                "concept_code": str(row.concept_code).strip(),
                "concept_name": str(row.concept_name).strip(),
            }
            for row in frame.itertuples(index=False)
            if str(getattr(row, "concept_code", "")).strip()
            and str(getattr(row, "concept_name", "")).strip()
        ]
        if requested:
            by_code = {item["concept_code"].casefold(): item for item in concepts}
            by_name = {item["concept_name"].casefold(): item for item in concepts}
            resolved: list[dict[str, str]] = []
            missing: list[str] = []
            for value in requested:
                item = by_code.get(value.casefold()) or by_name.get(value.casefold())
                if item is None:
                    missing.append(value)
                elif item not in resolved:
                    resolved.append(item)
            if missing:
                raise ValueError(f"未找到东方财富概念板块: {missing[:10]}")
            concepts = resolved
        return concepts[:limit] if limit > 0 else concepts

    def fetch_em_concept_constituents(
        self,
        concept_name: str,
        concept_code: str,
        *,
        snapshot_date: date | None = None,
    ) -> pd.DataFrame:
        symbol = concept_code or concept_name
        raw = self._run_call(
            f"stock_board_concept_cons_em:{symbol}",
            lambda: self._akshare.stock_board_concept_cons_em(symbol=symbol),
        )
        frame = _as_dataframe(raw)
        if frame.empty:
            return _empty_frame(EM_CONCEPT_CONS_COLUMNS)
        result = pd.DataFrame(index=frame.index)
        result["snapshot_date"] = snapshot_date or date.today()
        result["concept_code"] = concept_code
        result["concept_name"] = concept_name
        result["rank"] = pd.to_numeric(
            _value_series(frame, "序号", "排名", "rank"), errors="coerce"
        ).astype("Int64")
        result["symbol"] = _text_series(frame, "代码", "symbol", "code").map(
            _normalize_cn_stock_symbol
        )
        result["name"] = _text_series(frame, "名称", "name")
        result["last"] = _number_series(frame, "最新价", "last")
        result["change_percent"] = _number_series(frame, "涨跌幅", "change_percent")
        result["change_amount"] = _number_series(frame, "涨跌额", "change_amount")
        result["volume"] = _number_series(frame, "成交量", "volume")
        result["amount"] = _number_series(frame, "成交额", "amount")
        result["amplitude"] = _number_series(frame, "振幅", "amplitude")
        result["high"] = _number_series(frame, "最高", "high")
        result["low"] = _number_series(frame, "最低", "low")
        result["open"] = _number_series(frame, "今开", "开盘", "open")
        result["previous_close"] = _number_series(frame, "昨收", "previous_close")
        result["turnover_rate"] = _number_series(frame, "换手率", "turnover_rate")
        result["pe_dynamic"] = _number_series(frame, "市盈率-动态", "pe_dynamic")
        result["pb"] = _number_series(frame, "市净率", "pb")
        result["source"] = "akshare:stock_board_concept_cons_em"
        result["fetched_at"] = _utcnow()
        result = result[result["symbol"] != ""].copy()
        result = result.drop_duplicates(subset=["symbol"], keep="first")
        return result.loc[:, list(EM_CONCEPT_CONS_COLUMNS)].reset_index(drop=True)

    def fetch_em_concept_history(
        self,
        concept_name: str,
        concept_code: str,
        *,
        period: str,
        start_date: str | date,
        end_date: str | date,
        adjust: str = "",
    ) -> pd.DataFrame:
        normalized_period = str(period or "daily").strip().lower()
        if normalized_period not in {"daily", "weekly", "monthly"}:
            raise ValueError("东方财富概念历史周期只能是 daily、weekly 或 monthly。")
        normalized_adjust = str(adjust or "").strip().lower()
        if normalized_adjust not in {"", "qfq", "hfq"}:
            raise ValueError("东方财富概念历史复权只能是空字符串、qfq 或 hfq。")
        start = _date_value(start_date)
        end = _date_value(end_date)
        symbol = concept_code or concept_name
        raw = self._run_call(
            f"stock_board_concept_hist_em:{symbol}:{normalized_period}:{normalized_adjust or 'none'}",
            lambda: self._akshare.stock_board_concept_hist_em(
                symbol=symbol,
                period=normalized_period,
                start_date=start.strftime("%Y%m%d"),
                end_date=end.strftime("%Y%m%d"),
                adjust=normalized_adjust,
            ),
        )
        frame = _as_dataframe(raw)
        if frame.empty:
            return _empty_frame(EM_CONCEPT_HIST_COLUMNS)
        result = pd.DataFrame(index=frame.index)
        result["concept_code"] = concept_code
        result["concept_name"] = concept_name
        result["period"] = normalized_period
        result["adjust"] = normalized_adjust
        result["trade_date"] = pd.to_datetime(
            _value_series(frame, "日期", "date"), errors="coerce"
        ).dt.date
        result["open"] = _number_series(frame, "开盘", "open")
        result["high"] = _number_series(frame, "最高", "high")
        result["low"] = _number_series(frame, "最低", "low")
        result["close"] = _number_series(frame, "收盘", "close")
        result["change_percent"] = _number_series(frame, "涨跌幅", "change_percent")
        result["change_amount"] = _number_series(frame, "涨跌额", "change_amount")
        result["volume"] = _number_series(frame, "成交量", "volume")
        result["amount"] = _number_series(frame, "成交额", "amount")
        result["amplitude"] = _number_series(frame, "振幅", "amplitude")
        result["turnover_rate"] = _number_series(frame, "换手率", "turnover_rate")
        result["source"] = "akshare:stock_board_concept_hist_em"
        result["fetched_at"] = _utcnow()
        result = result[result["trade_date"].notna()].copy()
        result = result[(result["trade_date"] >= start) & (result["trade_date"] <= end)]
        normalized = result.loc[:, list(EM_CONCEPT_HIST_COLUMNS)]
        return normalized.sort_values("trade_date").reset_index(drop=True)

    def fetch_ths_concept_names(
        self,
        *,
        limit: int = 0,
        snapshot_date: date | None = None,
    ) -> pd.DataFrame:
        raw = self._run_call(
            "stock_board_concept_name_ths",
            self._akshare.stock_board_concept_name_ths,
        )
        frame = _as_dataframe(raw)
        if frame.empty:
            return _empty_frame(THS_CONCEPT_NAME_COLUMNS)
        fetched_at = _utcnow()
        result = pd.DataFrame(index=frame.index)
        result["snapshot_date"] = snapshot_date or date.today()
        result["concept_code"] = _text_series(frame, "code", "代码", "板块代码")
        result["concept_name"] = _text_series(frame, "name", "名称", "板块名称")
        result["source"] = "akshare:stock_board_concept_name_ths"
        result["fetched_at"] = fetched_at
        result = result[(result["concept_code"] != "") & (result["concept_name"] != "")]
        result = result.drop_duplicates(subset=["concept_code"], keep="first")
        result = result.sort_values(["concept_name", "concept_code"])
        if limit > 0:
            result = result.head(limit)
        normalized = result.loc[:, list(THS_CONCEPT_NAME_COLUMNS)].reset_index(drop=True)
        if limit <= 0:
            self._ths_concept_cache = normalized.copy()
        return normalized

    def resolve_ths_concepts(
        self,
        values: Iterable[Any] = (),
        *,
        limit: int = 0,
        directory: pd.DataFrame | None = None,
    ) -> list[dict[str, str]]:
        requested = normalize_ths_concept_list(values)
        if directory is None:
            directory = (
                self._ths_concept_cache.copy()
                if self._ths_concept_cache is not None
                else self.fetch_ths_concept_names()
            )
        frame = _as_dataframe(directory)
        concepts = [
            {
                "concept_code": str(row.concept_code).strip(),
                "concept_name": str(row.concept_name).strip(),
            }
            for row in frame.itertuples(index=False)
            if str(getattr(row, "concept_code", "")).strip()
            and str(getattr(row, "concept_name", "")).strip()
        ]
        if requested:
            by_code = {item["concept_code"].casefold(): item for item in concepts}
            by_name = {item["concept_name"].casefold(): item for item in concepts}
            resolved: list[dict[str, str]] = []
            missing: list[str] = []
            for value in requested:
                item = by_code.get(value.casefold()) or by_name.get(value.casefold())
                if item is None:
                    missing.append(value)
                elif item not in resolved:
                    resolved.append(item)
            if missing:
                raise ValueError(f"未找到同花顺概念板块: {missing[:10]}")
            concepts = resolved
        return concepts[:limit] if limit > 0 else concepts

    def fetch_ths_concept_index(
        self,
        concept_name: str,
        concept_code: str,
        *,
        start_date: str | date,
        end_date: str | date,
    ) -> pd.DataFrame:
        start = _date_value(start_date)
        end = _date_value(end_date)
        raw = self._run_call(
            f"stock_board_concept_index_ths:{concept_name}",
            lambda: self._akshare.stock_board_concept_index_ths(
                symbol=concept_name,
                start_date=start.strftime("%Y%m%d"),
                end_date=end.strftime("%Y%m%d"),
            ),
        )
        frame = _as_dataframe(raw)
        if frame.empty:
            return _empty_frame(THS_CONCEPT_INDEX_COLUMNS)
        fetched_at = _utcnow()
        result = pd.DataFrame(index=frame.index)
        result["concept_code"] = concept_code
        result["concept_name"] = concept_name
        result["trade_date"] = pd.to_datetime(
            _value_series(frame, "日期", "date"), errors="coerce"
        ).dt.date
        result["open"] = _number_series(frame, "开盘价", "开盘", "open")
        result["high"] = _number_series(frame, "最高价", "最高", "high")
        result["low"] = _number_series(frame, "最低价", "最低", "low")
        result["close"] = _number_series(frame, "收盘价", "收盘", "close")
        result["volume"] = _number_series(frame, "成交量", "volume")
        result["amount"] = _number_series(frame, "成交额", "amount")
        result["source"] = "akshare:stock_board_concept_index_ths"
        result["fetched_at"] = fetched_at
        result = result[result["trade_date"].notna()].copy()
        result = result[(result["trade_date"] >= start) & (result["trade_date"] <= end)]
        normalized = result.loc[:, list(THS_CONCEPT_INDEX_COLUMNS)]
        normalized = normalized.sort_values("trade_date").reset_index(drop=True)
        if not normalized.empty:
            normalized.attrs["coverage_by_symbol"] = {
                concept_code or concept_name: normalized["trade_date"].max()
            }
        return normalized

    def fetch_ths_concept_info(
        self,
        concept_name: str,
        concept_code: str,
        *,
        snapshot_date: date | None = None,
    ) -> pd.DataFrame:
        raw = self._run_call(
            f"stock_board_concept_info_ths:{concept_name}",
            lambda: self._akshare.stock_board_concept_info_ths(symbol=concept_name),
        )
        frame = _as_dataframe(raw)
        if frame.empty:
            return _empty_frame(THS_CONCEPT_INFO_COLUMNS)
        item = _text_series(frame, "项目", "item", "指标")
        value = _text_series(frame, "值", "value", "内容")
        if (item == "").all() and len(frame.columns) >= 2:
            item = frame.iloc[:, 0].fillna("").astype(str).str.strip()
            value = frame.iloc[:, 1].fillna("").astype(str).str.strip()
        result = pd.DataFrame(
            {
                "snapshot_date": snapshot_date or date.today(),
                "concept_code": concept_code,
                "concept_name": concept_name,
                "item": item,
                "value": value,
                "source": "akshare:stock_board_concept_info_ths",
                "fetched_at": _utcnow(),
            }
        )
        result = result[result["item"] != ""]
        return result.loc[:, list(THS_CONCEPT_INFO_COLUMNS)].reset_index(drop=True)

    def fetch_us_spot(
        self,
        *,
        limit: int = 0,
        snapshot_date: date | None = None,
    ) -> pd.DataFrame:
        raw, source = self._fetch_us_spot_raw()
        frame = _as_dataframe(raw)
        if frame.empty:
            return _empty_frame(SPOT_COLUMNS)

        fetched_at = _utcnow()
        result = pd.DataFrame(index=frame.index)
        if source == "akshare:stock_us_spot_em":
            result["em_code"] = _text_series(frame, "代码", "code")
            result["symbol"] = result["em_code"].map(_symbol_from_em_code)
            result["market_id"] = result["em_code"].map(_market_id_from_em_code)
            result["name"] = _coalesced_text_series(frame, "名称", "name", "cname")
        else:
            result["symbol"] = _text_series(frame, "symbol", "代码", "code").map(normalize_us_symbol)
            result["em_code"] = result["symbol"]
            result["market_id"] = _text_series(frame, "market", "市场")
            result["name"] = _coalesced_text_series(frame, "name", "cname", "名称")
        classification_names = (
            result["name"].fillna("").astype(str)
            + " "
            + _text_series(frame, "category", "证券类型", "类型")
        )
        result["instrument_type"] = [
            _instrument_type(symbol, classification_name, market_id)
            for symbol, classification_name, market_id in zip(
                result["symbol"],
                classification_names,
                result["market_id"],
            )
        ]
        result["snapshot_date"] = snapshot_date or date.today()
        result["snapshot_at"] = fetched_at
        result["last"] = _number_series(frame, "最新价", "latest", "price")
        result["change_amount"] = _number_series(frame, "涨跌额", "diff")
        result["change_percent"] = _number_series(frame, "涨跌幅", "chg")
        result["open"] = _number_series(frame, "开盘价", "开盘", "open")
        result["high"] = _number_series(frame, "最高价", "最高", "high")
        result["low"] = _number_series(frame, "最低价", "最低", "low")
        result["previous_close"] = _number_series(frame, "昨收价", "昨收", "preclose")
        result["market_cap"] = _number_series(frame, "总市值", "mktcap")
        result["pe"] = _number_series(frame, "市盈率", "pe")
        result["volume"] = _number_series(frame, "成交量", "volume")
        result["turnover"] = _number_series(frame, "成交额", "amount")
        result["amplitude"] = _number_series(frame, "振幅", "amplitude")
        result["turnover_rate"] = _number_series(frame, "换手率", "turnover_rate")
        result["source"] = source
        result["fetched_at"] = fetched_at
        result = result[(result["em_code"] != "") & (result["symbol"] != "")].copy()
        if source == "akshare:stock_us_spot":
            result = result[result["last"].notna()]
        if not self.config.include_pink:
            result = result[result["instrument_type"] != "pink"]
        if self.config.common_stock_only:
            result = result[result["instrument_type"] == "common_stock"]
        result = result.drop_duplicates(subset=["em_code"], keep="first").sort_values("symbol")
        if limit > 0:
            result = result.head(limit)
        normalized = result.loc[:, list(SPOT_COLUMNS)].reset_index(drop=True)
        if limit <= 0:
            self._spot_cache = normalized.copy()
        return normalized

    def _fetch_us_spot_raw(self) -> tuple[Any, str]:
        failures: list[Exception] = []
        sources = (
            ("stock_us_spot_em", "akshare:stock_us_spot_em"),
            ("stock_us_spot", "akshare:stock_us_spot"),
            ("get_us_stock_name", "akshare:get_us_stock_name"),
        )
        for method_name, source in sources:
            operation = getattr(self._akshare, method_name, None)
            if not callable(operation):
                continue
            try:
                return self._run_call(method_name, operation), source
            except RuntimeError as exc:
                failures.append(exc)
                logger.warning(
                    "AKShare US symbol source unavailable source=%s; trying fallback",
                    method_name,
                )
        cause = failures[-1] if failures else None
        raise RuntimeError(
            "AKShare 美股代码表获取失败：东方财富 stock_us_spot_em 与新浪 "
            "stock_us_spot/get_us_stock_name 均不可用；请检查网络或配置 sync.akshare.proxy。"
        ) from cause

    def resolve_us_symbols(
        self,
        values: Iterable[Any] = (),
        *,
        limit: int = 0,
        require_em_code: bool = False,
    ) -> list[dict[str, str]]:
        requested = [str(value or "").strip().upper() for value in values if str(value or "").strip()]
        if requested and require_em_code and all(re.match(r"^\d+\..+$", value) for value in requested):
            return _dedupe_symbols(
                (
                    {
                        "symbol": _symbol_from_em_code(value),
                        "em_code": value,
                    }
                    for value in requested
                ),
                limit=limit,
            )
        if requested and not require_em_code:
            resolved = [
                {
                    "symbol": _symbol_from_em_code(value),
                    "em_code": value if re.match(r"^\d+\.", value) else "",
                }
                for value in requested
            ]
            return _dedupe_symbols(resolved, limit=limit)

        spot = self._spot_cache.copy() if self._spot_cache is not None else self.fetch_us_spot()
        by_em_code = {
            str(row.em_code).upper(): {"symbol": str(row.symbol), "em_code": str(row.em_code)}
            for row in spot.itertuples(index=False)
        }
        by_symbol_key = {
            _symbol_lookup_key(row.symbol): {"symbol": str(row.symbol), "em_code": str(row.em_code)}
            for row in spot.itertuples(index=False)
        }

        if not requested:
            resolved = [
                {"symbol": str(row.symbol), "em_code": str(row.em_code)}
                for row in spot.itertuples(index=False)
            ]
        else:
            resolved = []
            missing: list[str] = []
            for value in requested:
                item = by_em_code.get(value) or by_symbol_key.get(_symbol_lookup_key(value))
                if item is None and re.match(r"^\d+\.", value):
                    item = {"symbol": _symbol_from_em_code(value), "em_code": value}
                if item is None:
                    if require_em_code:
                        missing.append(value)
                    item = {"symbol": _symbol_from_em_code(value), "em_code": ""}
                resolved.append(item)
            if missing:
                preview = ",".join(missing[:10])
                logger.warning(
                    "AKShare symbols missing from Eastmoney directory count=%s preview=%s; "
                    "keeping plain symbols for available fallback sources",
                    len(missing),
                    preview,
                )

        return _dedupe_symbols(resolved, limit=limit)

    def fetch_us_daily(
        self,
        *,
        em_code: str,
        symbol: str,
        start_date: str | date,
        end_date: str | date,
    ) -> pd.DataFrame:
        start = _date_value(start_date)
        end = _date_value(end_date)
        source = "akshare:stock_us_hist"
        actual_adjust = self.config.adjust
        has_eastmoney_code = bool(re.match(r"^\d+\..+$", normalize_us_symbol(em_code)))
        try:
            if not has_eastmoney_code:
                raise RuntimeError("Eastmoney market-prefixed code is unavailable")
            raw = self._run_call(
                f"stock_us_hist:{em_code}",
                lambda: self._akshare.stock_us_hist(
                    symbol=em_code,
                    period="daily",
                    start_date=start.strftime("%Y%m%d"),
                    end_date=end.strftime("%Y%m%d"),
                    adjust=self.config.adjust,
                ),
            )
        except RuntimeError:
            operation = getattr(self._akshare, "stock_us_daily", None)
            if not callable(operation):
                raise
            source = "akshare:stock_us_daily"
            actual_adjust = self.config.adjust if self.config.adjust in {"", "qfq"} else ""
            logger.warning(
                "AKShare daily Eastmoney source unavailable symbol=%s; falling back to Sina stock_us_daily",
                symbol,
            )
            raw = self._run_call(
                f"stock_us_daily:{symbol}",
                lambda: operation(symbol=symbol, adjust=actual_adjust),
            )
        frame = _as_dataframe(raw)
        if frame.empty:
            return _empty_frame(DAILY_COLUMNS)
        fetched_at = _utcnow()
        result = pd.DataFrame(index=frame.index)
        result["em_code"] = em_code or symbol
        result["symbol"] = symbol
        result["trade_date"] = pd.to_datetime(_value_series(frame, "日期", "date"), errors="coerce").dt.date
        result["open"] = _number_series(frame, "开盘", "open")
        result["high"] = _number_series(frame, "最高", "high")
        result["low"] = _number_series(frame, "最低", "low")
        result["close"] = _number_series(frame, "收盘", "close")
        result["volume"] = _number_series(frame, "成交量", "volume")
        result["turnover"] = _number_series(frame, "成交额", "amount")
        result["amplitude"] = _number_series(frame, "振幅")
        result["change_percent"] = _number_series(frame, "涨跌幅")
        result["change_amount"] = _number_series(frame, "涨跌额")
        result["turnover_rate"] = _number_series(frame, "换手率")
        result["adjust"] = actual_adjust
        result["source"] = source
        result["fetched_at"] = fetched_at
        result = result[result["trade_date"].notna()].copy()
        result = result[
            (result["trade_date"] >= start)
            & (result["trade_date"] <= end)
        ]
        normalized = result.loc[:, list(DAILY_COLUMNS)].sort_values("trade_date").reset_index(drop=True)
        if not normalized.empty:
            normalized.attrs["coverage_by_symbol"] = {symbol: normalized["trade_date"].max()}
        return normalized

    def fetch_us_minute(
        self,
        *,
        em_code: str,
        symbol: str,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
    ) -> pd.DataFrame:
        request: dict[str, str] = {"symbol": em_code}
        if start_date:
            request["start_date"] = f"{_date_value(start_date).isoformat()} 00:00:00"
        if end_date:
            request["end_date"] = f"{_date_value(end_date).isoformat()} 23:59:59"
        raw = self._run_call(
            f"stock_us_hist_min_em:{em_code}",
            lambda: self._akshare.stock_us_hist_min_em(**request),
        )
        frame = _as_dataframe(raw)
        if frame.empty:
            return _empty_frame(MINUTE_COLUMNS)
        fetched_at = _utcnow()
        result = pd.DataFrame(index=frame.index)
        result["em_code"] = em_code
        result["symbol"] = symbol
        result["trade_time"] = pd.to_datetime(_value_series(frame, "时间", "time"), errors="coerce")
        result["open"] = _number_series(frame, "开盘", "open")
        result["high"] = _number_series(frame, "最高", "high")
        result["low"] = _number_series(frame, "最低", "low")
        result["close"] = _number_series(frame, "收盘", "close")
        result["volume"] = _number_series(frame, "成交量", "volume")
        result["turnover"] = _number_series(frame, "成交额", "amount")
        result["latest"] = _number_series(frame, "最新价", "latest")
        result["source"] = "akshare:stock_us_hist_min_em"
        result["fetched_at"] = fetched_at
        result = result[result["trade_time"].notna()].copy()
        if start_date:
            result = result[result["trade_time"].dt.date >= _date_value(start_date)]
        if end_date:
            result = result[result["trade_time"].dt.date <= _date_value(end_date)]
        return result.loc[:, list(MINUTE_COLUMNS)].sort_values("trade_time").reset_index(drop=True)

    def fetch_us_company_profile(
        self,
        symbol: str,
        *,
        snapshot_date: date | None = None,
    ) -> pd.DataFrame:
        raw = self._run_call(
            f"stock_individual_basic_info_us_xq:{symbol}",
            lambda: self._akshare.stock_individual_basic_info_us_xq(symbol=symbol),
        )
        frame = _as_dataframe(raw)
        if frame.empty:
            return _empty_frame(PROFILE_COLUMNS)
        fetched_at = _utcnow()
        item = _text_series(frame, "item", "项目", "指标")
        value = _text_series(frame, "value", "值", "内容")
        if (item == "").all() and len(frame.columns) >= 2:
            item = frame.iloc[:, 0].fillna("").astype(str)
            value = frame.iloc[:, 1].fillna("").astype(str)
        result = pd.DataFrame(
            {
                "snapshot_date": snapshot_date or date.today(),
                "symbol": symbol,
                "item": item,
                "value": value,
                "source": "akshare:stock_individual_basic_info_us_xq",
                "fetched_at": fetched_at,
            }
        )
        return result[result["item"] != ""].loc[:, list(PROFILE_COLUMNS)].reset_index(drop=True)

    def fetch_us_financial_statement(
        self,
        symbol: str,
        *,
        statement_type: str,
        period_type: str,
    ) -> pd.DataFrame:
        raw = self._run_optional_call(
            f"stock_financial_us_report_em:{symbol}:{statement_type}:{period_type}",
            lambda: self._akshare.stock_financial_us_report_em(
                stock=symbol.replace(".", "_").replace("-", "_"),
                symbol=statement_type,
                indicator=period_type,
            ),
        )
        frame = _as_dataframe(raw)
        if frame.empty:
            return _empty_frame(FINANCIAL_STATEMENT_COLUMNS)
        fetched_at = _utcnow()
        rows: list[dict[str, Any]] = []
        for record in frame.to_dict("records"):
            report_date = _optional_date(_record_get(record, "REPORT_DATE", "STD_REPORT_DATE"))
            if report_date is None:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "statement_type": statement_type,
                    "period_type": period_type,
                    "report_date": report_date,
                    "report_type": _string(_record_get(record, "REPORT_TYPE", "REPORT")),
                    "secu_code": _string(_record_get(record, "SECUCODE")),
                    "security_name": _string(_record_get(record, "SECURITY_NAME_ABBR")),
                    "item_code": _string(_record_get(record, "STD_ITEM_CODE")),
                    "item_name": _string(_record_get(record, "ITEM_NAME")),
                    "amount": _optional_float(_record_get(record, "AMOUNT")),
                    "raw_json": _record_json(record),
                    "source": "akshare:stock_financial_us_report_em",
                    "fetched_at": fetched_at,
                }
            )
        return pd.DataFrame(rows, columns=FINANCIAL_STATEMENT_COLUMNS)

    def fetch_us_financial_indicator(
        self,
        symbol: str,
        *,
        period_type: str,
    ) -> pd.DataFrame:
        raw = self._run_optional_call(
            f"stock_financial_us_analysis_indicator_em:{symbol}:{period_type}",
            lambda: self._akshare.stock_financial_us_analysis_indicator_em(
                symbol=symbol,
                indicator=period_type,
            ),
        )
        frame = _as_dataframe(raw)
        if frame.empty:
            return _empty_frame(FINANCIAL_INDICATOR_COLUMNS)
        fetched_at = _utcnow()
        rows: list[dict[str, Any]] = []
        for record in frame.to_dict("records"):
            report_date = _optional_date(_record_get(record, "REPORT_DATE", "STD_REPORT_DATE"))
            if report_date is None:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "period_type": period_type,
                    "report_date": report_date,
                    "notice_date": _optional_date(_record_get(record, "NOTICE_DATE")),
                    "currency": _string(_record_get(record, "CURRENCY")),
                    "operate_income": _optional_float(_record_get(record, "OPERATE_INCOME")),
                    "operate_income_yoy": _optional_float(_record_get(record, "OPERATE_INCOME_YOY")),
                    "gross_profit": _optional_float(_record_get(record, "GROSS_PROFIT")),
                    "gross_profit_yoy": _optional_float(_record_get(record, "GROSS_PROFIT_YOY")),
                    "net_profit": _optional_float(_record_get(record, "PARENT_HOLDER_NETPROFIT")),
                    "net_profit_yoy": _optional_float(_record_get(record, "PARENT_HOLDER_NETPROFIT_YOY")),
                    "basic_eps": _optional_float(_record_get(record, "BASIC_EPS")),
                    "diluted_eps": _optional_float(_record_get(record, "DILUTED_EPS")),
                    "gross_profit_ratio": _optional_float(_record_get(record, "GROSS_PROFIT_RATIO")),
                    "net_profit_ratio": _optional_float(_record_get(record, "NET_PROFIT_RATIO")),
                    "roe": _optional_float(_record_get(record, "ROE_AVG")),
                    "roa": _optional_float(_record_get(record, "ROA")),
                    "current_ratio": _optional_float(_record_get(record, "CURRENT_RATIO")),
                    "quick_ratio": _optional_float(_record_get(record, "SPEED_RATIO")),
                    "debt_asset_ratio": _optional_float(_record_get(record, "DEBT_ASSET_RATIO")),
                    "raw_json": _record_json(record),
                    "source": "akshare:stock_financial_us_analysis_indicator_em",
                    "fetched_at": fetched_at,
                }
            )
        return pd.DataFrame(rows, columns=FINANCIAL_INDICATOR_COLUMNS)

    def fetch_us_valuation(
        self,
        symbol: str,
        *,
        indicator: str,
        period: str,
    ) -> pd.DataFrame:
        raw = self._run_call(
            f"stock_us_valuation_baidu:{symbol}:{indicator}:{period}",
            lambda: self._akshare.stock_us_valuation_baidu(
                symbol=symbol,
                indicator=indicator,
                period=period,
            ),
        )
        frame = _as_dataframe(raw)
        if frame.empty:
            return _empty_frame(VALUATION_COLUMNS)
        fetched_at = _utcnow()
        result = pd.DataFrame(index=frame.index)
        result["symbol"] = symbol
        result["indicator"] = indicator
        result["period"] = period
        result["trade_date"] = pd.to_datetime(_value_series(frame, "date", "日期"), errors="coerce").dt.date
        result["value"] = _number_series(frame, "value", "值")
        result["source"] = "akshare:stock_us_valuation_baidu"
        result["fetched_at"] = fetched_at
        result = result[result["trade_date"].notna()].copy()
        return result.loc[:, list(VALUATION_COLUMNS)].sort_values("trade_date").reset_index(drop=True)

    def fetch_us_index_daily(
        self,
        index_code: str,
        index_name: str,
        *,
        start_date: str | date,
        end_date: str | date,
    ) -> pd.DataFrame:
        start = _date_value(start_date)
        end = _date_value(end_date)
        raw = self._run_call(
            f"index_us_stock_sina:{index_code}",
            lambda: self._akshare.index_us_stock_sina(symbol=index_code),
        )
        frame = _as_dataframe(raw)
        if frame.empty:
            return _empty_frame(INDEX_COLUMNS)
        fetched_at = _utcnow()
        result = pd.DataFrame(index=frame.index)
        result["index_code"] = index_code
        result["index_name"] = index_name
        result["trade_date"] = pd.to_datetime(_value_series(frame, "date", "日期"), errors="coerce").dt.date
        result["open"] = _number_series(frame, "open", "开盘")
        result["high"] = _number_series(frame, "high", "最高")
        result["low"] = _number_series(frame, "low", "最低")
        result["close"] = _number_series(frame, "close", "收盘")
        result["volume"] = _number_series(frame, "volume", "成交量")
        result["amount"] = _number_series(frame, "amount", "成交额")
        result["source"] = "akshare:index_us_stock_sina"
        result["fetched_at"] = fetched_at
        result = result[result["trade_date"].notna()].copy()
        result = result[
            (result["trade_date"] >= start)
            & (result["trade_date"] <= end)
        ]
        normalized = result.loc[:, list(INDEX_COLUMNS)].sort_values("trade_date").reset_index(drop=True)
        if not normalized.empty:
            normalized.attrs["coverage_by_symbol"] = {index_code: normalized["trade_date"].max()}
        return normalized

    @property
    def _akshare(self) -> Any:
        if self._akshare_module is None:
            try:
                import akshare
            except ImportError as exc:
                raise RuntimeError(
                    "缺少 akshare 依赖，请运行 "
                    "`python3 scripts/install_provider_deps.py akshare --install`。"
                ) from exc
            self._akshare_module = akshare
        return self._akshare_module

    def _run_call(self, label: str, operation: Callable[[], Any]) -> Any:
        with self._request_lock:
            for attempt in range(self.config.retries + 1):
                self._wait_for_request_slot()
                try:
                    with _configured_proxy(self.config.proxy):
                        return operation()
                except Exception as exc:
                    if not _is_retryable_akshare_error(exc) or attempt >= self.config.retries:
                        raise RuntimeError(
                            f"AKShare 请求失败 request={label} error_type={type(exc).__name__}；"
                            "连接或超时错误请检查 sync.akshare.proxy，数据解析错误请升级 AKShare "
                            "或等待上游接口恢复。"
                        ) from exc
                    delay = self.config.retry_backoff_seconds * (2**attempt)
                    logger.warning(
                        "AKShare request failed request=%s attempt=%s/%s "
                        "error_type=%s; retrying in %.1fs",
                        label,
                        attempt + 1,
                        self.config.retries,
                        type(exc).__name__,
                        delay,
                    )
                    if delay > 0:
                        time.sleep(delay)
                finally:
                    self._last_request_at = time.monotonic()
        raise RuntimeError(f"AKShare request exhausted retries: {label}")

    def _run_optional_call(self, label: str, operation: Callable[[], Any]) -> Any:
        try:
            return self._run_call(label, operation)
        except RuntimeError as exc:
            cause = exc.__cause__
            if _is_missing_security_data_error(cause):
                logger.warning(
                    "AKShare security has no supported data request=%s error_type=%s; skipping",
                    label,
                    type(cause).__name__,
                )
                return pd.DataFrame()
            if isinstance(cause, TypeError) and _looks_like_signature_error(cause):
                raise RuntimeError(
                    f"AKShare SDK 接口签名不兼容 request={label}；请升级实际运行任务的 AKShare。"
                ) from exc
            raise

    def _wait_for_request_slot(self) -> None:
        if self._last_request_at <= 0:
            return
        remaining = self.config.request_interval_seconds - (time.monotonic() - self._last_request_at)
        if remaining > 0:
            time.sleep(remaining)


def normalize_us_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def normalize_us_symbol_list(values: Iterable[Any]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        symbol = normalize_us_symbol(value)
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        result.append(symbol)
    return result


def normalize_ths_concept_list(values: Iterable[Any]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        concept = str(value or "").strip()
        key = concept.casefold()
        if not concept or key in seen:
            continue
        seen.add(key)
        result.append(concept)
    return result


def _normalize_cn_stock_symbol(value: Any) -> str:
    symbol = str(value or "").strip()
    return symbol.zfill(6) if symbol.isdigit() and len(symbol) < 6 else symbol


@contextmanager
def _configured_proxy(proxy: str):
    normalized = str(proxy or "").strip()
    if not normalized:
        yield
        return
    with _PROXY_ENV_LOCK:
        previous = {key: os.environ.get(key) for key in _PROXY_ENV_KEYS}
        try:
            for key in _PROXY_ENV_KEYS:
                os.environ[key] = normalized
            yield
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


def _is_retryable_akshare_error(exc: Exception) -> bool:
    return not isinstance(exc, (AttributeError, IndexError, KeyError, TypeError, ValueError))


def _looks_like_signature_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(
        marker in text
        for marker in (
            "unexpected keyword argument",
            "required positional argument",
            "positional arguments but",
            "got multiple values for argument",
        )
    )


def _is_missing_security_data_error(exc: Exception | None) -> bool:
    if isinstance(exc, (IndexError, KeyError)):
        return True
    return isinstance(exc, TypeError) and not _looks_like_signature_error(exc)


def _instrument_type(symbol: str, name: str, market_id: str) -> str:
    upper_symbol = normalize_us_symbol(symbol)
    upper_name = str(name or "").upper()
    if market_id == "153":
        return "pink"
    if "^" in upper_symbol or re.search(r"-P[A-Z]?$", upper_symbol):
        return "preferred"
    if upper_symbol.endswith("W") and re.search(r"\b(WT|WTS|WARRANTS?)\b", upper_name):
        return "warrant"
    if upper_symbol.endswith("U") and re.search(r"\bUNITS?\b", upper_name):
        return "unit"
    if upper_symbol.endswith("R") and re.search(r"\bRIGHTS?\b", upper_name):
        return "right"
    if re.search(r"\b(ETF|ETN|FUND)\b", upper_name):
        return "fund"
    return "common_stock"


def _symbol_from_em_code(value: Any) -> str:
    text = normalize_us_symbol(value)
    match = re.match(r"^\d+\.(.+)$", text)
    return match.group(1) if match else text


def _market_id_from_em_code(value: Any) -> str:
    text = normalize_us_symbol(value)
    match = re.match(r"^(\d+)\..+$", text)
    return match.group(1) if match else ""


def _symbol_lookup_key(value: Any) -> str:
    return re.sub(r"[-._/]", "", normalize_us_symbol(value))


def _dedupe_symbols(
    values: Iterable[dict[str, str]],
    *,
    limit: int = 0,
) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in values:
        symbol = normalize_us_symbol(item.get("symbol"))
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        result.append(
            {
                "symbol": symbol,
                "em_code": normalize_us_symbol(item.get("em_code")),
            }
        )
    return result[:limit] if limit > 0 else result


def _as_dataframe(value: Any) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value.copy()
    return pd.DataFrame(value)


def _empty_frame(columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _value_series(frame: pd.DataFrame, *candidates: str) -> pd.Series:
    for name in candidates:
        if name in frame.columns:
            return frame[name]
    return pd.Series([None] * len(frame), index=frame.index, dtype=object)


def _text_series(frame: pd.DataFrame, *candidates: str) -> pd.Series:
    return _value_series(frame, *candidates).fillna("").astype(str).str.strip()


def _coalesced_text_series(frame: pd.DataFrame, *candidates: str) -> pd.Series:
    result = pd.Series([""] * len(frame), index=frame.index, dtype=object)
    for name in candidates:
        if name not in frame.columns:
            continue
        candidate = frame[name].fillna("").astype(str).str.strip()
        result = result.where(result != "", candidate)
    return result


def _number_series(frame: pd.DataFrame, *candidates: str) -> pd.Series:
    return pd.to_numeric(_value_series(frame, *candidates), errors="coerce")


def _record_get(record: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in record:
            return record[key]
    return None


def _string(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _optional_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = _date_text(value)
    parsed = pd.to_datetime(
        text,
        format="%Y%m%d" if len(text) == 8 and text.isdigit() else None,
        errors="coerce",
    )
    if pd.isna(parsed):
        return None
    return parsed.date()


def _date_value(value: str | date) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = _date_text(value)
    parsed = pd.to_datetime(
        text,
        format="%Y%m%d" if len(text) == 8 and text.isdigit() else None,
        errors="raise",
    )
    return parsed.date()


def _date_text(value: Any) -> str:
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return str(value).strip()


def _json_value(value: Any) -> Any:
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    item = getattr(value, "item", None)
    if callable(item) and not isinstance(value, (str, bytes)):
        try:
            return item()
        except Exception:
            pass
    return value


def _record_json(record: dict[str, Any]) -> str:
    return json.dumps(
        {str(key): _json_value(value) for key, value in record.items()},
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


__all__ = [
    "AkshareUSConfig",
    "AkshareUSProvider",
    "DAILY_COLUMNS",
    "EM_CONCEPT_CONS_COLUMNS",
    "EM_CONCEPT_HIST_COLUMNS",
    "EM_CONCEPT_NAME_COLUMNS",
    "FINANCIAL_INDICATOR_COLUMNS",
    "FINANCIAL_STATEMENT_COLUMNS",
    "INDEX_COLUMNS",
    "MINUTE_COLUMNS",
    "PROFILE_COLUMNS",
    "SPOT_COLUMNS",
    "THS_CONCEPT_INDEX_COLUMNS",
    "THS_CONCEPT_INFO_COLUMNS",
    "THS_CONCEPT_NAME_COLUMNS",
    "VALUATION_COLUMNS",
    "normalize_ths_concept_list",
    "normalize_us_symbol",
    "normalize_us_symbol_list",
]
