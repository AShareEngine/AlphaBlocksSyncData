#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""BaoStock 官方 SDK 适配层."""

from __future__ import annotations

from datetime import date, timedelta
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

from aiquantbase.runtime_config import load_runtime_config
from sync_data_system.config_paths import resolve_runtime_config_path
from sync_data_system.sources.baostock.specs import BAOSTOCK_TASK_SPECS


logger = logging.getLogger(__name__)

DEFAULT_KLINE_FIELDS = (
    "date,code,open,high,low,close,preclose,volume,amount,"
    "adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"
)


@dataclass(frozen=True)
class BaoStockConfig:
    user_id: str = "anonymous"
    password: str = "123456"

    @classmethod
    def from_env(cls, runtime_path: Optional[str | Path] = None) -> "BaoStockConfig":
        resolved_runtime_path = resolve_runtime_config_path(runtime_path)
        runtime = load_runtime_config(resolved_runtime_path)
        sync_config = runtime.sync.baostock
        return cls(
            user_id=str(sync_config.user_id or "anonymous").strip() or "anonymous",
            password=str(sync_config.password or "123456").strip() or "123456",
        )


class BaoStockSession:
    def __init__(self, config: BaoStockConfig) -> None:
        self.config = config
        self._bs = None
        self._connected = False

    def ensure_connected(self) -> None:
        if self._connected:
            return
        try:
            import baostock as bs
        except Exception as exc:  # pragma: no cover
            raise RuntimeError("未安装 baostock，请先执行 `pip install baostock`。") from exc

        result = bs.login(user_id=self.config.user_id, password=self.config.password)
        if getattr(result, "error_code", "") != "0":
            raise RuntimeError(
                f"BaoStock 登录失败 error_code={getattr(result, 'error_code', '')} "
                f"error_msg={getattr(result, 'error_msg', '')}"
            )
        self._bs = bs
        self._connected = True
        logger.info("BaoStock login success user_id=%s", self.config.user_id)

    @property
    def client(self):
        self.ensure_connected()
        return self._bs

    def close(self) -> None:
        if not self._connected or self._bs is None:
            return
        try:
            self._bs.logout()
        except Exception:
            logger.exception("BaoStock logout failed")
        finally:
            self._connected = False
            self._bs = None


class BaoStockProvider:
    def __init__(self, config: BaoStockConfig) -> None:
        self.session = BaoStockSession(config)
        self._all_stock_codes_cache: dict[str, list[str]] = {}

    def close(self) -> None:
        self.session.close()

    def fetch_dataframe(
        self,
        task: str,
        *,
        code: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        day: str | None = None,
        year: int | str | None = None,
        quarter: int | str | None = None,
        year_type: str | None = None,
        adjustflag: str = "3",
        frequency: str = "d",
    ):
        if pd is None:  # pragma: no cover
            raise RuntimeError("未安装 pandas，无法处理 BaoStock 返回数据。")

        spec = BAOSTOCK_TASK_SPECS[task]
        bs = self.session.client

        if task == "trade_dates":
            rs = bs.query_trade_dates(start_date=_to_baostock_day(start_date), end_date=_to_baostock_day(end_date))
        elif task == "all_stock":
            rs = bs.query_all_stock(day=_to_baostock_day(day))
        elif task == "stock_basic":
            rs = bs.query_stock_basic(code=to_baostock_code(code or ""))
        elif task == "adjust_factor":
            rs = bs.query_adjust_factor(
                code=to_baostock_code(code or ""),
                start_date=_to_baostock_day(start_date),
                end_date=_to_baostock_day(end_date),
            )
        elif task == "daily_kline":
            rs = bs.query_history_k_data_plus(
                to_baostock_code(code or ""),
                DEFAULT_KLINE_FIELDS,
                start_date=_to_baostock_day(start_date),
                end_date=_to_baostock_day(end_date),
                frequency=frequency,
                adjustflag=adjustflag,
            )
        elif task == "hs300_stocks":
            rs = bs.query_hs300_stocks(date=_to_baostock_day(day))
        elif task == "sz50_stocks":
            rs = bs.query_sz50_stocks(date=_to_baostock_day(day))
        elif task == "zz500_stocks":
            rs = bs.query_zz500_stocks(date=_to_baostock_day(day))
        elif task == "stock_industry":
            rs = bs.query_stock_industry(code=to_baostock_code(code or ""), date=_to_baostock_day(day))
        elif task == "dividend_data":
            rs = bs.query_dividend_data(
                code=to_baostock_code(code or ""),
                year=str(year or ""),
                yearType=str(year_type or "report"),
            )
        elif task == "profit_data":
            rs = bs.query_profit_data(code=to_baostock_code(code or ""), year=year, quarter=quarter)
        elif task == "operation_data":
            rs = bs.query_operation_data(code=to_baostock_code(code or ""), year=year, quarter=quarter)
        elif task == "growth_data":
            rs = bs.query_growth_data(code=to_baostock_code(code or ""), year=year, quarter=quarter)
        elif task == "dupont_data":
            rs = bs.query_dupont_data(code=to_baostock_code(code or ""), year=year, quarter=quarter)
        elif task == "balance_data":
            rs = bs.query_balance_data(code=to_baostock_code(code or ""), year=year, quarter=quarter)
        elif task == "cash_flow_data":
            rs = bs.query_cash_flow_data(code=to_baostock_code(code or ""), year=year, quarter=quarter)
        elif task == "performance_express_report":
            rs = bs.query_performance_express_report(
                to_baostock_code(code or ""),
                start_date=_to_baostock_day(start_date),
                end_date=_to_baostock_day(end_date),
            )
        elif task == "forecast_report":
            rs = bs.query_forecast_report(
                to_baostock_code(code or ""),
                start_date=_to_baostock_day(start_date),
                end_date=_to_baostock_day(end_date),
            )
        elif task == "deposit_rate_data":
            rs = bs.query_deposit_rate_data(start_date=_to_baostock_day(start_date), end_date=_to_baostock_day(end_date))
        elif task == "loan_rate_data":
            rs = bs.query_loan_rate_data(start_date=_to_baostock_day(start_date), end_date=_to_baostock_day(end_date))
        elif task == "required_reserve_ratio_data":
            rs = bs.query_required_reserve_ratio_data(
                start_date=_to_baostock_day(start_date),
                end_date=_to_baostock_day(end_date),
                yearType=str(year_type or "0"),
            )
        elif task == "money_supply_data_month":
            rs = bs.query_money_supply_data_month(
                start_date=_to_baostock_month(start_date),
                end_date=_to_baostock_month(end_date),
            )
        elif task == "money_supply_data_year":
            rs = bs.query_money_supply_data_year(
                start_date=_to_baostock_year(start_date),
                end_date=_to_baostock_year(end_date),
            )
        else:  # pragma: no cover
            raise ValueError(f"不支持的 BaoStock 任务: {task}")

        frame = _resultset_to_frame(rs)
        missing = [field for field in spec.fields if field not in frame.columns]
        if missing and not frame.empty:
            raise ValueError(f"BaoStock {task} 返回字段不匹配 missing={missing} actual={list(frame.columns)}")
        if missing and frame.empty:
            frame = pd.DataFrame(columns=list(spec.fields))
        return frame

    def fetch_all_stock_codes(self, day: str | None = None) -> list[str]:
        cache_key = _normalize_day_key(day)
        if cache_key in self._all_stock_codes_cache:
            return list(self._all_stock_codes_cache[cache_key])
        frame = self.fetch_dataframe("all_stock", day=day)
        if frame.empty or "code" not in frame.columns:
            self._all_stock_codes_cache[cache_key] = []
            return []
        codes = [normalize_baostock_code(value) for value in frame["code"].tolist()]
        resolved = [code for code in codes if code]
        self._all_stock_codes_cache[cache_key] = resolved
        return list(resolved)

    def fetch_latest_all_stock_codes(self, day: str | None = None, *, lookback_days: int = 30) -> tuple[str | None, list[str]]:
        """Return the nearest trading day with a non-empty all_stock universe."""
        anchor_day = _normalize_day_key(day) or date.today().strftime("%Y%m%d")
        candidate_days = [anchor_day]
        candidate_days.extend(day for day in self.resolve_recent_trading_days(anchor_day, lookback_days=lookback_days) if day != anchor_day)
        for candidate_day in candidate_days:
            codes = self.fetch_all_stock_codes(candidate_day)
            if codes:
                return candidate_day, codes
        return None, []

    def resolve_latest_trading_day(self, day: str | None = None, *, lookback_days: int = 30) -> str | None:
        days = self.resolve_recent_trading_days(day, lookback_days=lookback_days)
        return days[0] if days else None

    def resolve_recent_trading_days(self, day: str | None = None, *, lookback_days: int = 30) -> list[str]:
        anchor_day = _parse_day_value(day)
        start_day = anchor_day - timedelta(days=max(1, int(lookback_days)))
        frame = self.fetch_dataframe(
            "trade_dates",
            start_date=start_day.strftime("%Y%m%d"),
            end_date=anchor_day.strftime("%Y%m%d"),
        )
        if frame.empty or "calendar_date" not in frame.columns or "is_trading_day" not in frame.columns:
            return []

        trading_days: list[str] = []
        for _, row in frame.iterrows():
            if str(row.get("is_trading_day", "")).strip() != "1":
                continue
            normalized = _normalize_trade_day_output(row.get("calendar_date"))
            if normalized:
                trading_days.append(normalized)
        return list(reversed(trading_days))


def normalize_baostock_code(value: Any) -> str:
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


def to_baostock_code(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = re.fullmatch(r"(\d{6})\.(SH|SZ|BJ)", text, flags=re.IGNORECASE)
    if match:
        code, market = match.groups()
        return f"{market.lower()}.{code}"
    match = re.fullmatch(r"(?i)(sh|sz|bj)\.(\d{6})", text)
    if match:
        market, code = match.groups()
        return f"{market.lower()}.{code}"
    return text.lower()


def normalize_baostock_code_list(code_list: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for code in code_list:
        text = normalize_baostock_code(code)
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _resultset_to_frame(result) -> "pd.DataFrame":
    rows: list[list[Any]] = []
    fields = list(getattr(result, "fields", []) or [])
    error_code = str(getattr(result, "error_code", "") or "")
    error_msg = str(getattr(result, "error_msg", "") or "")
    if error_code != "0":
        raise RuntimeError(f"BaoStock 请求失败 error_code={error_code} error_msg={error_msg}")
    while result.error_code == "0" and result.next():
        rows.append(result.get_row_data())
    return pd.DataFrame(rows, columns=fields)


def _to_baostock_day(value: Any) -> str:
    text = _normalize_day_key(value)
    if not text:
        return ""
    if len(text) != 8:
        raise ValueError(f"BaoStock 日期必须是 YYYYMMDD 或 YYYY-MM-DD，当前值: {value!r}")
    return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"


def _normalize_day_key(value: Any) -> str:
    text = re.sub(r"[^0-9]", "", str(value or "").strip())
    if not text:
        return ""
    if len(text) != 8:
        raise ValueError(f"BaoStock 日期必须是 YYYYMMDD 或 YYYY-MM-DD，当前值: {value!r}")
    return text


def _to_baostock_month(value: Any) -> str:
    text = re.sub(r"[^0-9]", "", str(value or "").strip())
    if not text:
        return ""
    if len(text) < 6:
        raise ValueError(f"BaoStock 月份必须是 YYYYMM 或 YYYY-MM，当前值: {value!r}")
    return f"{text[0:4]}-{text[4:6]}"


def _to_baostock_year(value: Any) -> str:
    text = re.sub(r"[^0-9]", "", str(value or "").strip())
    if not text:
        return ""
    if len(text) < 4:
        raise ValueError(f"BaoStock 年份必须是 YYYY，当前值: {value!r}")
    return text[0:4]


def _parse_day_value(value: Any) -> date:
    text = re.sub(r"[^0-9]", "", str(value or "").strip())
    if not text:
        return date.today()
    if len(text) != 8:
        raise ValueError(f"BaoStock 日期必须是 YYYYMMDD 或 YYYY-MM-DD，当前值: {value!r}")
    return date(int(text[0:4]), int(text[4:6]), int(text[6:8]))


def _normalize_trade_day_output(value: Any) -> str:
    text = re.sub(r"[^0-9]", "", str(value or "").strip())
    if len(text) != 8:
        return ""
    return text


__all__ = [
    "BaoStockConfig",
    "BaoStockProvider",
    "BaoStockSession",
    "normalize_baostock_code",
    "normalize_baostock_code_list",
    "to_baostock_code",
]
