#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Free US market data provider using yfinance and FinanceDatabase."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

import pandas as pd

from sync_data_system.config_paths import resolve_runtime_config_path
from sync_data_system.providers.yfinance.specs import MarketGroupDefinition
from sync_data_system.runtime_config import load_runtime_config


logger = logging.getLogger(__name__)

MAIN_US_EXCHANGES = frozenset(
    {
        "ASE",
        "NASDAQ",
        "NASDAQ CAPITAL MARKET",
        "NASDAQ GLOBAL MARKET",
        "NASDAQ GLOBAL SELECT",
        "NCM",
        "NGM",
        "NMS",
        "NYSE",
        "NYSE AMERICAN",
        "NEW YORK STOCK EXCHANGE",
        "NYQ",
    }
)
OTC_EXCHANGES = frozenset({"OQB", "OQX", "OTC", "OTC MARKETS", "PNK"})

SYMBOL_MASTER_COLUMNS = (
    "symbol",
    "name",
    "currency",
    "sector",
    "industry_group",
    "industry",
    "exchange",
    "market",
    "country",
    "state",
    "city",
    "zipcode",
    "website",
    "market_cap",
    "summary",
    "isin",
    "cusip",
    "figi",
    "composite_figi",
    "shareclass_figi",
)

PRICE_COLUMNS = (
    "symbol",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
    "dividends",
    "stock_splits",
    "capital_gains",
    "source",
    "fetched_at",
)


@dataclass(frozen=True)
class YFinanceConfig:
    batch_size: int = 100
    threads: bool = True
    auto_adjust: bool = False
    repair: bool = False
    timeout: int = 30
    default_start_date: str = "2010-01-01"
    include_otc: bool = False

    @classmethod
    def from_env(cls, runtime_path: Optional[str | Path] = None) -> "YFinanceConfig":
        runtime = load_runtime_config(resolve_runtime_config_path(runtime_path))
        config = runtime.sync.yfinance
        return cls(
            batch_size=max(1, int(config.batch_size or 100)),
            threads=bool(config.threads),
            auto_adjust=bool(config.auto_adjust),
            repair=bool(config.repair),
            timeout=max(1, int(config.timeout or 30)),
            default_start_date=str(config.default_start_date or "2010-01-01").strip() or "2010-01-01",
            include_otc=bool(config.include_otc),
        )


class YFinanceProvider:
    def __init__(
        self,
        config: YFinanceConfig,
        *,
        yfinance_module: Any | None = None,
        finance_database_module: Any | None = None,
    ) -> None:
        self.config = config
        self._yfinance_module = yfinance_module
        self._finance_database_module = finance_database_module

    def close(self) -> None:
        return None

    def fetch_symbol_master(
        self,
        *,
        limit: int = 0,
        snapshot_date: date | None = None,
    ) -> pd.DataFrame:
        raw = self._finance_database.Equities().select()
        frame = _as_dataframe(raw)
        if frame.empty:
            return _empty_frame((*SYMBOL_MASTER_COLUMNS, "snapshot_date", "source"))

        frame = _normalize_columns(frame)
        if "symbol" not in frame.columns:
            first_column = str(frame.columns[0]) if len(frame.columns) else ""
            if first_column in {"index", "ticker", "code"}:
                frame = frame.rename(columns={first_column: "symbol"})
        if "symbol" not in frame.columns:
            raise ValueError("FinanceDatabase equities 数据缺少 symbol 字段。")

        frame["symbol"] = frame["symbol"].map(normalize_us_symbol)
        frame = frame[frame["symbol"] != ""].copy()
        frame = self._filter_us_listings(frame)
        frame = _ensure_columns(frame, SYMBOL_MASTER_COLUMNS)
        frame = frame.loc[:, list(SYMBOL_MASTER_COLUMNS)]
        frame = frame.drop_duplicates(subset=["symbol"], keep="first").sort_values("symbol")
        if limit > 0:
            frame = frame.head(limit)
        frame["snapshot_date"] = snapshot_date or date.today()
        frame["source"] = "financedatabase"
        return frame.reset_index(drop=True)

    def fetch_industry_membership(
        self,
        *,
        symbol_master: pd.DataFrame | None = None,
        limit: int = 0,
        snapshot_date: date | None = None,
    ) -> pd.DataFrame:
        master = symbol_master
        if master is None:
            master = self.fetch_symbol_master(limit=limit, snapshot_date=snapshot_date)
        columns = (
            "snapshot_date",
            "symbol",
            "sector",
            "industry_group",
            "industry",
            "exchange",
            "source",
        )
        if master.empty:
            return _empty_frame(columns)
        result = _ensure_columns(master.copy(), columns)
        result = result.loc[:, list(columns)]
        result["source"] = "financedatabase"
        return result.reset_index(drop=True)

    def fetch_daily(
        self,
        symbols: Sequence[str],
        *,
        start_date: str | date,
        end_date: str | date,
    ) -> pd.DataFrame:
        codes = normalize_us_symbol_list(symbols)
        if not codes:
            return _empty_frame(PRICE_COLUMNS)
        start = _parse_date(start_date)
        end = _parse_date(end_date)
        if start > end:
            return _empty_frame(PRICE_COLUMNS)

        raw = self._yfinance.download(
            tickers=codes,
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),
            interval="1d",
            group_by="ticker",
            auto_adjust=self.config.auto_adjust,
            actions=True,
            threads=self.config.threads,
            repair=self.config.repair,
            progress=False,
            timeout=self.config.timeout,
        )
        raw_frame = _as_dataframe(raw, reset_index=False)
        frames = [
            _standardize_price_frame(symbol_frame, symbol)
            for symbol, symbol_frame in _split_download_frame(raw_frame, codes)
        ]
        frames = [frame for frame in frames if not frame.empty]
        if not frames:
            return _empty_frame(PRICE_COLUMNS)
        result = pd.concat(frames, ignore_index=True)
        result = result.loc[:, list(PRICE_COLUMNS)].sort_values(["symbol", "trade_date"]).reset_index(drop=True)
        result.attrs["coverage_by_symbol"] = _coverage_by_symbol(result, "symbol", "trade_date")
        return result

    def fetch_corporate_actions(
        self,
        symbols: Sequence[str],
        *,
        start_date: str | date,
        end_date: str | date,
    ) -> pd.DataFrame:
        prices = self.fetch_daily(symbols, start_date=start_date, end_date=end_date)
        columns = (
            "symbol",
            "event_date",
            "dividend",
            "stock_split",
            "capital_gain",
            "source",
            "fetched_at",
        )
        if prices.empty:
            return _empty_frame(columns)
        result = prices.rename(
            columns={
                "trade_date": "event_date",
                "dividends": "dividend",
                "stock_splits": "stock_split",
                "capital_gains": "capital_gain",
            }
        )
        coverage = dict(prices.attrs.get("coverage_by_symbol", {}))
        action_total = (
            result[["dividend", "stock_split", "capital_gain"]]
            .fillna(0)
            .abs()
            .sum(axis=1)
        )
        result = result.loc[action_total > 0, list(columns)]
        result = result.reset_index(drop=True)
        result.attrs["coverage_by_symbol"] = coverage
        return result

    def fetch_group_daily(
        self,
        definitions: Sequence[MarketGroupDefinition],
        *,
        start_date: str | date,
        end_date: str | date,
    ) -> pd.DataFrame:
        mapping = pd.DataFrame(
            [
                {
                    "group_code": definition.code,
                    "group_name": definition.name,
                    "benchmark_symbol": normalize_us_symbol(definition.benchmark_symbol),
                }
                for definition in definitions
            ]
        )
        columns = (
            "group_code",
            "group_name",
            "benchmark_symbol",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "adj_close",
            "volume",
            "source",
            "fetched_at",
        )
        if mapping.empty:
            return _empty_frame(columns)
        prices = self.fetch_daily(
            mapping["benchmark_symbol"].tolist(),
            start_date=start_date,
            end_date=end_date,
        )
        if prices.empty:
            return _empty_frame(columns)
        prices = prices.rename(columns={"symbol": "benchmark_symbol"})
        result = mapping.merge(prices, on="benchmark_symbol", how="inner")
        result = result.loc[:, list(columns)].reset_index(drop=True)
        result.attrs["coverage_by_symbol"] = _coverage_by_symbol(
            result,
            "benchmark_symbol",
            "trade_date",
        )
        return result

    def fetch_concept_membership(
        self,
        definitions: Sequence[MarketGroupDefinition],
        *,
        snapshot_date: date | None = None,
    ) -> pd.DataFrame:
        columns = (
            "snapshot_date",
            "concept_code",
            "concept_name",
            "etf_symbol",
            "symbol",
            "holding_name",
            "weight",
            "membership_scope",
            "source",
            "fetched_at",
        )
        fetched_at = _utcnow()
        snapshot = snapshot_date or date.today()
        rows: list[dict[str, Any]] = []
        for definition in definitions:
            for etf_symbol in definition.holding_etfs:
                normalized_etf = normalize_us_symbol(etf_symbol)
                try:
                    holdings = self._yfinance.Ticker(normalized_etf).funds_data.top_holdings
                    frame = _normalize_holdings(holdings)
                except Exception as exc:
                    logger.warning("Unable to fetch ETF top holdings etf=%s: %s", normalized_etf, exc)
                    continue
                for item in frame.to_dict("records"):
                    rows.append(
                        {
                            "snapshot_date": snapshot,
                            "concept_code": definition.code,
                            "concept_name": definition.name,
                            "etf_symbol": normalized_etf,
                            "symbol": normalize_us_symbol(item.get("symbol")),
                            "holding_name": str(item.get("holding_name") or ""),
                            "weight": _optional_float(item.get("weight")),
                            "membership_scope": "top_holdings",
                            "source": "yfinance",
                            "fetched_at": fetched_at,
                        }
                    )
        if not rows:
            return _empty_frame(columns)
        result = pd.DataFrame(rows)
        result = result[result["symbol"] != ""].drop_duplicates(
            subset=["snapshot_date", "concept_code", "etf_symbol", "symbol"],
            keep="first",
        )
        return result.loc[:, list(columns)].reset_index(drop=True)

    def _filter_us_listings(self, frame: pd.DataFrame) -> pd.DataFrame:
        allowed = set(MAIN_US_EXCHANGES)
        if self.config.include_otc:
            allowed.update(OTC_EXCHANGES)
        mask = pd.Series(False, index=frame.index)
        for column in ("exchange", "market"):
            if column not in frame.columns:
                continue
            normalized = frame[column].fillna("").astype(str).str.strip().str.upper()
            mask |= normalized.isin(allowed)
            mask |= normalized.str.contains(r"\bNASDAQ\b|\bNYSE\b|NEW YORK STOCK EXCHANGE", regex=True)
            if self.config.include_otc:
                mask |= normalized.str.contains(r"\bOTC\b|PINK", regex=True)
        return frame.loc[mask].copy()

    @property
    def _yfinance(self) -> Any:
        if self._yfinance_module is None:
            try:
                import yfinance
            except ImportError as exc:
                raise RuntimeError(
                    "缺少 yfinance 依赖，请运行 "
                    "`python3 scripts/install_provider_deps.py yfinance`。"
                ) from exc
            self._yfinance_module = yfinance
        return self._yfinance_module

    @property
    def _finance_database(self) -> Any:
        if self._finance_database_module is None:
            try:
                import financedatabase
            except ImportError as exc:
                raise RuntimeError(
                    "缺少 financedatabase 依赖，请运行 "
                    "`python3 scripts/install_provider_deps.py yfinance`。"
                ) from exc
            self._finance_database_module = financedatabase
        return self._finance_database_module


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


def _as_dataframe(value: Any, *, reset_index: bool = True) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        frame = value.copy()
    else:
        frame = pd.DataFrame(value)
    if reset_index and not isinstance(frame.index, pd.RangeIndex):
        frame = frame.reset_index()
    return frame


def _normalize_column_name(value: Any) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")
    aliases = {
        "adjclose": "adj_close",
        "adj_close": "adj_close",
        "capital_gains": "capital_gains",
        "stock_splits": "stock_splits",
        "holding_percent": "weight",
        "holding_percentage": "weight",
        "ticker": "symbol",
    }
    return aliases.get(text, text)


def _normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    result.columns = [_normalize_column_name(column) for column in result.columns]
    return result


def _ensure_columns(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    result = frame.copy()
    for column in columns:
        if column not in result.columns:
            result[column] = None
    return result


def _empty_frame(columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _split_download_frame(
    frame: pd.DataFrame,
    symbols: Sequence[str],
) -> list[tuple[str, pd.DataFrame]]:
    if frame.empty:
        return []
    if not isinstance(frame.columns, pd.MultiIndex):
        return [(symbols[0], frame.copy())] if len(symbols) == 1 else []

    normalized_symbols = {normalize_us_symbol(symbol): symbol for symbol in symbols}
    for level in range(frame.columns.nlevels):
        available = {
            normalize_us_symbol(value): value
            for value in frame.columns.get_level_values(level).unique()
        }
        if not set(normalized_symbols).intersection(available):
            continue
        result: list[tuple[str, pd.DataFrame]] = []
        for normalized, original_symbol in normalized_symbols.items():
            if normalized not in available:
                continue
            sliced = frame.xs(available[normalized], axis=1, level=level, drop_level=True)
            result.append((original_symbol, sliced.copy()))
        return result

    if len(symbols) == 1:
        flattened = frame.copy()
        flattened.columns = [column[0] for column in flattened.columns]
        return [(symbols[0], flattened)]
    return []


def _standardize_price_frame(frame: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if frame.empty:
        return _empty_frame(PRICE_COLUMNS)
    result = frame.copy()
    if isinstance(result.columns, pd.MultiIndex):
        result.columns = [column[-1] for column in result.columns]
    result = _normalize_columns(result)
    result = result.reset_index()
    result = _normalize_columns(result)
    date_column = next(
        (column for column in ("date", "datetime", "index") if column in result.columns),
        str(result.columns[0]),
    )
    result = result.rename(columns={date_column: "trade_date"})
    result["trade_date"] = pd.to_datetime(result["trade_date"], errors="coerce").dt.date
    result["symbol"] = normalize_us_symbol(symbol)
    for column in (
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "dividends",
        "stock_splits",
        "capital_gains",
    ):
        if column not in result.columns:
            result[column] = 0.0 if column in {"dividends", "stock_splits", "capital_gains"} else None
        result[column] = pd.to_numeric(result[column], errors="coerce")
    result["source"] = "yfinance"
    result["fetched_at"] = _utcnow()
    result = result[result["trade_date"].notna() & result["close"].notna()]
    return _ensure_columns(result, PRICE_COLUMNS).loc[:, list(PRICE_COLUMNS)]


def _normalize_holdings(value: Any) -> pd.DataFrame:
    frame = _as_dataframe(value)
    if frame.empty:
        return _empty_frame(("symbol", "holding_name", "weight"))
    frame = _normalize_columns(frame)
    if "symbol" not in frame.columns:
        first_column = str(frame.columns[0])
        frame = frame.rename(columns={first_column: "symbol"})
    if "holding_name" not in frame.columns and "name" in frame.columns:
        frame = frame.rename(columns={"name": "holding_name"})
    if "weight" not in frame.columns:
        candidate = next(
            (column for column in frame.columns if "percent" in column or column.endswith("weight")),
            None,
        )
        frame["weight"] = frame[candidate] if candidate else None
    return _ensure_columns(frame, ("symbol", "holding_name", "weight")).loc[
        :, ["symbol", "holding_name", "weight"]
    ]


def _parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    digits = "".join(character for character in text if character.isdigit())
    if len(digits) != 8:
        raise ValueError(f"日期必须是 YYYYMMDD / YYYY-MM-DD，当前值: {value!r}")
    return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))


def _optional_float(value: Any) -> float | None:
    try:
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _coverage_by_symbol(
    frame: pd.DataFrame,
    symbol_column: str,
    date_column: str,
) -> dict[str, date]:
    if frame.empty or symbol_column not in frame.columns or date_column not in frame.columns:
        return {}
    coverage: dict[str, date] = {}
    for symbol, values in frame.groupby(symbol_column)[date_column]:
        maximum = values.dropna().max()
        if isinstance(maximum, datetime):
            maximum = maximum.date()
        if isinstance(maximum, date):
            coverage[normalize_us_symbol(symbol)] = maximum
    return coverage


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


__all__ = [
    "MAIN_US_EXCHANGES",
    "OTC_EXCHANGES",
    "PRICE_COLUMNS",
    "SYMBOL_MASTER_COLUMNS",
    "YFinanceConfig",
    "YFinanceProvider",
    "normalize_us_symbol",
    "normalize_us_symbol_list",
]
