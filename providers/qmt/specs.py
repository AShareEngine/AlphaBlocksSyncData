#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""QMT REST task specifications."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class QmtTaskSpec:
    task: str
    method: str
    path: str
    table_name: str
    item_collection_key: str = ""
    response_key: str = ""
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
    default_markets: tuple[str, ...] = ()
    default_index_codes: tuple[str, ...] = ()
    default_table_names: tuple[str, ...] = ()
    default_code_markets: tuple[str, ...] = ()
    row_kind: str = "item"
    cursor_path: tuple[str, ...] = ()
    cursor_granularity: str = "day"
    auto_symbol_universe: bool = False
    auto_symbol_universe_sector: str = ""
    auto_historical_symbol_universe_sector: str = ""
    auto_include_historical_universe: bool = True

    @property
    def supports_incremental(self) -> bool:
        return bool(self.uses_begin_end and self.cursor_path)


QMT_TASK_SPECS: dict[str, QmtTaskSpec] = {
    "kline_history": QmtTaskSpec(
        task="kline_history",
        method="POST",
        path="/kline-history",
        table_name="qmt_kline_history",
        item_collection_key="items",
        uses_symbols=True,
        uses_begin_end=True,
        uses_period=True,
        uses_fields=True,
        uses_adjust_type=True,
        uses_fill_data=True,
        default_period="1d",
        row_kind="bar",
        cursor_path=("time_ms",),
        auto_symbol_universe=True,
    ),
    "tick_history": QmtTaskSpec(
        task="tick_history",
        method="POST",
        path="/tick-history",
        table_name="qmt_tick_history",
        item_collection_key="items",
        uses_symbols=True,
        uses_begin_end=True,
        uses_fields=True,
        uses_adjust_type=True,
        row_kind="tick",
        cursor_path=("time_ms",),
        auto_symbol_universe=True,
    ),
    "full_tick": QmtTaskSpec(
        task="full_tick",
        method="POST",
        path="/full-tick",
        table_name="qmt_full_tick",
        item_collection_key="items",
        uses_symbols=True,
        row_kind="tick",
        auto_symbol_universe=True,
        auto_include_historical_universe=False,
    ),
    "financial": QmtTaskSpec(
        task="financial",
        method="POST",
        path="/financial",
        table_name="qmt_financial",
        item_collection_key="items",
        uses_symbols=True,
        uses_table_names=True,
        uses_begin_end=True,
        row_kind="financial",
        default_table_names=("Balance", "Income", "CashFlow"),
        auto_symbol_universe=True,
    ),
    "instrument": QmtTaskSpec(
        task="instrument",
        method="GET",
        path="/instrument/{symbol}",
        table_name="qmt_instrument",
        response_key="",
        uses_symbol=True,
        uses_complete=True,
        row_kind="fields",
        auto_symbol_universe=True,
    ),
    "trading_calendar": QmtTaskSpec(
        task="trading_calendar",
        method="POST",
        path="/trading-calendar",
        table_name="qmt_trading_calendar",
        uses_market=True,
        uses_begin_end=True,
        row_kind="calendar_date",
        cursor_path=("date",),
        default_markets=("SH", "SZ"),
    ),
    "index_weight": QmtTaskSpec(
        task="index_weight",
        method="POST",
        path="/index-weight",
        table_name="qmt_index_weight",
        uses_index_code=True,
        row_kind="component",
        default_index_codes=("000300.SH",),
    ),
    "sectors": QmtTaskSpec(
        task="sectors",
        method="GET",
        path="/sectors",
        table_name="qmt_sectors",
        item_collection_key="items",
        uses_sector_name=True,
        row_kind="sector",
    ),
    "l2_quote": QmtTaskSpec(
        task="l2_quote",
        method="POST",
        path="/l2/quote",
        table_name="qmt_l2_quote",
        item_collection_key="items",
        uses_symbols=True,
        uses_begin_end=True,
        row_kind="quote",
        cursor_path=("time_ms",),
        auto_symbol_universe=True,
        auto_include_historical_universe=False,
    ),
    "l2_order": QmtTaskSpec(
        task="l2_order",
        method="POST",
        path="/l2/order",
        table_name="qmt_l2_order",
        item_collection_key="items",
        uses_symbols=True,
        uses_begin_end=True,
        row_kind="order",
        cursor_path=("time_ms",),
        auto_symbol_universe=True,
        auto_include_historical_universe=False,
    ),
    "l2_transaction": QmtTaskSpec(
        task="l2_transaction",
        method="POST",
        path="/l2/transaction",
        table_name="qmt_l2_transaction",
        item_collection_key="items",
        uses_symbols=True,
        uses_begin_end=True,
        row_kind="transaction",
        cursor_path=("time_ms",),
        auto_symbol_universe=True,
        auto_include_historical_universe=False,
    ),
    "market_data_ex": QmtTaskSpec(
        task="market_data_ex",
        method="POST",
        path="/market-data-ex",
        table_name="qmt_market_data_ex",
        item_collection_key="items",
        uses_symbols=True,
        uses_begin_end=True,
        uses_period=True,
        uses_count=True,
        uses_fields=True,
        uses_adjust_type=True,
        uses_fill_data=True,
        default_period="1d",
        row_kind="frame",
        cursor_path=("time_ms",),
        auto_symbol_universe=True,
    ),
    "local_data": QmtTaskSpec(
        task="local_data",
        method="POST",
        path="/local-data",
        table_name="qmt_local_data",
        item_collection_key="items",
        uses_symbols=True,
        uses_begin_end=True,
        uses_period=True,
        uses_count=True,
        uses_fields=True,
        uses_adjust_type=True,
        uses_fill_data=True,
        default_period="1d",
        row_kind="frame",
        cursor_path=("time_ms",),
        auto_symbol_universe=True,
    ),
    "full_kline": QmtTaskSpec(
        task="full_kline",
        method="POST",
        path="/full-kline",
        table_name="qmt_full_kline",
        item_collection_key="items",
        uses_symbols=True,
        uses_period=True,
        uses_count=True,
        uses_fields=True,
        uses_adjust_type=True,
        uses_fill_data=True,
        default_period="1d",
        row_kind="bar",
        auto_symbol_universe=True,
        auto_include_historical_universe=False,
    ),
    "instrument_type": QmtTaskSpec(
        task="instrument_type",
        method="GET",
        path="/instrument-type/{symbol}",
        table_name="qmt_instrument_type",
        uses_symbol=True,
        row_kind="type",
        auto_symbol_universe=True,
    ),
    "trade_times": QmtTaskSpec(
        task="trade_times",
        method="GET",
        path="/trade-times/{symbol}",
        table_name="qmt_trade_times",
        uses_symbol=True,
        row_kind="trade_times",
        auto_symbol_universe=True,
        auto_include_historical_universe=False,
    ),
    "main_contract": QmtTaskSpec(
        task="main_contract",
        method="GET",
        path="/main-contract/{code_market}",
        table_name="qmt_main_contract",
        uses_code_market=True,
        row_kind="main_contract",
        default_code_markets=("IF.CFFEX",),
    ),
    "trading_dates": QmtTaskSpec(
        task="trading_dates",
        method="POST",
        path="/trading-dates",
        table_name="qmt_trading_dates",
        uses_market=True,
        uses_begin_end=True,
        uses_count=True,
        row_kind="calendar_date",
        cursor_path=("date",),
        default_markets=("SH", "SZ"),
    ),
    "holidays": QmtTaskSpec(
        task="holidays",
        method="GET",
        path="/holidays",
        table_name="qmt_holidays",
        row_kind="holiday",
    ),
    "periods": QmtTaskSpec(
        task="periods",
        method="GET",
        path="/periods",
        table_name="qmt_periods",
        row_kind="period",
    ),
    "data_dir": QmtTaskSpec(
        task="data_dir",
        method="GET",
        path="/data-dir",
        table_name="qmt_data_dir",
        row_kind="data_dir",
    ),
    "divid_factors": QmtTaskSpec(
        task="divid_factors",
        method="POST",
        path="/divid-factors",
        table_name="qmt_divid_factors",
        uses_stock_code=True,
        uses_begin_end=True,
        row_kind="factor_collection",
        auto_symbol_universe=True,
    ),
    "cb_info": QmtTaskSpec(
        task="cb_info",
        method="GET",
        path="/cb-info/{symbol}",
        table_name="qmt_cb_info",
        uses_symbol=True,
        row_kind="fields",
        auto_symbol_universe=True,
        auto_symbol_universe_sector="沪深转债",
        auto_historical_symbol_universe_sector="过期沪深转债",
    ),
    "ipo_info": QmtTaskSpec(
        task="ipo_info",
        method="GET",
        path="/ipo-info",
        table_name="qmt_ipo_info",
        row_kind="ipo",
    ),
    "etf_info": QmtTaskSpec(
        task="etf_info",
        method="GET",
        path="/etf-info",
        table_name="qmt_etf_info",
        row_kind="fields",
    ),
    "download_history": QmtTaskSpec(
        task="download_history",
        method="POST",
        path="/download/history",
        table_name="qmt_download_history",
        uses_stock_code=True,
        uses_begin_end=True,
        uses_period=True,
        uses_incrementally=True,
        default_period="1d",
        row_kind="download_result",
        auto_symbol_universe=True,
    ),
    "download_history_batch": QmtTaskSpec(
        task="download_history_batch",
        method="POST",
        path="/download/history/batch",
        table_name="qmt_download_history_batch",
        uses_symbols=True,
        uses_begin_end=True,
        uses_period=True,
        uses_incrementally=True,
        default_period="1d",
        row_kind="download_result",
        auto_symbol_universe=True,
    ),
    "download_financial": QmtTaskSpec(
        task="download_financial",
        method="POST",
        path="/download/financial",
        table_name="qmt_download_financial",
        uses_symbols=True,
        uses_table_names=True,
        uses_begin_end=True,
        row_kind="download_result",
        default_table_names=("Balance", "Income", "CashFlow"),
        auto_symbol_universe=True,
    ),
    "download_index_weight": QmtTaskSpec(
        task="download_index_weight",
        method="POST",
        path="/download/index-weight",
        table_name="qmt_download_index_weight",
        uses_index_code=True,
        row_kind="download_result",
    ),
    "download_history_contracts": QmtTaskSpec(
        task="download_history_contracts",
        method="POST",
        path="/download/history-contracts",
        table_name="qmt_download_history_contracts",
        uses_market=True,
        row_kind="download_result",
        default_markets=("SH", "SZ"),
    ),
    "download_sector": QmtTaskSpec(
        task="download_sector",
        method="POST",
        path="/download/sector",
        table_name="qmt_download_sector",
        uses_sector_name=True,
        row_kind="download_result",
    ),
    "download_holiday": QmtTaskSpec(
        task="download_holiday",
        method="POST",
        path="/download/holiday",
        table_name="qmt_download_holiday",
        row_kind="download_result",
    ),
    "download_cb": QmtTaskSpec(
        task="download_cb",
        method="POST",
        path="/download/cb",
        table_name="qmt_download_cb",
        row_kind="download_result",
    ),
    "download_etf": QmtTaskSpec(
        task="download_etf",
        method="POST",
        path="/download/etf",
        table_name="qmt_download_etf",
        row_kind="download_result",
    ),
}

QMT_TASK_CHOICES = tuple(QMT_TASK_SPECS.keys())
def order_by_columns_for_spec(spec: QmtTaskSpec) -> tuple[str, ...]:
    if spec.row_kind in {"bar", "tick", "quote"}:
        return ("symbol", "time_ms")
    if spec.row_kind == "order":
        return ("symbol", "time_ms", "entrust_no")
    if spec.row_kind == "transaction":
        return ("symbol", "time_ms", "trade_index")
    if spec.row_kind == "component":
        return ("index_code", "symbol")
    if spec.row_kind == "sector":
        return ("sector_name",)
    if spec.row_kind == "financial":
        return ("symbol", "table_name")
    if spec.row_kind in {"fields", "type", "trade_times", "frame"}:
        return ("symbol",)
    if spec.row_kind == "calendar_date":
        return ("market", "date")
    if spec.row_kind == "main_contract":
        return ("code_market",)
    if spec.row_kind == "factor_collection":
        return ("stock_code",)
    if spec.row_kind == "ipo":
        return ("securityCode",)
    if spec.row_kind == "holiday":
        return ("date",)
    if spec.row_kind == "period":
        return ("period",)
    if spec.row_kind == "data_dir":
        return ("data_dir",)
    if spec.row_kind == "download_result":
        return ("function",)
    return ("symbol",)


__all__ = [
    "QMT_TASK_CHOICES",
    "QMT_TASK_SPECS",
    "QmtTaskSpec",
    "order_by_columns_for_spec",
]
