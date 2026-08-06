#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AKShare task definitions."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AkshareTaskSpec:
    task: str
    table_name: str
    cursor_field: str = ""
    uses_codes: bool = False
    kind: str = "snapshot"

    @property
    def supports_incremental(self) -> bool:
        return bool(self.cursor_field)


AKSHARE_TASK_SPECS: dict[str, AkshareTaskSpec] = {
    "us_spot": AkshareTaskSpec("us_spot", "ak_us_spot"),
    "us_daily_kline": AkshareTaskSpec(
        "us_daily_kline",
        "ak_us_daily_kline",
        cursor_field="trade_date",
        uses_codes=True,
        kind="price",
    ),
    "us_minute_kline": AkshareTaskSpec(
        "us_minute_kline",
        "ak_us_minute_kline",
        uses_codes=True,
        kind="minute",
    ),
    "us_company_profile": AkshareTaskSpec(
        "us_company_profile",
        "ak_us_company_profile",
        uses_codes=True,
    ),
    "us_financial_statement": AkshareTaskSpec(
        "us_financial_statement",
        "ak_us_financial_statement",
        uses_codes=True,
    ),
    "us_financial_indicator": AkshareTaskSpec(
        "us_financial_indicator",
        "ak_us_financial_indicator",
        uses_codes=True,
    ),
    "us_valuation": AkshareTaskSpec(
        "us_valuation",
        "ak_us_valuation",
        uses_codes=True,
    ),
    "us_index_daily": AkshareTaskSpec(
        "us_index_daily",
        "ak_us_index_daily",
        cursor_field="trade_date",
        kind="index",
    ),
    "stock_board_concept_name_ths": AkshareTaskSpec(
        "stock_board_concept_name_ths",
        "ak_stock_board_concept_name_ths",
    ),
    "stock_board_concept_index_ths": AkshareTaskSpec(
        "stock_board_concept_index_ths",
        "ak_stock_board_concept_index_ths",
        cursor_field="trade_date",
        uses_codes=True,
        kind="index",
    ),
    "stock_board_concept_info_ths": AkshareTaskSpec(
        "stock_board_concept_info_ths",
        "ak_stock_board_concept_info_ths",
        uses_codes=True,
    ),
}

AKSHARE_TASK_CHOICES = tuple(AKSHARE_TASK_SPECS)

US_INDEX_NAMES = {
    ".IXIC": "NASDAQ Composite",
    ".DJI": "Dow Jones Industrial Average",
    ".INX": "S&P 500",
    ".NDX": "NASDAQ 100",
}

FINANCIAL_STATEMENT_TYPES = (
    "资产负债表",
    "综合损益表",
    "现金流量表",
)

FINANCIAL_PERIOD_TYPES = (
    "年报",
    "单季报",
    "累计季报",
)

VALUATION_INDICATORS = (
    "总市值",
    "市盈率(TTM)",
    "市盈率(静)",
    "市净率",
    "市现率",
)


__all__ = [
    "AKSHARE_TASK_CHOICES",
    "AKSHARE_TASK_SPECS",
    "FINANCIAL_PERIOD_TYPES",
    "FINANCIAL_STATEMENT_TYPES",
    "US_INDEX_NAMES",
    "VALUATION_INDICATORS",
    "AkshareTaskSpec",
]
