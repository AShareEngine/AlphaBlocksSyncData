#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""yfinance / FinanceDatabase task and group definitions."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class YFinanceTaskSpec:
    task: str
    table_name: str
    cursor_field: str = ""
    uses_codes: bool = False
    kind: str = "snapshot"

    @property
    def supports_incremental(self) -> bool:
        return bool(self.cursor_field)


@dataclass(frozen=True)
class MarketGroupDefinition:
    code: str
    name: str
    benchmark_symbol: str
    member_etfs: tuple[str, ...] = ()

    @property
    def holding_etfs(self) -> tuple[str, ...]:
        return self.member_etfs or (self.benchmark_symbol,)


SECTOR_DEFINITIONS: tuple[MarketGroupDefinition, ...] = (
    MarketGroupDefinition("communication_services", "Communication Services", "XLC"),
    MarketGroupDefinition("consumer_discretionary", "Consumer Discretionary", "XLY"),
    MarketGroupDefinition("consumer_staples", "Consumer Staples", "XLP"),
    MarketGroupDefinition("energy", "Energy", "XLE"),
    MarketGroupDefinition("financials", "Financials", "XLF"),
    MarketGroupDefinition("health_care", "Health Care", "XLV"),
    MarketGroupDefinition("industrials", "Industrials", "XLI"),
    MarketGroupDefinition("materials", "Materials", "XLB"),
    MarketGroupDefinition("real_estate", "Real Estate", "XLRE"),
    MarketGroupDefinition("technology", "Technology", "XLK"),
    MarketGroupDefinition("utilities", "Utilities", "XLU"),
)


CONCEPT_DEFINITIONS: tuple[MarketGroupDefinition, ...] = (
    MarketGroupDefinition("artificial_intelligence", "Artificial Intelligence", "AIQ", ("AIQ", "BOTZ", "ROBO")),
    MarketGroupDefinition("semiconductors", "Semiconductors", "SMH", ("SMH", "SOXX")),
    MarketGroupDefinition("cybersecurity", "Cybersecurity", "CIBR", ("CIBR", "HACK")),
    MarketGroupDefinition("clean_energy", "Clean Energy", "ICLN", ("ICLN", "TAN")),
    MarketGroupDefinition("biotechnology", "Biotechnology", "XBI", ("XBI", "IBB")),
)


YFINANCE_TASK_SPECS: dict[str, YFinanceTaskSpec] = {
    "symbol_master": YFinanceTaskSpec("symbol_master", "yf_symbol_master"),
    "daily_kline": YFinanceTaskSpec(
        "daily_kline",
        "yf_daily_kline",
        cursor_field="trade_date",
        uses_codes=True,
        kind="price",
    ),
    "corporate_actions": YFinanceTaskSpec(
        "corporate_actions",
        "yf_corporate_actions",
        cursor_field="event_date",
        uses_codes=True,
        kind="action",
    ),
    "industry_membership": YFinanceTaskSpec("industry_membership", "yf_industry_membership"),
    "sector_daily": YFinanceTaskSpec(
        "sector_daily",
        "yf_sector_daily",
        cursor_field="trade_date",
        kind="sector_price",
    ),
    "concept_daily": YFinanceTaskSpec(
        "concept_daily",
        "yf_concept_daily",
        cursor_field="trade_date",
        kind="concept_price",
    ),
    "concept_membership": YFinanceTaskSpec("concept_membership", "yf_concept_membership"),
}

YFINANCE_TASK_CHOICES = tuple(YFINANCE_TASK_SPECS)


__all__ = [
    "CONCEPT_DEFINITIONS",
    "MarketGroupDefinition",
    "SECTOR_DEFINITIONS",
    "YFINANCE_TASK_CHOICES",
    "YFINANCE_TASK_SPECS",
    "YFinanceTaskSpec",
]
