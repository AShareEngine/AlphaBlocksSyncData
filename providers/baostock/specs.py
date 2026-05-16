#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""BaoStock 同步任务规格."""

from __future__ import annotations

import re
from dataclasses import dataclass


def camel_to_snake(value: str) -> str:
    text = str(value).strip()
    if not text:
        return ""
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    text = re.sub(r"[^0-9a-zA-Z_]+", "_", text)
    return text.strip("_").lower()


@dataclass(frozen=True)
class BaoStockTaskSpec:
    task: str
    api_name: str
    table_name: str
    fields: tuple[str, ...]
    uses_code: bool = False
    uses_day: bool = False
    uses_begin_end: bool = False
    uses_year: bool = False
    uses_quarter: bool = False
    uses_year_type: bool = False
    auto_code_universe: bool = False
    cursor_fields: tuple[str, ...] = ()
    cursor_granularity: str = ""
    persist_request_columns: bool = True

    @property
    def field_columns(self) -> tuple[str, ...]:
        return tuple(camel_to_snake(field) for field in self.fields)

    @property
    def has_code_field(self) -> bool:
        return "code" in self.fields

    @property
    def cursor_columns(self) -> tuple[str, ...]:
        return tuple(camel_to_snake(field) for field in self.cursor_fields)


BAOSTOCK_TASK_SPECS: dict[str, BaoStockTaskSpec] = {
    "trade_dates": BaoStockTaskSpec(
        task="trade_dates",
        api_name="query_trade_dates",
        table_name="bs_trade_dates",
        fields=("calendar_date", "is_trading_day"),
        uses_begin_end=True,
        cursor_fields=("calendar_date",),
        cursor_granularity="day",
    ),
    "all_stock": BaoStockTaskSpec(
        task="all_stock",
        api_name="query_all_stock",
        table_name="bs_all_stock",
        fields=("code", "tradeStatus", "code_name"),
        uses_day=True,
        cursor_fields=("query_date",),
        cursor_granularity="day",
    ),
    "stock_basic": BaoStockTaskSpec(
        task="stock_basic",
        api_name="query_stock_basic",
        table_name="bs_stock_basic",
        fields=("code", "code_name", "ipoDate", "outDate", "type", "status"),
        uses_code=True,
        auto_code_universe=True,
    ),
    "adjust_factor": BaoStockTaskSpec(
        task="adjust_factor",
        api_name="query_adjust_factor",
        table_name="bs_adjust_factor",
        fields=("code", "dividOperateDate", "foreAdjustFactor", "backAdjustFactor", "adjustFactor"),
        uses_code=True,
        uses_begin_end=True,
        auto_code_universe=True,
        cursor_fields=("dividOperateDate",),
        cursor_granularity="day",
    ),
    "daily_kline": BaoStockTaskSpec(
        task="daily_kline",
        api_name="query_history_k_data_plus",
        table_name="bs_daily_kline",
        fields=(
            "date",
            "code",
            "open",
            "high",
            "low",
            "close",
            "preclose",
            "volume",
            "amount",
            "adjustflag",
            "turn",
            "tradestatus",
            "pctChg",
            "peTTM",
            "pbMRQ",
            "psTTM",
            "pcfNcfTTM",
            "isST",
        ),
        uses_code=True,
        uses_begin_end=True,
        auto_code_universe=True,
        cursor_fields=("date",),
        cursor_granularity="day",
        persist_request_columns=False,
    ),
    "hs300_stocks": BaoStockTaskSpec(
        task="hs300_stocks",
        api_name="query_hs300_stocks",
        table_name="bs_hs300_stocks",
        fields=("updateDate", "code", "code_name"),
        uses_day=True,
        cursor_fields=("updateDate",),
        cursor_granularity="day",
    ),
    "sz50_stocks": BaoStockTaskSpec(
        task="sz50_stocks",
        api_name="query_sz50_stocks",
        table_name="bs_sz50_stocks",
        fields=("updateDate", "code", "code_name"),
        uses_day=True,
        cursor_fields=("updateDate",),
        cursor_granularity="day",
    ),
    "zz500_stocks": BaoStockTaskSpec(
        task="zz500_stocks",
        api_name="query_zz500_stocks",
        table_name="bs_zz500_stocks",
        fields=("updateDate", "code", "code_name"),
        uses_day=True,
        cursor_fields=("updateDate",),
        cursor_granularity="day",
    ),
    "stock_industry": BaoStockTaskSpec(
        task="stock_industry",
        api_name="query_stock_industry",
        table_name="bs_stock_industry",
        fields=("updateDate", "code", "code_name", "industry", "industryClassification"),
        uses_code=True,
        uses_day=True,
        auto_code_universe=True,
        cursor_fields=("updateDate",),
        cursor_granularity="day",
    ),
    "dividend_data": BaoStockTaskSpec(
        task="dividend_data",
        api_name="query_dividend_data",
        table_name="bs_dividend_data",
        fields=(
            "code",
            "dividPreNoticeDate",
            "dividAgmPumDate",
            "dividPlanAnnounceDate",
            "dividPlanDate",
            "dividRegistDate",
            "dividOperateDate",
            "dividPayDate",
            "dividStockMarketDate",
            "dividCashPsBeforeTax",
            "dividCashPsAfterTax",
            "dividStocksPs",
            "dividCashStock",
            "dividReserveToStockPs",
        ),
        uses_code=True,
        uses_year=True,
        uses_year_type=True,
        auto_code_universe=True,
        cursor_fields=("dividOperateDate",),
        cursor_granularity="day",
    ),
    "profit_data": BaoStockTaskSpec(
        task="profit_data",
        api_name="query_profit_data",
        table_name="bs_profit_data",
        fields=(
            "code",
            "pubDate",
            "statDate",
            "roeAvg",
            "npMargin",
            "gpMargin",
            "netProfit",
            "epsTTM",
            "MBRevenue",
            "totalShare",
            "liqaShare",
        ),
        uses_code=True,
        uses_year=True,
        uses_quarter=True,
        auto_code_universe=True,
        cursor_fields=("statDate",),
        cursor_granularity="day",
    ),
    "operation_data": BaoStockTaskSpec(
        task="operation_data",
        api_name="query_operation_data",
        table_name="bs_operation_data",
        fields=("code", "pubDate", "statDate", "NRTurnRatio", "NRTurnDays", "INVTurnRatio", "INVTurnDays", "CATurnRatio", "AssetTurnRatio"),
        uses_code=True,
        uses_year=True,
        uses_quarter=True,
        auto_code_universe=True,
        cursor_fields=("statDate",),
        cursor_granularity="day",
    ),
    "growth_data": BaoStockTaskSpec(
        task="growth_data",
        api_name="query_growth_data",
        table_name="bs_growth_data",
        fields=("code", "pubDate", "statDate", "YOYEquity", "YOYAsset", "YOYNI", "YOYEPSBasic", "YOYPNI"),
        uses_code=True,
        uses_year=True,
        uses_quarter=True,
        auto_code_universe=True,
        cursor_fields=("statDate",),
        cursor_granularity="day",
    ),
    "dupont_data": BaoStockTaskSpec(
        task="dupont_data",
        api_name="query_dupont_data",
        table_name="bs_dupont_data",
        fields=(
            "code",
            "pubDate",
            "statDate",
            "dupontROE",
            "dupontAssetStoEquity",
            "dupontAssetTurn",
            "dupontPnitoni",
            "dupontNitogr",
            "dupontTaxBurden",
            "dupontIntburden",
            "dupontEbittogr",
        ),
        uses_code=True,
        uses_year=True,
        uses_quarter=True,
        auto_code_universe=True,
        cursor_fields=("statDate",),
        cursor_granularity="day",
    ),
    "balance_data": BaoStockTaskSpec(
        task="balance_data",
        api_name="query_balance_data",
        table_name="bs_balance_data",
        fields=("code", "pubDate", "statDate", "currentRatio", "quickRatio", "cashRatio", "YOYLiability", "liabilityToAsset", "assetToEquity"),
        uses_code=True,
        uses_year=True,
        uses_quarter=True,
        auto_code_universe=True,
        cursor_fields=("statDate",),
        cursor_granularity="day",
    ),
    "cash_flow_data": BaoStockTaskSpec(
        task="cash_flow_data",
        api_name="query_cash_flow_data",
        table_name="bs_cash_flow_data",
        fields=(
            "code",
            "pubDate",
            "statDate",
            "CAToAsset",
            "NCAToAsset",
            "tangibleAssetToAsset",
            "ebitToInterest",
            "CFOToOR",
            "CFOToNP",
            "CFOToGr",
        ),
        uses_code=True,
        uses_year=True,
        uses_quarter=True,
        auto_code_universe=True,
        cursor_fields=("statDate",),
        cursor_granularity="day",
    ),
    "performance_express_report": BaoStockTaskSpec(
        task="performance_express_report",
        api_name="query_performance_express_report",
        table_name="bs_performance_express_report",
        fields=(
            "code",
            "performanceExpPubDate",
            "performanceExpStatDate",
            "performanceExpUpdateDate",
            "performanceExpressTotalAsset",
            "performanceExpressNetAsset",
            "performanceExpressEPSChgPct",
            "performanceExpressROEWa",
            "performanceExpressEPSDiluted",
            "performanceExpressGRYOY",
            "performanceExpressOPYOY",
        ),
        uses_code=True,
        uses_begin_end=True,
        auto_code_universe=True,
        cursor_fields=("performanceExpUpdateDate",),
        cursor_granularity="day",
    ),
    "forecast_report": BaoStockTaskSpec(
        task="forecast_report",
        api_name="query_forecast_report",
        table_name="bs_forecast_report",
        fields=(
            "code",
            "profitForcastExpPubDate",
            "profitForcastExpStatDate",
            "profitForcastType",
            "profitForcastAbstract",
            "profitForcastChgPctUp",
            "profitForcastChgPctDwn",
        ),
        uses_code=True,
        uses_begin_end=True,
        auto_code_universe=True,
        cursor_fields=("profitForcastExpPubDate",),
        cursor_granularity="day",
    ),
    "deposit_rate_data": BaoStockTaskSpec(
        task="deposit_rate_data",
        api_name="query_deposit_rate_data",
        table_name="bs_deposit_rate_data",
        fields=(
            "pubDate",
            "demandDepositRate",
            "fixedDepositRate3Month",
            "fixedDepositRate6Month",
            "fixedDepositRate1Year",
            "fixedDepositRate2Year",
            "fixedDepositRate3Year",
            "fixedDepositRate5Year",
            "installmentFixedDepositRate1Year",
            "installmentFixedDepositRate3Year",
            "installmentFixedDepositRate5Year",
        ),
        uses_begin_end=True,
        cursor_fields=("pubDate",),
        cursor_granularity="day",
    ),
    "loan_rate_data": BaoStockTaskSpec(
        task="loan_rate_data",
        api_name="query_loan_rate_data",
        table_name="bs_loan_rate_data",
        fields=(
            "pubDate",
            "loanRate6Month",
            "loanRate6MonthTo1Year",
            "loanRate1YearTo3Year",
            "loanRate3YearTo5Year",
            "loanRateAbove5Year",
            "mortgateRateBelow5Year",
            "mortgateRateAbove5Year",
        ),
        uses_begin_end=True,
        cursor_fields=("pubDate",),
        cursor_granularity="day",
    ),
    "required_reserve_ratio_data": BaoStockTaskSpec(
        task="required_reserve_ratio_data",
        api_name="query_required_reserve_ratio_data",
        table_name="bs_required_reserve_ratio_data",
        fields=(
            "pubDate",
            "effectiveDate",
            "bigInstitutionsRatioPre",
            "bigInstitutionsRatioAfter",
            "mediumInstitutionsRatioPre",
            "mediumInstitutionsRatioAfter",
        ),
        uses_begin_end=True,
        uses_year_type=True,
        cursor_fields=("effectiveDate",),
        cursor_granularity="day",
    ),
    "money_supply_data_month": BaoStockTaskSpec(
        task="money_supply_data_month",
        api_name="query_money_supply_data_month",
        table_name="bs_money_supply_data_month",
        fields=("statYear", "statMonth", "m0Month", "m0YOY", "m0ChainRelative", "m1Month", "m1YOY", "m1ChainRelative", "m2Month", "m2YOY", "m2ChainRelative"),
        uses_begin_end=True,
        cursor_fields=("statYear", "statMonth"),
        cursor_granularity="month",
    ),
    "money_supply_data_year": BaoStockTaskSpec(
        task="money_supply_data_year",
        api_name="query_money_supply_data_year",
        table_name="bs_money_supply_data_year",
        fields=("statYear", "m0Year", "m0YearYOY", "m1Year", "m1YearYOY", "m2Year", "m2YearYOY"),
        uses_begin_end=True,
        cursor_fields=("statYear",),
        cursor_granularity="year",
    ),
}

BAOSTOCK_TASK_CHOICES = tuple(BAOSTOCK_TASK_SPECS.keys())


def request_columns_for_spec(spec: BaoStockTaskSpec) -> tuple[str, ...]:
    if not spec.persist_request_columns:
        return ()
    columns: list[str] = []
    if spec.uses_day:
        columns.append("query_date")
    if spec.uses_year_type:
        columns.append("request_year_type")
    return tuple(columns)


def table_columns_for_spec(spec: BaoStockTaskSpec) -> tuple[str, ...]:
    columns: list[str] = list(request_columns_for_spec(spec))
    columns.extend(spec.field_columns)
    return tuple(columns)


def order_by_columns_for_spec(spec: BaoStockTaskSpec) -> tuple[str, ...]:
    table_columns = table_columns_for_spec(spec)
    columns = set(table_columns)
    ordered: list[str] = []
    for candidate in (
        "code",
        "date",
        "calendar_date",
        "update_date",
        "pub_date",
        "stat_date",
        "divid_operate_date",
        "performance_exp_pub_date",
        "profit_forcast_exp_pub_date",
        "query_date",
    ):
        if candidate in columns and candidate not in ordered:
            ordered.append(candidate)
    if not ordered and table_columns:
        ordered.append(table_columns[0])
    return tuple(ordered)


__all__ = [
    "BAOSTOCK_TASK_CHOICES",
    "BAOSTOCK_TASK_SPECS",
    "BaoStockTaskSpec",
    "camel_to_snake",
    "order_by_columns_for_spec",
    "request_columns_for_spec",
    "table_columns_for_spec",
]
