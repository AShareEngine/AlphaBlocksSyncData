#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AmazingData SDK 第一阶段的 ClickHouse 表结构定义.

这层只关心数仓建模，不关心业务编排：
- 表名
- 字段类型
- MergeTree 策略
- 分区键与排序键

第一阶段先覆盖 `BaseData` 和已明确的 `InfoData` 表结构。

命名约定：
- ClickHouse 表名统一使用小写，前缀固定为 `ad_`
- ClickHouse 列名统一使用小写 `snake_case`
- SDK 原始大写字段名只保留在返回 schema / 字段映射层，不直接进入数据库 DDL
"""

from __future__ import annotations

from sync_data_system.data_models import (
    BALANCE_SHEET_DATE_FIELDS,
    BALANCE_SHEET_FIELD_NAMES,
    BALANCE_SHEET_INT_FIELDS,
    BALANCE_SHEET_STRING_FIELDS,
    CASH_FLOW_DATE_FIELDS,
    CASH_FLOW_FIELD_NAMES,
    CASH_FLOW_INT_FIELDS,
    CASH_FLOW_STRING_FIELDS,
    INCOME_DATE_FIELDS,
    INCOME_FIELD_NAMES,
    INCOME_INT_FIELDS,
    INCOME_STRING_FIELDS,
    PROFIT_EXPRESS_DATE_FIELDS,
    PROFIT_EXPRESS_FIELD_NAMES,
    PROFIT_EXPRESS_INT_FIELDS,
    PROFIT_EXPRESS_STRING_FIELDS,
    PROFIT_NOTICE_DATE_FIELDS,
    PROFIT_NOTICE_FIELD_NAMES,
    PROFIT_NOTICE_INT_FIELDS,
    PROFIT_NOTICE_STRING_FIELDS,
    KZZ_ISSUANCE_FIELD_NAMES,
    KZZ_ISSUANCE_INT_FIELDS,
    KZZ_ISSUANCE_STRING_FIELDS,
)


AD_TRADE_CALENDAR_TABLE = "ad_trade_calendar"
AD_CODE_INFO_TABLE = "ad_code_info"
AD_HIST_CODE_DAILY_TABLE = "ad_hist_code_daily"
AD_ADJ_FACTOR_TABLE = "ad_adj_factor"
AD_BACKWARD_FACTOR_TABLE = "ad_backward_factor"
AD_ETF_PCF_TABLE = "ad_etf_pcf"
AD_ETF_PCF_CONSTITUENT_TABLE = "ad_etf_pcf_constituent"
AD_SYNC_TASK_LOG_TABLE = "ad_sync_task_log"
AD_SYNC_CHECKPOINT_TABLE = "ad_sync_checkpoint"
AD_STOCK_BASIC_TABLE = "ad_stock_basic"
AD_HISTORY_STOCK_STATUS_TABLE = "ad_history_stock_status"
AD_BJ_CODE_MAPPING_TABLE = "ad_bj_code_mapping"
AD_BALANCE_SHEET_TABLE = "ad_balance_sheet"
AD_CASH_FLOW_TABLE = "ad_cash_flow"
AD_INCOME_TABLE = "ad_income"
AD_PROFIT_EXPRESS_TABLE = "ad_profit_express"
AD_PROFIT_NOTICE_TABLE = "ad_profit_notice"
AD_FUND_SHARE_TABLE = "ad_fund_share"
AD_FUND_IOPV_TABLE = "ad_fund_iopv"
AD_KZZ_ISSUANCE_TABLE = "ad_kzz_issuance"
AD_KZZ_SHARE_TABLE = "ad_kzz_share"
AD_KZZ_SUSPEND_TABLE = "ad_kzz_suspend"
AD_OPTION_BASIC_INFO_TABLE = "ad_option_basic_info"
AD_OPTION_STD_CTR_SPECS_TABLE = "ad_option_std_ctr_specs"
AD_OPTION_MON_CTR_SPECS_TABLE = "ad_option_mon_ctr_specs"
AD_TREASURY_YIELD_TABLE = "ad_treasury_yield"
AD_KZZ_CONV_CHANGE_TABLE = "ad_kzz_conv_change"
AD_KZZ_CORR_TABLE = "ad_kzz_corr"
AD_KZZ_CALL_EXPLANATION_TABLE = "ad_kzz_call_explanation"
AD_KZZ_PUT_EXPLANATION_TABLE = "ad_kzz_put_explanation"
AD_KZZ_PUT_CALL_ITEM_TABLE = "ad_kzz_put_call_item"
AD_KZZ_PUT_TABLE = "ad_kzz_put"
AD_KZZ_CALL_TABLE = "ad_kzz_call"
AD_KZZ_CONV_TABLE = "ad_kzz_conv"
AD_BLOCK_TRADING_TABLE = "ad_block_trading"
AD_LONG_HU_BANG_TABLE = "ad_long_hu_bang"
AD_MARGIN_DETAIL_TABLE = "ad_margin_detail"
AD_MARGIN_SUMMARY_TABLE = "ad_margin_summary"
AD_SHARE_HOLDER_TABLE = "ad_share_holder"
AD_HOLDER_NUM_TABLE = "ad_holder_num"
AD_EQUITY_STRUCTURE_TABLE = "ad_equity_structure"
AD_EQUITY_PLEDGE_FREEZE_TABLE = "ad_equity_pledge_freeze"
AD_EQUITY_RESTRICTED_TABLE = "ad_equity_restricted"
AD_DIVIDEND_TABLE = "ad_dividend"
AD_RIGHT_ISSUE_TABLE = "ad_right_issue"
AD_INDEX_CONSTITUENT_TABLE = "ad_index_constituent"
AD_INDEX_WEIGHT_TABLE = "ad_index_weight"
AD_INDUSTRY_BASE_INFO_TABLE = "ad_industry_base_info"
AD_INDUSTRY_CONSTITUENT_TABLE = "ad_industry_constituent"
AD_INDUSTRY_WEIGHT_TABLE = "ad_industry_weight"
AD_INDUSTRY_DAILY_TABLE = "ad_industry_daily"
AD_MARKET_KLINE_DAILY_TABLE = "ad_market_kline_daily"
AD_MARKET_KLINE_MINUTE_TABLE = "ad_market_kline_minute"
AD_MARKET_SNAPSHOT_TABLE = "ad_market_snapshot"


def _statement_clickhouse_type(
    field_name: str,
    date_fields: frozenset[str],
    int_fields: frozenset[str],
    string_fields: frozenset[str],
) -> str:
    if field_name in date_fields:
        return "Nullable(Date)"
    if field_name in int_fields:
        return "Nullable(Int32)"
    if field_name in string_fields:
        return "Nullable(String)"
    return "Nullable(Float64)"


BALANCE_SHEET_COLUMN_TYPES = tuple(
    (
        field_name,
        _statement_clickhouse_type(
            field_name,
            BALANCE_SHEET_DATE_FIELDS,
            BALANCE_SHEET_INT_FIELDS,
            BALANCE_SHEET_STRING_FIELDS,
        ),
    )
    for field_name in BALANCE_SHEET_FIELD_NAMES
)

CASH_FLOW_COLUMN_TYPES = tuple(
    (
        field_name,
        _statement_clickhouse_type(
            field_name,
            CASH_FLOW_DATE_FIELDS,
            CASH_FLOW_INT_FIELDS,
            CASH_FLOW_STRING_FIELDS,
        ),
    )
    for field_name in CASH_FLOW_FIELD_NAMES
)

INCOME_COLUMN_TYPES = tuple(
    (
        field_name,
        _statement_clickhouse_type(
            field_name,
            INCOME_DATE_FIELDS,
            INCOME_INT_FIELDS,
            INCOME_STRING_FIELDS,
        ),
    )
    for field_name in INCOME_FIELD_NAMES
)

PROFIT_EXPRESS_COLUMN_TYPES = tuple(
    (
        field_name,
        _statement_clickhouse_type(
            field_name,
            PROFIT_EXPRESS_DATE_FIELDS,
            PROFIT_EXPRESS_INT_FIELDS,
            PROFIT_EXPRESS_STRING_FIELDS,
        ),
    )
    for field_name in PROFIT_EXPRESS_FIELD_NAMES
)

PROFIT_NOTICE_COLUMN_TYPES = tuple(
    (
        field_name,
        _statement_clickhouse_type(
            field_name,
            PROFIT_NOTICE_DATE_FIELDS,
            PROFIT_NOTICE_INT_FIELDS,
            PROFIT_NOTICE_STRING_FIELDS,
        ),
    )
    for field_name in PROFIT_NOTICE_FIELD_NAMES
)

KZZ_ISSUANCE_COLUMN_TYPES = tuple(
    (
        field_name,
        _statement_clickhouse_type(
            field_name,
            frozenset(),
            KZZ_ISSUANCE_INT_FIELDS,
            KZZ_ISSUANCE_STRING_FIELDS,
        ),
    )
    for field_name in KZZ_ISSUANCE_FIELD_NAMES
)

KZZ_ISSUANCE_COLUMNS_SQL = ",\n".join(
    f"    {field_name} {field_type}"
    for field_name, field_type in KZZ_ISSUANCE_COLUMN_TYPES
)


CREATE_AD_TRADE_CALENDAR_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_TRADE_CALENDAR_TABLE}
(
    -- 数据库列统一使用小写 snake_case
    trade_date Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (trade_date)
"""


CREATE_AD_CODE_INFO_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_CODE_INFO_TABLE}
(
    security_type LowCardinality(String),
    code String,
    symbol Nullable(String),
    security_status_raw Nullable(String),
    pre_close Nullable(Float64),
    high_limited Nullable(Float64),
    low_limited Nullable(Float64),
    price_tick Nullable(Float64)
)
ENGINE = ReplacingMergeTree
ORDER BY (security_type, code)
"""


CREATE_AD_HIST_CODE_DAILY_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_HIST_CODE_DAILY_TABLE}
(
    trade_date Date,
    security_type LowCardinality(String),
    code String
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (security_type, trade_date, code)
"""


CREATE_AD_ADJ_FACTOR_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_ADJ_FACTOR_TABLE}
(
    trade_date Date,
    code String,
    factor_value Float64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (code, trade_date)
"""


CREATE_AD_BACKWARD_FACTOR_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_BACKWARD_FACTOR_TABLE}
(
    trade_date Date,
    code String,
    factor_value Float64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (code, trade_date)
"""


CREATE_AD_ETF_PCF_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_ETF_PCF_TABLE}
(
    etf_code String,
    creation_redemption_unit Nullable(Int32),
    max_cash_ratio Nullable(String),
    publish Nullable(String),
    creation Nullable(String),
    redemption Nullable(String),
    creation_redemption_switch Nullable(String),
    record_num Nullable(Int32),
    total_record_num Nullable(Int32),
    estimate_cash_component Nullable(Float64),
    trading_day Nullable(Date),
    pre_trading_day Nullable(Date),
    cash_component Nullable(Float64),
    nav_per_cu Nullable(Float64),
    nav Nullable(Float64),
    symbol Nullable(String),
    fund_management_company Nullable(String),
    underlying_security_id Nullable(String),
    underlying_security_id_source Nullable(String),
    dividend_per_cu Nullable(Float64),
    creation_limit Nullable(Float64),
    redemption_limit Nullable(Float64),
    creation_limit_per_user Nullable(Float64),
    redemption_limit_per_user Nullable(Float64),
    net_creation_limit Nullable(Float64),
    net_redemption_limit Nullable(Float64),
    net_creation_limit_per_user Nullable(Float64),
    net_redemption_limit_per_user Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ifNull(trading_day, toDate('1970-01-01')))
ORDER BY (etf_code, ifNull(trading_day, toDate('1970-01-01')))
"""


CREATE_AD_ETF_PCF_CONSTITUENT_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_ETF_PCF_CONSTITUENT_TABLE}
(
    etf_code String,
    trading_day Nullable(Date),
    underlying_symbol Nullable(String),
    component_share Nullable(Float64),
    substitute_flag Nullable(String),
    premium_ratio Nullable(Float64),
    discount_ratio Nullable(Float64),
    creation_cash_substitute Nullable(Float64),
    redemption_cash_substitute Nullable(Float64),
    substitution_cash_amount Nullable(Float64),
    underlying_security_id Nullable(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ifNull(trading_day, toDate('1970-01-01')))
ORDER BY (etf_code, ifNull(trading_day, toDate('1970-01-01')), ifNull(underlying_symbol, ''))
"""


CREATE_AD_SYNC_TASK_LOG_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_SYNC_TASK_LOG_TABLE}
(
    task_name LowCardinality(String),
    scope_key String,
    run_date Date,
    status LowCardinality(String),
    target_table LowCardinality(String),
    start_date Nullable(Date),
    end_date Nullable(Date),
    row_count UInt64,
    message Nullable(String),
    started_at DateTime64(3),
    finished_at DateTime64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(run_date)
ORDER BY (task_name, scope_key, run_date, started_at)
"""


CREATE_AD_SYNC_CHECKPOINT_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_SYNC_CHECKPOINT_TABLE}
(
    task_name LowCardinality(String),
    scope_key String,
    run_date Date,
    status LowCardinality(String),
    target_table LowCardinality(String),
    checkpoint_date Nullable(Date),
    row_count UInt64,
    message Nullable(String),
    finished_at DateTime64(3)
)
ENGINE = ReplacingMergeTree(finished_at)
PARTITION BY toYYYYMM(run_date)
ORDER BY (task_name, scope_key, run_date)
"""


CREATE_AD_STOCK_BASIC_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_STOCK_BASIC_TABLE}
(
    snapshot_date Date,
    market_code String,
    security_name Nullable(String),
    comp_name Nullable(String),
    pinyin Nullable(String),
    comp_name_eng Nullable(String),
    list_date Nullable(Int32),
    delist_date Nullable(Int32),
    listplate_name Nullable(String),
    comp_sname_eng Nullable(String),
    is_listed Nullable(Int32)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (snapshot_date, market_code)
"""


CREATE_AD_HISTORY_STOCK_STATUS_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_HISTORY_STOCK_STATUS_TABLE}
(
    trade_date Date,
    market_code String,
    preclose Nullable(Float64),
    high_limited Nullable(Float64),
    low_limited Nullable(Float64),
    price_high_lmt_rate Nullable(Float64),
    price_low_lmt_rate Nullable(Float64),
    is_st_sec Nullable(String),
    is_susp_sec Nullable(String),
    is_wd_sec Nullable(String),
    is_xr_sec Nullable(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (trade_date, market_code)
"""


CREATE_AD_BJ_CODE_MAPPING_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_BJ_CODE_MAPPING_TABLE}
(
    old_code String,
    new_code String,
    security_name Nullable(String),
    listing_date Nullable(Int32)
)
ENGINE = ReplacingMergeTree
ORDER BY (old_code, new_code)
"""


CREATE_AD_BALANCE_SHEET_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_BALANCE_SHEET_TABLE}
(
    market_code String,
    report_date Nullable(Date),
    report_date_raw Nullable(String),
    payload_json String
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ifNull(report_date, toDate('1970-01-01')))
ORDER BY (market_code, ifNull(report_date, toDate('1970-01-01')))
"""


CREATE_AD_CASH_FLOW_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_CASH_FLOW_TABLE}
(
    market_code String,
    report_date Nullable(Date),
    report_date_raw Nullable(String),
    payload_json String
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ifNull(report_date, toDate('1970-01-01')))
ORDER BY (market_code, ifNull(report_date, toDate('1970-01-01')))
"""


CREATE_AD_INCOME_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_INCOME_TABLE}
(
    market_code String,
    report_date Nullable(Date),
    report_date_raw Nullable(String),
    payload_json String
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ifNull(report_date, toDate('1970-01-01')))
ORDER BY (market_code, ifNull(report_date, toDate('1970-01-01')))
"""


CREATE_AD_PROFIT_EXPRESS_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_PROFIT_EXPRESS_TABLE}
(
    market_code String,
    report_date Nullable(Date),
    report_date_raw Nullable(String),
    payload_json String
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ifNull(report_date, toDate('1970-01-01')))
ORDER BY (market_code, ifNull(report_date, toDate('1970-01-01')))
"""


CREATE_AD_PROFIT_NOTICE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_PROFIT_NOTICE_TABLE}
(
    market_code String,
    report_date Nullable(Date),
    report_date_raw Nullable(String),
    payload_json String
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ifNull(report_date, toDate('1970-01-01')))
ORDER BY (market_code, ifNull(report_date, toDate('1970-01-01')))
"""


CREATE_AD_FUND_SHARE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_FUND_SHARE_TABLE}
(
    market_code String,
    fund_share Nullable(Float64),
    change_reason Nullable(String),
    is_consolidated_data Nullable(Int32),
    ann_date Nullable(Date),
    total_share Nullable(Float64),
    change_date Nullable(Date),
    float_share Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ifNull(change_date, toDate('1970-01-01')))
ORDER BY (
    market_code,
    ifNull(change_date, toDate('1970-01-01')),
    ifNull(ann_date, toDate('1970-01-01'))
)
"""


CREATE_AD_FUND_IOPV_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_FUND_IOPV_TABLE}
(
    market_code String,
    price_date Nullable(String),
    iopv_nav Nullable(Float64)
)
ENGINE = ReplacingMergeTree
ORDER BY (market_code, ifNull(price_date, ''))
SETTINGS allow_nullable_key = 1
"""


CREATE_AD_KZZ_ISSUANCE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_KZZ_ISSUANCE_TABLE}
(
{KZZ_ISSUANCE_COLUMNS_SQL}
)
ENGINE = ReplacingMergeTree
ORDER BY (market_code, ifNull(ann_dt, ''), ifNull(listed_date, ''))
SETTINGS allow_nullable_key = 1
"""


CREATE_AD_KZZ_SHARE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_KZZ_SHARE_TABLE}
(
    change_date Nullable(String),
    ann_date Nullable(String),
    market_code String,
    bond_share Nullable(Float64),
    conv_share Nullable(Float64),
    change_reason Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (market_code, ifNull(change_date, ''), ifNull(ann_date, ''))
SETTINGS allow_nullable_key = 1
"""


CREATE_AD_KZZ_SUSPEND_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_KZZ_SUSPEND_TABLE}
(
    market_code String,
    suspend_date Nullable(String),
    suspend_type Nullable(Int32),
    resump_date Nullable(String),
    change_reason Nullable(String),
    change_reason_code Nullable(Int32),
    resump_time Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (market_code, ifNull(suspend_date, ''), ifNull(resump_date, ''))
SETTINGS allow_nullable_key = 1
"""


CREATE_AD_OPTION_BASIC_INFO_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_OPTION_BASIC_INFO_TABLE}
(
    contract_full_name Nullable(String),
    contract_type Nullable(String),
    delivery_month Nullable(String),
    expiry_date Nullable(String),
    exercise_price Nullable(Float64),
    exercise_end_date Nullable(String),
    start_trade_date Nullable(String),
    listing_ref_price Nullable(Float64),
    last_trade_date Nullable(String),
    exchange_code Nullable(String),
    delivery_date Nullable(String),
    contract_unit Nullable(Int32),
    is_trade Nullable(String),
    exchange_short_name Nullable(String),
    contract_adjust_flag Nullable(String),
    market_code String
)
ENGINE = ReplacingMergeTree
ORDER BY (market_code, ifNull(expiry_date, ''), ifNull(last_trade_date, ''))
SETTINGS allow_nullable_key = 1
"""


CREATE_AD_OPTION_STD_CTR_SPECS_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_OPTION_STD_CTR_SPECS_TABLE}
(
    exercise_date Nullable(String),
    contract_unit Nullable(Int32),
    position_declare_min Nullable(String),
    quote_currency_unit Nullable(String),
    last_trading_date Nullable(String),
    position_limit Nullable(String),
    delist_date Nullable(String),
    notional_value Nullable(String),
    exercise_method Nullable(String),
    delivery_method Nullable(String),
    settlement_month Nullable(String),
    trading_fee Nullable(String),
    exchange_name Nullable(String),
    option_en_name Nullable(String),
    contract_value Nullable(Float64),
    is_simulation Nullable(Int32),
    contract_unit_dimension Nullable(String),
    option_strike_price Nullable(String),
    is_simulation_trade Nullable(String),
    listed_date Nullable(String),
    option_name Nullable(String),
    premium Nullable(String),
    option_type Nullable(String),
    trading_hours_desc Nullable(String),
    final_settlement_date Nullable(String),
    final_settlement_price Nullable(String),
    min_price_unit Nullable(String),
    market_code String,
    contract_multiplier Nullable(Int32)
)
ENGINE = ReplacingMergeTree
ORDER BY (market_code, ifNull(exercise_date, ''), ifNull(last_trading_date, ''))
SETTINGS allow_nullable_key = 1
"""


CREATE_AD_OPTION_MON_CTR_SPECS_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_OPTION_MON_CTR_SPECS_TABLE}
(
    code_old Nullable(String),
    change_date Nullable(String),
    market_code String,
    name_new Nullable(String),
    exercise_price_new Nullable(Float64),
    name_old Nullable(String),
    code_new Nullable(String),
    exercise_price_old Nullable(Float64),
    unit_old Nullable(Float64),
    unit_new Nullable(Float64),
    change_reason Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (market_code, ifNull(change_date, ''), ifNull(code_old, ''), ifNull(code_new, ''))
SETTINGS allow_nullable_key = 1
"""


CREATE_AD_TREASURY_YIELD_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_TREASURY_YIELD_TABLE}
(
    term String,
    trade_date Date,
    yield_value Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (term, trade_date)
"""


CREATE_AD_KZZ_CONV_CHANGE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_KZZ_CONV_CHANGE_TABLE}
(
    market_code String,
    change_date Nullable(Date),
    ann_date Nullable(Date),
    conv_price Nullable(Float64),
    change_reason Nullable(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ifNull(change_date, toDate('1970-01-01')))
ORDER BY (
    market_code,
    ifNull(change_date, toDate('1970-01-01')),
    ifNull(ann_date, toDate('1970-01-01'))
)
"""


CREATE_AD_KZZ_CORR_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_KZZ_CORR_TABLE}
(
    market_code String,
    start_date Nullable(Date),
    end_date Nullable(Date),
    corr_trig_calc_max_period Nullable(Float64),
    corr_trig_calc_period Nullable(Float64),
    spec_corr_trig_ratio Nullable(Float64),
    corr_conv_price_floor_desc Nullable(String),
    ref_price_is_avg_price Nullable(Int32),
    corr_times_limit Nullable(String),
    is_timepoint_corr_clause_flag Nullable(Int32),
    timepoint_count Nullable(Float64),
    timepoint_corr_text_clause Nullable(String),
    spec_corr_range Nullable(Float64),
    is_spec_down_corr_clause_flag Nullable(Int32)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ifNull(start_date, toDate('1970-01-01')))
ORDER BY (
    market_code,
    ifNull(start_date, toDate('1970-01-01')),
    ifNull(end_date, toDate('1970-01-01'))
)
"""


CREATE_AD_KZZ_CALL_EXPLANATION_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_KZZ_CALL_EXPLANATION_TABLE}
(
    market_code String,
    call_date Nullable(Date),
    call_price Nullable(Float64),
    call_announcement_date Nullable(Date),
    call_ful_res_ann_date Nullable(Date),
    call_amount Nullable(Float64),
    call_outstanding_amount Nullable(Float64),
    call_date_pub Nullable(Date),
    call_fund_arrival_date Nullable(Date),
    call_record_day Nullable(Date),
    call_reason Nullable(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ifNull(call_date, toDate('1970-01-01')))
ORDER BY (
    market_code,
    ifNull(call_date, toDate('1970-01-01')),
    ifNull(call_announcement_date, toDate('1970-01-01'))
)
"""


CREATE_AD_KZZ_PUT_EXPLANATION_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_KZZ_PUT_EXPLANATION_TABLE}
(
    market_code String,
    put_fund_arrival_date Nullable(String),
    put_price Nullable(Float64),
    put_announcement_date Nullable(String),
    put_ex_date Nullable(String),
    put_amount Nullable(Float64),
    put_outstanding Nullable(Float64),
    repurchase_start_date Nullable(String),
    repurchase_end_date Nullable(String),
    resale_start_date Nullable(String),
    fund_end_date Nullable(String),
    repurchase_code Nullable(String),
    resale_amount Nullable(Float64),
    resale_imp_amount Nullable(Float64),
    resale_end_date Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (market_code, ifNull(put_announcement_date, ''), ifNull(fund_end_date, ''))
SETTINGS allow_nullable_key = 1
"""


CREATE_AD_KZZ_PUT_CALL_ITEM_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_KZZ_PUT_CALL_ITEM_TABLE}
(
    market_code String,
    mand_put_period Nullable(String),
    mand_put_price Nullable(Float64),
    mand_put_start_date Nullable(Date),
    mand_put_end_date Nullable(Date),
    mand_put_text Nullable(String),
    is_mand_put_contain_current Nullable(Int32),
    con_put_start_date Nullable(Date),
    con_put_end_date Nullable(Date),
    max_put_triper Nullable(Float64),
    put_triperiod Nullable(Float64),
    add_put_con Nullable(String),
    add_put_price_ins Nullable(String),
    put_num_ins Nullable(String),
    put_pro_period Nullable(Float64),
    put_no_pery Nullable(Float64),
    is_put_item Nullable(Int32),
    is_term_put_item Nullable(Int32),
    is_mand_put_item Nullable(Int32),
    is_time_put_item Nullable(Int32),
    time_put_no Nullable(Float64),
    time_put_item Nullable(String),
    term_put_price Nullable(Float64),
    con_call_start_date Nullable(Date),
    con_call_end_date Nullable(Date),
    call_tri_con_ins Nullable(String),
    max_call_triper Nullable(Float64),
    call_tri_per Nullable(Float64),
    call_num_ber_ins Nullable(String),
    is_call_item Nullable(Int32),
    call_pro_period Nullable(Float64),
    call_no_pery Nullable(Float64),
    is_time_call_item Nullable(Int32),
    time_call_no Nullable(Float64),
    time_call_text Nullable(String),
    expired_redemption_price Nullable(Float64),
    put_tri_cond_desc Nullable(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY market_code
ORDER BY (market_code, ifNull(mand_put_start_date, toDate('1970-01-01')))
SETTINGS allow_nullable_key = 1
"""


CREATE_AD_KZZ_PUT_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_KZZ_PUT_TABLE}
(
    market_code String,
    put_price Nullable(Float64),
    begin_date Nullable(Date),
    end_date Nullable(Date),
    tri_ratio Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY market_code
ORDER BY (market_code, ifNull(begin_date, toDate('1970-01-01')), ifNull(end_date, toDate('1970-01-01')))
SETTINGS allow_nullable_key = 1
"""


CREATE_AD_KZZ_CALL_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_KZZ_CALL_TABLE}
(
    market_code String,
    call_price Nullable(Float64),
    begin_date Nullable(Date),
    end_date Nullable(Date),
    tri_ratio Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY market_code
ORDER BY (market_code, ifNull(begin_date, toDate('1970-01-01')), ifNull(end_date, toDate('1970-01-01')))
SETTINGS allow_nullable_key = 1
"""


CREATE_AD_KZZ_CONV_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_KZZ_CONV_TABLE}
(
    market_code String,
    ann_date Nullable(Date),
    conv_code Nullable(String),
    conv_name Nullable(String),
    conv_price Nullable(Float64),
    currency_code Nullable(String),
    conv_start_date Nullable(Date),
    conv_end_date Nullable(Date),
    trade_date_last Nullable(Date),
    forced_conv_date Nullable(Date),
    forced_conv_price Nullable(Float64),
    rel_conv_month Nullable(Float64),
    is_forced Nullable(Float64),
    forced_conv_reason Nullable(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY market_code
ORDER BY (market_code, ifNull(ann_date, toDate('1970-01-01')))
SETTINGS allow_nullable_key = 1
"""


CREATE_AD_BLOCK_TRADING_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_BLOCK_TRADING_TABLE}
(
    market_code String,
    trade_date Date,
    bshare_price Nullable(Float64),
    bshare_volume Nullable(Float64),
    b_frequency Nullable(Int32),
    block_avg_volume Nullable(Float64),
    bshare_amount Nullable(Float64),
    bbuyer_name Nullable(String),
    bseller_name Nullable(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (market_code, trade_date)
"""


CREATE_AD_LONG_HU_BANG_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_LONG_HU_BANG_TABLE}
(
    market_code String,
    trade_date Date,
    security_name Nullable(String),
    reason_type Nullable(String),
    reason_type_name Nullable(String),
    change_range Nullable(Float64),
    trader_name Nullable(String),
    buy_amount Nullable(Float64),
    sell_amount Nullable(Float64),
    flow_mark Nullable(Int32),
    total_amount Nullable(Float64),
    total_volume Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (
    market_code,
    trade_date,
    ifNull(reason_type, ''),
    ifNull(flow_mark, -1),
    ifNull(trader_name, ''),
    ifNull(buy_amount, -1),
    ifNull(sell_amount, -1)
)
"""


CREATE_AD_MARGIN_DETAIL_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_MARGIN_DETAIL_TABLE}
(
    market_code String,
    security_name Nullable(String),
    trade_date Date,
    borrow_money_balance Nullable(Float64),
    purch_with_borrow_money Nullable(Float64),
    repayment_of_borrow_money Nullable(Float64),
    sec_lending_balance Nullable(Float64),
    sales_of_borrowed_sec Nullable(Int64),
    repayment_of_borrow_sec Nullable(Int64),
    sec_lending_balance_vol Nullable(Int64),
    margin_trade_balance Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (market_code, trade_date)
"""


CREATE_AD_MARGIN_SUMMARY_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_MARGIN_SUMMARY_TABLE}
(
    trade_date Date,
    sum_borrow_money_balance Nullable(Float64),
    sum_purch_with_borrow_money Nullable(Float64),
    sum_repayment_of_borrow_money Nullable(Float64),
    sum_sec_lending_balance Nullable(Float64),
    sum_sales_of_borrowed_sec Nullable(Int64),
    sum_margin_trade_balance Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (trade_date)
"""


CREATE_AD_SHARE_HOLDER_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_SHARE_HOLDER_TABLE}
(
    ann_date Nullable(Date),
    market_code String,
    holder_enddate Nullable(Date),
    holder_type Nullable(Int32),
    qty_num Nullable(Int32),
    holder_name Nullable(String),
    holder_holder_category Nullable(Int32),
    holder_quantity Nullable(Float64),
    holder_pct Nullable(Float64),
    holder_sharecategoryname Nullable(String),
    float_qty Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ifNull(holder_enddate, toDate('1970-01-01')))
ORDER BY (
    market_code,
    ifNull(holder_enddate, toDate('1970-01-01')),
    ifNull(qty_num, -1)
)
"""


CREATE_AD_HOLDER_NUM_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_HOLDER_NUM_TABLE}
(
    market_code String,
    ann_dt Nullable(Date),
    holder_enddate Nullable(Date),
    holder_total_num Nullable(Float64),
    holder_num Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ifNull(holder_enddate, toDate('1970-01-01')))
ORDER BY (market_code, ifNull(holder_enddate, toDate('1970-01-01')))
"""


CREATE_AD_EQUITY_STRUCTURE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_EQUITY_STRUCTURE_TABLE}
(
    market_code String,
    ann_date Nullable(Date),
    change_date Nullable(Date),
    share_change_reason_str Nullable(String),
    ex_change_date Nullable(Date),
    current_sign Nullable(Int32),
    is_valid Nullable(Int32),
    tot_share Nullable(Float64),
    float_share Nullable(Float64),
    float_a_share Nullable(Float64),
    float_b_share Nullable(Float64),
    float_hk_share Nullable(Float64),
    float_os_share Nullable(Float64),
    tot_tradable_share Nullable(Float64),
    rtd_a_share_inst Nullable(Float64),
    rtd_a_share_domesnp Nullable(Float64),
    rtd_share_senior Nullable(Float64),
    rtd_a_share_foreign Nullable(Float64),
    rtd_a_share_forjur Nullable(Float64),
    rtd_a_share_fornp Nullable(Float64),
    restricted_b_share Nullable(Float64),
    other_rtd_share Nullable(Float64),
    non_tradable_share Nullable(Float64),
    ntrd_share_state_pct Nullable(Float64),
    ntrd_share_state Nullable(Float64),
    ntrd_share_statejur Nullable(Float64),
    ntrd_share_domesjur Nullable(Float64),
    ntrd_share_domes_initiator Nullable(Float64),
    ntrd_share_ipojuris Nullable(Float64),
    ntrd_share_genjuris Nullable(Float64),
    ntrd_share_stra_investor Nullable(Float64),
    ntrd_share_fund Nullable(Float64),
    ntrd_share_nat Nullable(Float64),
    tran_share Nullable(Float64),
    float_share_senior Nullable(Float64),
    share_inemp Nullable(Float64),
    preferred_share Nullable(Float64),
    ntrd_share_nlist_frgn Nullable(Float64),
    staq_share Nullable(Float64),
    net_share Nullable(Float64),
    share_change_reason Nullable(String),
    tot_a_share Nullable(Float64),
    tot_b_share Nullable(Float64),
    otca_share Nullable(Float64),
    otcb_share Nullable(Float64),
    tot_otc_share Nullable(Float64),
    share_hk Nullable(Float64),
    pre_non_tradable_share Nullable(Float64),
    restricted_a_share Nullable(Float64),
    rtd_a_share_state Nullable(Float64),
    rtd_a_share_statejur Nullable(Float64),
    rtd_a_share_other_domes Nullable(Float64),
    rtd_a_share_other_domesjur Nullable(Float64),
    tot_restricted_share Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ifNull(change_date, toDate('1970-01-01')))
ORDER BY (
    market_code,
    ifNull(change_date, toDate('1970-01-01')),
    ifNull(ann_date, toDate('1970-01-01'))
)
"""


CREATE_AD_EQUITY_PLEDGE_FREEZE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_EQUITY_PLEDGE_FREEZE_TABLE}
(
    market_code String,
    ann_date Nullable(Date),
    holder_name Nullable(String),
    holder_type_code Nullable(Int32),
    total_holding_shr Nullable(Float64),
    total_holding_shr_ratio Nullable(Float64),
    fro_shares Nullable(Float64),
    fro_shr_to_total_holding_ratio Nullable(Float64),
    fro_shr_to_total_ratio Nullable(Float64),
    total_pledge_shr Nullable(Float64),
    is_equity_pledge_repo Nullable(Int32),
    begin_date Nullable(Date),
    end_date Nullable(Date),
    is_disfrozen Nullable(Int32),
    frozen_institution Nullable(String),
    disfrozen_time Nullable(Date),
    shr_category_code Nullable(Int32),
    freeze_type Nullable(Int32)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ifNull(ann_date, toDate('1970-01-01')))
ORDER BY (
    market_code,
    ifNull(ann_date, toDate('1970-01-01')),
    ifNull(holder_name, '')
)
"""


CREATE_AD_EQUITY_RESTRICTED_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_EQUITY_RESTRICTED_TABLE}
(
    market_code String,
    list_date Nullable(Date),
    share_ratio Nullable(Float64),
    share_lst_type_name Nullable(String),
    share_lst Nullable(Int64),
    share_lst_is_ann Nullable(Int32),
    close_price Nullable(Float64),
    share_lst_market_value Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ifNull(list_date, toDate('1970-01-01')))
ORDER BY (market_code, ifNull(list_date, toDate('1970-01-01')))
"""


CREATE_AD_DIVIDEND_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_DIVIDEND_TABLE}
(
    market_code String,
    div_progress Nullable(String),
    dvd_per_share_stk Nullable(Float64),
    dvd_per_share_pre_tax_cash Nullable(Float64),
    dvd_per_share_after_tax_cash Nullable(Float64),
    date_eqy_record Nullable(Date),
    date_ex Nullable(Date),
    date_dvd_payout Nullable(Date),
    listingdate_of_dvd_shr Nullable(Date),
    div_prelandate Nullable(Date),
    div_smtgdate Nullable(Date),
    date_dvd_ann Nullable(Date),
    div_basedate Nullable(Date),
    div_baseshare Nullable(Float64),
    currency_code Nullable(String),
    ann_date Nullable(Date),
    is_changed Nullable(Int32),
    report_period Nullable(String),
    div_change Nullable(String),
    div_bonusrate Nullable(Float64),
    div_conversedrate Nullable(Float64),
    remark Nullable(String),
    div_preann_date Nullable(Date),
    div_target Nullable(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ifNull(ann_date, toDate('1970-01-01')))
ORDER BY (
    market_code,
    ifNull(ann_date, toDate('1970-01-01')),
    ifNull(report_period, '')
)
"""


CREATE_AD_RIGHT_ISSUE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_RIGHT_ISSUE_TABLE}
(
    market_code String,
    progress Nullable(Int32),
    price Nullable(Float64),
    ratio Nullable(Float64),
    amt_plan Nullable(Float64),
    amt_real Nullable(Float64),
    collection_fund Nullable(Float64),
    shareb_reg_date Nullable(Date),
    ex_dividend_date Nullable(Date),
    listed_date Nullable(Date),
    pay_start_date Nullable(Date),
    pay_end_date Nullable(Date),
    preplan_date Nullable(Date),
    smtg_ann_date Nullable(Date),
    pass_date Nullable(Date),
    approved_date Nullable(Date),
    execute_date Nullable(Date),
    result_date Nullable(Date),
    list_ann_date Nullable(Date),
    guarantor Nullable(String),
    guartype Nullable(Float64),
    rightsissue_code Nullable(String),
    ann_date Nullable(Date),
    rightsissue_year Nullable(String),
    rightsissue_desc Nullable(String),
    rightsissue_name Nullable(String),
    ratio_denominator Nullable(Float64),
    ratio_molecular Nullable(Float64),
    subs_method Nullable(String),
    expected_fund_raising Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ifNull(ann_date, toDate('1970-01-01')))
ORDER BY (
    market_code,
    ifNull(ann_date, toDate('1970-01-01')),
    ifNull(rightsissue_year, '')
)
"""


CREATE_AD_INDEX_CONSTITUENT_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_INDEX_CONSTITUENT_TABLE}
(
    index_code String,
    con_code String,
    in_date Nullable(Date),
    out_date Nullable(Date),
    index_name Nullable(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ifNull(in_date, toDate('1970-01-01')))
ORDER BY (index_code, con_code, ifNull(in_date, toDate('1970-01-01')))
"""


CREATE_AD_INDEX_WEIGHT_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_INDEX_WEIGHT_TABLE}
(
    index_code String,
    con_code String,
    trade_date Date,
    total_share Nullable(Float64),
    free_share_ratio Nullable(Float64),
    calc_share Nullable(Float64),
    weight_factor Nullable(Float64),
    weight Nullable(Float64),
    close Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (index_code, con_code, trade_date)
"""


CREATE_AD_INDUSTRY_BASE_INFO_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_INDUSTRY_BASE_INFO_TABLE}
(
    index_code String,
    industry_code Nullable(String),
    level_type Nullable(Int32),
    level1_name Nullable(String),
    level2_name Nullable(String),
    level3_name Nullable(String),
    is_pub Nullable(Int32),
    change_reason Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (index_code, ifNull(industry_code, ''))
"""


CREATE_AD_INDUSTRY_CONSTITUENT_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_INDUSTRY_CONSTITUENT_TABLE}
(
    index_code String,
    con_code String,
    in_date Nullable(Date),
    out_date Nullable(Date),
    index_name Nullable(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ifNull(in_date, toDate('1970-01-01')))
ORDER BY (index_code, con_code, ifNull(in_date, toDate('1970-01-01')))
"""


CREATE_AD_INDUSTRY_WEIGHT_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_INDUSTRY_WEIGHT_TABLE}
(
    index_code String,
    con_code String,
    trade_date Date,
    weight Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (index_code, con_code, trade_date)
"""


CREATE_AD_INDUSTRY_DAILY_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_INDUSTRY_DAILY_TABLE}
(
    index_code String,
    trade_date Date,
    open Nullable(Float64),
    high Nullable(Float64),
    close Nullable(Float64),
    low Nullable(Float64),
    amount Nullable(Float64),
    volume Nullable(Float64),
    pb Nullable(Float64),
    pe Nullable(Float64),
    total_cap Nullable(Float64),
    a_float_cap Nullable(Float64),
    pre_close Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (index_code, trade_date)
"""


CREATE_AD_MARKET_KLINE_DAILY_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_MARKET_KLINE_DAILY_TABLE}
(
    trade_time DateTime64(3),
    code String,
    open Nullable(Float64),
    high Nullable(Float64),
    low Nullable(Float64),
    close Nullable(Float64),
    volume Nullable(Float64),
    amount Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(toDate(trade_time))
ORDER BY (code, trade_time)
"""


CREATE_AD_MARKET_KLINE_MINUTE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_MARKET_KLINE_MINUTE_TABLE}
(
    trade_time DateTime64(3),
    code String,
    open Nullable(Float64),
    high Nullable(Float64),
    low Nullable(Float64),
    close Nullable(Float64),
    volume Nullable(Float64),
    amount Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(toDate(trade_time))
ORDER BY (code, trade_time)
"""


CREATE_AD_MARKET_SNAPSHOT_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_MARKET_SNAPSHOT_TABLE}
(
    trade_time DateTime64(3),
    code String,
    pre_close Nullable(Float64),
    last Nullable(Float64),
    open Nullable(Float64),
    high Nullable(Float64),
    low Nullable(Float64),
    close Nullable(Float64),
    volume Nullable(Float64),
    amount Nullable(Float64),
    num_trades Nullable(Float64),
    high_limited Nullable(Float64),
    low_limited Nullable(Float64),
    ask_price1 Nullable(Float64),
    ask_price2 Nullable(Float64),
    ask_price3 Nullable(Float64),
    ask_price4 Nullable(Float64),
    ask_price5 Nullable(Float64),
    ask_volume1 Nullable(Int64),
    ask_volume2 Nullable(Int64),
    ask_volume3 Nullable(Int64),
    ask_volume4 Nullable(Int64),
    ask_volume5 Nullable(Int64),
    bid_price1 Nullable(Float64),
    bid_price2 Nullable(Float64),
    bid_price3 Nullable(Float64),
    bid_price4 Nullable(Float64),
    bid_price5 Nullable(Float64),
    bid_volume1 Nullable(Int64),
    bid_volume2 Nullable(Int64),
    bid_volume3 Nullable(Int64),
    bid_volume4 Nullable(Int64),
    bid_volume5 Nullable(Int64),
    iopv Nullable(Float64),
    trading_phase_code Nullable(String),
    total_long_position Nullable(Int64),
    pre_settle Nullable(Float64),
    auction_price Nullable(Float64),
    auction_volume Nullable(Int64),
    settle Nullable(Float64),
    contract_type Nullable(String),
    expire_date Nullable(Int32),
    underlying_security_code Nullable(String),
    exercise_price Nullable(Float64),
    action_day Nullable(String),
    trading_day Nullable(String),
    pre_open_interest Nullable(Int64),
    open_interest Nullable(Int64),
    average_price Nullable(Float64),
    nominal_price Nullable(Float64),
    ref_price Nullable(Float64),
    bid_price_limit_up Nullable(Float64),
    bid_price_limit_down Nullable(Float64),
    offer_price_limit_up Nullable(Float64),
    offer_price_limit_down Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(toDate(trade_time))
ORDER BY (code, trade_time)
"""


BASE_DATA_TABLE_DDLS = (
    CREATE_AD_TRADE_CALENDAR_TABLE,
    CREATE_AD_CODE_INFO_TABLE,
    CREATE_AD_HIST_CODE_DAILY_TABLE,
    CREATE_AD_ADJ_FACTOR_TABLE,
    CREATE_AD_BACKWARD_FACTOR_TABLE,
    CREATE_AD_ETF_PCF_TABLE,
    CREATE_AD_ETF_PCF_CONSTITUENT_TABLE,
    CREATE_AD_SYNC_TASK_LOG_TABLE,
    CREATE_AD_SYNC_CHECKPOINT_TABLE,
)

INFO_DATA_TABLE_DDLS = (
    CREATE_AD_STOCK_BASIC_TABLE,
    CREATE_AD_HISTORY_STOCK_STATUS_TABLE,
    CREATE_AD_BJ_CODE_MAPPING_TABLE,
    CREATE_AD_BALANCE_SHEET_TABLE,
    CREATE_AD_CASH_FLOW_TABLE,
    CREATE_AD_INCOME_TABLE,
    CREATE_AD_PROFIT_EXPRESS_TABLE,
    CREATE_AD_PROFIT_NOTICE_TABLE,
    CREATE_AD_FUND_SHARE_TABLE,
    CREATE_AD_FUND_IOPV_TABLE,
    CREATE_AD_KZZ_ISSUANCE_TABLE,
    CREATE_AD_KZZ_SHARE_TABLE,
    CREATE_AD_KZZ_SUSPEND_TABLE,
    CREATE_AD_OPTION_BASIC_INFO_TABLE,
    CREATE_AD_OPTION_STD_CTR_SPECS_TABLE,
    CREATE_AD_OPTION_MON_CTR_SPECS_TABLE,
    CREATE_AD_TREASURY_YIELD_TABLE,
    CREATE_AD_KZZ_CONV_CHANGE_TABLE,
    CREATE_AD_KZZ_CORR_TABLE,
    CREATE_AD_KZZ_CALL_EXPLANATION_TABLE,
    CREATE_AD_KZZ_PUT_EXPLANATION_TABLE,
    CREATE_AD_KZZ_PUT_CALL_ITEM_TABLE,
    CREATE_AD_KZZ_PUT_TABLE,
    CREATE_AD_KZZ_CALL_TABLE,
    CREATE_AD_KZZ_CONV_TABLE,
    CREATE_AD_BLOCK_TRADING_TABLE,
    CREATE_AD_LONG_HU_BANG_TABLE,
    CREATE_AD_MARGIN_DETAIL_TABLE,
    CREATE_AD_MARGIN_SUMMARY_TABLE,
    CREATE_AD_SHARE_HOLDER_TABLE,
    CREATE_AD_HOLDER_NUM_TABLE,
    CREATE_AD_EQUITY_STRUCTURE_TABLE,
    CREATE_AD_EQUITY_PLEDGE_FREEZE_TABLE,
    CREATE_AD_EQUITY_RESTRICTED_TABLE,
    CREATE_AD_DIVIDEND_TABLE,
    CREATE_AD_RIGHT_ISSUE_TABLE,
    CREATE_AD_INDEX_CONSTITUENT_TABLE,
    CREATE_AD_INDEX_WEIGHT_TABLE,
    CREATE_AD_INDUSTRY_BASE_INFO_TABLE,
    CREATE_AD_INDUSTRY_CONSTITUENT_TABLE,
    CREATE_AD_INDUSTRY_WEIGHT_TABLE,
    CREATE_AD_INDUSTRY_DAILY_TABLE,
)

MARKET_DATA_TABLE_DDLS = (
    CREATE_AD_MARKET_KLINE_DAILY_TABLE,
    CREATE_AD_MARKET_KLINE_MINUTE_TABLE,
    CREATE_AD_MARKET_SNAPSHOT_TABLE,
)


def iter_base_data_table_ddls() -> tuple[str, ...]:
    """按固定顺序返回 BaseData 所需 DDL."""

    return BASE_DATA_TABLE_DDLS


def iter_info_data_table_ddls() -> tuple[str, ...]:
    """按固定顺序返回 InfoData 当前已实现接口所需 DDL."""

    return INFO_DATA_TABLE_DDLS


def iter_market_data_table_ddls() -> tuple[str, ...]:
    """按固定顺序返回 MarketData 当前已实现接口所需 DDL."""

    return MARKET_DATA_TABLE_DDLS


__all__ = [
    "AD_BALANCE_SHEET_TABLE",
    "AD_CASH_FLOW_TABLE",
    "AD_CODE_INFO_TABLE",
    "AD_DIVIDEND_TABLE",
    "AD_EQUITY_STRUCTURE_TABLE",
    "AD_EQUITY_PLEDGE_FREEZE_TABLE",
    "AD_EQUITY_RESTRICTED_TABLE",
    "AD_ETF_PCF_TABLE",
    "AD_ETF_PCF_CONSTITUENT_TABLE",
    "AD_FUND_SHARE_TABLE",
    "AD_FUND_IOPV_TABLE",
    "AD_HISTORY_STOCK_STATUS_TABLE",
    "AD_HIST_CODE_DAILY_TABLE",
    "AD_HOLDER_NUM_TABLE",
    "AD_INDUSTRY_BASE_INFO_TABLE",
    "AD_INDUSTRY_CONSTITUENT_TABLE",
    "AD_INDUSTRY_DAILY_TABLE",
    "AD_INDUSTRY_WEIGHT_TABLE",
    "AD_INDEX_CONSTITUENT_TABLE",
    "AD_INDEX_WEIGHT_TABLE",
    "AD_INCOME_TABLE",
    "AD_KZZ_CONV_CHANGE_TABLE",
    "AD_KZZ_ISSUANCE_TABLE",
    "AD_KZZ_PUT_CALL_ITEM_TABLE",
    "AD_KZZ_PUT_TABLE",
    "AD_KZZ_CALL_TABLE",
    "AD_KZZ_CONV_TABLE",
    "AD_KZZ_SHARE_TABLE",
    "AD_KZZ_SUSPEND_TABLE",
    "AD_BLOCK_TRADING_TABLE",
    "AD_LONG_HU_BANG_TABLE",
    "AD_MARGIN_DETAIL_TABLE",
    "AD_MARGIN_SUMMARY_TABLE",
    "AD_OPTION_BASIC_INFO_TABLE",
    "AD_OPTION_STD_CTR_SPECS_TABLE",
    "AD_OPTION_MON_CTR_SPECS_TABLE",
    "AD_TREASURY_YIELD_TABLE",
    "AD_KZZ_CORR_TABLE",
    "AD_KZZ_CALL_EXPLANATION_TABLE",
    "AD_KZZ_PUT_EXPLANATION_TABLE",
    "AD_MARKET_KLINE_DAILY_TABLE",
    "AD_MARKET_KLINE_MINUTE_TABLE",
    "AD_MARKET_SNAPSHOT_TABLE",
    "AD_ADJ_FACTOR_TABLE",
    "AD_BACKWARD_FACTOR_TABLE",
    "AD_BJ_CODE_MAPPING_TABLE",
    "AD_PROFIT_EXPRESS_TABLE",
    "AD_PROFIT_NOTICE_TABLE",
    "AD_RIGHT_ISSUE_TABLE",
    "AD_SHARE_HOLDER_TABLE",
    "AD_SYNC_CHECKPOINT_TABLE",
    "AD_STOCK_BASIC_TABLE",
    "AD_SYNC_TASK_LOG_TABLE",
    "AD_TRADE_CALENDAR_TABLE",
    "BALANCE_SHEET_COLUMN_TYPES",
    "CASH_FLOW_COLUMN_TYPES",
    "CREATE_AD_ETF_PCF_CONSTITUENT_TABLE",
    "CREATE_AD_BJ_CODE_MAPPING_TABLE",
    "CREATE_AD_FUND_IOPV_TABLE",
    "CREATE_AD_KZZ_ISSUANCE_TABLE",
    "CREATE_AD_KZZ_SHARE_TABLE",
    "CREATE_AD_KZZ_SUSPEND_TABLE",
    "CREATE_AD_KZZ_PUT_CALL_ITEM_TABLE",
    "CREATE_AD_KZZ_PUT_TABLE",
    "CREATE_AD_KZZ_CALL_TABLE",
    "CREATE_AD_KZZ_CONV_TABLE",
    "CREATE_AD_BLOCK_TRADING_TABLE",
    "CREATE_AD_LONG_HU_BANG_TABLE",
    "CREATE_AD_MARGIN_DETAIL_TABLE",
    "CREATE_AD_MARGIN_SUMMARY_TABLE",
    "CREATE_AD_OPTION_BASIC_INFO_TABLE",
    "CREATE_AD_OPTION_STD_CTR_SPECS_TABLE",
    "CREATE_AD_OPTION_MON_CTR_SPECS_TABLE",
    "CREATE_AD_TREASURY_YIELD_TABLE",
    "CREATE_AD_KZZ_PUT_EXPLANATION_TABLE",
    "INCOME_COLUMN_TYPES",
    "KZZ_ISSUANCE_COLUMN_TYPES",
    "BASE_DATA_TABLE_DDLS",
    "INFO_DATA_TABLE_DDLS",
    "MARKET_DATA_TABLE_DDLS",
    "PROFIT_EXPRESS_COLUMN_TYPES",
    "PROFIT_NOTICE_COLUMN_TYPES",
    "iter_base_data_table_ddls",
    "iter_info_data_table_ddls",
    "iter_market_data_table_ddls",
]
