#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""BaseData 使用的数据模型与日期转换工具."""

from __future__ import annotations

import re
from dataclasses import dataclass, field, make_dataclass
from datetime import date, datetime
from typing import Optional, Sequence


def utcnow() -> datetime:
    """统一生成无时区的 UTC 时间戳，方便写入 ClickHouse DateTime64."""

    return datetime.utcnow()


def to_ch_date(value: date | datetime | int | str) -> date:
    """把常见日期输入统一转换为 ClickHouse `Date`.

    支持以下输入：
    - `date`
    - `datetime`
    - `20240327` 这种 8 位整数
    - `20240327` / `2024-03-27` 这种字符串
    """

    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, int):
        s = f"{value:08d}"
        return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
    if isinstance(value, str):
        cleaned = re.sub(r"[^0-9]", "", value.strip())
        if len(cleaned) != 8:
            raise ValueError(f"无法识别日期字符串: {value!r}")
        return date(int(cleaned[0:4]), int(cleaned[4:6]), int(cleaned[6:8]))
    raise TypeError(f"不支持的日期类型: {type(value)!r}")


def to_yyyymmdd(value: date | datetime | int | str) -> int:
    """统一转换回 `YYYYMMDD` 整数表示."""

    return int(to_ch_date(value).strftime("%Y%m%d"))


def normalize_code_list(code_list: Sequence[str]) -> list[str]:
    """清洗并去重证券代码，保留原始顺序."""

    seen: set[str] = set()
    normalized: list[str] = []
    for code in code_list:
        text = str(code).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


BJ_920_ONLY_SECURITY_TYPES = frozenset(
    {
        "EXTRA_STOCK_A",
        "SH_A",
        "SZ_A",
        "BJ_A",
        "EXTRA_STOCK_A_SH_SZ",
    }
)


def should_keep_security_code(code: str, security_type: str | None = None) -> bool:
    """统一的证券代码过滤规则.

    当前规则：
    - 对股票代码池，如果代码以 `.BJ` 结尾，则只保留 `920xxx.BJ`
    - 其他代码维持原样
    """

    text = str(code).strip()
    if not text:
        return False
    if security_type in BJ_920_ONLY_SECURITY_TYPES and text.endswith(".BJ"):
        prefix = text.split(".", 1)[0]
        return prefix.startswith("920")
    return True


@dataclass(frozen=True)
class CalendarQuery:
    """交易日历查询参数."""

    data_type: str = "str"


@dataclass(frozen=True)
class CodeInfoQuery:
    """证券基础信息查询参数."""

    security_type: str


@dataclass(frozen=True)
class HistCodeQuery:
    """历史代码表查询参数."""

    security_type: str
    start_date: date
    end_date: date
    local_path: str


@dataclass(frozen=True)
class PriceFactorQuery:
    """复权因子查询参数."""

    code_list: tuple[str, ...]
    local_path: str
    is_local: bool = True


@dataclass(frozen=True)
class StockBasicQuery:
    """证券基础信息查询参数."""

    code_list: tuple[str, ...]


@dataclass(frozen=True)
class HistoryStockStatusQuery:
    """历史证券状态查询参数."""

    code_list: tuple[str, ...]
    local_path: str
    is_local: bool = True
    begin_date: Optional[date] = None
    end_date: Optional[date] = None


@dataclass(frozen=True)
class InfoPayloadQuery:
    """通用信息类 JSON 记录查询参数."""

    code_list: tuple[str, ...]
    begin_date: Optional[date] = None
    end_date: Optional[date] = None


@dataclass(frozen=True)
class MarketKlineQuery:
    """K 线查询参数."""

    code_list: tuple[str, ...]
    begin_date: date
    end_date: date
    begin_time: Optional[int] = None
    end_time: Optional[int] = None


@dataclass(frozen=True)
class MarketSnapshotQuery:
    """历史快照查询参数."""

    code_list: tuple[str, ...]
    begin_date: date
    end_date: date
    begin_time: Optional[int] = None
    end_time: Optional[int] = None


@dataclass(frozen=True)
class TradeCalendarRow:
    """交易日历落库行."""

    trade_date: date


@dataclass(frozen=True)
class CodeInfoRow:
    """证券基础信息落库行."""

    security_type: str
    code: str
    symbol: Optional[str] = None
    security_status_raw: Optional[str] = None
    pre_close: Optional[float] = None
    high_limited: Optional[float] = None
    low_limited: Optional[float] = None
    price_tick: Optional[float] = None


@dataclass(frozen=True)
class HistCodeDailyRow:
    """历史代码表按日成员行.

    这里使用日级拍平结构，而不是“批次 + 明细”模式，
    是因为在 ClickHouse 中按交易日和代码组合查询会更直接。
    """

    trade_date: date
    security_type: str
    code: str


@dataclass(frozen=True)
class PriceFactorRow:
    """复权因子落库行."""

    trade_date: date
    code: str
    factor_value: float


@dataclass(frozen=True)
class EtfPcfRow:
    """`get_etf_pcf` 主表落库行."""

    etf_code: str = ""
    creation_redemption_unit: Optional[int] = None
    max_cash_ratio: Optional[str] = None
    publish: Optional[str] = None
    creation: Optional[str] = None
    redemption: Optional[str] = None
    creation_redemption_switch: Optional[str] = None
    record_num: Optional[int] = None
    total_record_num: Optional[int] = None
    estimate_cash_component: Optional[float] = None
    trading_day: Optional[date] = None
    pre_trading_day: Optional[date] = None
    cash_component: Optional[float] = None
    nav_per_cu: Optional[float] = None
    nav: Optional[float] = None
    symbol: Optional[str] = None
    fund_management_company: Optional[str] = None
    underlying_security_id: Optional[str] = None
    underlying_security_id_source: Optional[str] = None
    dividend_per_cu: Optional[float] = None
    creation_limit: Optional[float] = None
    redemption_limit: Optional[float] = None
    creation_limit_per_user: Optional[float] = None
    redemption_limit_per_user: Optional[float] = None
    net_creation_limit: Optional[float] = None
    net_redemption_limit: Optional[float] = None
    net_creation_limit_per_user: Optional[float] = None
    net_redemption_limit_per_user: Optional[float] = None


@dataclass(frozen=True)
class EtfPcfConstituentRow:
    """`get_etf_pcf` 成分明细落库行."""

    etf_code: str = ""
    trading_day: Optional[date] = None
    underlying_symbol: Optional[str] = None
    component_share: Optional[float] = None
    substitute_flag: Optional[str] = None
    premium_ratio: Optional[float] = None
    discount_ratio: Optional[float] = None
    creation_cash_substitute: Optional[float] = None
    redemption_cash_substitute: Optional[float] = None
    substitution_cash_amount: Optional[float] = None
    underlying_security_id: Optional[str] = None


@dataclass(frozen=True)
class StockBasicRow:
    """`get_stock_basic` 落库行.

    入库字段统一使用数据库风格的小写 snake_case。
    """

    snapshot_date: date
    market_code: str
    security_name: Optional[str] = None
    comp_name: Optional[str] = None
    pinyin: Optional[str] = None
    comp_name_eng: Optional[str] = None
    list_date: Optional[int] = None
    delist_date: Optional[int] = None
    listplate_name: Optional[str] = None
    comp_sname_eng: Optional[str] = None
    is_listed: Optional[int] = None


@dataclass(frozen=True)
class HistoryStockStatusRow:
    """`get_history_stock_status` 落库行."""

    trade_date: date
    market_code: str
    preclose: Optional[float] = None
    high_limited: Optional[float] = None
    low_limited: Optional[float] = None
    price_high_lmt_rate: Optional[float] = None
    price_low_lmt_rate: Optional[float] = None
    is_st_sec: Optional[str] = None
    is_susp_sec: Optional[str] = None
    is_wd_sec: Optional[str] = None
    is_xr_sec: Optional[str] = None


@dataclass(frozen=True)
class BjCodeMappingRow:
    """`get_bj_code_mapping` 落库行."""

    old_code: str
    new_code: str
    security_name: Optional[str] = None
    listing_date: Optional[int] = None


@dataclass(frozen=True)
class ShareHolderRow:
    """`get_share_holder` 落库行."""

    ann_date: Optional[date] = None
    market_code: str = ""
    holder_enddate: Optional[date] = None
    holder_type: Optional[int] = None
    qty_num: Optional[int] = None
    holder_name: Optional[str] = None
    holder_holder_category: Optional[int] = None
    holder_quantity: Optional[float] = None
    holder_pct: Optional[float] = None
    holder_sharecategoryname: Optional[str] = None
    float_qty: Optional[float] = None


@dataclass(frozen=True)
class HolderNumRow:
    """`get_holder_num` 落库行."""

    market_code: str = ""
    ann_dt: Optional[date] = None
    holder_enddate: Optional[date] = None
    holder_total_num: Optional[float] = None
    holder_num: Optional[float] = None


@dataclass(frozen=True)
class EquityStructureRow:
    """`get_equity_structure` 落库行."""

    market_code: str = ""
    ann_date: Optional[date] = None
    change_date: Optional[date] = None
    share_change_reason_str: Optional[str] = None
    ex_change_date: Optional[date] = None
    current_sign: Optional[int] = None
    is_valid: Optional[int] = None
    tot_share: Optional[float] = None
    float_share: Optional[float] = None
    float_a_share: Optional[float] = None
    float_b_share: Optional[float] = None
    float_hk_share: Optional[float] = None
    float_os_share: Optional[float] = None
    tot_tradable_share: Optional[float] = None
    rtd_a_share_inst: Optional[float] = None
    rtd_a_share_domesnp: Optional[float] = None
    rtd_share_senior: Optional[float] = None
    rtd_a_share_foreign: Optional[float] = None
    rtd_a_share_forjur: Optional[float] = None
    rtd_a_share_fornp: Optional[float] = None
    restricted_b_share: Optional[float] = None
    other_rtd_share: Optional[float] = None
    non_tradable_share: Optional[float] = None
    ntrd_share_state_pct: Optional[float] = None
    ntrd_share_state: Optional[float] = None
    ntrd_share_statejur: Optional[float] = None
    ntrd_share_domesjur: Optional[float] = None
    ntrd_share_domes_initiator: Optional[float] = None
    ntrd_share_ipojuris: Optional[float] = None
    ntrd_share_genjuris: Optional[float] = None
    ntrd_share_stra_investor: Optional[float] = None
    ntrd_share_fund: Optional[float] = None
    ntrd_share_nat: Optional[float] = None
    tran_share: Optional[float] = None
    float_share_senior: Optional[float] = None
    share_inemp: Optional[float] = None
    preferred_share: Optional[float] = None
    ntrd_share_nlist_frgn: Optional[float] = None
    staq_share: Optional[float] = None
    net_share: Optional[float] = None
    share_change_reason: Optional[str] = None
    tot_a_share: Optional[float] = None
    tot_b_share: Optional[float] = None
    otca_share: Optional[float] = None
    otcb_share: Optional[float] = None
    tot_otc_share: Optional[float] = None
    share_hk: Optional[float] = None
    pre_non_tradable_share: Optional[float] = None
    restricted_a_share: Optional[float] = None
    rtd_a_share_state: Optional[float] = None
    rtd_a_share_statejur: Optional[float] = None
    rtd_a_share_other_domes: Optional[float] = None
    rtd_a_share_other_domesjur: Optional[float] = None
    tot_restricted_share: Optional[float] = None


@dataclass(frozen=True)
class EquityPledgeFreezeRow:
    """`get_equity_pledge_freeze` 落库行."""

    market_code: str = ""
    ann_date: Optional[date] = None
    holder_name: Optional[str] = None
    holder_type_code: Optional[int] = None
    total_holding_shr: Optional[float] = None
    total_holding_shr_ratio: Optional[float] = None
    fro_shares: Optional[float] = None
    fro_shr_to_total_holding_ratio: Optional[float] = None
    fro_shr_to_total_ratio: Optional[float] = None
    total_pledge_shr: Optional[float] = None
    is_equity_pledge_repo: Optional[int] = None
    begin_date: Optional[date] = None
    end_date: Optional[date] = None
    is_disfrozen: Optional[int] = None
    frozen_institution: Optional[str] = None
    disfrozen_time: Optional[date] = None
    shr_category_code: Optional[int] = None
    freeze_type: Optional[int] = None


@dataclass(frozen=True)
class EquityRestrictedRow:
    """`get_equity_restricted` 落库行."""

    market_code: str = ""
    list_date: Optional[date] = None
    share_ratio: Optional[float] = None
    share_lst_type_name: Optional[str] = None
    share_lst: Optional[int] = None
    share_lst_is_ann: Optional[int] = None
    close_price: Optional[float] = None
    share_lst_market_value: Optional[float] = None


@dataclass(frozen=True)
class DividendRow:
    """`get_dividend` 落库行."""

    market_code: str = ""
    div_progress: Optional[str] = None
    dvd_per_share_stk: Optional[float] = None
    dvd_per_share_pre_tax_cash: Optional[float] = None
    dvd_per_share_after_tax_cash: Optional[float] = None
    date_eqy_record: Optional[date] = None
    date_ex: Optional[date] = None
    date_dvd_payout: Optional[date] = None
    listingdate_of_dvd_shr: Optional[date] = None
    div_prelandate: Optional[date] = None
    div_smtgdate: Optional[date] = None
    date_dvd_ann: Optional[date] = None
    div_basedate: Optional[date] = None
    div_baseshare: Optional[float] = None
    currency_code: Optional[str] = None
    ann_date: Optional[date] = None
    is_changed: Optional[int] = None
    report_period: Optional[str] = None
    div_change: Optional[str] = None
    div_bonusrate: Optional[float] = None
    div_conversedrate: Optional[float] = None
    remark: Optional[str] = None
    div_preann_date: Optional[date] = None
    div_target: Optional[str] = None


@dataclass(frozen=True)
class RightIssueRow:
    """`get_right_issue` 落库行."""

    market_code: str = ""
    progress: Optional[int] = None
    price: Optional[float] = None
    ratio: Optional[float] = None
    amt_plan: Optional[float] = None
    amt_real: Optional[float] = None
    collection_fund: Optional[float] = None
    shareb_reg_date: Optional[date] = None
    ex_dividend_date: Optional[date] = None
    listed_date: Optional[date] = None
    pay_start_date: Optional[date] = None
    pay_end_date: Optional[date] = None
    preplan_date: Optional[date] = None
    smtg_ann_date: Optional[date] = None
    pass_date: Optional[date] = None
    approved_date: Optional[date] = None
    execute_date: Optional[date] = None
    result_date: Optional[date] = None
    list_ann_date: Optional[date] = None
    guarantor: Optional[str] = None
    guartype: Optional[float] = None
    rightsissue_code: Optional[str] = None
    ann_date: Optional[date] = None
    rightsissue_year: Optional[str] = None
    rightsissue_desc: Optional[str] = None
    rightsissue_name: Optional[str] = None
    ratio_denominator: Optional[float] = None
    ratio_molecular: Optional[float] = None
    subs_method: Optional[str] = None
    expected_fund_raising: Optional[float] = None


@dataclass(frozen=True)
class IndexConstituentRow:
    """`get_index_constituent` 落库行."""

    index_code: str = ""
    con_code: str = ""
    in_date: Optional[date] = None
    out_date: Optional[date] = None
    index_name: Optional[str] = None


@dataclass(frozen=True)
class IndexWeightRow:
    """`get_index_weight` 落库行."""

    index_code: str = ""
    con_code: str = ""
    trade_date: date = field(default_factory=date.today)
    total_share: Optional[float] = None
    free_share_ratio: Optional[float] = None
    calc_share: Optional[float] = None
    weight_factor: Optional[float] = None
    weight: Optional[float] = None
    close: Optional[float] = None


@dataclass(frozen=True)
class IndustryBaseInfoRow:
    """`get_industry_base_info` 落库行."""

    index_code: str = ""
    industry_code: Optional[str] = None
    level_type: Optional[int] = None
    level1_name: Optional[str] = None
    level2_name: Optional[str] = None
    level3_name: Optional[str] = None
    is_pub: Optional[int] = None
    change_reason: Optional[str] = None


@dataclass(frozen=True)
class IndustryConstituentRow:
    """`get_industry_constituent` 落库行."""

    index_code: str = ""
    con_code: str = ""
    in_date: Optional[date] = None
    out_date: Optional[date] = None
    index_name: Optional[str] = None


@dataclass(frozen=True)
class IndustryWeightRow:
    """`get_industry_weight` 落库行."""

    index_code: str = ""
    con_code: str = ""
    trade_date: date = field(default_factory=date.today)
    weight: Optional[float] = None


@dataclass(frozen=True)
class IndustryDailyRow:
    """`get_industry_daily` 落库行."""

    index_code: str = ""
    trade_date: date = field(default_factory=date.today)
    open: Optional[float] = None
    high: Optional[float] = None
    close: Optional[float] = None
    low: Optional[float] = None
    amount: Optional[float] = None
    volume: Optional[float] = None
    pb: Optional[float] = None
    pe: Optional[float] = None
    total_cap: Optional[float] = None
    a_float_cap: Optional[float] = None
    pre_close: Optional[float] = None


@dataclass(frozen=True)
class FundShareRow:
    """`get_fund_share` 落库行."""

    market_code: str = ""
    fund_share: Optional[float] = None
    change_reason: Optional[str] = None
    is_consolidated_data: Optional[int] = None
    ann_date: Optional[date] = None
    total_share: Optional[float] = None
    change_date: Optional[date] = None
    float_share: Optional[float] = None


@dataclass(frozen=True)
class FundIopvRow:
    """`get_fund_iopv` 落库行."""

    market_code: str = ""
    price_date: Optional[str] = None
    iopv_nav: Optional[float] = None


@dataclass(frozen=True)
class KzzShareRow:
    """`get_kzz_share` 落库行."""

    change_date: Optional[str] = None
    ann_date: Optional[str] = None
    market_code: str = ""
    bond_share: Optional[float] = None
    conv_share: Optional[float] = None
    change_reason: Optional[str] = None


@dataclass(frozen=True)
class KzzSuspendRow:
    """`get_kzz_suspend` 落库行."""

    market_code: str = ""
    suspend_date: Optional[str] = None
    suspend_type: Optional[int] = None
    resump_date: Optional[str] = None
    change_reason: Optional[str] = None
    change_reason_code: Optional[int] = None
    resump_time: Optional[str] = None


@dataclass(frozen=True)
class OptionBasicInfoRow:
    """`get_option_basic_info` 落库行."""

    contract_full_name: Optional[str] = None
    contract_type: Optional[str] = None
    delivery_month: Optional[str] = None
    expiry_date: Optional[str] = None
    exercise_price: Optional[float] = None
    exercise_end_date: Optional[str] = None
    start_trade_date: Optional[str] = None
    listing_ref_price: Optional[float] = None
    last_trade_date: Optional[str] = None
    exchange_code: Optional[str] = None
    delivery_date: Optional[str] = None
    contract_unit: Optional[int] = None
    is_trade: Optional[str] = None
    exchange_short_name: Optional[str] = None
    contract_adjust_flag: Optional[str] = None
    market_code: str = ""


@dataclass(frozen=True)
class OptionStdCtrSpecsRow:
    """`get_option_std_ctr_specs` 落库行."""

    exercise_date: Optional[str] = None
    contract_unit: Optional[int] = None
    position_declare_min: Optional[str] = None
    quote_currency_unit: Optional[str] = None
    last_trading_date: Optional[str] = None
    position_limit: Optional[str] = None
    delist_date: Optional[str] = None
    notional_value: Optional[str] = None
    exercise_method: Optional[str] = None
    delivery_method: Optional[str] = None
    settlement_month: Optional[str] = None
    trading_fee: Optional[str] = None
    exchange_name: Optional[str] = None
    option_en_name: Optional[str] = None
    contract_value: Optional[float] = None
    is_simulation: Optional[int] = None
    contract_unit_dimension: Optional[str] = None
    option_strike_price: Optional[str] = None
    is_simulation_trade: Optional[str] = None
    listed_date: Optional[str] = None
    option_name: Optional[str] = None
    premium: Optional[str] = None
    option_type: Optional[str] = None
    trading_hours_desc: Optional[str] = None
    final_settlement_date: Optional[str] = None
    final_settlement_price: Optional[str] = None
    min_price_unit: Optional[str] = None
    market_code: str = ""
    contract_multiplier: Optional[int] = None


@dataclass(frozen=True)
class OptionMonCtrSpecsRow:
    """`get_option_mon_ctr_specs` 落库行."""

    code_old: Optional[str] = None
    change_date: Optional[str] = None
    market_code: str = ""
    name_new: Optional[str] = None
    exercise_price_new: Optional[float] = None
    name_old: Optional[str] = None
    code_new: Optional[str] = None
    exercise_price_old: Optional[float] = None
    unit_old: Optional[float] = None
    unit_new: Optional[float] = None
    change_reason: Optional[str] = None


@dataclass(frozen=True)
class TreasuryYieldRow:
    """`get_treasury_yield` 落库行."""

    term: str
    trade_date: date
    yield_value: Optional[float] = None


KZZ_ISSUANCE_FIELD_NAMES = (
    "market_code",
    "stock_code",
    "crncy_code",
    "ann_dt",
    "pre_plan_date",
    "smtg_ann_date",
    "listed_ann_date",
    "listed_date",
    "plan_schedule",
    "is_separation",
    "recommender",
    "clause_is_int_cha_de_po_rate",
    "clause_is_com_int",
    "clause_com_int_rate",
    "clause_com_int_desc",
    "clause_init_conv_price_item",
    "clause_conv_adj_item",
    "clause_conv_period_item",
    "clause_ini_conv_price",
    "clause_ini_conv_premium_ratio",
    "clause_put_item",
    "clause_call_item",
    "clause_spec_down_adj",
    "clause_orig_ration_arr_item",
    "list_pass_date",
    "list_permit_date",
    "list_ann_date",
    "list_result_ann_date",
    "list_type",
    "list_fee",
    "list_ration_date",
    "list_ration_reg_date",
    "list_ration_paymt_date",
    "list_ration_code",
    "list_ration_name",
    "list_ration_price",
    "list_ration_ratio_de",
    "list_ration_ratio_mo",
    "list_ration_vol",
    "list_household",
    "list_onl_date",
    "list_pchase_code_onl",
    "list_pch_name_onl",
    "list_pch_price_onl",
    "list_issue_vol_onl",
    "list_code_onl",
    "list_excess_pch_onl",
    "result_ef_subscr_p_off",
    "result_suc_rate_off",
    "list_date_inst_off",
    "list_vol_inst_off",
    "result_suc_rate_on",
    "list_effect_pc_hvol_off",
    "list_eff_pc_h_of",
    "list_suc_rate_off",
    "pre_ration_vol",
    "list_issue_size",
    "list_issue_quantity",
    "min_off_inst_subscr_qty",
    "off_inst_dep_ratio",
    "max_off_inst_subscr_qty",
    "off_subscr_unit_inc_desc",
    "is_conv_bonds",
    "min_unline_public",
    "max_unline_public",
    "term_year",
    "interest_type",
    "coupon_rate",
    "interest_fre_quency",
    "result_suc_rate_on2",
    "coupon_txt",
    "ratio_annce_date",
    "ratio_date",
)

KZZ_ISSUANCE_INT_FIELDS = frozenset(
    {
        "is_separation",
        "clause_is_int_cha_de_po_rate",
        "clause_is_com_int",
        "is_conv_bonds",
    }
)

KZZ_ISSUANCE_STRING_FIELDS = frozenset(
    {
        "market_code",
        "stock_code",
        "crncy_code",
        "ann_dt",
        "pre_plan_date",
        "smtg_ann_date",
        "listed_ann_date",
        "listed_date",
        "plan_schedule",
        "recommender",
        "clause_com_int_desc",
        "clause_init_conv_price_item",
        "clause_conv_adj_item",
        "clause_conv_period_item",
        "clause_put_item",
        "clause_call_item",
        "clause_spec_down_adj",
        "clause_orig_ration_arr_item",
        "list_pass_date",
        "list_permit_date",
        "list_ann_date",
        "list_result_ann_date",
        "list_type",
        "list_ration_date",
        "list_ration_reg_date",
        "list_ration_paymt_date",
        "list_ration_code",
        "list_ration_name",
        "list_onl_date",
        "list_pchase_code_onl",
        "list_pch_name_onl",
        "list_date_inst_off",
        "off_inst_dep_ratio",
        "off_subscr_unit_inc_desc",
        "interest_type",
        "interest_fre_quency",
        "coupon_txt",
        "ratio_annce_date",
        "ratio_date",
    }
)

@dataclass(frozen=True)
class KzzConvChangeRow:
    """`get_kzz_conv_change` 落库行."""

    market_code: str = ""
    change_date: Optional[date] = None
    ann_date: Optional[date] = None
    conv_price: Optional[float] = None
    change_reason: Optional[str] = None


@dataclass(frozen=True)
class KzzCorrRow:
    """`get_kzz_corr` 落库行."""

    market_code: str = ""
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    corr_trig_calc_max_period: Optional[float] = None
    corr_trig_calc_period: Optional[float] = None
    spec_corr_trig_ratio: Optional[float] = None
    corr_conv_price_floor_desc: Optional[str] = None
    ref_price_is_avg_price: Optional[int] = None
    corr_times_limit: Optional[str] = None
    is_timepoint_corr_clause_flag: Optional[int] = None
    timepoint_count: Optional[float] = None
    timepoint_corr_text_clause: Optional[str] = None
    spec_corr_range: Optional[float] = None
    is_spec_down_corr_clause_flag: Optional[int] = None


@dataclass(frozen=True)
class KzzCallExplanationRow:
    """`get_kzz_call_explanation` 落库行."""

    market_code: str = ""
    call_date: Optional[date] = None
    call_price: Optional[float] = None
    call_announcement_date: Optional[date] = None
    call_ful_res_ann_date: Optional[date] = None
    call_amount: Optional[float] = None
    call_outstanding_amount: Optional[float] = None
    call_date_pub: Optional[date] = None
    call_fund_arrival_date: Optional[date] = None
    call_record_day: Optional[date] = None
    call_reason: Optional[str] = None


@dataclass(frozen=True)
class KzzPutExplanationRow:
    """`get_kzz_put_explanation` 落库行."""

    market_code: str = ""
    put_fund_arrival_date: Optional[str] = None
    put_price: Optional[float] = None
    put_announcement_date: Optional[str] = None
    put_ex_date: Optional[str] = None
    put_amount: Optional[float] = None
    put_outstanding: Optional[float] = None
    repurchase_start_date: Optional[str] = None
    repurchase_end_date: Optional[str] = None
    resale_start_date: Optional[str] = None
    fund_end_date: Optional[str] = None
    repurchase_code: Optional[str] = None
    resale_amount: Optional[float] = None
    resale_imp_amount: Optional[float] = None
    resale_end_date: Optional[str] = None


@dataclass(frozen=True)
class KzzPutCallItemRow:
    """`get_kzz_put_call_item` 落库行."""

    market_code: str = ""
    mand_put_period: Optional[str] = None
    mand_put_price: Optional[float] = None
    mand_put_start_date: Optional[date] = None
    mand_put_end_date: Optional[date] = None
    mand_put_text: Optional[str] = None
    is_mand_put_contain_current: Optional[int] = None
    con_put_start_date: Optional[date] = None
    con_put_end_date: Optional[date] = None
    max_put_triper: Optional[float] = None
    put_triperiod: Optional[float] = None
    add_put_con: Optional[str] = None
    add_put_price_ins: Optional[str] = None
    put_num_ins: Optional[str] = None
    put_pro_period: Optional[float] = None
    put_no_pery: Optional[float] = None
    is_put_item: Optional[int] = None
    is_term_put_item: Optional[int] = None
    is_mand_put_item: Optional[int] = None
    is_time_put_item: Optional[int] = None
    time_put_no: Optional[float] = None
    time_put_item: Optional[str] = None
    term_put_price: Optional[float] = None
    con_call_start_date: Optional[date] = None
    con_call_end_date: Optional[date] = None
    call_tri_con_ins: Optional[str] = None
    max_call_triper: Optional[float] = None
    call_tri_per: Optional[float] = None
    call_num_ber_ins: Optional[str] = None
    is_call_item: Optional[int] = None
    call_pro_period: Optional[float] = None
    call_no_pery: Optional[float] = None
    is_time_call_item: Optional[int] = None
    time_call_no: Optional[float] = None
    time_call_text: Optional[str] = None
    expired_redemption_price: Optional[float] = None
    put_tri_cond_desc: Optional[str] = None


@dataclass(frozen=True)
class KzzPutRow:
    """`get_kzz_put` 落库行."""

    market_code: str = ""
    put_price: Optional[float] = None
    begin_date: Optional[date] = None
    end_date: Optional[date] = None
    tri_ratio: Optional[float] = None


@dataclass(frozen=True)
class KzzCallRow:
    """`get_kzz_call` 落库行."""

    market_code: str = ""
    call_price: Optional[float] = None
    begin_date: Optional[date] = None
    end_date: Optional[date] = None
    tri_ratio: Optional[float] = None


@dataclass(frozen=True)
class KzzConvRow:
    """`get_kzz_conv` 落库行."""

    market_code: str = ""
    ann_date: Optional[date] = None
    conv_code: Optional[str] = None
    conv_name: Optional[str] = None
    conv_price: Optional[float] = None
    currency_code: Optional[str] = None
    conv_start_date: Optional[date] = None
    conv_end_date: Optional[date] = None
    trade_date_last: Optional[date] = None
    forced_conv_date: Optional[date] = None
    forced_conv_price: Optional[float] = None
    rel_conv_month: Optional[float] = None
    is_forced: Optional[float] = None
    forced_conv_reason: Optional[str] = None


@dataclass(frozen=True)
class BlockTradingRow:
    """`get_block_trading` 落库行."""

    market_code: str = ""
    trade_date: Optional[date] = None
    bshare_price: Optional[float] = None
    bshare_volume: Optional[float] = None
    b_frequency: Optional[int] = None
    block_avg_volume: Optional[float] = None
    bshare_amount: Optional[float] = None
    bbuyer_name: Optional[str] = None
    bseller_name: Optional[str] = None


@dataclass(frozen=True)
class LongHuBangRow:
    """`get_long_hu_bang` 落库行."""

    market_code: str = ""
    trade_date: Optional[date] = None
    security_name: Optional[str] = None
    reason_type: Optional[str] = None
    reason_type_name: Optional[str] = None
    change_range: Optional[float] = None
    trader_name: Optional[str] = None
    buy_amount: Optional[float] = None
    sell_amount: Optional[float] = None
    flow_mark: Optional[int] = None
    total_amount: Optional[float] = None
    total_volume: Optional[float] = None


@dataclass(frozen=True)
class MarginDetailRow:
    """`get_margin_detail` 落库行."""

    market_code: str = ""
    security_name: Optional[str] = None
    trade_date: Optional[date] = None
    borrow_money_balance: Optional[float] = None
    purch_with_borrow_money: Optional[float] = None
    repayment_of_borrow_money: Optional[float] = None
    sec_lending_balance: Optional[float] = None
    sales_of_borrowed_sec: Optional[int] = None
    repayment_of_borrow_sec: Optional[int] = None
    sec_lending_balance_vol: Optional[int] = None
    margin_trade_balance: Optional[float] = None


@dataclass(frozen=True)
class MarginSummaryRow:
    """`get_margin_summary` 落库行."""

    trade_date: Optional[date] = None
    sum_borrow_money_balance: Optional[float] = None
    sum_purch_with_borrow_money: Optional[float] = None
    sum_repayment_of_borrow_money: Optional[float] = None
    sum_sec_lending_balance: Optional[float] = None
    sum_sales_of_borrowed_sec: Optional[int] = None
    sum_margin_trade_balance: Optional[float] = None


BALANCE_SHEET_FIELD_NAMES = (
    "market_code",
    "security_name",
    "statement_type",
    "report_type",
    "reporting_period",
    "ann_date",
    "actual_ann_date",
    "acc_payable",
    "acc_receivable",
    "acc_receivables",
    "accrued_exp",
    "acct_payable",
    "acct_receivable",
    "act_trading_sec",
    "act_uw_sec",
    "adv_prem",
    "adv_receipt",
    "agency_assets",
    "agency_business_liab",
    "anticipation_liab",
    "asset_dep_funds_oth_fin_inst",
    "bonds_payable",
    "cap_resv",
    "cap_stock",
    "cash_central_bank_deposits",
    "ced_insur_cont_reserves_rcv",
    "claims_payable",
    "clients_fund_deposit",
    "clients_reserves",
    "cnvd_diff_foreign_curr_stat",
    "comp_type_code",
    "const_in_proc",
    "const_in_proc_total",
    "consump_bio_assets",
    "cont_assets",
    "cont_liabilities",
    "currency_cap",
    "currency_code",
    "debt_inv",
    "deferred_inc_noncur_liab",
    "deferred_income",
    "deferred_tax_assets",
    "deferred_tax_liab",
    "dep_received_ib_dep",
    "deposit_cap_recog",
    "deposit_taking",
    "deposits_received",
    "der_fin_assets",
    "deri_fin_liab",
    "develop_exp",
    "disposal_fix_assets",
    "div_payable",
    "div_receivable",
    "empl_pay_payable",
    "engin_mat",
    "fin_assets_ava_for_sale",
    "fin_assets_cost_sharing",
    "fin_assets_fair_value",
    "fixed_assets",
    "fixed_assets_total",
    "fixed_term_deposits",
    "goodwill",
    "gua_deposits_paid",
    "gua_pledge_loans",
    "hold_assets_for_sale",
    "hold_to_mty_inv",
    "inc_pledge_loan",
    "incl_trading_seat_fees",
    "ind_acct_assets",
    "ind_acct_liab",
    "insured_deposit_inv",
    "insured_div_payable",
    "int_receivable",
    "intangible_assets",
    "interest_payable",
    "inv",
    "inv_realestate",
    "lease_liability",
    "lend_funds",
    "lending_funds",
    "less_treasury_stk",
    "lia_hfs",
    "liab_dep_funds_oth_fin_inst",
    "life_insur_resv",
    "loan_central_bank",
    "loans_and_advances",
    "loans_from_oth_banks",
    "lt_deferred_exp",
    "lt_emp_comp_pay",
    "lt_equity_inv",
    "lt_health_insur_resv",
    "lt_loan",
    "lt_payable",
    "lt_payable_total",
    "lt_receivables",
    "minority_equity",
    "nom_risks_prep",
    "noncur_assets_due_within_1y",
    "noncur_liab_due_within_1y",
    "notes_payable",
    "notes_receivable",
    "oil_and_gas_assets",
    "oth_comp_income",
    "oth_equity_tools",
    "oth_equity_tools_pre_shr",
    "oth_noncur_assets",
    "other_assets",
    "other_cur_assets",
    "other_cur_liab",
    "other_debt_inv",
    "other_equity_inv",
    "other_liab",
    "other_noncur_fin_assets",
    "other_noncur_liab",
    "other_payable",
    "other_payable_total",
    "other_rcv_total",
    "other_receivable",
    "other_sustain_bond",
    "out_loss_resv",
    "payable",
    "payable_for_reinsurer",
    "precious_metal",
    "prepayment",
    "prod_bio_assets",
    "rcv_ced_claim_resv",
    "rcv_ced_life_insur_resv",
    "rcv_ced_lt_health_insur_resv",
    "rcv_ced_unearned_prem_resv",
    "rcv_financing",
    "rcv_inv",
    "receivable_prem",
    "red_mon_cap_for_sale",
    "reinsurance_acc_rcv",
    "rsrv_fund_insur_cont",
    "sell_repo_fin_assets",
    "service_charge_comm_payable",
    "settle_funds",
    "spe_assets_bal_diff",
    "spe_cur_assets_diff",
    "spe_cur_liab_diff",
    "spe_liab_bal_diff",
    "spe_liab_equity_bal_diff",
    "spe_noncur_assets_diff",
    "spe_noncur_liab_diff",
    "spe_share_equity_bal_diff",
    "special_payable",
    "special_resv",
    "st_bonds_payable",
    "st_borrowing",
    "st_fin_payable",
    "subr_rcv",
    "surplus_resv",
    "tax_payable",
    "tot_assets_bal_diff",
    "tot_cur_assets_diff",
    "tot_cur_liab_diff",
    "tot_liab_bal_diff",
    "tot_liab_equity_bal_diff",
    "tot_noncur_assets",
    "tot_noncur_assets_diff",
    "tot_noncur_liab_diff",
    "tot_share",
    "tot_share_equity_bal_diff",
    "tot_share_equity_excl_min_int",
    "tot_share_equity_incl_min_int",
    "total_assets",
    "total_cur_assets",
    "total_cur_liab",
    "total_liab",
    "total_liab_share_equity",
    "total_noncur_liab",
    "trading_fin_liab",
    "trading_finassets",
    "unamortized_exp",
    "unconfirmed_inv_loss",
    "undistributed_pro",
    "unearned_prem_resv",
    "use_right_assets",
)

BALANCE_SHEET_DATE_FIELDS = frozenset({"reporting_period", "ann_date", "actual_ann_date"})
BALANCE_SHEET_INT_FIELDS = frozenset({"comp_type_code"})
BALANCE_SHEET_STRING_FIELDS = frozenset(
    {
        "market_code",
        "security_name",
        "statement_type",
        "report_type",
        "currency_code",
    }
)


def _structured_field_type(
    field_name: str,
    *,
    date_fields: frozenset[str],
    int_fields: frozenset[str],
    string_fields: frozenset[str],
):
    if field_name in date_fields:
        return Optional[date]
    if field_name in int_fields:
        return Optional[int]
    if field_name in string_fields:
        return Optional[str]
    return Optional[float]


KzzIssuanceRow = make_dataclass(
    "KzzIssuanceRow",
    [
        (
            field_name,
            _structured_field_type(
                field_name,
                date_fields=frozenset(),
                int_fields=KZZ_ISSUANCE_INT_FIELDS,
                string_fields=KZZ_ISSUANCE_STRING_FIELDS,
            ),
            field(default=None),
        )
        for field_name in KZZ_ISSUANCE_FIELD_NAMES
    ],
    namespace={"__doc__": "`get_kzz_issuance` 落库行."},
    frozen=True,
)


BalanceSheetRow = make_dataclass(
    "BalanceSheetRow",
    [
        (
            field_name,
            _structured_field_type(
                field_name,
                date_fields=BALANCE_SHEET_DATE_FIELDS,
                int_fields=BALANCE_SHEET_INT_FIELDS,
                string_fields=BALANCE_SHEET_STRING_FIELDS,
            ),
            field(default=None),
        )
        for field_name in BALANCE_SHEET_FIELD_NAMES
    ],
    namespace={"__doc__": "`get_balance_sheet` 落库行."},
    frozen=True,
)

CASH_FLOW_FIELD_NAMES = (
    "market_code",
    "security_name",
    "statement_type",
    "report_type",
    "reporting_period",
    "ann_date",
    "actual_ann_date",
    "absorb_cash_recp_inv",
    "amort_intan_assets",
    "amort_lt_deferred_exp",
    "beg_bal_cash_cash_equ",
    "cash_end_bal",
    "cash_for_charge",
    "cash_paid_insur_policy",
    "cash_paid_inv",
    "cash_paid_pur_const_fiolta",
    "cash_pay_claims_oic",
    "cash_pay_dist_div_pro_int",
    "cash_pay_employee",
    "cash_pay_for_debt",
    "cash_pay_goods_services",
    "cash_rece_borrow",
    "cash_rece_issue_bonds",
    "cash_recp_inv_income",
    "cash_recp_prem_oic",
    "cash_recp_recov_inv",
    "cash_recp_sg_and_rs",
    "comp_type_code",
    "conv_corp_bonds_due_within_1y",
    "conv_debt_into_cap",
    "credit_impair_loss",
    "currency_code",
    "decr_defe_inc_tax_assets",
    "decr_deferred_expense",
    "decr_inventory",
    "decr_opera_receivable",
    "depre_fa_oga_pba",
    "eff_fx_fluc_cash",
    "end_bal_cash_cash_equ",
    "financial_exp",
    "fixed_assets_fin_lease",
    "free_cash_flow",
    "incl_cash_recp_saims",
    "incl_div_pro_paid_sms",
    "incr_accrued_exp",
    "incr_defe_inc_tax_liab",
    "incr_opera_payable",
    "ind_net_cash_flows_opera_act",
    "ind_net_incr_cash_and_equ",
    "inv_loss",
    "is_calculation",
    "less_open_bal_cash",
    "less_open_bal_cash_equ",
    "loss_disp_fiolta",
    "loss_fairvalue_chg",
    "loss_fixed_assets",
    "net_cash_flows_fin_act",
    "net_cash_flows_inv_act",
    "net_cash_flows_opera_act",
    "net_cash_paid_sobu",
    "net_cash_rec_sec",
    "net_cash_recp_disp_fiolta",
    "net_cash_recp_disp_sobu",
    "net_cash_recp_reinsu_bus",
    "net_incr_borr_fund",
    "net_incr_borr_ofi",
    "net_incr_cash_and_cash_equ",
    "net_incr_cus_loan_adv",
    "net_incr_dep_cb_ib",
    "net_incr_dep_cus_and_ib",
    "net_incr_dismantle_cap",
    "net_incr_disp_faas",
    "net_incr_disp_tfa",
    "net_incr_insured_save",
    "net_incr_int_and_charge",
    "net_incr_loans_central_bank",
    "net_incr_pledge_loan",
    "net_incr_repu_bus_fund",
    "net_profit",
    "oth_cash_pay_inv_act",
    "oth_cash_pay_opera_act",
    "oth_cash_recp_inv_act",
    "other_assets_impair_loss",
    "other_cash_pay_fin_act",
    "other_cash_recp_fin_act",
    "other_cash_recp_oper_act",
    "others",
    "pay_all_tax",
    "plus_assets_depre_prep",
    "plus_end_bal_cash_equ",
    "recp_tax_refund",
    "spe_bal_cash_inflow_fin_act",
    "spe_bal_cash_inflow_inv_act",
    "spe_bal_cash_inflow_opera_act",
    "spe_bal_cash_outflow_fin",
    "spe_bal_cash_outflow_inv",
    "spe_bal_cash_outflow_opera",
    "spe_bal_netcash_inc_diff_ind",
    "spe_bal_netcash_incr_diff",
    "spe_bal_netcash_opera_ind",
    "tot_bal_cash_inflow_fin_act",
    "tot_bal_cash_inflow_inv_act",
    "tot_bal_cash_inflow_opera_act",
    "tot_bal_cash_outflow_fin",
    "tot_bal_cash_outflow_inv",
    "tot_bal_cash_outflow_opera",
    "tot_bal_netcash_flow_fin",
    "tot_bal_netcash_flow_inv",
    "tot_bal_netcash_flow_opera",
    "tot_bal_netcash_inc_diff_ind",
    "tot_bal_netcash_incr_diff",
    "tot_bal_netcash_opera_ind",
    "tot_cash_inflow_fin_act",
    "tot_cash_inflow_inv_act",
    "tot_cash_inflow_oper_act",
    "tot_cash_outflow_fin_act",
    "tot_cash_outflow_inv_act",
    "tot_cash_outflow_opera_act",
    "unconfirmed_inv_loss",
    "use_right_asset_dep",
)

CASH_FLOW_DATE_FIELDS = frozenset({"reporting_period", "ann_date", "actual_ann_date"})
CASH_FLOW_INT_FIELDS = frozenset({"is_calculation"})
CASH_FLOW_STRING_FIELDS = frozenset(
    {
        "market_code",
        "security_name",
        "statement_type",
        "report_type",
        "comp_type_code",
        "currency_code",
    }
)

CashFlowRow = make_dataclass(
    "CashFlowRow",
    [
        (
            field_name,
            _structured_field_type(
                field_name,
                date_fields=CASH_FLOW_DATE_FIELDS,
                int_fields=CASH_FLOW_INT_FIELDS,
                string_fields=CASH_FLOW_STRING_FIELDS,
            ),
            field(default=None),
        )
        for field_name in CASH_FLOW_FIELD_NAMES
    ],
    namespace={"__doc__": "`get_cash_flow` 落库行."},
    frozen=True,
)

INCOME_FIELD_NAMES = (
    "market_code",
    "security_name",
    "statement_type",
    "report_type",
    "reporting_period",
    "ann_date",
    "actual_ann_date",
    "amort_cost_fin_assets_ear",
    "basic_eps",
    "beg_undistributed_pro",
    "capitalized_com_stock_div",
    "comments",
    "common_stock_div_payable",
    "comp_type_code",
    "continued_net_opera_pro",
    "credit_impair_loss",
    "currency_code",
    "diluted_eps",
    "distributive_pro",
    "distributive_pro_shareholder",
    "div_exp_insur",
    "ebit",
    "ebitda",
    "employee_welfare",
    "end_net_opera_pro",
    "ext_insur_cont_rsrv",
    "ext_unearned_prem_res",
    "fin_exp_int_exp",
    "fin_exp_int_inc",
    "gain_disposal_assets",
    "handling_chrg_comm_fee",
    "incl_inc_inv_jv_entp",
    "incl_less_loss_disp_ncur_asset",
    "incl_reinsur_prem_inc",
    "income_tax",
    "insur_exp",
    "insur_prem",
    "interest_inc",
    "is_calculation",
    "less_admin_exp",
    "less_amort_compen_exp",
    "less_amort_insur_cont_rsrv",
    "less_amort_reinsur_exp",
    "less_assets_impair_loss",
    "less_bus_tax_surcharge",
    "less_fin_exp",
    "less_handling_chrg_comm_fee",
    "less_interest_exp",
    "less_non_opera_exp",
    "less_opera_cost",
    "less_reinsur_prem",
    "less_selling_exp",
    "min_int_inc",
    "net_exposure_hedging_gain",
    "net_handling_chrg_comm_fee",
    "net_inc_ec_asset_mgmt_bus",
    "net_inc_sec_brok_bus",
    "net_inc_sec_uw_bus",
    "net_interest_inc",
    "net_pro_after_ded_nr_gl",
    "net_pro_after_ded_nr_gl_cor",
    "net_pro_excl_min_int_inc",
    "net_pro_incl_min_int_inc",
    "net_pro_under_int_acc_sta",
    "opera_exp",
    "opera_profit",
    "opera_rev",
    "oth_assets_impair_loss",
    "oth_bus_cost",
    "oth_bus_inc",
    "oth_compre_inc",
    "oth_income",
    "oth_net_opera_inc",
    "plus_net_fx_inc",
    "plus_net_gain_chg_fv",
    "plus_net_inv_inc",
    "plus_non_opera_rev",
    "plus_oth_net_bus_inc",
    "preferred_share_div_payable",
    "prem_bus_inc",
    "rd_exp",
    "reinsurance_exp",
    "spe_bal_net_pro_marg",
    "spe_bal_opera_pro_marg",
    "spe_bal_tot_opera_cost_dif",
    "spe_bal_tot_opera_inc_dif",
    "spe_bal_tot_pro_marg",
    "spe_tot_opera_cost_dif_state",
    "spe_tot_opera_inc_dif_state",
    "surr_value",
    "tot_bal_net_pro_marg",
    "tot_bal_opera_pro_marg",
    "tot_bal_tot_pro_marg",
    "tot_compen_exp",
    "tot_compre_inc",
    "tot_compre_inc_min_share",
    "tot_compre_inc_parent_comp",
    "tot_opera_cost",
    "tot_opera_cost2",
    "tot_opera_rev",
    "total_profit",
    "transfer_housing_revo_funds",
    "transfer_others",
    "transfer_surplus_reserve",
    "unconfirmed_inv_loss",
    "withdraw_any_surplus_resv",
    "withdraw_ent_develop_fund",
    "withdraw_leg_pub_wel_fund",
    "withdraw_leg_surplus",
    "withdraw_resv_fund",
)

INCOME_DATE_FIELDS = frozenset({"reporting_period", "ann_date", "actual_ann_date"})
INCOME_INT_FIELDS = frozenset()
INCOME_STRING_FIELDS = frozenset(
    {
        "market_code",
        "security_name",
        "statement_type",
        "report_type",
        "comments",
        "comp_type_code",
        "currency_code",
        "spe_tot_opera_cost_dif_state",
        "spe_tot_opera_inc_dif_state",
    }
)

IncomeRow = make_dataclass(
    "IncomeRow",
    [
        (
            field_name,
            _structured_field_type(
                field_name,
                date_fields=INCOME_DATE_FIELDS,
                int_fields=INCOME_INT_FIELDS,
                string_fields=INCOME_STRING_FIELDS,
            ),
            field(default=None),
        )
        for field_name in INCOME_FIELD_NAMES
    ],
    namespace={"__doc__": "`get_income` 落库行."},
    frozen=True,
)

PROFIT_EXPRESS_FIELD_NAMES = (
    "market_code",
    "reporting_period",
    "ann_date",
    "actual_ann_date",
    "total_assets",
    "net_pro_excl_min_int_inc",
    "tot_opera_rev",
    "total_profit",
    "opera_profit",
    "eps_basic",
    "tot_share_equ_excl_min_int",
    "is_audit",
    "roe_weighted",
    "last_year_revised_net_pro",
    "performance_summary",
    "net_asset_ps",
    "memo",
    "yoy_gr_gross_pro",
    "yoy_gr_gross_rev",
    "yoy_gr_net_profit_parent",
    "yoy_gr_tot_pro",
    "yoy_id_waroe",
    "yoy_gr_eps_basic",
    "growth_rate_equity",
    "growth_rate_assets",
    "growth_rate_naps",
    "last_year_tot_opera_rev",
    "last_year_total_profit",
    "last_year_opera_pro",
    "last_year_eps_diluted",
    "last_year_net_profit",
    "initial_net_asset_ps",
    "initial_net_assets",
)

PROFIT_EXPRESS_DATE_FIELDS = frozenset({"reporting_period", "ann_date", "actual_ann_date"})
PROFIT_EXPRESS_INT_FIELDS = frozenset()
PROFIT_EXPRESS_STRING_FIELDS = frozenset({"market_code", "performance_summary", "memo"})

ProfitExpressRow = make_dataclass(
    "ProfitExpressRow",
    [
        (
            field_name,
            _structured_field_type(
                field_name,
                date_fields=PROFIT_EXPRESS_DATE_FIELDS,
                int_fields=PROFIT_EXPRESS_INT_FIELDS,
                string_fields=PROFIT_EXPRESS_STRING_FIELDS,
            ),
            field(default=None),
        )
        for field_name in PROFIT_EXPRESS_FIELD_NAMES
    ],
    namespace={"__doc__": "`get_profit_express` 落库行."},
    frozen=True,
)

PROFIT_NOTICE_FIELD_NAMES = (
    "market_code",
    "security_name",
    "p_typecode",
    "reporting_period",
    "ann_date",
    "p_change_max",
    "p_change_min",
    "net_profit_max",
    "net_profit_min",
    "first_ann_date",
    "p_number",
    "p_reason",
    "p_summary",
    "p_net_parent_firm",
    "report_type",
)

PROFIT_NOTICE_DATE_FIELDS = frozenset({"reporting_period", "ann_date", "first_ann_date"})
PROFIT_NOTICE_INT_FIELDS = frozenset()
PROFIT_NOTICE_STRING_FIELDS = frozenset(
    {
        "market_code",
        "security_name",
        "p_typecode",
        "p_reason",
        "p_summary",
        "report_type",
    }
)

ProfitNoticeRow = make_dataclass(
    "ProfitNoticeRow",
    [
        (
            field_name,
            _structured_field_type(
                field_name,
                date_fields=PROFIT_NOTICE_DATE_FIELDS,
                int_fields=PROFIT_NOTICE_INT_FIELDS,
                string_fields=PROFIT_NOTICE_STRING_FIELDS,
            ),
            field(default=None),
        )
        for field_name in PROFIT_NOTICE_FIELD_NAMES
    ],
    namespace={"__doc__": "`get_profit_notice` 落库行."},
    frozen=True,
)


@dataclass(frozen=True)
class InfoPayloadRow:
    """通用信息类原始记录落库行.

    在字段清单未完全稳定前，先保留：
    - `market_code`
    - `report_date`
    - `report_date_raw`
    - `payload_json`
    """

    market_code: str
    report_date: Optional[date] = None
    report_date_raw: Optional[str] = None
    payload_json: str = "{}"


@dataclass(frozen=True)
class MarketKlineRow:
    """`query_kline` 落库行."""

    trade_time: datetime
    code: str
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None
    amount: Optional[float] = None


@dataclass(frozen=True)
class MarketSnapshotRow:
    """`query_snapshot` 落库行."""

    trade_time: datetime
    code: str
    pre_close: Optional[float] = None
    last: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None
    amount: Optional[float] = None
    num_trades: Optional[float] = None
    high_limited: Optional[float] = None
    low_limited: Optional[float] = None
    ask_price1: Optional[float] = None
    ask_price2: Optional[float] = None
    ask_price3: Optional[float] = None
    ask_price4: Optional[float] = None
    ask_price5: Optional[float] = None
    ask_volume1: Optional[int] = None
    ask_volume2: Optional[int] = None
    ask_volume3: Optional[int] = None
    ask_volume4: Optional[int] = None
    ask_volume5: Optional[int] = None
    bid_price1: Optional[float] = None
    bid_price2: Optional[float] = None
    bid_price3: Optional[float] = None
    bid_price4: Optional[float] = None
    bid_price5: Optional[float] = None
    bid_volume1: Optional[int] = None
    bid_volume2: Optional[int] = None
    bid_volume3: Optional[int] = None
    bid_volume4: Optional[int] = None
    bid_volume5: Optional[int] = None
    iopv: Optional[float] = None
    trading_phase_code: Optional[str] = None
    total_long_position: Optional[int] = None
    pre_settle: Optional[float] = None
    auction_price: Optional[float] = None
    auction_volume: Optional[int] = None
    settle: Optional[float] = None
    contract_type: Optional[str] = None
    expire_date: Optional[int] = None
    underlying_security_code: Optional[str] = None
    exercise_price: Optional[float] = None
    action_day: Optional[str] = None
    trading_day: Optional[str] = None
    pre_open_interest: Optional[int] = None
    open_interest: Optional[int] = None
    average_price: Optional[float] = None
    nominal_price: Optional[float] = None
    ref_price: Optional[float] = None
    bid_price_limit_up: Optional[float] = None
    bid_price_limit_down: Optional[float] = None
    offer_price_limit_up: Optional[float] = None
    offer_price_limit_down: Optional[float] = None


@dataclass(frozen=True)
class SyncTaskLogRow:
    """同步任务日志行.

    这张表用于记录每次接口级同步的执行结果，支撑两类能力：
    1. 运行日志审计
    2. 当天同步成功后再次执行时的跳过判断
    """

    task_name: str
    scope_key: str
    run_date: date
    status: str
    target_table: str
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    row_count: int = 0
    message: Optional[str] = None
    started_at: datetime = field(default_factory=utcnow)
    finished_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class SyncCheckpointRow:
    """同步断点记录."""

    task_name: str
    scope_key: str
    run_date: date
    status: str
    target_table: str
    checkpoint_date: Optional[date] = None
    row_count: int = 0
    message: Optional[str] = None
    finished_at: datetime = field(default_factory=utcnow)


__all__ = [
    "BALANCE_SHEET_DATE_FIELDS",
    "BALANCE_SHEET_FIELD_NAMES",
    "BALANCE_SHEET_INT_FIELDS",
    "BALANCE_SHEET_STRING_FIELDS",
    "BalanceSheetRow",
    "BjCodeMappingRow",
    "CASH_FLOW_DATE_FIELDS",
    "CASH_FLOW_FIELD_NAMES",
    "CASH_FLOW_INT_FIELDS",
    "CASH_FLOW_STRING_FIELDS",
    "CalendarQuery",
    "CashFlowRow",
    "CodeInfoQuery",
    "CodeInfoRow",
    "BJ_920_ONLY_SECURITY_TYPES",
    "DividendRow",
    "EtfPcfRow",
    "EtfPcfConstituentRow",
    "EquityPledgeFreezeRow",
    "EquityRestrictedRow",
    "EquityStructureRow",
    "FundIopvRow",
    "FundShareRow",
    "HolderNumRow",
    "HistoryStockStatusQuery",
    "HistoryStockStatusRow",
    "InfoPayloadQuery",
    "InfoPayloadRow",
    "INCOME_DATE_FIELDS",
    "INCOME_FIELD_NAMES",
    "INCOME_INT_FIELDS",
    "INCOME_STRING_FIELDS",
    "IndustryBaseInfoRow",
    "IndustryConstituentRow",
    "IndustryDailyRow",
    "IndustryWeightRow",
    "IndexConstituentRow",
    "IndexWeightRow",
    "IncomeRow",
    "KZZ_ISSUANCE_FIELD_NAMES",
    "KZZ_ISSUANCE_INT_FIELDS",
    "KZZ_ISSUANCE_STRING_FIELDS",
    "KzzIssuanceRow",
    "KzzShareRow",
    "KzzSuspendRow",
    "KzzConvChangeRow",
    "KzzCorrRow",
    "KzzCallExplanationRow",
    "KzzPutExplanationRow",
    "KzzPutCallItemRow",
    "KzzPutRow",
    "KzzCallRow",
    "KzzConvRow",
    "BlockTradingRow",
    "LongHuBangRow",
    "MarginDetailRow",
    "MarginSummaryRow",
    "MarketKlineQuery",
    "MarketKlineRow",
    "MarketSnapshotQuery",
    "MarketSnapshotRow",
    "OptionBasicInfoRow",
    "OptionMonCtrSpecsRow",
    "OptionStdCtrSpecsRow",
    "HistCodeDailyRow",
    "HistCodeQuery",
    "PriceFactorQuery",
    "PriceFactorRow",
    "PROFIT_EXPRESS_DATE_FIELDS",
    "PROFIT_EXPRESS_FIELD_NAMES",
    "PROFIT_EXPRESS_INT_FIELDS",
    "PROFIT_EXPRESS_STRING_FIELDS",
    "ProfitExpressRow",
    "PROFIT_NOTICE_DATE_FIELDS",
    "PROFIT_NOTICE_FIELD_NAMES",
    "PROFIT_NOTICE_INT_FIELDS",
    "PROFIT_NOTICE_STRING_FIELDS",
    "ProfitNoticeRow",
    "RightIssueRow",
    "ShareHolderRow",
    "StockBasicQuery",
    "StockBasicRow",
    "SyncCheckpointRow",
    "SyncTaskLogRow",
    "TreasuryYieldRow",
    "TradeCalendarRow",
    "normalize_code_list",
    "should_keep_security_code",
    "to_ch_date",
    "to_yyyymmdd",
    "utcnow",
]
