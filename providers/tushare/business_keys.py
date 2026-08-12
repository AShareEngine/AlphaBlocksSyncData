#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Natural keys for the read-only Tushare endpoints.

The fields below are the stable dimensions of a returned record.  They are
deliberately separate from values which may be corrected by Tushare later.
For example, ``close`` is not part of the ``daily`` key and ``ann_date`` is
not part of the wide financial-statement key, so a corrected row replaces the
older value in a ReplacingMergeTree.

Keep this registry exhaustive.  A catalog update must declare a key before a
new endpoint can create a ClickHouse table; silently guessing a narrow key can
discard valid records.
"""

from __future__ import annotations

from typing import Iterable


# Grouping endpoints by key keeps the registry readable while still making the
# choice for every endpoint explicit and auditable.
BUSINESS_KEY_GROUPS: tuple[tuple[tuple[str, ...], tuple[str, ...]], ...] = (
    (
        ("ts_code",),
        (
            "etf_basic", "etf_index", "cb_basic", "cb_issue", "fund_basic",
            "mkt_idx_bmk", "fx_obasic", "index_basic", "opt_basic",
            "fut_basic", "hk_basic", "sge_basic", "us_basic", "stock_basic",
            "stock_company", "new_share", "ths_index",
        ),
    ),
    (
        ("ts_code", "trade_date"),
        (
            "fund_adj", "fund_daily", "cb_daily", "cb_factor_pro", "repo_daily",
            "fund_factor_pro", "fund_share", "fx_daily", "ci_daily",
            "daily_info", "idx_factor_pro", "index_daily", "index_dailybasic",
            "index_global", "index_monthly", "index_weekly", "sw_daily",
            "sz_daily_info", "opt_daily", "ft_limit", "fut_daily",
            "fut_index_daily", "fut_mapping", "fut_settle", "hk_adjfactor",
            "hk_daily", "hk_daily_adj", "sge_daily", "us_adjfactor", "us_daily",
            "us_daily_adj", "margin_detail", "margin_secs", "slb_len_mm",
            "slb_sec", "stk_premarket", "dc_daily", "dc_index", "kpl_list",
            "limit_cpt_list", "limit_list_d", "limit_list_ths", "limit_step",
            "stk_auction", "tdx_daily", "tdx_index", "ths_daily", "ccass_hold",
            "cyq_perf", "stk_auction_c", "stk_auction_o", "stk_factor",
            "stk_factor_pro", "adj_factor", "bak_daily", "daily", "daily_basic",
            "monthly", "stk_limit", "weekly", "moneyflow",
            "moneyflow_cnt_ths", "moneyflow_dc", "moneyflow_ind_ths",
            "moneyflow_ths",
        ),
    ),
    (
        ("ts_code", "trade_time"),
        (
            "rt_etf_k", "rt_idx_k", "rt_sw_k",
        ),
    ),
    (
        ("ts_code", "freq", "time"),
        ("rt_etf_min", "rt_etf_min_daily", "rt_idx_min", "rt_idx_min_daily"),
    ),
    (
        ("code", "freq", "time"),
        ("rt_fut_min", "rt_fut_min_daily"),
    ),
    (
        ("ts_code", "freq", "time"),
        ("rt_min_daily",),
    ),
    (
        ("ts_code", "trade_date", "freq"),
        ("stk_nineturn", "stk_week_month_adj", "stk_weekly_monthly", "fut_weekly_monthly"),
    ),
    (
        ("date",),
        (
            "stk_account", "stk_account_old", "cctv_news", "gz_index", "hibor",
            "shibor", "shibor_lpr", "wz_index", "us_tbr", "us_tltr", "us_trltr",
            "us_trycr", "us_tycr",
        ),
    ),
    (
        ("month",),
        ("cn_cpi", "cn_ppi", "cn_pmi", "sf_month", "cn_m"),
    ),
    (("quarter",), ("cn_gdp",)),
    (("trade_date",), ("slb_len", "ggt_daily", "moneyflow_hsgt", "moneyflow_mkt_dc")),
    (("cal_date",), ("hk_tradecal", "us_tradecal")),
    (("exchange", "cal_date"), ("fut_trade_cal", "trade_cal")),
    (("id",), ("p_list",)),
    (("id", "ts_code"), ("p_get",)),
    (("name",), ("fund_company", "hm_list")),
    (("ts_code", "trade_time"), ("rt_etf_sz_iopv",)),
    (("code", "freq", "time"), ("rt_min",)),
    (("ts_code", "trade_time"), ("rt_k",)),
    (("ts_code",), ("rt_hk_k",)),
    (("trade_date", "exchange_id"), ("margin",)),
    (("date", "curr_type"), ("libor",)),
    (("date", "bank"), ("shibor_quote",)),
    (("trade_date", "ts_code", "con_code"), ("etf_sh_cons", "etf_sz_cons", "dc_member", "tdx_member")),
    (("trade_date", "ts_code"), ("etf_share_size", "bc_bestotcqt", "bond_blk")),
    (("trade_date", "ts_code", "buy_dp", "sell_dp", "price", "vol", "amount"), ("bond_blk_detail",)),
    (("ann_date", "url"), ("idx_anns",)),
    # qt_time is the bank quote's update time, not a stable record dimension.
    # The same bank can revise its quote during a day; that revision should
    # replace the earlier value for the bond instead of creating a new key.
    (("trade_date", "bank", "ts_code"), ("bc_otcqt",)),
    (("ts_code", "ann_date", "call_type"), ("cb_call",)),
    (("ts_code", "change_date"), ("cb_price_chg",)),
    (("ts_code", "rate_start_date", "rate_end_date"), ("cb_rate",)),
    (("ts_code", "rating_date", "rating_com_name", "rating_type"), ("cb_rating",)),
    (("ts_code", "end_date"), ("cb_share", "pledge_stat", "stk_holdernumber", "express", "fina_audit", "fina_indicator", "disclosure_date")),
    (("date", "time", "currency", "country", "event"), ("eco_cal",)),
    (("ts_code", "end_date", "holder_rank"), ("top10_cb_holders",)),
    (("trade_date", "ts_code", "curve_name", "curve_type", "curve_term"), ("yc_cb",)),
    (("ts_code", "base_date", "div_proc"), ("fund_div",)),
    # Upstream fund-manager rows can legitimately omit begin_date.  The
    # announcement identifies the published manager record and is also the
    # endpoint cursor, so it is the stable event dimension for replacement.
    (("ts_code", "ann_date", "name"), ("fund_manager",)),
    (("ts_code", "nav_date"), ("fund_nav",)),
    (("ts_code", "end_date", "symbol"), ("fund_portfolio",)),
    (("url",), ("anns_d", "npr")),
    (("ts_code", "pub_time", "q"), ("irm_qa_sh", "irm_qa_sz")),
    (("pub_time", "title", "src"), ("major_news",)),
    (("pub_date", "title"), ("monetary_policy",)),
    (("datetime", "title", "channels"), ("news",)),
    (("trade_date", "ts_code", "title", "inst_csname", "author"), ("research_report",)),
    (("month", "publish_date", "title", "issuing_org"), ("cn_schedule",)),
    (("l1_code", "l2_code", "l3_code", "ts_code", "in_date"), ("ci_index_member", "index_member_all")),
    (("index_code", "industry_code"), ("index_classify",)),
    (("index_code", "con_code", "trade_date"), ("index_weight",)),
    (("trade_date", "symbol", "broker", "exchange"), ("fut_holding",)),
    (("exchange", "prd", "week_date"), ("fut_weekly_detail",)),
    (("trade_date", "symbol", "warehouse", "wh_id", "grade", "brand", "place", "pd", "is_ct"), ("fut_wsr",)),
    (("ts_code", "end_date", "ind_name"), ("hk_balancesheet", "hk_cashflow", "hk_income")),
    (("ts_code", "end_date", "ind_type", "report_type"), ("hk_fina_indicator", "us_fina_indicator")),
    (("ts_code", "end_date", "ind_type", "ind_name", "report_type"), ("us_balancesheet", "us_cashflow", "us_income")),
    (("trade_date", "ts_code", "tenor"), ("slb_sec_detail",)),
    (("ts_code", "trade_date", "buyer", "seller", "price"), ("block_trade",)),
    (("ts_code", "ann_date", "holder_name", "start_date", "pledge_amount"), ("pledge_detail",)),
    (("ts_code", "ann_date"), ("repurchase",)),
    (("ts_code", "float_date", "holder_name", "share_type"), ("share_float",)),
    (("ts_code", "start_date", "type"), ("stk_alert",)),
    (("ts_code", "trade_date", "period"), ("stk_high_shock", "stk_shock")),
    (("ts_code", "ann_date", "holder_name", "in_de", "begin_date"), ("stk_holdertrade",)),
    (("ts_code", "end_date", "holder_name"), ("top10_floatholders", "top10_holders")),
    (("trade_date", "ts_code"), ("bak_basic",)),
    (("o_code",), ("bse_mapping",)),
    (("ts_code", "start_date"), ("namechange",)),
    (("ts_code", "pub_date", "st_type"), ("st",)),
    (("ts_code", "name", "begin_date"), ("stk_managers",)),
    (("ts_code", "end_date", "name", "title"), ("stk_rewards",)),
    (("ts_code", "trade_date", "type"), ("stock_hsgt", "stock_st")),
    (("theme_code", "trade_date"), ("dc_concept",)),
    (("theme_code", "ts_code", "trade_date"), ("dc_concept_cons",)),
    (("trade_date", "data_type", "ts_code", "rank_time"), ("dc_hot", "ths_hot")),
    (("trade_date", "ts_code", "hm_name", "tag"), ("hm_detail",)),
    (("con_code", "ts_code", "trade_date"), ("kpl_concept_cons",)),
    # The official THS member contract marks in_date as "暂无" and the live
    # payload leaves it empty.  This is a current membership snapshot, whose
    # natural relation key is the index and constituent pair.
    (("ts_code", "con_code"), ("ths_member",)),
    (("trade_date", "ts_code", "exalter", "side", "reason"), ("top_inst",)),
    (("trade_date", "ts_code", "reason"), ("top_list",)),
    (("month", "broker", "ts_code"), ("broker_recommend",)),
    (("trade_date", "ts_code", "col_participant_id"), ("ccass_hold_detail",)),
    (("ts_code", "trade_date", "price"), ("cyq_chips",)),
    (("code", "ts_code", "trade_date", "exchange"), ("hk_hold",)),
    (("ts_code", "report_date", "report_title", "org_name", "author_name"), ("report_rc",)),
    (("hk_code", "ts_code", "trade_date"), ("stk_ah_comparison",)),
    (("ts_code", "surv_date", "rece_org"), ("stk_surv",)),
    (("trade_date", "ts_code", "market_type"), ("ggt_top10", "hsgt_top10")),
    (("ts_code", "trade_date", "suspend_timing"), ("suspend_d",)),
    (("ts_code", "end_date", "report_type", "comp_type", "end_type"), ("balancesheet", "cashflow", "income")),
    (("ts_code", "end_date", "ann_date"), ("dividend",)),
    (("ts_code", "end_date", "bz_code", "bz_item", "curr_type"), ("fina_mainbz",)),
    (("ts_code", "ann_date", "end_date", "type"), ("forecast",)),
    (("trade_date", "content_type", "ts_code"), ("moneyflow_ind_dc",)),
    (("ts_code", "freq", "trade_time"), ("etf_mins", "idx_mins", "sw_mins", "opt_mins", "ft_mins", "hk_mins", "stk_mins")),
    (("ts_code", "trade_date", "asset", "freq", "adj"), ("pro_bar",)),
)


def _build_business_keys(
    groups: Iterable[tuple[tuple[str, ...], tuple[str, ...]]],
) -> dict[str, tuple[str, ...]]:
    result: dict[str, tuple[str, ...]] = {}
    for key_fields, tasks in groups:
        if not key_fields:
            raise ValueError("Tushare business key cannot be empty")
        for task in tasks:
            if task in result:
                raise ValueError(f"duplicate Tushare business-key declaration: {task}")
            result[task] = key_fields
    return result


TUSHARE_BUSINESS_KEYS = _build_business_keys(BUSINESS_KEY_GROUPS)

# Optional key inputs need their provider defaults materialized into the row so
# direct CLI calls and generated plans use the same key representation.
TUSHARE_BUSINESS_KEY_DEFAULTS: dict[str, dict[str, str]] = {
    # The financial-statement gateway legitimately returns blank end_type for
    # some reports.  It remains an explicit key dimension so a non-blank
    # report type cannot collide with the ordinary blank series.
    "balancesheet": {"end_type": ""},
    "cashflow": {"end_type": ""},
    "income": {"end_type": ""},
    # Main-business rows can identify the item by bz_item even when the
    # optional upstream classification code is blank.
    "fina_mainbz": {"bz_code": ""},
    "pro_bar": {"asset": "E", "freq": "D", "adj": ""},
    "etf_mins": {"freq": "1min"},
    "idx_mins": {"freq": "1min"},
    "sw_mins": {"freq": "1min"},
    "opt_mins": {"freq": "1min"},
    "ft_mins": {"freq": "1min"},
    "hk_mins": {"freq": "1min"},
    "stk_mins": {"freq": "1min"},
    "rt_etf_min": {"freq": "1min"},
    "rt_etf_min_daily": {"freq": "1min"},
    "rt_idx_min": {"freq": "1min"},
    "rt_idx_min_daily": {"freq": "1min"},
    "rt_min": {"freq": "1min"},
}


__all__ = [
    "BUSINESS_KEY_GROUPS",
    "TUSHARE_BUSINESS_KEY_DEFAULTS",
    "TUSHARE_BUSINESS_KEYS",
]
