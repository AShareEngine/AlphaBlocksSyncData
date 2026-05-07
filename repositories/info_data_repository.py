#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""InfoData 对应的 ClickHouse 读写层."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict
from datetime import date

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

from sync_data_system.amazingdata_constants import (
    HISTORY_STOCK_STATUS_FIELD_DB_MAPPING,
    HISTORY_STOCK_STATUS_FIELDS,
    STOCK_BASIC_FIELD_DB_MAPPING,
    STOCK_BASIC_FIELDS,
)
from sync_data_system.clickhouse_tables import (
    AD_BALANCE_SHEET_TABLE,
    AD_BJ_CODE_MAPPING_TABLE,
    AD_CASH_FLOW_TABLE,
    AD_DIVIDEND_TABLE,
    AD_EQUITY_STRUCTURE_TABLE,
    AD_EQUITY_PLEDGE_FREEZE_TABLE,
    AD_EQUITY_RESTRICTED_TABLE,
    AD_FUND_IOPV_TABLE,
    AD_FUND_SHARE_TABLE,
    AD_HISTORY_STOCK_STATUS_TABLE,
    AD_HOLDER_NUM_TABLE,
    AD_INDUSTRY_BASE_INFO_TABLE,
    AD_INDUSTRY_CONSTITUENT_TABLE,
    AD_INDUSTRY_DAILY_TABLE,
    AD_INDUSTRY_WEIGHT_TABLE,
    AD_INDEX_CONSTITUENT_TABLE,
    AD_INDEX_WEIGHT_TABLE,
    AD_INCOME_TABLE,
    AD_BLOCK_TRADING_TABLE,
    AD_KZZ_CALL_EXPLANATION_TABLE,
    AD_KZZ_CALL_TABLE,
    AD_KZZ_CORR_TABLE,
    AD_KZZ_CONV_CHANGE_TABLE,
    AD_KZZ_CONV_TABLE,
    AD_KZZ_ISSUANCE_TABLE,
    AD_KZZ_PUT_CALL_ITEM_TABLE,
    AD_KZZ_PUT_TABLE,
    AD_KZZ_SHARE_TABLE,
    AD_KZZ_SUSPEND_TABLE,
    AD_LONG_HU_BANG_TABLE,
    AD_MARGIN_DETAIL_TABLE,
    AD_MARGIN_SUMMARY_TABLE,
    AD_OPTION_BASIC_INFO_TABLE,
    AD_OPTION_STD_CTR_SPECS_TABLE,
    AD_OPTION_MON_CTR_SPECS_TABLE,
    AD_TREASURY_YIELD_TABLE,
    AD_KZZ_PUT_EXPLANATION_TABLE,
    AD_PROFIT_EXPRESS_TABLE,
    AD_PROFIT_NOTICE_TABLE,
    AD_RIGHT_ISSUE_TABLE,
    AD_SHARE_HOLDER_TABLE,
    AD_STOCK_BASIC_TABLE,
    BALANCE_SHEET_COLUMN_TYPES,
    CASH_FLOW_COLUMN_TYPES,
    INCOME_COLUMN_TYPES,
    KZZ_ISSUANCE_COLUMN_TYPES,
    PROFIT_EXPRESS_COLUMN_TYPES,
    PROFIT_NOTICE_COLUMN_TYPES,
    iter_info_data_table_ddls,
)
from sync_data_system.data_models import (
    BALANCE_SHEET_FIELD_NAMES,
    BalanceSheetRow,
    BlockTradingRow,
    BjCodeMappingRow,
    CASH_FLOW_FIELD_NAMES,
    CashFlowRow,
    DividendRow,
    EquityPledgeFreezeRow,
    EquityRestrictedRow,
    EquityStructureRow,
    FundIopvRow,
    FundShareRow,
    HistoryStockStatusQuery,
    HistoryStockStatusRow,
    InfoPayloadQuery,
    InfoPayloadRow,
    INCOME_FIELD_NAMES,
    IndustryBaseInfoRow,
    IndustryConstituentRow,
    IndustryDailyRow,
    IndustryWeightRow,
    IndexConstituentRow,
    IndexWeightRow,
    IncomeRow,
    KzzCallRow,
    KzzCallExplanationRow,
    KzzConvRow,
    KzzPutExplanationRow,
    KzzPutCallItemRow,
    KzzPutRow,
    KzzConvChangeRow,
    KzzCorrRow,
    KZZ_ISSUANCE_FIELD_NAMES,
    KzzIssuanceRow,
    KzzShareRow,
    KzzSuspendRow,
    OptionBasicInfoRow,
    OptionMonCtrSpecsRow,
    OptionStdCtrSpecsRow,
    LongHuBangRow,
    MarginDetailRow,
    MarginSummaryRow,
    TreasuryYieldRow,
    HolderNumRow,
    PROFIT_EXPRESS_FIELD_NAMES,
    ProfitExpressRow,
    PROFIT_NOTICE_FIELD_NAMES,
    ProfitNoticeRow,
    ShareHolderRow,
    StockBasicQuery,
    StockBasicRow,
    RightIssueRow,
)
from sync_data_system.repositories.base_data_repository import BaseDataRepository


logger = logging.getLogger(__name__)


class InfoDataRepository(BaseDataRepository):
    """InfoData 的 repository.

    先复用 BaseDataRepository 已经抽好的通用能力：
    - 批量写入
    - 同步日志
    - 当日成功跳过判断
    """

    STOCK_BASIC_COLUMNS = (
        "snapshot_date",
        "market_code",
        "security_name",
        "comp_name",
        "pinyin",
        "comp_name_eng",
        "list_date",
        "delist_date",
        "listplate_name",
        "comp_sname_eng",
        "is_listed",
    )
    HISTORY_STOCK_STATUS_COLUMNS = (
        "trade_date",
        "market_code",
        "preclose",
        "high_limited",
        "low_limited",
        "price_high_lmt_rate",
        "price_low_lmt_rate",
        "is_st_sec",
        "is_susp_sec",
        "is_wd_sec",
        "is_xr_sec",
    )
    BJ_CODE_MAPPING_COLUMNS = (
        "old_code",
        "new_code",
        "security_name",
        "listing_date",
    )
    INFO_PAYLOAD_COLUMNS = (
        "market_code",
        "report_date",
        "report_date_raw",
        "payload_json",
    )
    BALANCE_SHEET_COLUMNS = BALANCE_SHEET_FIELD_NAMES
    BALANCE_SHEET_INSERT_COLUMNS = BALANCE_SHEET_COLUMNS + (
        "report_date",
        "report_date_raw",
        "payload_json",
    )
    CASH_FLOW_COLUMNS = CASH_FLOW_FIELD_NAMES
    CASH_FLOW_INSERT_COLUMNS = CASH_FLOW_COLUMNS + (
        "report_date",
        "report_date_raw",
        "payload_json",
    )
    INCOME_COLUMNS = INCOME_FIELD_NAMES
    INCOME_INSERT_COLUMNS = INCOME_COLUMNS + (
        "report_date",
        "report_date_raw",
        "payload_json",
    )
    PROFIT_EXPRESS_COLUMNS = PROFIT_EXPRESS_FIELD_NAMES
    PROFIT_EXPRESS_INSERT_COLUMNS = PROFIT_EXPRESS_COLUMNS + (
        "report_date",
        "report_date_raw",
        "payload_json",
    )
    PROFIT_NOTICE_COLUMNS = PROFIT_NOTICE_FIELD_NAMES
    PROFIT_NOTICE_INSERT_COLUMNS = PROFIT_NOTICE_COLUMNS + (
        "report_date",
        "report_date_raw",
        "payload_json",
    )
    FUND_SHARE_COLUMNS = (
        "market_code",
        "fund_share",
        "change_reason",
        "is_consolidated_data",
        "ann_date",
        "total_share",
        "change_date",
        "float_share",
    )
    FUND_IOPV_COLUMNS = (
        "market_code",
        "price_date",
        "iopv_nav",
    )
    KZZ_ISSUANCE_COLUMNS = KZZ_ISSUANCE_FIELD_NAMES
    KZZ_SHARE_COLUMNS = (
        "change_date",
        "ann_date",
        "market_code",
        "bond_share",
        "conv_share",
        "change_reason",
    )
    KZZ_SUSPEND_COLUMNS = (
        "market_code",
        "suspend_date",
        "suspend_type",
        "resump_date",
        "change_reason",
        "change_reason_code",
        "resump_time",
    )
    OPTION_BASIC_INFO_COLUMNS = (
        "contract_full_name",
        "contract_type",
        "delivery_month",
        "expiry_date",
        "exercise_price",
        "exercise_end_date",
        "start_trade_date",
        "listing_ref_price",
        "last_trade_date",
        "exchange_code",
        "delivery_date",
        "contract_unit",
        "is_trade",
        "exchange_short_name",
        "contract_adjust_flag",
        "market_code",
    )
    OPTION_STD_CTR_SPECS_COLUMNS = (
        "exercise_date",
        "contract_unit",
        "position_declare_min",
        "quote_currency_unit",
        "last_trading_date",
        "position_limit",
        "delist_date",
        "notional_value",
        "exercise_method",
        "delivery_method",
        "settlement_month",
        "trading_fee",
        "exchange_name",
        "option_en_name",
        "contract_value",
        "is_simulation",
        "contract_unit_dimension",
        "option_strike_price",
        "is_simulation_trade",
        "listed_date",
        "option_name",
        "premium",
        "option_type",
        "trading_hours_desc",
        "final_settlement_date",
        "final_settlement_price",
        "min_price_unit",
        "market_code",
        "contract_multiplier",
    )
    OPTION_MON_CTR_SPECS_COLUMNS = (
        "code_old",
        "change_date",
        "market_code",
        "name_new",
        "exercise_price_new",
        "name_old",
        "code_new",
        "exercise_price_old",
        "unit_old",
        "unit_new",
        "change_reason",
    )
    TREASURY_YIELD_COLUMNS = (
        "term",
        "trade_date",
        "yield_value",
    )
    KZZ_CONV_CHANGE_COLUMNS = (
        "market_code",
        "change_date",
        "ann_date",
        "conv_price",
        "change_reason",
    )
    KZZ_CORR_COLUMNS = (
        "market_code",
        "start_date",
        "end_date",
        "corr_trig_calc_max_period",
        "corr_trig_calc_period",
        "spec_corr_trig_ratio",
        "corr_conv_price_floor_desc",
        "ref_price_is_avg_price",
        "corr_times_limit",
        "is_timepoint_corr_clause_flag",
        "timepoint_count",
        "timepoint_corr_text_clause",
        "spec_corr_range",
        "is_spec_down_corr_clause_flag",
    )
    KZZ_CALL_EXPLANATION_COLUMNS = (
        "market_code",
        "call_date",
        "call_price",
        "call_announcement_date",
        "call_ful_res_ann_date",
        "call_amount",
        "call_outstanding_amount",
        "call_date_pub",
        "call_fund_arrival_date",
        "call_record_day",
        "call_reason",
    )
    KZZ_PUT_EXPLANATION_COLUMNS = (
        "market_code",
        "put_fund_arrival_date",
        "put_price",
        "put_announcement_date",
        "put_ex_date",
        "put_amount",
        "put_outstanding",
        "repurchase_start_date",
        "repurchase_end_date",
        "resale_start_date",
        "fund_end_date",
        "repurchase_code",
        "resale_amount",
        "resale_imp_amount",
        "resale_end_date",
    )
    KZZ_PUT_CALL_ITEM_COLUMNS = (
        "market_code",
        "mand_put_period",
        "mand_put_price",
        "mand_put_start_date",
        "mand_put_end_date",
        "mand_put_text",
        "is_mand_put_contain_current",
        "con_put_start_date",
        "con_put_end_date",
        "max_put_triper",
        "put_triperiod",
        "add_put_con",
        "add_put_price_ins",
        "put_num_ins",
        "put_pro_period",
        "put_no_pery",
        "is_put_item",
        "is_term_put_item",
        "is_mand_put_item",
        "is_time_put_item",
        "time_put_no",
        "time_put_item",
        "term_put_price",
        "con_call_start_date",
        "con_call_end_date",
        "call_tri_con_ins",
        "max_call_triper",
        "call_tri_per",
        "call_num_ber_ins",
        "is_call_item",
        "call_pro_period",
        "call_no_pery",
        "is_time_call_item",
        "time_call_no",
        "time_call_text",
        "expired_redemption_price",
        "put_tri_cond_desc",
    )
    KZZ_PUT_COLUMNS = (
        "market_code",
        "put_price",
        "begin_date",
        "end_date",
        "tri_ratio",
    )
    KZZ_CALL_COLUMNS = (
        "market_code",
        "call_price",
        "begin_date",
        "end_date",
        "tri_ratio",
    )
    KZZ_CONV_COLUMNS = (
        "market_code",
        "ann_date",
        "conv_code",
        "conv_name",
        "conv_price",
        "currency_code",
        "conv_start_date",
        "conv_end_date",
        "trade_date_last",
        "forced_conv_date",
        "forced_conv_price",
        "rel_conv_month",
        "is_forced",
        "forced_conv_reason",
    )
    BLOCK_TRADING_COLUMNS = (
        "market_code",
        "trade_date",
        "bshare_price",
        "bshare_volume",
        "b_frequency",
        "block_avg_volume",
        "bshare_amount",
        "bbuyer_name",
        "bseller_name",
    )
    LONG_HU_BANG_COLUMNS = (
        "market_code",
        "trade_date",
        "security_name",
        "reason_type",
        "reason_type_name",
        "change_range",
        "trader_name",
        "buy_amount",
        "sell_amount",
        "flow_mark",
        "total_amount",
        "total_volume",
    )
    MARGIN_DETAIL_COLUMNS = (
        "market_code",
        "security_name",
        "trade_date",
        "borrow_money_balance",
        "purch_with_borrow_money",
        "repayment_of_borrow_money",
        "sec_lending_balance",
        "sales_of_borrowed_sec",
        "repayment_of_borrow_sec",
        "sec_lending_balance_vol",
        "margin_trade_balance",
    )
    MARGIN_SUMMARY_COLUMNS = (
        "trade_date",
        "sum_borrow_money_balance",
        "sum_purch_with_borrow_money",
        "sum_repayment_of_borrow_money",
        "sum_sec_lending_balance",
        "sum_sales_of_borrowed_sec",
        "sum_margin_trade_balance",
    )
    SHARE_HOLDER_COLUMNS = (
        "ann_date",
        "market_code",
        "holder_enddate",
        "holder_type",
        "qty_num",
        "holder_name",
        "holder_holder_category",
        "holder_quantity",
        "holder_pct",
        "holder_sharecategoryname",
        "float_qty",
    )
    HOLDER_NUM_COLUMNS = (
        "market_code",
        "ann_dt",
        "holder_enddate",
        "holder_total_num",
        "holder_num",
    )
    EQUITY_STRUCTURE_COLUMNS = (
        "market_code",
        "ann_date",
        "change_date",
        "share_change_reason_str",
        "ex_change_date",
        "current_sign",
        "is_valid",
        "tot_share",
        "float_share",
        "float_a_share",
        "float_b_share",
        "float_hk_share",
        "float_os_share",
        "tot_tradable_share",
        "rtd_a_share_inst",
        "rtd_a_share_domesnp",
        "rtd_share_senior",
        "rtd_a_share_foreign",
        "rtd_a_share_forjur",
        "rtd_a_share_fornp",
        "restricted_b_share",
        "other_rtd_share",
        "non_tradable_share",
        "ntrd_share_state_pct",
        "ntrd_share_state",
        "ntrd_share_statejur",
        "ntrd_share_domesjur",
        "ntrd_share_domes_initiator",
        "ntrd_share_ipojuris",
        "ntrd_share_genjuris",
        "ntrd_share_stra_investor",
        "ntrd_share_fund",
        "ntrd_share_nat",
        "tran_share",
        "float_share_senior",
        "share_inemp",
        "preferred_share",
        "ntrd_share_nlist_frgn",
        "staq_share",
        "net_share",
        "share_change_reason",
        "tot_a_share",
        "tot_b_share",
        "otca_share",
        "otcb_share",
        "tot_otc_share",
        "share_hk",
        "pre_non_tradable_share",
        "restricted_a_share",
        "rtd_a_share_state",
        "rtd_a_share_statejur",
        "rtd_a_share_other_domes",
        "rtd_a_share_other_domesjur",
        "tot_restricted_share",
    )
    EQUITY_PLEDGE_FREEZE_COLUMNS = (
        "market_code",
        "ann_date",
        "holder_name",
        "holder_type_code",
        "total_holding_shr",
        "total_holding_shr_ratio",
        "fro_shares",
        "fro_shr_to_total_holding_ratio",
        "fro_shr_to_total_ratio",
        "total_pledge_shr",
        "is_equity_pledge_repo",
        "begin_date",
        "end_date",
        "is_disfrozen",
        "frozen_institution",
        "disfrozen_time",
        "shr_category_code",
        "freeze_type",
    )
    EQUITY_RESTRICTED_COLUMNS = (
        "market_code",
        "list_date",
        "share_ratio",
        "share_lst_type_name",
        "share_lst",
        "share_lst_is_ann",
        "close_price",
        "share_lst_market_value",
    )
    DIVIDEND_COLUMNS = (
        "market_code",
        "div_progress",
        "dvd_per_share_stk",
        "dvd_per_share_pre_tax_cash",
        "dvd_per_share_after_tax_cash",
        "date_eqy_record",
        "date_ex",
        "date_dvd_payout",
        "listingdate_of_dvd_shr",
        "div_prelandate",
        "div_smtgdate",
        "date_dvd_ann",
        "div_basedate",
        "div_baseshare",
        "currency_code",
        "ann_date",
        "is_changed",
        "report_period",
        "div_change",
        "div_bonusrate",
        "div_conversedrate",
        "remark",
        "div_preann_date",
        "div_target",
    )
    RIGHT_ISSUE_COLUMNS = (
        "market_code",
        "progress",
        "price",
        "ratio",
        "amt_plan",
        "amt_real",
        "collection_fund",
        "shareb_reg_date",
        "ex_dividend_date",
        "listed_date",
        "pay_start_date",
        "pay_end_date",
        "preplan_date",
        "smtg_ann_date",
        "pass_date",
        "approved_date",
        "execute_date",
        "result_date",
        "list_ann_date",
        "guarantor",
        "guartype",
        "rightsissue_code",
        "ann_date",
        "rightsissue_year",
        "rightsissue_desc",
        "rightsissue_name",
        "ratio_denominator",
        "ratio_molecular",
        "subs_method",
        "expected_fund_raising",
    )
    INDEX_CONSTITUENT_COLUMNS = (
        "index_code",
        "con_code",
        "in_date",
        "out_date",
        "index_name",
    )
    INDEX_WEIGHT_COLUMNS = (
        "index_code",
        "con_code",
        "trade_date",
        "total_share",
        "free_share_ratio",
        "calc_share",
        "weight_factor",
        "weight",
        "close",
    )
    INDUSTRY_BASE_INFO_COLUMNS = (
        "index_code",
        "industry_code",
        "level_type",
        "level1_name",
        "level2_name",
        "level3_name",
        "is_pub",
        "change_reason",
    )
    INDUSTRY_CONSTITUENT_COLUMNS = (
        "index_code",
        "con_code",
        "in_date",
        "out_date",
        "index_name",
    )
    INDUSTRY_WEIGHT_COLUMNS = (
        "index_code",
        "con_code",
        "trade_date",
        "weight",
    )
    INDUSTRY_DAILY_COLUMNS = (
        "index_code",
        "trade_date",
        "open",
        "high",
        "close",
        "low",
        "amount",
        "volume",
        "pb",
        "pe",
        "total_cap",
        "a_float_cap",
        "pre_close",
    )

    def load_latest_date_by_codes(
        self,
        table_name: str,
        code_field: str,
        date_field: str,
        code_list: list[str],
    ):
        if not code_list:
            return None

        sql = f"""
        SELECT {code_field}, max({date_field}) AS latest_date
        FROM {table_name}
        WHERE {code_field} IN {{code_list:Array(String)}}
        GROUP BY {code_field}
        ORDER BY {code_field}
        """
        rows = self.client.query_rows(sql, {"code_list": code_list})
        if len(rows) != len(code_list):
            return None
        latest_dates = [row[1] for row in rows if len(row) > 1 and row[1] is not None]
        if not latest_dates:
            return None
        return min(latest_dates)

    def load_latest_date(self, table_name: str, date_field: str):
        sql = f"""
        SELECT max({date_field})
        FROM {table_name}
        WHERE {date_field} IS NOT NULL
        """
        return self.client.query_value(sql)

    def load_existing_values(self, table_name: str, field_name: str, values: Sequence[str]) -> set[str]:
        if not values:
            return set()
        sql = f"""
        SELECT {field_name}
        FROM {table_name}
        WHERE {field_name} IN {{values:Array(String)}}
        GROUP BY {field_name}
        """
        rows = self.client.query_rows(sql, {"values": list(values)})
        return {str(row[0]) for row in rows if row and row[0] is not None}

    def ensure_tables(self) -> None:
        super().ensure_tables()
        for ddl in iter_info_data_table_ddls():
            self.client.command(ddl)
        self._ensure_table_columns(AD_BALANCE_SHEET_TABLE, BALANCE_SHEET_COLUMN_TYPES)
        self._ensure_table_columns(AD_CASH_FLOW_TABLE, CASH_FLOW_COLUMN_TYPES)
        self._ensure_table_columns(AD_INCOME_TABLE, INCOME_COLUMN_TYPES)
        self._ensure_table_columns(AD_KZZ_ISSUANCE_TABLE, KZZ_ISSUANCE_COLUMN_TYPES)
        self._ensure_table_columns(AD_PROFIT_EXPRESS_TABLE, PROFIT_EXPRESS_COLUMN_TYPES)
        self._ensure_table_columns(AD_PROFIT_NOTICE_TABLE, PROFIT_NOTICE_COLUMN_TYPES)

    @staticmethod
    def _build_structured_payload(record: dict, data_columns: tuple[str, ...]) -> tuple:
        report_date = record.get("reporting_period") or record.get("ann_date")
        report_date_raw = report_date.isoformat() if report_date is not None else None
        payload_json = json.dumps(record, ensure_ascii=False, default=str, separators=(",", ":"))
        return tuple(record.get(column) for column in data_columns) + (report_date, report_date_raw, payload_json)

    def _save_structured_rows(
        self,
        table: str,
        data_columns: tuple[str, ...],
        insert_columns: tuple[str, ...],
        rows,
    ) -> int:
        payload: list[tuple] = []
        for row in rows:
            record = asdict(row)
            payload.append(self._build_structured_payload(record, data_columns))
        if not payload:
            return 0
        self.client.insert_rows(table, insert_columns, payload)
        logger.info("Inserted %s rows into %s single_insert=true", len(payload), table)
        return len(payload)

    def save_stock_basic_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_STOCK_BASIC_TABLE,
            columns=self.STOCK_BASIC_COLUMNS,
            rows=rows,
            partition_field="snapshot_date",
        )

    def save_history_stock_status_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_HISTORY_STOCK_STATUS_TABLE,
            columns=self.HISTORY_STOCK_STATUS_COLUMNS,
            rows=rows,
            partition_field="trade_date",
        )

    def save_bj_code_mapping_rows(self, rows: Iterable[BjCodeMappingRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_BJ_CODE_MAPPING_TABLE,
            columns=self.BJ_CODE_MAPPING_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_balance_sheet_rows(self, rows) -> int:
        return self._save_structured_rows(
            table=AD_BALANCE_SHEET_TABLE,
            data_columns=self.BALANCE_SHEET_COLUMNS,
            insert_columns=self.BALANCE_SHEET_INSERT_COLUMNS,
            rows=rows,
        )

    def save_cash_flow_rows(self, rows) -> int:
        return self._save_structured_rows(
            table=AD_CASH_FLOW_TABLE,
            data_columns=self.CASH_FLOW_COLUMNS,
            insert_columns=self.CASH_FLOW_INSERT_COLUMNS,
            rows=rows,
        )

    def save_income_rows(self, rows) -> int:
        return self._save_structured_rows(
            table=AD_INCOME_TABLE,
            data_columns=self.INCOME_COLUMNS,
            insert_columns=self.INCOME_INSERT_COLUMNS,
            rows=rows,
        )

    def save_profit_express_rows(self, rows) -> int:
        return self._save_structured_rows(
            table=AD_PROFIT_EXPRESS_TABLE,
            data_columns=self.PROFIT_EXPRESS_COLUMNS,
            insert_columns=self.PROFIT_EXPRESS_INSERT_COLUMNS,
            rows=rows,
        )

    def save_profit_notice_rows(self, rows) -> int:
        return self._save_structured_rows(
            table=AD_PROFIT_NOTICE_TABLE,
            data_columns=self.PROFIT_NOTICE_COLUMNS,
            insert_columns=self.PROFIT_NOTICE_INSERT_COLUMNS,
            rows=rows,
        )

    def save_fund_share_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_FUND_SHARE_TABLE,
            columns=self.FUND_SHARE_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_fund_iopv_rows(self, rows: Iterable[FundIopvRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_FUND_IOPV_TABLE,
            columns=self.FUND_IOPV_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_kzz_issuance_rows(self, rows: Iterable[KzzIssuanceRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_KZZ_ISSUANCE_TABLE,
            columns=self.KZZ_ISSUANCE_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_kzz_share_rows(self, rows: Iterable[KzzShareRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_KZZ_SHARE_TABLE,
            columns=self.KZZ_SHARE_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_kzz_suspend_rows(self, rows: Iterable[KzzSuspendRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_KZZ_SUSPEND_TABLE,
            columns=self.KZZ_SUSPEND_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_option_basic_info_rows(self, rows: Iterable[OptionBasicInfoRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_OPTION_BASIC_INFO_TABLE,
            columns=self.OPTION_BASIC_INFO_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_option_std_ctr_specs_rows(self, rows: Iterable[OptionStdCtrSpecsRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_OPTION_STD_CTR_SPECS_TABLE,
            columns=self.OPTION_STD_CTR_SPECS_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_option_mon_ctr_specs_rows(self, rows: Iterable[OptionMonCtrSpecsRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_OPTION_MON_CTR_SPECS_TABLE,
            columns=self.OPTION_MON_CTR_SPECS_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_treasury_yield_rows(self, rows: Iterable[TreasuryYieldRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_TREASURY_YIELD_TABLE,
            columns=self.TREASURY_YIELD_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_kzz_conv_change_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_KZZ_CONV_CHANGE_TABLE,
            columns=self.KZZ_CONV_CHANGE_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_kzz_corr_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_KZZ_CORR_TABLE,
            columns=self.KZZ_CORR_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_kzz_call_explanation_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_KZZ_CALL_EXPLANATION_TABLE,
            columns=self.KZZ_CALL_EXPLANATION_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_kzz_put_explanation_rows(self, rows: Iterable[KzzPutExplanationRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_KZZ_PUT_EXPLANATION_TABLE,
            columns=self.KZZ_PUT_EXPLANATION_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_kzz_put_call_item_rows(self, rows: Iterable[KzzPutCallItemRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_KZZ_PUT_CALL_ITEM_TABLE,
            columns=self.KZZ_PUT_CALL_ITEM_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_kzz_put_rows(self, rows: Iterable[KzzPutRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_KZZ_PUT_TABLE,
            columns=self.KZZ_PUT_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_kzz_call_rows(self, rows: Iterable[KzzCallRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_KZZ_CALL_TABLE,
            columns=self.KZZ_CALL_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_kzz_conv_rows(self, rows: Iterable[KzzConvRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_KZZ_CONV_TABLE,
            columns=self.KZZ_CONV_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_block_trading_rows(self, rows: Iterable[BlockTradingRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_BLOCK_TRADING_TABLE,
            columns=self.BLOCK_TRADING_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_long_hu_bang_rows(self, rows: Iterable[LongHuBangRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_LONG_HU_BANG_TABLE,
            columns=self.LONG_HU_BANG_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_margin_detail_rows(self, rows: Iterable[MarginDetailRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_MARGIN_DETAIL_TABLE,
            columns=self.MARGIN_DETAIL_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_margin_summary_rows(self, rows: Iterable[MarginSummaryRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_MARGIN_SUMMARY_TABLE,
            columns=self.MARGIN_SUMMARY_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_share_holder_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_SHARE_HOLDER_TABLE,
            columns=self.SHARE_HOLDER_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_holder_num_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_HOLDER_NUM_TABLE,
            columns=self.HOLDER_NUM_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_equity_structure_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_EQUITY_STRUCTURE_TABLE,
            columns=self.EQUITY_STRUCTURE_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_equity_pledge_freeze_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_EQUITY_PLEDGE_FREEZE_TABLE,
            columns=self.EQUITY_PLEDGE_FREEZE_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_equity_restricted_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_EQUITY_RESTRICTED_TABLE,
            columns=self.EQUITY_RESTRICTED_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_dividend_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_DIVIDEND_TABLE,
            columns=self.DIVIDEND_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_right_issue_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_RIGHT_ISSUE_TABLE,
            columns=self.RIGHT_ISSUE_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_index_constituent_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_INDEX_CONSTITUENT_TABLE,
            columns=self.INDEX_CONSTITUENT_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_index_weight_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_INDEX_WEIGHT_TABLE,
            columns=self.INDEX_WEIGHT_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_industry_base_info_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_INDUSTRY_BASE_INFO_TABLE,
            columns=self.INDUSTRY_BASE_INFO_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_industry_constituent_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_INDUSTRY_CONSTITUENT_TABLE,
            columns=self.INDUSTRY_CONSTITUENT_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_industry_weight_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_INDUSTRY_WEIGHT_TABLE,
            columns=self.INDUSTRY_WEIGHT_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_industry_daily_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_INDUSTRY_DAILY_TABLE,
            columns=self.INDUSTRY_DAILY_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def load_industry_base_index_codes(self) -> list[str]:
        sql = f"""
        SELECT index_code
        FROM {AD_INDUSTRY_BASE_INFO_TABLE}
        WHERE index_code != ''
        GROUP BY index_code
        ORDER BY index_code
        """
        rows = self.client.query_rows(sql)
        return [str(row[0]) for row in rows if row and row[0] is not None]

    def load_latest_stock_basic_snapshot_date(self, code_list: list[str]):
        if not code_list:
            return None

        sql = f"""
        SELECT market_code, max(snapshot_date) AS latest_snapshot_date
        FROM {AD_STOCK_BASIC_TABLE}
        WHERE market_code IN {{code_list:Array(String)}}
        GROUP BY market_code
        ORDER BY market_code
        """
        rows = self.client.query_rows(sql, {"code_list": code_list})
        if len(rows) != len(code_list):
            return None
        latest_dates = [row[1] for row in rows if len(row) > 1 and row[1] is not None]
        if not latest_dates:
            return None
        return min(latest_dates)

    def load_latest_history_stock_status_trade_date(self, code_list: list[str]):
        if not code_list:
            return None

        sql = f"""
        SELECT market_code, max(trade_date) AS latest_trade_date
        FROM {AD_HISTORY_STOCK_STATUS_TABLE}
        WHERE market_code IN {{code_list:Array(String)}}
        GROUP BY market_code
        ORDER BY market_code
        """
        rows = self.client.query_rows(sql, {"code_list": code_list})
        if len(rows) != len(code_list):
            return None
        latest_dates = [row[1] for row in rows if len(row) > 1 and row[1] is not None]
        if not latest_dates:
            return None
        return min(latest_dates)

    def load_bj_code_mapping_frame(self):
        if pd is None:  # pragma: no cover
            raise RuntimeError("未安装 pandas，无法返回 DataFrame。")

        sql = f"""
        SELECT
            old_code,
            new_code,
            security_name,
            listing_date
        FROM {AD_BJ_CODE_MAPPING_TABLE} FINAL
        ORDER BY old_code, new_code
        """
        return self.client.query_df(sql)

    def load_info_payload_frame(self, table_name: str, query: InfoPayloadQuery):
        if pd is None:  # pragma: no cover
            raise RuntimeError("未安装 pandas，无法返回 DataFrame。")
        if not query.code_list:
            return pd.DataFrame(columns=["market_code", "report_date", "report_date_raw", "payload_json"])

        sql = f"""
        SELECT
            market_code,
            report_date,
            report_date_raw,
            payload_json
        FROM {table_name}
        WHERE market_code IN {{code_list:Array(String)}}
        """
        parameters = {"code_list": list(query.code_list)}
        if query.begin_date is not None:
            sql += "\n  AND report_date >= {begin_date:Date}"
            parameters["begin_date"] = query.begin_date
        if query.end_date is not None:
            sql += "\n  AND report_date <= {end_date:Date}"
            parameters["end_date"] = query.end_date
        sql += "\nORDER BY market_code, report_date"
        return self.client.query_df(sql, parameters)

    def load_stock_basic_frame(self, query: StockBasicQuery):
        if pd is None:  # pragma: no cover
            raise RuntimeError("未安装 pandas，无法返回 DataFrame。")

        if not query.code_list:
            return pd.DataFrame(columns=list(STOCK_BASIC_FIELDS))

        snapshot_date = self.load_latest_stock_basic_snapshot_date(list(query.code_list))
        if snapshot_date is None:
            return pd.DataFrame(columns=list(STOCK_BASIC_FIELDS))

        sql = f"""
        SELECT
            market_code,
            any(security_name) AS security_name,
            any(comp_name) AS comp_name,
            any(pinyin) AS pinyin,
            any(comp_name_eng) AS comp_name_eng,
            any(list_date) AS list_date,
            any(delist_date) AS delist_date,
            any(listplate_name) AS listplate_name,
            any(comp_sname_eng) AS comp_sname_eng,
            any(is_listed) AS is_listed
        FROM {AD_STOCK_BASIC_TABLE}
        WHERE snapshot_date = {{snapshot_date:Date}}
          AND market_code IN {{code_list:Array(String)}}
        GROUP BY market_code
        ORDER BY market_code
        """
        frame = self.client.query_df(
            sql,
            {
                "snapshot_date": snapshot_date,
                "code_list": list(query.code_list),
            },
        )
        if frame.empty:
            return frame

        rename_map = {db_name: raw_name for raw_name, db_name in STOCK_BASIC_FIELD_DB_MAPPING.items()}
        frame = frame.rename(columns=rename_map)
        frame["_code_order"] = frame["MARKET_CODE"].apply(lambda value: query.code_list.index(value))
        frame = frame.sort_values("_code_order").drop(columns=["_code_order"]).reset_index(drop=True)
        return frame.loc[:, list(STOCK_BASIC_FIELDS)]

    def load_history_stock_status_frame(self, query: HistoryStockStatusQuery):
        if pd is None:  # pragma: no cover
            raise RuntimeError("未安装 pandas，无法返回 DataFrame。")

        if not query.code_list:
            return pd.DataFrame(columns=list(HISTORY_STOCK_STATUS_FIELDS))

        sql = f"""
        SELECT
            market_code,
            trade_date,
            any(preclose) AS preclose,
            any(high_limited) AS high_limited,
            any(low_limited) AS low_limited,
            any(price_high_lmt_rate) AS price_high_lmt_rate,
            any(price_low_lmt_rate) AS price_low_lmt_rate,
            any(is_st_sec) AS is_st_sec,
            any(is_susp_sec) AS is_susp_sec,
            any(is_wd_sec) AS is_wd_sec,
            any(is_xr_sec) AS is_xr_sec
        FROM {AD_HISTORY_STOCK_STATUS_TABLE}
        WHERE market_code IN {{code_list:Array(String)}}
        """
        parameters = {
            "code_list": list(query.code_list),
        }
        if query.begin_date is not None:
            sql += "\n  AND trade_date >= {begin_date:Date}"
            parameters["begin_date"] = query.begin_date
        if query.end_date is not None:
            sql += "\n  AND trade_date <= {end_date:Date}"
            parameters["end_date"] = query.end_date
        sql += "\nGROUP BY market_code, trade_date\nORDER BY market_code, trade_date"

        frame = self.client.query_df(sql, parameters)
        if frame.empty:
            return frame

        frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.strftime("%Y%m%d")
        rename_map = {db_name: raw_name for raw_name, db_name in HISTORY_STOCK_STATUS_FIELD_DB_MAPPING.items()}
        frame = frame.rename(columns=rename_map)
        frame["_code_order"] = frame["MARKET_CODE"].apply(lambda value: query.code_list.index(value))
        frame = frame.sort_values(["_code_order", "TRADE_DATE"]).drop(columns=["_code_order"]).reset_index(drop=True)
        return frame.loc[:, list(HISTORY_STOCK_STATUS_FIELDS)]


__all__ = ["InfoDataRepository"]
