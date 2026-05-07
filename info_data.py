#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AmazingData 风格的 InfoData 实现.

当前已实现两个接口：
- `get_stock_basic`
- `get_history_stock_status`
"""

from __future__ import annotations

import hashlib
import logging
from datetime import date, datetime, timedelta
from typing import Callable, Iterable, Optional, Protocol, Sequence

from sync_data_system.amazingdata_constants import HISTORY_STOCK_STATUS_FIELDS, STOCK_BASIC_FIELDS, SyncStatus
from sync_data_system.base_data import BaseDataCacheMissError, BaseDataParameterError
from sync_data_system.clickhouse_client import ClickHouseConfig, create_clickhouse_client
from sync_data_system.clickhouse_tables import (
    AD_BALANCE_SHEET_TABLE,
    AD_BJ_CODE_MAPPING_TABLE,
    AD_BLOCK_TRADING_TABLE,
    AD_CASH_FLOW_TABLE,
    AD_FUND_SHARE_TABLE,
    AD_FUND_IOPV_TABLE,
    AD_HISTORY_STOCK_STATUS_TABLE,
    AD_INDUSTRY_BASE_INFO_TABLE,
    AD_INDUSTRY_CONSTITUENT_TABLE,
    AD_INDUSTRY_DAILY_TABLE,
    AD_INDUSTRY_WEIGHT_TABLE,
    AD_INDEX_CONSTITUENT_TABLE,
    AD_INDEX_WEIGHT_TABLE,
    AD_INCOME_TABLE,
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
    AD_STOCK_BASIC_TABLE,
)
from sync_data_system.data_models import (
    BalanceSheetRow,
    BjCodeMappingRow,
    BlockTradingRow,
    CashFlowRow,
    DividendRow,
    EquityPledgeFreezeRow,
    EquityRestrictedRow,
    EquityStructureRow,
    FundIopvRow,
    FundShareRow,
    HistoryStockStatusQuery,
    HistoryStockStatusRow,
    InfoPayloadRow,
    IndustryBaseInfoRow,
    IndustryConstituentRow,
    IndustryDailyRow,
    IndustryWeightRow,
    IndexConstituentRow,
    IndexWeightRow,
    KzzCallRow,
    KzzCallExplanationRow,
    KzzConvRow,
    KzzPutExplanationRow,
    KzzPutCallItemRow,
    KzzPutRow,
    KzzConvChangeRow,
    KzzCorrRow,
    LongHuBangRow,
    MarginDetailRow,
    MarginSummaryRow,
    HolderNumRow,
    IncomeRow,
    ProfitExpressRow,
    ProfitNoticeRow,
    ShareHolderRow,
    StockBasicQuery,
    StockBasicRow,
    SyncTaskLogRow,
    KzzIssuanceRow,
    KzzShareRow,
    KzzSuspendRow,
    OptionBasicInfoRow,
    OptionMonCtrSpecsRow,
    OptionStdCtrSpecsRow,
    TreasuryYieldRow,
    normalize_code_list,
    RightIssueRow,
    to_ch_date,
    utcnow,
)
from sync_data_system.repositories.info_data_repository import InfoDataRepository


logger = logging.getLogger(__name__)


class InfoDataSyncProvider(Protocol):
    """InfoData 远端同步协议."""

    def fetch_bj_code_mapping(self) -> Iterable[BjCodeMappingRow]:
        ...

    def fetch_fund_iopv(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[FundIopvRow]:
        ...

    def fetch_stock_basic(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[StockBasicRow]:
        ...

    def fetch_history_stock_status(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[HistoryStockStatusRow]:
        ...

    def fetch_balance_sheet(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[BalanceSheetRow]:
        ...

    def fetch_cash_flow(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[CashFlowRow]:
        ...

    def fetch_income(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[IncomeRow]:
        ...

    def fetch_profit_express(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[ProfitExpressRow]:
        ...

    def fetch_profit_notice(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[ProfitNoticeRow]:
        ...

    def fetch_fund_share(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[FundShareRow]:
        ...

    def fetch_kzz_conv_change(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[KzzConvChangeRow]:
        ...

    def fetch_kzz_issuance(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[KzzIssuanceRow]:
        ...

    def fetch_kzz_share(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[KzzShareRow]:
        ...

    def fetch_kzz_suspend(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[KzzSuspendRow]:
        ...

    def fetch_option_basic_info(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[OptionBasicInfoRow]:
        ...

    def fetch_option_std_ctr_specs(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[OptionStdCtrSpecsRow]:
        ...

    def fetch_option_mon_ctr_specs(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[OptionMonCtrSpecsRow]:
        ...

    def fetch_treasury_yield(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[TreasuryYieldRow]:
        ...

    def fetch_kzz_corr(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[KzzCorrRow]:
        ...

    def fetch_kzz_call_explanation(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[KzzCallExplanationRow]:
        ...

    def fetch_kzz_put_explanation(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[KzzPutExplanationRow]:
        ...

    def fetch_kzz_put_call_item(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[KzzPutCallItemRow]:
        ...

    def fetch_kzz_put(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[KzzPutRow]:
        ...

    def fetch_kzz_call(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[KzzCallRow]:
        ...

    def fetch_kzz_conv(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[KzzConvRow]:
        ...

    def fetch_block_trading(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[BlockTradingRow]:
        ...

    def fetch_long_hu_bang(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[LongHuBangRow]:
        ...

    def fetch_margin_detail(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[MarginDetailRow]:
        ...

    def fetch_margin_summary(
        self,
        code_list: Sequence[str] | None = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[MarginSummaryRow]:
        ...

    def fetch_share_holder(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[ShareHolderRow]:
        ...

    def fetch_holder_num(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[HolderNumRow]:
        ...

    def fetch_equity_structure(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[EquityStructureRow]:
        ...

    def fetch_equity_pledge_freeze(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[EquityPledgeFreezeRow]:
        ...

    def fetch_equity_restricted(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[EquityRestrictedRow]:
        ...

    def fetch_dividend(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[DividendRow]:
        ...

    def fetch_right_issue(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[RightIssueRow]:
        ...

    def fetch_index_constituent(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[IndexConstituentRow]:
        ...

    def fetch_index_weight(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[IndexWeightRow]:
        ...

    def fetch_industry_base_info(self) -> Iterable[IndustryBaseInfoRow]:
        ...

    def fetch_industry_constituent(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[IndustryConstituentRow]:
        ...

    def fetch_industry_weight(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[IndustryWeightRow]:
        ...

    def fetch_industry_daily(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[IndustryDailyRow]:
        ...


class InfoData:
    """InfoData SDK 兼容层."""

    def __init__(
        self,
        repository: InfoDataRepository,
        sync_provider: Optional[InfoDataSyncProvider] = None,
    ) -> None:
        self.repository = repository
        self.sync_provider = sync_provider

    @classmethod
    def from_clickhouse_config(
        cls,
        config: ClickHouseConfig,
        sync_provider: Optional[InfoDataSyncProvider] = None,
        ensure_tables: bool = True,
        insert_batch_size: int = 5000,
    ) -> "InfoData":
        connection = create_clickhouse_client(config)
        repository = InfoDataRepository(connection, insert_batch_size=insert_batch_size)
        instance = cls(repository=repository, sync_provider=sync_provider)
        if ensure_tables:
            instance.ensure_tables()
        return instance

    def ensure_tables(self) -> None:
        self.repository.ensure_tables()

    def close(self) -> None:
        self.repository.client.close()

    def get_stock_basic(self, code_list: Sequence[str]):
        """获取证券基础信息.

        SDK 返回列名保持文档原始大写风格；
        数据库存储字段统一使用小写 snake_case。
        """

        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")

        self.sync_stock_basic(normalized_codes, force=False)
        frame = self.repository.load_stock_basic_frame(StockBasicQuery(code_list=tuple(normalized_codes)))
        if frame.empty:
            raise BaseDataCacheMissError(
                f"未找到 code_count={len(normalized_codes)} 的 stock_basic 数据。"
        )
        return frame.loc[:, list(STOCK_BASIC_FIELDS)]

    def get_bj_code_mapping(
        self,
        local_path: str,
        is_local: bool = True,
    ):
        normalized_local_path = str(local_path)
        self._validate_local_path(normalized_local_path)

        self.sync_bj_code_mapping(force=False)

        frame = self.repository.load_bj_code_mapping_frame()
        if frame.empty:
            raise BaseDataCacheMissError("未找到 bj_code_mapping 数据。")
        return frame.rename(
            columns={
                "old_code": "OLD_CODE",
                "new_code": "NEW_CODE",
                "security_name": "SECURITY_NAME",
                "listing_date": "LISTING_DATE",
            }
        ).loc[:, ["OLD_CODE", "NEW_CODE", "SECURITY_NAME", "LISTING_DATE"]]

    def get_history_stock_status(
        self,
        code_list: Sequence[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ):
        """获取历史证券状态信息.

        SDK 返回列名保持文档原始大写风格；
        数据库存储字段统一使用小写 snake_case。
        """

        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        self._validate_local_path(local_path)

        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)

        # 这里对日期 + code 的接口统一先尝试增量同步。
        # 当前同步链路固定走远端补数，再从 ClickHouse 返回结果。
        self.sync_history_stock_status(
            code_list=normalized_codes,
            begin_date=begin,
            end_date=end,
            force=False,
        )

        query = HistoryStockStatusQuery(
            code_list=tuple(normalized_codes),
            local_path=local_path,
            is_local=is_local,
            begin_date=begin,
            end_date=end,
        )
        frame = self.repository.load_history_stock_status_frame(query)
        if frame.empty:
            raise BaseDataCacheMissError(
                f"未找到 code_count={len(normalized_codes)} 的 history_stock_status 数据。"
            )
        return frame.loc[:, list(HISTORY_STOCK_STATUS_FIELDS)]

    def sync_stock_basic(self, code_list: Sequence[str], force: bool = False) -> int:
        """同步 `ad_stock_basic`."""

        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")

        scope_key = self._build_code_scope_key("get_stock_basic", normalized_codes)
        latest_date = self.repository.load_sync_checkpoint_date("get_stock_basic", scope_key)
        return self._run_sync_job(
            task_name="get_stock_basic",
            scope_key=scope_key,
            target_table=AD_STOCK_BASIC_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda start_date: self._provider_fetch_stock_basic(normalized_codes, start_date),
            save_rows=self.repository.save_stock_basic_rows,
            row_date_getter=lambda row: row.snapshot_date,
            force=force,
            skip_if_success_today=True,
        )

    def sync_bj_code_mapping(self, force: bool = False) -> int:
        scope_key = "all"
        latest_date = self.repository.load_sync_checkpoint_date("get_bj_code_mapping", scope_key)
        return self._run_sync_job(
            task_name="get_bj_code_mapping",
            scope_key=scope_key,
            target_table=AD_BJ_CODE_MAPPING_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_bj_code_mapping(),
            save_rows=self.repository.save_bj_code_mapping_rows,
            row_date_getter=lambda _row: None,
            force=force,
            skip_if_success_today=True,
        )

    def sync_fund_iopv(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_fund_iopv", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self.repository.load_sync_checkpoint_date("get_fund_iopv", scope_key)
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        return self._run_sync_job(
            task_name="get_fund_iopv",
            scope_key=scope_key,
            target_table=AD_FUND_IOPV_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_fund_iopv(normalized_codes, sync_start, end),
            save_rows=self.repository.save_fund_iopv_rows,
            row_date_getter=lambda row: self._coerce_optional_date_text(row.price_date),
            force=force,
        )

    def sync_history_stock_status(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        """同步 `ad_history_stock_status`."""

        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")

        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_history_stock_status", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self._load_latest_table_date(
            table_name=AD_HISTORY_STOCK_STATUS_TABLE,
            code_field="market_code",
            date_field="trade_date",
            code_list=normalized_codes,
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        return self._run_sync_job(
            task_name="get_history_stock_status",
            scope_key=scope_key,
            target_table=AD_HISTORY_STOCK_STATUS_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_history_stock_status(
                normalized_codes,
                start_date=sync_start,
                end_date=end,
            ),
            save_rows=self.repository.save_history_stock_status_rows,
            row_date_getter=lambda row: row.trade_date,
            force=force,
        )

    def sync_balance_sheet(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_balance_sheet", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self._load_latest_table_date(
            table_name=AD_BALANCE_SHEET_TABLE,
            code_field="market_code",
            date_field="report_date",
            code_list=normalized_codes,
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        return self._run_sync_job(
            task_name="get_balance_sheet",
            scope_key=scope_key,
            target_table=AD_BALANCE_SHEET_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_balance_sheet(normalized_codes, sync_start, end),
            save_rows=self.repository.save_balance_sheet_rows,
            row_date_getter=lambda row: row.ann_date or row.reporting_period,
            force=force,
        )

    def sync_cash_flow(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_cash_flow", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self._load_latest_table_date(
            table_name=AD_CASH_FLOW_TABLE,
            code_field="market_code",
            date_field="report_date",
            code_list=normalized_codes,
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        return self._run_sync_job(
            task_name="get_cash_flow",
            scope_key=scope_key,
            target_table=AD_CASH_FLOW_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_cash_flow(normalized_codes, sync_start, end),
            save_rows=self.repository.save_cash_flow_rows,
            row_date_getter=lambda row: row.ann_date or row.reporting_period,
            force=force,
        )

    def sync_income(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_income", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self._load_latest_table_date(
            table_name=AD_INCOME_TABLE,
            code_field="market_code",
            date_field="report_date",
            code_list=normalized_codes,
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        return self._run_sync_job(
            task_name="get_income",
            scope_key=scope_key,
            target_table=AD_INCOME_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_income(normalized_codes, sync_start, end),
            save_rows=self.repository.save_income_rows,
            row_date_getter=lambda row: row.ann_date or row.reporting_period,
            force=force,
        )

    def sync_profit_express(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_profit_express", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self._load_latest_table_date(
            table_name=AD_PROFIT_EXPRESS_TABLE,
            code_field="market_code",
            date_field="report_date",
            code_list=normalized_codes,
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        return self._run_sync_job(
            task_name="get_profit_express",
            scope_key=scope_key,
            target_table=AD_PROFIT_EXPRESS_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_profit_express(normalized_codes, sync_start, end),
            save_rows=self.repository.save_profit_express_rows,
            row_date_getter=lambda row: row.ann_date or row.reporting_period,
            force=force,
        )

    def sync_profit_notice(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_profit_notice", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self._load_latest_table_date(
            table_name=AD_PROFIT_NOTICE_TABLE,
            code_field="market_code",
            date_field="report_date",
            code_list=normalized_codes,
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        return self._run_sync_job(
            task_name="get_profit_notice",
            scope_key=scope_key,
            target_table=AD_PROFIT_NOTICE_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_profit_notice(normalized_codes, sync_start, end),
            save_rows=self.repository.save_profit_notice_rows,
            row_date_getter=lambda row: row.ann_date or row.first_ann_date or row.reporting_period,
            force=force,
        )

    def sync_fund_share(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_fund_share", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self._load_latest_table_date(
            table_name=AD_FUND_SHARE_TABLE,
            code_field="market_code",
            date_field="change_date",
            code_list=normalized_codes,
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        return self._run_sync_job(
            task_name="get_fund_share",
            scope_key=scope_key,
            target_table=AD_FUND_SHARE_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_fund_share(normalized_codes, sync_start, end),
            save_rows=self.repository.save_fund_share_rows,
            row_date_getter=lambda row: row.change_date or row.ann_date,
            force=force,
        )

    def sync_kzz_issuance(
        self,
        code_list: Sequence[str],
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        scope_key = self._build_code_scope_key("get_kzz_issuance", normalized_codes)
        latest_date = self.repository.load_sync_checkpoint_date("get_kzz_issuance", scope_key)
        return self._run_sync_job(
            task_name="get_kzz_issuance",
            scope_key=scope_key,
            target_table=AD_KZZ_ISSUANCE_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_kzz_issuance(normalized_codes, None, None),
            save_rows=self.repository.save_kzz_issuance_rows,
            row_date_getter=lambda row: self._coerce_optional_date_text(row.ann_dt),
            force=force,
            skip_if_success_today=True,
        )

    def sync_kzz_share(
        self,
        code_list: Sequence[str],
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        scope_key = self._build_code_scope_key("get_kzz_share", normalized_codes)
        latest_date = self.repository.load_sync_checkpoint_date("get_kzz_share", scope_key)
        return self._run_sync_job(
            task_name="get_kzz_share",
            scope_key=scope_key,
            target_table=AD_KZZ_SHARE_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_kzz_share(normalized_codes, None, None),
            save_rows=self.repository.save_kzz_share_rows,
            row_date_getter=lambda row: self._coerce_optional_date_text(row.change_date) or self._coerce_optional_date_text(row.ann_date),
            force=force,
            skip_if_success_today=True,
        )

    def sync_kzz_suspend(
        self,
        code_list: Sequence[str],
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        scope_key = self._build_code_scope_key("get_kzz_suspend", normalized_codes)
        latest_date = self.repository.load_sync_checkpoint_date("get_kzz_suspend", scope_key)
        return self._run_sync_job(
            task_name="get_kzz_suspend",
            scope_key=scope_key,
            target_table=AD_KZZ_SUSPEND_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_kzz_suspend(normalized_codes, None, None),
            save_rows=self.repository.save_kzz_suspend_rows,
            row_date_getter=lambda row: self._coerce_optional_date_text(row.suspend_date) or self._coerce_optional_date_text(row.resump_date),
            force=force,
            skip_if_success_today=True,
        )

    def sync_option_basic_info(
        self,
        code_list: Sequence[str],
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        if not force:
            existing_codes = self.repository.load_existing_values(
                table_name=AD_OPTION_BASIC_INFO_TABLE,
                field_name="market_code",
                values=normalized_codes,
            )
            pending_codes = [code for code in normalized_codes if code not in existing_codes]
            if not pending_codes:
                logger.info("get_option_basic_info 已有现成数据，跳过 code_count=%s", len(normalized_codes))
                return 0
            normalized_codes = pending_codes
        scope_key = self._build_code_scope_key("get_option_basic_info", normalized_codes)
        latest_date = self.repository.load_sync_checkpoint_date("get_option_basic_info", scope_key)
        return self._run_sync_job(
            task_name="get_option_basic_info",
            scope_key=scope_key,
            target_table=AD_OPTION_BASIC_INFO_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_option_basic_info(normalized_codes, None, None),
            save_rows=self.repository.save_option_basic_info_rows,
            row_date_getter=lambda row: self._coerce_optional_date_text(row.expiry_date) or self._coerce_optional_date_text(row.last_trade_date),
            force=force,
            skip_if_success_today=True,
        )

    def sync_option_std_ctr_specs(
        self,
        code_list: Sequence[str],
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        latest_date_raw = self.repository.load_latest_date(
            table_name=AD_OPTION_STD_CTR_SPECS_TABLE,
            date_field="listed_date",
        )
        latest_date = self._coerce_optional_date_text(str(latest_date_raw) if latest_date_raw is not None else None)
        scope_key = self._build_code_scope_key("get_option_std_ctr_specs", normalized_codes)
        return self._run_sync_job(
            task_name="get_option_std_ctr_specs",
            scope_key=scope_key,
            target_table=AD_OPTION_STD_CTR_SPECS_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: (
                row
                for row in self._provider_fetch_option_std_ctr_specs(normalized_codes, None, None)
                if latest_date is None
                or (self._coerce_optional_date_text(row.listed_date) is not None and self._coerce_optional_date_text(row.listed_date) > latest_date)
            ),
            save_rows=self.repository.save_option_std_ctr_specs_rows,
            row_date_getter=lambda row: self._coerce_optional_date_text(row.exercise_date) or self._coerce_optional_date_text(row.last_trading_date),
            force=force,
        )

    def sync_option_mon_ctr_specs(
        self,
        code_list: Sequence[str],
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        scope_key = self._build_code_scope_key("get_option_mon_ctr_specs", normalized_codes)
        latest_date_raw = self.repository.load_latest_date_by_codes(
            table_name=AD_OPTION_MON_CTR_SPECS_TABLE,
            code_field="market_code",
            date_field="change_date",
            code_list=normalized_codes,
        )
        latest_date = self._coerce_optional_date_text(str(latest_date_raw) if latest_date_raw is not None else None)
        return self._run_sync_job(
            task_name="get_option_mon_ctr_specs",
            scope_key=scope_key,
            target_table=AD_OPTION_MON_CTR_SPECS_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: (
                row
                for row in self._provider_fetch_option_mon_ctr_specs(normalized_codes, None, None)
                if latest_date is None
                or (self._coerce_optional_date_text(row.change_date) is not None and self._coerce_optional_date_text(row.change_date) > latest_date)
            ),
            save_rows=self.repository.save_option_mon_ctr_specs_rows,
            row_date_getter=lambda row: self._coerce_optional_date_text(row.change_date),
            force=force,
        )

    def sync_treasury_yield(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_treasury_yield", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self.repository.load_latest_date_by_codes(
            table_name=AD_TREASURY_YIELD_TABLE,
            code_field="term",
            date_field="trade_date",
            code_list=normalized_codes,
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        if self._skip_if_empty_incremental_window(
            task_name="get_treasury_yield",
            scope_key=scope_key,
            target_table=AD_TREASURY_YIELD_TABLE,
            latest_date=latest_date,
            sync_start=sync_start,
            end_date=end,
            force=force,
        ):
            return 0
        return self._run_sync_job(
            task_name="get_treasury_yield",
            scope_key=scope_key,
            target_table=AD_TREASURY_YIELD_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_treasury_yield(normalized_codes, sync_start, end),
            save_rows=self.repository.save_treasury_yield_rows,
            row_date_getter=lambda row: row.trade_date,
            force=force,
        )

    def sync_kzz_conv_change(
        self,
        code_list: Sequence[str],
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        scope_key = self._build_code_scope_key("get_kzz_conv_change", normalized_codes)
        latest_date = self.repository.load_sync_checkpoint_date("get_kzz_conv_change", scope_key)
        return self._run_sync_job(
            task_name="get_kzz_conv_change",
            scope_key=scope_key,
            target_table=AD_KZZ_CONV_CHANGE_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_kzz_conv_change(normalized_codes, None, None),
            save_rows=self.repository.save_kzz_conv_change_rows,
            row_date_getter=lambda row: row.change_date or row.ann_date,
            force=force,
            skip_if_success_today=True,
        )

    def sync_kzz_corr(
        self,
        code_list: Sequence[str],
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        scope_key = self._build_code_scope_key("get_kzz_corr", normalized_codes)
        latest_date = self.repository.load_sync_checkpoint_date("get_kzz_corr", scope_key)
        return self._run_sync_job(
            task_name="get_kzz_corr",
            scope_key=scope_key,
            target_table=AD_KZZ_CORR_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_kzz_corr(normalized_codes, None, None),
            save_rows=self.repository.save_kzz_corr_rows,
            row_date_getter=lambda row: row.end_date or row.start_date,
            force=force,
            skip_if_success_today=True,
        )

    def sync_kzz_call_explanation(
        self,
        code_list: Sequence[str],
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        scope_key = self._build_code_scope_key("get_kzz_call_explanation", normalized_codes)
        latest_date = self.repository.load_sync_checkpoint_date("get_kzz_call_explanation", scope_key)
        return self._run_sync_job(
            task_name="get_kzz_call_explanation",
            scope_key=scope_key,
            target_table=AD_KZZ_CALL_EXPLANATION_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_kzz_call_explanation(normalized_codes, None, None),
            save_rows=self.repository.save_kzz_call_explanation_rows,
            row_date_getter=lambda row: row.call_date or row.call_announcement_date,
            force=force,
            skip_if_success_today=True,
        )

    def sync_kzz_put_explanation(
        self,
        code_list: Sequence[str],
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        scope_key = self._build_code_scope_key("get_kzz_put_explanation", normalized_codes)
        latest_date = self.repository.load_sync_checkpoint_date("get_kzz_put_explanation", scope_key)
        return self._run_sync_job(
            task_name="get_kzz_put_explanation",
            scope_key=scope_key,
            target_table=AD_KZZ_PUT_EXPLANATION_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_kzz_put_explanation(normalized_codes, None, None),
            save_rows=self.repository.save_kzz_put_explanation_rows,
            row_date_getter=lambda row: self._coerce_optional_date_text(row.put_announcement_date) or self._coerce_optional_date_text(row.fund_end_date),
            force=force,
            skip_if_success_today=True,
        )

    def sync_kzz_put_call_item(
        self,
        code_list: Sequence[str],
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        scope_key = self._build_code_scope_key("get_kzz_put_call_item", normalized_codes)
        latest_date = self.repository.load_sync_checkpoint_date("get_kzz_put_call_item", scope_key)
        return self._run_sync_job(
            task_name="get_kzz_put_call_item",
            scope_key=scope_key,
            target_table=AD_KZZ_PUT_CALL_ITEM_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_kzz_put_call_item(normalized_codes, None, None),
            save_rows=self.repository.save_kzz_put_call_item_rows,
            row_date_getter=lambda row: row.con_call_end_date or row.mand_put_end_date or row.con_put_end_date,
            force=force,
            skip_if_success_today=True,
        )

    def sync_kzz_put(
        self,
        code_list: Sequence[str],
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        scope_key = self._build_code_scope_key("get_kzz_put", normalized_codes)
        latest_date = self.repository.load_sync_checkpoint_date("get_kzz_put", scope_key)
        return self._run_sync_job(
            task_name="get_kzz_put",
            scope_key=scope_key,
            target_table=AD_KZZ_PUT_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_kzz_put(normalized_codes, None, None),
            save_rows=self.repository.save_kzz_put_rows,
            row_date_getter=lambda row: row.end_date or row.begin_date,
            force=force,
            skip_if_success_today=True,
        )

    def sync_kzz_call(
        self,
        code_list: Sequence[str],
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        scope_key = self._build_code_scope_key("get_kzz_call", normalized_codes)
        latest_date = self.repository.load_sync_checkpoint_date("get_kzz_call", scope_key)
        return self._run_sync_job(
            task_name="get_kzz_call",
            scope_key=scope_key,
            target_table=AD_KZZ_CALL_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_kzz_call(normalized_codes, None, None),
            save_rows=self.repository.save_kzz_call_rows,
            row_date_getter=lambda row: row.end_date or row.begin_date,
            force=force,
            skip_if_success_today=True,
        )

    def sync_kzz_conv(
        self,
        code_list: Sequence[str],
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        scope_key = self._build_code_scope_key("get_kzz_conv", normalized_codes)
        latest_date = self.repository.load_sync_checkpoint_date("get_kzz_conv", scope_key)
        return self._run_sync_job(
            task_name="get_kzz_conv",
            scope_key=scope_key,
            target_table=AD_KZZ_CONV_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_kzz_conv(normalized_codes, None, None),
            save_rows=self.repository.save_kzz_conv_rows,
            row_date_getter=lambda row: row.ann_date or row.conv_end_date or row.forced_conv_date,
            force=force,
            skip_if_success_today=True,
        )

    def sync_block_trading(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_block_trading", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self._load_latest_table_date(
            table_name=AD_BLOCK_TRADING_TABLE,
            code_field="market_code",
            date_field="trade_date",
            code_list=normalized_codes,
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        if self._skip_if_empty_incremental_window(
            task_name="get_block_trading",
            scope_key=scope_key,
            target_table=AD_BLOCK_TRADING_TABLE,
            latest_date=latest_date,
            sync_start=sync_start,
            end_date=end,
            force=force,
        ):
            return 0
        return self._run_sync_job(
            task_name="get_block_trading",
            scope_key=scope_key,
            target_table=AD_BLOCK_TRADING_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_block_trading(normalized_codes, sync_start, end),
            save_rows=self.repository.save_block_trading_rows,
            row_date_getter=lambda row: row.trade_date,
            force=force,
        )

    def sync_long_hu_bang(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_long_hu_bang", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self._load_latest_table_date(
            table_name=AD_LONG_HU_BANG_TABLE,
            code_field="market_code",
            date_field="trade_date",
            code_list=normalized_codes,
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        if self._skip_if_empty_incremental_window(
            task_name="get_long_hu_bang",
            scope_key=scope_key,
            target_table=AD_LONG_HU_BANG_TABLE,
            latest_date=latest_date,
            sync_start=sync_start,
            end_date=end,
            force=force,
        ):
            return 0
        return self._run_sync_job(
            task_name="get_long_hu_bang",
            scope_key=scope_key,
            target_table=AD_LONG_HU_BANG_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_long_hu_bang(normalized_codes, sync_start, end),
            save_rows=self.repository.save_long_hu_bang_rows,
            row_date_getter=lambda row: row.trade_date,
            force=force,
        )

    def sync_margin_detail(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_margin_detail", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self._load_latest_table_date(
            table_name=AD_MARGIN_DETAIL_TABLE,
            code_field="market_code",
            date_field="trade_date",
            code_list=normalized_codes,
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        if self._skip_if_empty_incremental_window(
            task_name="get_margin_detail",
            scope_key=scope_key,
            target_table=AD_MARGIN_DETAIL_TABLE,
            latest_date=latest_date,
            sync_start=sync_start,
            end_date=end,
            force=force,
        ):
            return 0
        return self._run_sync_job(
            task_name="get_margin_detail",
            scope_key=scope_key,
            target_table=AD_MARGIN_DETAIL_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_margin_detail(normalized_codes, sync_start, end),
            save_rows=self.repository.save_margin_detail_rows,
            row_date_getter=lambda row: row.trade_date,
            force=force,
        )

    def sync_margin_summary(
        self,
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        begin_text = begin.isoformat() if begin is not None else ""
        end_text = end.isoformat() if end is not None else ""
        scope_key = f"task=get_margin_summary|begin_date={begin_text}|end_date={end_text}"
        latest_date = self.repository.load_latest_date(
            table_name=AD_MARGIN_SUMMARY_TABLE,
            date_field="trade_date",
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        if self._skip_if_empty_incremental_window(
            task_name="get_margin_summary",
            scope_key=scope_key,
            target_table=AD_MARGIN_SUMMARY_TABLE,
            latest_date=latest_date,
            sync_start=sync_start,
            end_date=end,
            force=force,
        ):
            return 0
        return self._run_sync_job(
            task_name="get_margin_summary",
            scope_key=scope_key,
            target_table=AD_MARGIN_SUMMARY_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_margin_summary(sync_start, end),
            save_rows=self.repository.save_margin_summary_rows,
            row_date_getter=lambda row: row.trade_date,
            force=force,
        )

    def sync_share_holder(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_share_holder", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self._load_latest_table_date(
            table_name="ad_share_holder",
            code_field="market_code",
            date_field="holder_enddate",
            code_list=normalized_codes,
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        return self._run_sync_job(
            task_name="get_share_holder",
            scope_key=scope_key,
            target_table="ad_share_holder",
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_share_holder(normalized_codes, sync_start, end),
            save_rows=self.repository.save_share_holder_rows,
            row_date_getter=lambda row: row.holder_enddate,
            force=force,
        )

    def sync_holder_num(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_holder_num", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self._load_latest_table_date(
            table_name="ad_holder_num",
            code_field="market_code",
            date_field="holder_enddate",
            code_list=normalized_codes,
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        return self._run_sync_job(
            task_name="get_holder_num",
            scope_key=scope_key,
            target_table="ad_holder_num",
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_holder_num(normalized_codes, sync_start, end),
            save_rows=self.repository.save_holder_num_rows,
            row_date_getter=lambda row: row.holder_enddate,
            force=force,
        )

    def sync_equity_structure(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_equity_structure", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self._load_latest_table_date(
            table_name="ad_equity_structure",
            code_field="market_code",
            date_field="change_date",
            code_list=normalized_codes,
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        return self._run_sync_job(
            task_name="get_equity_structure",
            scope_key=scope_key,
            target_table="ad_equity_structure",
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_equity_structure(normalized_codes, sync_start, end),
            save_rows=self.repository.save_equity_structure_rows,
            row_date_getter=lambda row: row.change_date,
            force=force,
        )

    def sync_equity_pledge_freeze(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_equity_pledge_freeze", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self._load_latest_table_date(
            table_name="ad_equity_pledge_freeze",
            code_field="market_code",
            date_field="ann_date",
            code_list=normalized_codes,
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        return self._run_sync_job(
            task_name="get_equity_pledge_freeze",
            scope_key=scope_key,
            target_table="ad_equity_pledge_freeze",
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_equity_pledge_freeze(normalized_codes, sync_start, end),
            save_rows=self.repository.save_equity_pledge_freeze_rows,
            row_date_getter=lambda row: row.ann_date,
            force=force,
        )

    def sync_equity_restricted(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_equity_restricted", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self._load_latest_table_date(
            table_name="ad_equity_restricted",
            code_field="market_code",
            date_field="list_date",
            code_list=normalized_codes,
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        return self._run_sync_job(
            task_name="get_equity_restricted",
            scope_key=scope_key,
            target_table="ad_equity_restricted",
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_equity_restricted(normalized_codes, sync_start, end),
            save_rows=self.repository.save_equity_restricted_rows,
            row_date_getter=lambda row: row.list_date,
            force=force,
        )

    def sync_dividend(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_dividend", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self._load_latest_table_date(
            table_name="ad_dividend",
            code_field="market_code",
            date_field="ann_date",
            code_list=normalized_codes,
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        return self._run_sync_job(
            task_name="get_dividend",
            scope_key=scope_key,
            target_table="ad_dividend",
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_dividend(normalized_codes, sync_start, end),
            save_rows=self.repository.save_dividend_rows,
            row_date_getter=lambda row: row.ann_date,
            force=force,
        )

    def sync_right_issue(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_right_issue", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self._load_latest_table_date(
            table_name="ad_right_issue",
            code_field="market_code",
            date_field="ann_date",
            code_list=normalized_codes,
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        return self._run_sync_job(
            task_name="get_right_issue",
            scope_key=scope_key,
            target_table="ad_right_issue",
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_right_issue(normalized_codes, sync_start, end),
            save_rows=self.repository.save_right_issue_rows,
            row_date_getter=lambda row: row.ann_date,
            force=force,
        )

    def sync_index_constituent(
        self,
        code_list: Sequence[str],
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        scope_key = self._build_code_scope_key("get_index_constituent", normalized_codes)
        latest_date = self.repository.load_sync_checkpoint_date("get_index_constituent", scope_key)
        return self._run_sync_job(
            task_name="get_index_constituent",
            scope_key=scope_key,
            target_table=AD_INDEX_CONSTITUENT_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_index_constituent(normalized_codes, None, None),
            save_rows=self.repository.save_index_constituent_rows,
            row_date_getter=lambda row: row.out_date or row.in_date,
            force=force,
            skip_if_success_today=True,
        )

    def sync_index_weight(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_index_weight", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self._load_latest_table_date(
            table_name=AD_INDEX_WEIGHT_TABLE,
            code_field="index_code",
            date_field="trade_date",
            code_list=normalized_codes,
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        return self._run_sync_job(
            task_name="get_index_weight",
            scope_key=scope_key,
            target_table=AD_INDEX_WEIGHT_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_index_weight(normalized_codes, sync_start, end),
            save_rows=self.repository.save_index_weight_rows,
            row_date_getter=lambda row: row.trade_date,
            force=force,
        )

    def sync_industry_base_info(self, force: bool = False) -> int:
        scope_key = "all"
        latest_date = self.repository.load_sync_checkpoint_date("get_industry_base_info", scope_key)
        return self._run_sync_job(
            task_name="get_industry_base_info",
            scope_key=scope_key,
            target_table=AD_INDUSTRY_BASE_INFO_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_industry_base_info(),
            save_rows=self.repository.save_industry_base_info_rows,
            row_date_getter=lambda row: None,
            force=force,
            skip_if_success_today=True,
        )

    def sync_industry_constituent(
        self,
        code_list: Sequence[str],
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        scope_key = self._build_code_scope_key("get_industry_constituent", normalized_codes)
        latest_date = self.repository.load_sync_checkpoint_date("get_industry_constituent", scope_key)
        return self._run_sync_job(
            task_name="get_industry_constituent",
            scope_key=scope_key,
            target_table=AD_INDUSTRY_CONSTITUENT_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_industry_constituent(normalized_codes, None, None),
            save_rows=self.repository.save_industry_constituent_rows,
            row_date_getter=lambda row: row.out_date or row.in_date,
            force=force,
            skip_if_success_today=True,
        )

    def sync_industry_weight(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_industry_weight", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self._load_latest_table_date(
            table_name=AD_INDUSTRY_WEIGHT_TABLE,
            code_field="index_code",
            date_field="trade_date",
            code_list=normalized_codes,
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        return self._run_sync_job(
            task_name="get_industry_weight",
            scope_key=scope_key,
            target_table=AD_INDUSTRY_WEIGHT_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_industry_weight(normalized_codes, sync_start, end),
            save_rows=self.repository.save_industry_weight_rows,
            row_date_getter=lambda row: row.trade_date,
            force=force,
        )

    def sync_industry_daily(
        self,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key("get_industry_daily", normalized_codes, begin_date=begin, end_date=end)
        latest_date = self._load_latest_table_date(
            table_name=AD_INDUSTRY_DAILY_TABLE,
            code_field="index_code",
            date_field="trade_date",
            code_list=normalized_codes,
        )
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        return self._run_sync_job(
            task_name="get_industry_daily",
            scope_key=scope_key,
            target_table=AD_INDUSTRY_DAILY_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_industry_daily(normalized_codes, sync_start, end),
            save_rows=self.repository.save_industry_daily_rows,
            row_date_getter=lambda row: row.trade_date,
            force=force,
        )

    def _sync_info_payload(
        self,
        task_name: str,
        target_table: str,
        code_list: Sequence[str],
        begin_date: Optional[date | int | str],
        end_date: Optional[date | int | str],
        fetch_rows: Callable[[Sequence[str], Optional[date], Optional[date]], Iterable[InfoPayloadRow]],
        save_rows: Callable[[Iterable[InfoPayloadRow]], int],
        force: bool,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")

        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = self._build_code_scope_key(task_name, normalized_codes, begin_date=begin, end_date=end)
        latest_date = None
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        return self._run_sync_job(
            task_name=task_name,
            scope_key=scope_key,
            target_table=target_table,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: fetch_rows(normalized_codes, sync_start, end),
            save_rows=save_rows,
            row_date_getter=lambda row: row.report_date,
            force=force,
        )

    def _run_sync_job(
        self,
        task_name: str,
        scope_key: str,
        target_table: str,
        latest_date: Optional[date],
        fetch_rows: Callable[[Optional[date]], Iterable[object]],
        save_rows: Callable[[Iterable[object]], int],
        row_date_getter: Callable[[object], Optional[date]],
        force: bool,
        skip_if_success_today: bool = False,
    ) -> int:
        run_date = datetime.now().date()
        started_at = utcnow()

        if not force and skip_if_success_today and self.repository.has_successful_sync_today(task_name, scope_key, run_date):
            message = f"{task_name} 已在 {run_date} 同步成功，本次跳过。"
            logger.info(message)
            self._write_sync_log(
                task_name=task_name,
                scope_key=scope_key,
                run_date=run_date,
                status=SyncStatus.SKIPPED,
                target_table=target_table,
                start_date=latest_date,
                end_date=latest_date,
                row_count=0,
                message=message,
                started_at=started_at,
                finished_at=utcnow(),
            )
            return 0

        if self.sync_provider is None:
            message = f"{task_name} 未配置 sync_provider，无法执行同步。"
            logger.warning(message)
            self._write_sync_log(
                task_name=task_name,
                scope_key=scope_key,
                run_date=run_date,
                status=SyncStatus.FAILED,
                target_table=target_table,
                start_date=latest_date,
                end_date=latest_date,
                row_count=0,
                message=message,
                started_at=started_at,
                finished_at=utcnow(),
            )
            return 0

        stats = {
            "row_count": 0,
            "min_date": None,
            "max_date": latest_date,
        }

        def tracked_rows() -> Iterable[object]:
            for row in fetch_rows(latest_date):
                stats["row_count"] += 1
                row_date = row_date_getter(row)
                if row_date is not None:
                    if stats["min_date"] is None or row_date < stats["min_date"]:
                        stats["min_date"] = row_date
                    if stats["max_date"] is None or row_date > stats["max_date"]:
                        stats["max_date"] = row_date
                yield row

        logger.info(
            "Start sync task=%s scope=%s target_table=%s latest_date=%s",
            task_name,
            scope_key,
            target_table,
            latest_date,
        )

        try:
            inserted_count = save_rows(tracked_rows())
        except Exception as exc:
            message = f"{task_name} 同步失败: {exc}"
            logger.exception(message)
            self._write_sync_log(
                task_name=task_name,
                scope_key=scope_key,
                run_date=run_date,
                status=SyncStatus.FAILED,
                target_table=target_table,
                start_date=latest_date or stats["min_date"],
                end_date=stats["max_date"],
                row_count=stats["row_count"],
                message=message,
                started_at=started_at,
                finished_at=utcnow(),
            )
            raise

        message = (
            f"{task_name} 同步完成，"
            f"latest_date={latest_date}, inserted_rows={inserted_count}, observed_rows={stats['row_count']}"
        )
        logger.info(message)
        self._write_sync_log(
            task_name=task_name,
            scope_key=scope_key,
            run_date=run_date,
            status=SyncStatus.SUCCESS,
            target_table=target_table,
            start_date=latest_date or stats["min_date"],
            end_date=stats["max_date"],
            row_count=inserted_count,
            message=message,
            started_at=started_at,
            finished_at=utcnow(),
        )
        return inserted_count

    def _provider_fetch_stock_basic(self, code_list: Sequence[str], start_date: Optional[date]):
        return self.sync_provider.fetch_stock_basic(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
        )

    def _provider_fetch_bj_code_mapping(self):
        return self.sync_provider.fetch_bj_code_mapping()  # type: ignore[union-attr]

    def _provider_fetch_history_stock_status(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_history_stock_status(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_balance_sheet(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_balance_sheet(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_cash_flow(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_cash_flow(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_income(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_income(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_profit_express(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_profit_express(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_profit_notice(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_profit_notice(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_fund_share(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_fund_share(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_fund_iopv(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_fund_iopv(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_kzz_issuance(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_kzz_issuance(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_kzz_share(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_kzz_share(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_kzz_suspend(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_kzz_suspend(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_option_basic_info(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_option_basic_info(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_option_std_ctr_specs(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_option_std_ctr_specs(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_option_mon_ctr_specs(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_option_mon_ctr_specs(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_treasury_yield(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_treasury_yield(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_kzz_conv_change(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_kzz_conv_change(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_kzz_corr(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_kzz_corr(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_kzz_call_explanation(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_kzz_call_explanation(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_kzz_put_explanation(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_kzz_put_explanation(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_kzz_put_call_item(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_kzz_put_call_item(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_kzz_put(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_kzz_put(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_kzz_call(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_kzz_call(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_kzz_conv(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_kzz_conv(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_block_trading(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_block_trading(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_long_hu_bang(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_long_hu_bang(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_margin_detail(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_margin_detail(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_margin_summary(
        self,
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_margin_summary(  # type: ignore[union-attr]
            code_list=None,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_share_holder(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_share_holder(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_holder_num(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_holder_num(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_equity_structure(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_equity_structure(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_equity_pledge_freeze(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_equity_pledge_freeze(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_equity_restricted(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_equity_restricted(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_dividend(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_dividend(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_right_issue(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_right_issue(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_index_constituent(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_index_constituent(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_index_weight(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_index_weight(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_industry_base_info(self):
        return self.sync_provider.fetch_industry_base_info()  # type: ignore[union-attr]

    def _provider_fetch_industry_constituent(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_industry_constituent(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_industry_weight(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_industry_weight(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_industry_daily(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_industry_daily(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _load_latest_table_date(
        self,
        table_name: str,
        code_field: str,
        date_field: str,
        code_list: Sequence[str],
    ):
        return self.repository.load_latest_date_by_codes(
            table_name=table_name,
            code_field=code_field,
            date_field=date_field,
            code_list=list(code_list),
        )

    def _write_sync_log(
        self,
        task_name: str,
        scope_key: str,
        run_date: date,
        status: str,
        target_table: str,
        start_date: Optional[date],
        end_date: Optional[date],
        row_count: int,
        message: str,
        started_at: datetime,
        finished_at: datetime,
    ) -> None:
        try:
            self.repository.insert_sync_log(
                SyncTaskLogRow(
                    task_name=task_name,
                    scope_key=scope_key,
                    run_date=run_date,
                    status=status,
                    target_table=target_table,
                    start_date=start_date,
                    end_date=end_date,
                    row_count=max(0, int(row_count)),
                    message=message,
                    started_at=started_at,
                    finished_at=finished_at,
                )
            )
        except Exception:
            logger.exception("写入同步日志失败 task=%s scope=%s status=%s", task_name, scope_key, status)

    @staticmethod
    def _build_code_scope_key(
        task_name: str,
        code_list: Sequence[str],
        begin_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> str:
        digest = hashlib.sha1(",".join(sorted(code_list)).encode("utf-8")).hexdigest()[:12]
        begin_text = begin_date.isoformat() if begin_date is not None else ""
        end_text = end_date.isoformat() if end_date is not None else ""
        return (
            f"task={task_name}|code_count={len(code_list)}|codes_sha1={digest}"
            f"|begin_date={begin_text}|end_date={end_text}"
        )

    @staticmethod
    def _resolve_incremental_start_date(
        latest_date: Optional[date],
        requested_begin_date: Optional[date],
    ) -> Optional[date]:
        if latest_date is None:
            return requested_begin_date
        next_date = latest_date + timedelta(days=1)
        if requested_begin_date is None:
            return next_date
        return max(next_date, requested_begin_date)

    @staticmethod
    def _validate_optional_date_range(begin_date: Optional[date], end_date: Optional[date]) -> None:
        if begin_date is not None and end_date is not None and begin_date > end_date:
            raise BaseDataParameterError("begin_date 不能大于 end_date。")

    @staticmethod
    def _validate_local_path(local_path: str) -> None:
        if not isinstance(local_path, str):
            raise BaseDataParameterError("local_path 必须是字符串。")

    def _skip_if_empty_incremental_window(
        self,
        task_name: str,
        scope_key: str,
        target_table: str,
        latest_date: Optional[date],
        sync_start: Optional[date],
        end_date: Optional[date],
        force: bool,
    ) -> bool:
        if force:
            return False
        if sync_start is None or end_date is None:
            return False
        if sync_start <= end_date:
            return False

        run_date = datetime.now().date()
        started_at = utcnow()
        message = (
            f"{task_name} 增量窗口为空，跳过。"
            f" latest_date={latest_date} sync_start={sync_start} end_date={end_date}"
        )
        logger.info(message)
        self._write_sync_log(
            task_name=task_name,
            scope_key=scope_key,
            run_date=run_date,
            status=SyncStatus.SKIPPED,
            target_table=target_table,
            start_date=sync_start,
            end_date=end_date,
            row_count=0,
            message=message,
            started_at=started_at,
            finished_at=utcnow(),
        )
        return True

    @staticmethod
    def _coerce_optional_date_text(value: Optional[str]) -> Optional[date]:
        if value is None:
            return None
        try:
            return to_ch_date(value)
        except Exception:
            return None


__all__ = [
    "InfoData",
    "InfoDataSyncProvider",
]
