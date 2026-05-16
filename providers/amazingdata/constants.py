#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AmazingData SDK 常量定义.

基于《AmazingData开发手册.docx》附录 4.1 / 4.2 整理。

这份文件先解决两件事：
1. 把手册里已经明确给出的枚举值、代码表和值域先固定下来。
2. 把行情返回结构里会反复出现的字段名先标准化，方便后续 SDK 包装层直接复用。

注意：
- 这里尽量保留手册原始枚举值，避免对外 SDK 调用时再做额外转换。
- `Period` 在附录里只给出了 `Period.xxx.value` 的引用形式，没有列出底层整数值。
  因此这里先定义“逻辑周期名”和“文档引用名”，真实整数值应在接入官方 SDK 时读取。
- 文档转文本后存在少量 OCR/排版噪声，例如 `ask _volume1`、`pre_settle:`。
  本文件会提供字段别名映射，内部统一使用清洗后的规范字段名。
"""

from __future__ import annotations


class SecurityType:
    """`security_type` 常量.

    手册附录 4.1.1 ~ 4.1.3 中的原始枚举值。
    后续 SDK 方法入参应直接使用这些值，不再做二次翻译。
    """

    # 股票
    EXTRA_STOCK_A = "EXTRA_STOCK_A"
    SH_A = "SH_A"
    SZ_A = "SZ_A"
    BJ_A = "BJ_A"
    EXTRA_STOCK_A_SH_SZ = "EXTRA_STOCK_A_SH_SZ"

    # 指数
    EXTRA_INDEX_A_SH_SZ = "EXTRA_INDEX_A_SH_SZ"
    EXTRA_INDEX_A = "EXTRA_INDEX_A"
    SH_INDEX = "SH_INDEX"
    SZ_INDEX = "SZ_INDEX"
    BJ_INDEX = "BJ_INDEX"

    # ETF
    SH_ETF = "SH_ETF"
    SZ_ETF = "SZ_ETF"
    EXTRA_ETF = "EXTRA_ETF"

    # 可转债
    SH_KZZ = "SH_KZZ"
    SZ_KZZ = "SZ_KZZ"
    EXTRA_KZZ = "EXTRA_KZZ"

    # 港股通
    SH_HKT = "SH_HKT"
    SZ_HKT = "SZ_HKT"
    EXTRA_HKT = "EXTRA_HKT"

    # 逆回购
    SH_GLRA = "SH_GLRA"
    SZ_GLRA = "SZ_GLRA"
    EXTRA_GLRA = "EXTRA_GLRA"

    # 期货
    EXTRA_FUTURE = "EXTRA_FUTURE"
    ZJ_FUTURE = "ZJ_FUTURE"
    SQ_FUTURE = "SQ_FUTURE"
    DS_FUTURE = "DS_FUTURE"
    ZS_FUTURE = "ZS_FUTURE"
    SN_FUTURE = "SN_FUTURE"

    # 期权
    EXTRA_ETF_OP = "EXTRA_ETF_OP"
    SH_OPTION = "SH_OPTION"
    SZ_OPTION = "SZ_OPTION"


# 股票同步第一阶段最常用的 security_type。
STOCK_SYNC_SECURITY_TYPES = (
    SecurityType.EXTRA_STOCK_A,
    SecurityType.SH_A,
    SecurityType.SZ_A,
    SecurityType.BJ_A,
    SecurityType.EXTRA_STOCK_A_SH_SZ,
)

# 行情、财务和权息场景里常见的资产分类集合。
INDEX_SECURITY_TYPES = (
    SecurityType.EXTRA_INDEX_A_SH_SZ,
    SecurityType.EXTRA_INDEX_A,
    SecurityType.SH_INDEX,
    SecurityType.SZ_INDEX,
    SecurityType.BJ_INDEX,
)

ETF_SECURITY_TYPES = (
    SecurityType.SH_ETF,
    SecurityType.SZ_ETF,
    SecurityType.EXTRA_ETF,
)

KZZ_SECURITY_TYPES = (
    SecurityType.SH_KZZ,
    SecurityType.SZ_KZZ,
    SecurityType.EXTRA_KZZ,
)

OPTION_SECURITY_TYPES = (
    SecurityType.EXTRA_ETF_OP,
    SecurityType.SH_OPTION,
    SecurityType.SZ_OPTION,
)


# 少量 security_type 在文档转文本后出现了空格等噪声，这里统一做兼容。
SECURITY_TYPE_ALIASES = {
    "EXTRA_ GLRA": SecurityType.EXTRA_GLRA,
}


class Market:
    """`market` 常量.

    这些值直接对应手册附录 4.1.4 的市场类型。
    如果接口明确要求 `market` 入参，应只接受这里的原始枚举值。
    """

    SH = "SH"
    SZ = "SZ"
    BJ = "BJ"
    SHF = "SHF"
    CFE = "CFE"
    DCE = "DCE"
    CZC = "CZC"
    INE = "INE"
    SHN = "SHN"
    SZN = "SZN"
    HK = "HK"


# 股票同步当前主要覆盖的交易所。
STOCK_EXCHANGE_MARKETS = (
    Market.SH,
    Market.SZ,
    Market.BJ,
)


class SecurityStatus:
    """`security_status` 标志位常量.

    手册附录 4.1.6 描述的是“状态标志”，不是互斥单选枚举。
    后续如果接口返回多个状态，包装层需要支持多标志并存，不要假设只有一个状态码。
    """

    HALT = 1
    EX_RIGHTS = 2
    EX_DIVIDEND = 3
    RISK_WARNING = 4
    DELISTING_REORGANIZATION = 5
    FIRST_LISTING_DAY = 6
    REFINANCING = 7
    FIRST_DAY_AFTER_RESUME = 8
    ONLINE_VOTING = 9
    ADDITIONAL_SHARES_LISTED = 10
    CONTRACT_ADJUSTMENT = 11
    AGREEMENT_TRANSFER_AFTER_SUSPENSION = 12
    DOUBLE_TO_SINGLE_ADJUSTMENT = 13
    SPECIAL_BOND_TRANSFER = 14
    EARLY_LISTING_STAGE = 15
    FIRST_DAY_OF_DELISTING_REORGANIZATION = 16
    NEWLY_ADDED_SHARES = 57
    ELIGIBLE_AS_MARGIN_COLLATERAL = 62
    MARGIN_FINANCING_TARGET = 63
    SECURITIES_LENDING_TARGET = 64
    ELIGIBLE_FOR_PLEDGE = 65
    CROSS_MARKET = 66
    CONVERSION_OR_PUT_PERIOD = 67


# 状态码到中文说明的映射，便于日志和调试输出。
SECURITY_STATUS_LABELS = {
    SecurityStatus.HALT: "停牌",
    SecurityStatus.EX_RIGHTS: "除权",
    SecurityStatus.EX_DIVIDEND: "除息",
    SecurityStatus.RISK_WARNING: "风险警示",
    SecurityStatus.DELISTING_REORGANIZATION: "退市整理期",
    SecurityStatus.FIRST_LISTING_DAY: "上市首日",
    SecurityStatus.REFINANCING: "公司再融资",
    SecurityStatus.FIRST_DAY_AFTER_RESUME: "恢复上市首日",
    SecurityStatus.ONLINE_VOTING: "网络投票",
    SecurityStatus.ADDITIONAL_SHARES_LISTED: "增发股份上市",
    SecurityStatus.CONTRACT_ADJUSTMENT: "合约调整",
    SecurityStatus.AGREEMENT_TRANSFER_AFTER_SUSPENSION: "暂停上市后协议转让",
    SecurityStatus.DOUBLE_TO_SINGLE_ADJUSTMENT: "实施双转单调整",
    SecurityStatus.SPECIAL_BOND_TRANSFER: "特定债券转让",
    SecurityStatus.EARLY_LISTING_STAGE: "上市初期",
    SecurityStatus.FIRST_DAY_OF_DELISTING_REORGANIZATION: "退市整理期首日",
    SecurityStatus.NEWLY_ADDED_SHARES: "新增股份",
    SecurityStatus.ELIGIBLE_AS_MARGIN_COLLATERAL: "是否可作为融资融券可充抵保证金证券",
    SecurityStatus.MARGIN_FINANCING_TARGET: "是否为融资标的",
    SecurityStatus.SECURITIES_LENDING_TARGET: "是否为融券标的",
    SecurityStatus.ELIGIBLE_FOR_PLEDGE: "是否可质押入库",
    SecurityStatus.CROSS_MARKET: "是否跨市场",
    SecurityStatus.CONVERSION_OR_PUT_PERIOD: "是否处于转股回售期",
}


class PeriodName:
    """逻辑周期名.

    手册附录 4.1.7 只列出了 `Period.min1.value` 这类 SDK 属性路径，
    没有给出底层整数值，因此这里先固定逻辑名，后续由适配层去读取真实 `.value`。
    """

    MIN1 = "min1"
    MIN3 = "min3"
    MIN5 = "min5"
    MIN10 = "min10"
    MIN15 = "min15"
    MIN30 = "min30"
    MIN60 = "min60"
    MIN120 = "min120"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    SEASON = "season"
    YEAR = "year"


# 逻辑周期名到手册引用名的映射。
# 后续如果接入官方 SDK，可以通过这些 key 去解析 `ad.constant.Period.<name>.value`。
PERIOD_DOC_REFS = {
    PeriodName.MIN1: "Period.min1.value",
    PeriodName.MIN3: "Period.min3.value",
    PeriodName.MIN5: "Period.min5.value",
    PeriodName.MIN10: "Period.min10.value",
    PeriodName.MIN15: "Period.min15.value",
    PeriodName.MIN30: "Period.min30.value",
    PeriodName.MIN60: "Period.min60.value",
    PeriodName.MIN120: "Period.min120.value",
    PeriodName.DAY: "Period.day.value",
    PeriodName.WEEK: "Period.week.value",
    PeriodName.MONTH: "Period.month.value",
    PeriodName.SEASON: "Period.season.value",
    PeriodName.YEAR: "Period.year.value",
}


# 股票同步第一阶段用得到的最小周期集合。
STOCK_SYNC_PERIODS = (
    PeriodName.MIN1,
    PeriodName.DAY,
    PeriodName.WEEK,
    PeriodName.MONTH,
)


class SnapshotKind:
    """历史快照的结构类别."""

    SNAPSHOT = "snapshot"
    SNAPSHOT_INDEX = "snapshot_index"
    SNAPSHOT_OPTION = "snapshot_option"
    SNAPSHOT_HKT = "snapshot_hkt"
    SNAPSHOT_FUTURE = "snapshot_future"


class ReportType:
    """财务报告期代码.

    手册附录 4.1.8 中的 `REPORT_TYPE` 与报告期月份对应。
    """

    MARCH = 1
    JUNE = 2
    SEPTEMBER = 3
    DECEMBER = 4


REPORT_TYPE_TO_MONTH = {
    ReportType.MARCH: 3,
    ReportType.JUNE: 6,
    ReportType.SEPTEMBER: 9,
    ReportType.DECEMBER: 12,
}


class FactorType:
    """复权因子类型.

    这不是手册里的原始枚举，但属于我们内部分支判断需要的常量。
    数据库存储改为前复权 / 后复权分表，不再把 `factor_type` 写入库表。
    """

    ADJ = "adj"
    BACKWARD = "backward"


class SyncStatus:
    """同步任务状态."""

    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


# 报表类型代码表，原样保留手册中的整数编码。
# 这组映射未来会被财务接口的 schema 和过滤条件复用。
STATEMENT_TYPE_LABELS = {
    1: "合并报表",
    2: "合并报表(单季度)",
    3: "合并报表(单季度调整)",
    4: "合并报表(调整)",
    5: "合并报表(更正前)",
    6: "母公司报表",
    7: "母公司报表(单季度)",
    8: "母公司报表(单季度调整)",
    9: "母公司报表(调整)",
    10: "母公司报表(更正前)",
    11: "合并报表(未公开)",
    12: "合并报表(调整未公开)",
    13: "合并报表(单季度未公开)",
    14: "合并报表(单季度调整未公开)",
    15: "母公司报表(未公开)",
    16: "母公司报表(调整未公开)",
    17: "母公司报表(单季度未公开)",
    18: "母公司报表(单季度调整未公开)",
    19: "合并报表(调整借壳前)",
    20: "合并调整",
    21: "合并报表(单季度借壳前)",
    22: "合并报表(单季度调整借壳前)",
    23: "母公司报表(借壳前)",
    24: "母公司报表(调整借壳前)",
    25: "母公司报表(单季度借壳前)",
    26: "母公司报表(单季度调整借壳前)",
    27: "合并报表(第一次更正)",
    28: "合并报表(第二次更正)",
    29: "合并调整(第一次更正)",
    30: "合并报表(单月度)",
    31: "合并调整(第二次更正)",
    32: "母公司调整(第二次更正)",
    33: "母公司调整(第一次更正)",
    34: "母公司报表(第二次更正)",
    35: "母公司报表(第一次更正)",
    36: "合并报表(第三次更正)",
    37: "合并调整(第三次更正)",
    38: "母公司报表(第三次更正)",
    39: "母公司调整(第三次更正)",
    40: "母公司报表(单月度)",
    41: "合并报表(业绩快报)",
    42: "合并调整(第一次)",
    43: "合并调整(第二次)",
    44: "合并调整(第三次)",
    45: "合并报表(第四次更正)",
    46: "合并调整(第四次更正)",
    47: "母公司报表(第四次更正)",
    48: "母公司调整(第四次更正)",
    50: "合并调整(更正前)",
    51: "合并报表(下半年报)",
    60: "母公司调整(更正前)",
    70: "合并报表(借壳前)",
    80: "合并报表(预测)",
    81: "合并报表(公司预测)",
    90: "项目资产报表",
    91: "合并报表(日历年)",
}


# 分红进度代码表，后续 `get_dividend` 结果可以直接复用。
DIVIDEND_PROGRESS_LABELS = {
    1: "董事会预案",
    2: "股东大会通过",
    3: "实施",
    4: "未通过",
    12: "停止实施",
    17: "股东提议",
    19: "董事会预案预披露",
}


# 文档 OCR/转文本后出现的字段名噪声统一在这里清洗。
# 后续无论是服务端原始字段，还是文档生成的 schema，都应优先转换到规范字段名。
FIELD_NAME_ALIASES = {
    "ask _volume1": "ask_volume1",
    "ask _volume2": "ask_volume2",
    "ask _volume3": "ask_volume3",
    "ask _volume4": "ask_volume4",
    "ask _volume5": "ask_volume5",
    "bid _volume1": "bid_volume1",
    "bid _volume2": "bid_volume2",
    "bid _volume3": "bid_volume3",
    "bid _volume4": "bid_volume4",
    "bid _volume5": "bid_volume5",
    "pre_settle:": "pre_settle",
    "underlying_security_cod": "underlying_security_code",
}


# 五档行情字段是一类高复用字段，单独抽出来，避免 Snapshot/SnapshotOption 等结构重复维护。
ASK_PRICE_FIELDS = (
    "ask_price1",
    "ask_price2",
    "ask_price3",
    "ask_price4",
    "ask_price5",
)

ASK_VOLUME_FIELDS = (
    "ask_volume1",
    "ask_volume2",
    "ask_volume3",
    "ask_volume4",
    "ask_volume5",
)

BID_PRICE_FIELDS = (
    "bid_price1",
    "bid_price2",
    "bid_price3",
    "bid_price4",
    "bid_price5",
)

BID_VOLUME_FIELDS = (
    "bid_volume1",
    "bid_volume2",
    "bid_volume3",
    "bid_volume4",
    "bid_volume5",
)

ORDER_BOOK_FIELDS = ASK_PRICE_FIELDS + ASK_VOLUME_FIELDS + BID_PRICE_FIELDS + BID_VOLUME_FIELDS


# Level-1 股票/ETF/可转债快照字段。
SNAPSHOT_FIELDS = (
    "code",
    "trade_time",
    "pre_close",
    "last",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "num_trades",
    "high_limited",
    "low_limited",
) + ORDER_BOOK_FIELDS + (
    "iopv",
    "trading_phase_code",
)


# ETF 期权快照字段。
SNAPSHOT_OPTION_FIELDS = (
    "code",
    "trade_time",
    "trading_phase_code",
    "total_long_position",
    "volume",
    "amount",
    "pre_close",
    "pre_settle",
    "auction_price",
    "auction_volume",
    "last",
    "open",
    "high",
    "low",
    "close",
    "settle",
    "high_limited",
    "low_limited",
) + ORDER_BOOK_FIELDS + (
    "contract_type",
    "expire_date",
    "underlying_security_code",
    "exercise_price",
)


# 期货快照字段。虽然股票同步首阶段不会直接使用，但后续 MarketData 结构会共用这套定义。
SNAPSHOT_FUTURE_FIELDS = (
    "code",
    "trade_time",
    "action_day",
    "trading_day",
    "pre_close",
    "pre_settle",
    "pre_open_interest",
    "open_interest",
    "last",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "high_limited",
    "low_limited",
) + ORDER_BOOK_FIELDS + (
    "average_price",
    "settle",
)


# 指数快照字段。指数没有五档盘口和 iopv。
SNAPSHOT_INDEX_FIELDS = (
    "code",
    "trade_time",
    "last",
    "pre_close",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
)


# 港股通快照字段。
SNAPSHOT_HKT_FIELDS = (
    "code",
    "trade_time",
    "pre_close",
    "last",
    "high",
    "low",
    "volume",
    "amount",
    "nominal_price",
    "ref_price",
    "bid_price_limit_up",
    "bid_price_limit_down",
    "offer_price_limit_up",
    "offer_price_limit_down",
    "high_limited",
    "low_limited",
) + ORDER_BOOK_FIELDS + (
    "trading_phase_code",
)


SNAPSHOT_KIND_TO_FIELDS = {
    SnapshotKind.SNAPSHOT: SNAPSHOT_FIELDS,
    SnapshotKind.SNAPSHOT_INDEX: SNAPSHOT_INDEX_FIELDS,
    SnapshotKind.SNAPSHOT_OPTION: SNAPSHOT_OPTION_FIELDS,
    SnapshotKind.SNAPSHOT_HKT: SNAPSHOT_HKT_FIELDS,
    SnapshotKind.SNAPSHOT_FUTURE: SNAPSHOT_FUTURE_FIELDS,
}


# K 线字段是最基础的一组行情结构，日线和分钟线都可以复用这套字段定义。
KLINE_FIELDS = (
    "code",
    "trade_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
)


# `get_stock_basic` 返回字段。
# 这里保留手册里的原始大写字段名，原因是这类基础信息通常会直接映射服务端列名，
# 后续无论是 DataFrame 返回还是落库字段映射，都更适合先以原始列名为准。
STOCK_BASIC_FIELDS = (
    "MARKET_CODE",
    "SECURITY_NAME",
    "COMP_NAME",
    "PINYIN",
    "COMP_NAME_ENG",
    "LISTDATE",
    "DELISTDATE",
    "LISTPLATE_NAME",
    "COMP_SNAME_ENG",
    "IS_LISTED",
)


# `get_stock_basic` 的数据库字段名统一转成小写 snake_case。
STOCK_BASIC_FIELD_DB_MAPPING = {
    "MARKET_CODE": "market_code",
    "SECURITY_NAME": "security_name",
    "COMP_NAME": "comp_name",
    "PINYIN": "pinyin",
    "COMP_NAME_ENG": "comp_name_eng",
    "LISTDATE": "list_date",
    "DELISTDATE": "delist_date",
    "LISTPLATE_NAME": "listplate_name",
    "COMP_SNAME_ENG": "comp_sname_eng",
    "IS_LISTED": "is_listed",
}

STOCK_BASIC_DB_FIELDS = tuple(STOCK_BASIC_FIELD_DB_MAPPING[field] for field in STOCK_BASIC_FIELDS)


# `get_stock_basic` 字段类型说明。
# 类型名先使用轻量字符串，而不是 Python/SQL 类型对象，方便后续同时服务：
# - SDK 返回 schema 文档
# - DataFrame 字段校验
# - ClickHouse 建表映射
STOCK_BASIC_FIELD_TYPES = {
    "MARKET_CODE": "string",
    "SECURITY_NAME": "string",
    "COMP_NAME": "string",
    "PINYIN": "string",
    "COMP_NAME_ENG": "string",
    "LISTDATE": "int",
    "DELISTDATE": "int",
    "LISTPLATE_NAME": "string",
    "COMP_SNAME_ENG": "string",
    "IS_LISTED": "int",
}


STOCK_BASIC_DB_FIELD_TYPES = {
    "snapshot_date": "date",
    "market_code": "string",
    "security_name": "string",
    "comp_name": "string",
    "pinyin": "string",
    "comp_name_eng": "string",
    "list_date": "int",
    "delist_date": "int",
    "listplate_name": "string",
    "comp_sname_eng": "string",
    "is_listed": "int",
}


# `get_stock_basic` 字段中文说明。
STOCK_BASIC_FIELD_LABELS = {
    "MARKET_CODE": "证券代码",
    "SECURITY_NAME": "证券简称",
    "COMP_NAME": "证券中文名称",
    "PINYIN": "中文拼音简称",
    "COMP_NAME_ENG": "证券英文名称",
    "LISTDATE": "上市日期",
    "DELISTDATE": "退市日期",
    "LISTPLATE_NAME": "上市板块名称",
    "COMP_SNAME_ENG": "英文名称缩写",
    "IS_LISTED": "上市状态",
}


# `get_stock_basic.IS_LISTED` 枚举说明。
STOCK_BASIC_IS_LISTED_LABELS = {
    1: "上市交易",
    3: "终止上市",
}


# `get_history_stock_status` 返回字段。
# 这里额外保留 `MARKET_CODE`，虽然当前补充字段列表里未显式给出，
# 但多证券查询场景必须要有证券代码列才能区分不同证券的历史状态记录。
HISTORY_STOCK_STATUS_FIELDS = (
    "MARKET_CODE",
    "TRADE_DATE",
    "PRECLOSE",
    "HIGH_LIMITED",
    "LOW_LIMITED",
    "PRICE_HIGH_LMT_RATE",
    "PRICE_LOW_LMT_RATE",
    "IS_ST_SEC",
    "IS_SUSP_SEC",
    "IS_WD_SEC",
    "IS_XR_SEC",
)

HISTORY_STOCK_STATUS_FIELD_DB_MAPPING = {
    "MARKET_CODE": "market_code",
    "TRADE_DATE": "trade_date",
    "PRECLOSE": "preclose",
    "HIGH_LIMITED": "high_limited",
    "LOW_LIMITED": "low_limited",
    "PRICE_HIGH_LMT_RATE": "price_high_lmt_rate",
    "PRICE_LOW_LMT_RATE": "price_low_lmt_rate",
    "IS_ST_SEC": "is_st_sec",
    "IS_SUSP_SEC": "is_susp_sec",
    "IS_WD_SEC": "is_wd_sec",
    "IS_XR_SEC": "is_xr_sec",
}

HISTORY_STOCK_STATUS_DB_FIELDS = tuple(
    HISTORY_STOCK_STATUS_FIELD_DB_MAPPING[field] for field in HISTORY_STOCK_STATUS_FIELDS
)

HISTORY_STOCK_STATUS_FIELD_TYPES = {
    "MARKET_CODE": "string",
    "TRADE_DATE": "string",
    "PRECLOSE": "float",
    "HIGH_LIMITED": "float",
    "LOW_LIMITED": "float",
    "PRICE_HIGH_LMT_RATE": "float",
    "PRICE_LOW_LMT_RATE": "float",
    "IS_ST_SEC": "string",
    "IS_SUSP_SEC": "string",
    "IS_WD_SEC": "string",
    "IS_XR_SEC": "string",
}

HISTORY_STOCK_STATUS_DB_FIELD_TYPES = {
    "trade_date": "date",
    "market_code": "string",
    "preclose": "float",
    "high_limited": "float",
    "low_limited": "float",
    "price_high_lmt_rate": "float",
    "price_low_lmt_rate": "float",
    "is_st_sec": "string",
    "is_susp_sec": "string",
    "is_wd_sec": "string",
    "is_xr_sec": "string",
}

HISTORY_STOCK_STATUS_FIELD_LABELS = {
    "MARKET_CODE": "证券代码",
    "TRADE_DATE": "日期",
    "PRECLOSE": "前收价",
    "HIGH_LIMITED": "涨停价",
    "LOW_LIMITED": "跌停价",
    "PRICE_HIGH_LMT_RATE": "涨停价上限",
    "PRICE_LOW_LMT_RATE": "跌停价下限",
    "IS_ST_SEC": "是否 ST",
    "IS_SUSP_SEC": "是否停牌",
    "IS_WD_SEC": "是否除息",
    "IS_XR_SEC": "是否除权",
}


# 股票同步第一阶段常用字段集合。
STOCK_SYNC_FIELD_SETS = {
    "history_stock_status": HISTORY_STOCK_STATUS_FIELDS,
    "snapshot": SNAPSHOT_FIELDS,
    "snapshot_index": SNAPSHOT_INDEX_FIELDS,
    "kline": KLINE_FIELDS,
    "stock_basic": STOCK_BASIC_FIELDS,
}


__all__ = [
    "ASK_PRICE_FIELDS",
    "ASK_VOLUME_FIELDS",
    "BID_PRICE_FIELDS",
    "BID_VOLUME_FIELDS",
    "DIVIDEND_PROGRESS_LABELS",
    "ETF_SECURITY_TYPES",
    "FIELD_NAME_ALIASES",
    "FactorType",
    "HISTORY_STOCK_STATUS_DB_FIELDS",
    "HISTORY_STOCK_STATUS_DB_FIELD_TYPES",
    "HISTORY_STOCK_STATUS_FIELD_DB_MAPPING",
    "HISTORY_STOCK_STATUS_FIELD_LABELS",
    "HISTORY_STOCK_STATUS_FIELD_TYPES",
    "HISTORY_STOCK_STATUS_FIELDS",
    "INDEX_SECURITY_TYPES",
    "KLINE_FIELDS",
    "KZZ_SECURITY_TYPES",
    "Market",
    "OPTION_SECURITY_TYPES",
    "ORDER_BOOK_FIELDS",
    "PERIOD_DOC_REFS",
    "PeriodName",
    "REPORT_TYPE_TO_MONTH",
    "ReportType",
    "SECURITY_TYPE_ALIASES",
    "SECURITY_STATUS_LABELS",
    "SNAPSHOT_KIND_TO_FIELDS",
    "SnapshotKind",
    "SNAPSHOT_FIELDS",
    "SNAPSHOT_FUTURE_FIELDS",
    "SNAPSHOT_HKT_FIELDS",
    "SNAPSHOT_INDEX_FIELDS",
    "SNAPSHOT_OPTION_FIELDS",
    "STATEMENT_TYPE_LABELS",
    "STOCK_BASIC_DB_FIELDS",
    "STOCK_BASIC_DB_FIELD_TYPES",
    "STOCK_BASIC_FIELD_DB_MAPPING",
    "STOCK_BASIC_FIELDS",
    "STOCK_BASIC_FIELD_LABELS",
    "STOCK_BASIC_FIELD_TYPES",
    "STOCK_BASIC_IS_LISTED_LABELS",
    "STOCK_EXCHANGE_MARKETS",
    "STOCK_SYNC_FIELD_SETS",
    "STOCK_SYNC_PERIODS",
    "STOCK_SYNC_SECURITY_TYPES",
    "SyncStatus",
    "SecurityStatus",
    "SecurityType",
]
