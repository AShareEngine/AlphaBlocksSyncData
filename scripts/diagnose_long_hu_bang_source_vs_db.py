#!/usr/bin/env python3
"""Compare AmazingData long_hu_bang source rows with ClickHouse rows.

This is a read-only diagnostic script. It helps answer whether a long_hu_bang
gap comes from the upstream SDK response, our SDK-to-row mapping, or the
ClickHouse persisted table.

Example:
  python sync_data_system/scripts/diagnose_long_hu_bang_source_vs_db.py \
    --code 601360.SH --date 2023-02-27
"""

from __future__ import annotations

import argparse
import math
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
for path in (str(REPO_ROOT), str(SRC_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

import pandas as pd

from sync_data_system.providers.amazingdata.provider import (
    AmazingDataSDKConfig,
    AmazingDataSDKProvider,
    _as_date,
    _as_float,
    _as_int,
    _as_str,
    _count_sdk_result_rows,
    _iter_records_from_sdk_result,
    _record_get,
)
from sync_data_system.clickhouse_client import ClickHouseConfig, create_clickhouse_client


DEFAULT_RUNTIME = REPO_ROOT / "config" / "runtime.local.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare AmazingData get_long_hu_bang source data with ClickHouse ad_long_hu_bang.",
    )
    parser.add_argument("--code", default="601360.SH", help="Market code, for example 601360.SH")
    parser.add_argument("--date", default="2023-02-27", help="Trade date in YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--runtime", default=str(DEFAULT_RUNTIME), help="runtime.local.yaml path")
    parser.add_argument("--max-rows", type=int, default=80, help="Max rows to print per section")
    parser.add_argument("--skip-db", action="store_true", help="Only print SDK raw and mapped rows")
    parser.add_argument(
        "--close-sdk",
        action="store_true",
        help="Call provider.close() before exit. Disabled by default because some SDK environments hang during logout.",
    )
    return parser.parse_args()


def parse_date(value: str) -> date:
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    return datetime.strptime(text, "%Y-%m-%d").date()


def yyyymmdd(value: date) -> int:
    return int(value.strftime("%Y%m%d"))


def flatten_sdk_result(obj: Any) -> pd.DataFrame:
    if isinstance(obj, pd.DataFrame):
        return obj.reset_index(drop=False)
    if isinstance(obj, dict):
        frames: list[pd.DataFrame] = []
        for key, value in obj.items():
            if value is None:
                continue
            if isinstance(value, pd.DataFrame):
                frame = value.reset_index(drop=False).copy()
                if not any(col.lower() in {"market_code", "marketcode", "code"} for col in frame.columns):
                    frame.insert(0, "__dict_key__", key)
                frames.append(frame)
                continue
            if isinstance(value, dict):
                child = flatten_sdk_result(value)
                if not child.empty:
                    child.insert(0, "__dict_key__", key)
                    frames.append(child)
                continue
            frames.append(pd.DataFrame([{"__dict_key__": key, "__value__": repr(value)}]))
        if frames:
            return pd.concat(frames, ignore_index=True, sort=False)
        return pd.DataFrame()
    if obj is None:
        return pd.DataFrame()
    return pd.DataFrame([{"__result_type__": type(obj).__name__, "__repr__": repr(obj)}])


def fetch_sdk_raw(provider: AmazingDataSDKProvider, code: str, trade_date: date) -> tuple[Any, pd.DataFrame]:
    kwargs = {
        "code_list": [code],
        "local_path": provider.config.local_path,
        "is_local": False,
        "begin_date": yyyymmdd(trade_date),
        "end_date": yyyymmdd(trade_date),
    }
    raw = provider.session.info.get_long_hu_bang(**kwargs)
    return raw, flatten_sdk_result(raw)


def map_sdk_result(raw_result: Any) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for record in _iter_records_from_sdk_result(raw_result, action="get_long_hu_bang"):
        market_code = _as_str(_record_get(record, "MARKETCODE", "MARKET_CODE", "market_code", "marketcode"))
        trade_date = _as_date(_record_get(record, "TRADEDATE", "TRADE_DATE", "trade_date"))
        if not market_code or trade_date is None:
            continue
        rows.append(
            {
                "market_code": market_code,
                "trade_date": trade_date,
                "security_name": _as_str(_record_get(record, "SECURITYNAME", "security_name")),
                "reason_type": _as_str(_record_get(record, "REASONTYPE", "reason_type")),
                "reason_type_name": _as_str(_record_get(record, "REASON_TYPE_NAME", "reason_type_name")),
                "change_range": _as_float(_record_get(record, "CHANGE_RANGE", "change_range")),
                "trader_name": _as_str(_record_get(record, "TRADER_NAME", "trader_name")),
                "buy_amount": _as_float(_record_get(record, "BUY_AMOUNT", "buy_amount")),
                "sell_amount": _as_float(_record_get(record, "SELL_AMOUNT", "sell_amount")),
                "flow_mark": _as_int(_record_get(record, "FLOW_MARK", "flow_mark")),
                "total_amount": _as_float(_record_get(record, "TOTAL_AMOUNT", "total_amount")),
                "total_volume": _as_float(_record_get(record, "TOTAL_VOLUME", "total_volume")),
            }
        )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def quote_sql(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def fetch_db_rows(runtime_path: Path, code: str, trade_date: date) -> pd.DataFrame:
    sql = f"""
        SELECT
          market_code,
          trade_date,
          security_name,
          reason_type,
          reason_type_name,
          change_range,
          trader_name,
          buy_amount,
          sell_amount,
          flow_mark,
          total_amount,
          total_volume
        FROM starlight.ad_long_hu_bang
        WHERE market_code = {quote_sql(code)}
          AND trade_date = {quote_sql(trade_date.isoformat())}
        ORDER BY market_code, trade_date, reason_type, trader_name
    """
    config = ClickHouseConfig.from_env(runtime_path=runtime_path)
    connection = create_clickhouse_client(config)
    try:
        return connection.query_df(sql)
    finally:
        connection.close()


def numeric_sum(df: pd.DataFrame, column: str) -> float | None:
    if df.empty or column not in df.columns:
        return None
    values = pd.to_numeric(df[column], errors="coerce")
    if values.notna().sum() == 0:
        return None
    return float(values.sum(skipna=True))


def has_positive(df: pd.DataFrame, column: str) -> bool:
    value = numeric_sum(df, column)
    return value is not None and value > 0


def same_number(left: float | None, right: float | None) -> bool:
    if left is None and right is None:
        return True
    if left is None or right is None:
        return False
    return math.isclose(float(left), float(right), rel_tol=1e-9, abs_tol=1e-6)


def print_frame(title: str, df: pd.DataFrame, max_rows: int) -> None:
    print(f"\n== {title} ==", flush=True)
    print(f"rows={len(df)}", flush=True)
    if df.empty:
        print("<EMPTY>", flush=True)
        return
    with pd.option_context(
        "display.max_columns",
        None,
        "display.width",
        240,
        "display.max_colwidth",
        80,
    ):
        print(df.head(max_rows).to_string(index=False), flush=True)


def print_summary(raw_result: Any, raw_df: pd.DataFrame, mapped_df: pd.DataFrame, db_df: pd.DataFrame) -> None:
    raw_buy_columns = [col for col in raw_df.columns if "buy" in str(col).lower()]
    raw_sell_columns = [col for col in raw_df.columns if "sell" in str(col).lower()]
    mapped_buy_sum = numeric_sum(mapped_df, "buy_amount")
    mapped_sell_sum = numeric_sum(mapped_df, "sell_amount")
    db_buy_sum = numeric_sum(db_df, "buy_amount")
    db_sell_sum = numeric_sum(db_df, "sell_amount")

    print("\n== Summary ==", flush=True)
    print(f"sdk_result_type={type(raw_result).__name__}", flush=True)
    print(f"sdk_result_rows={_count_sdk_result_rows(raw_result)}", flush=True)
    print(f"raw_buy_like_columns={raw_buy_columns}", flush=True)
    print(f"raw_sell_like_columns={raw_sell_columns}", flush=True)
    print(f"sdk_mapped_rows={len(mapped_df)} mapped_buy_sum={mapped_buy_sum} mapped_sell_sum={mapped_sell_sum}", flush=True)
    print(f"db_rows={len(db_df)} db_buy_sum={db_buy_sum} db_sell_sum={db_sell_sum}", flush=True)

    if has_positive(mapped_df, "buy_amount") and not has_positive(db_df, "buy_amount"):
        verdict = "DB_SYNC_OR_PERSISTENCE_GAP: SDK mapped rows have positive buy_amount, but DB does not."
    elif not has_positive(mapped_df, "buy_amount") and not has_positive(db_df, "buy_amount"):
        if raw_buy_columns:
            verdict = "SOURCE_OR_MAPPING_GAP: DB matches mapped rows with no positive buy_amount; inspect raw buy-like columns."
        else:
            verdict = "SOURCE_GAP: SDK raw/mapped rows do not expose positive buy_amount, and DB matches that."
    elif len(mapped_df) != len(db_df) or not same_number(mapped_buy_sum, db_buy_sum) or not same_number(mapped_sell_sum, db_sell_sum):
        verdict = "DB_DIFFERS_FROM_SDK_MAPPING: mapped SDK rows and DB rows differ."
    else:
        verdict = "SDK_MAPPING_AND_DB_MATCH: persisted rows match the SDK mapping."
    print(f"verdict={verdict}", flush=True)


def main() -> int:
    args = parse_args()
    trade_date = parse_date(args.date)
    runtime_path = Path(args.runtime).expanduser().resolve()

    print("long_hu_bang diagnostic", flush=True)
    print(f"repo={REPO_ROOT}", flush=True)
    print(f"runtime={runtime_path}", flush=True)
    print(f"code={args.code}", flush=True)
    print(f"date={trade_date.isoformat()}", flush=True)

    sdk_config = AmazingDataSDKConfig.from_env(runtime_path=runtime_path)
    provider = AmazingDataSDKProvider(sdk_config)
    raw_result = None
    try:
        print("\n-- fetching SDK raw get_long_hu_bang ...", flush=True)
        raw_result, raw_df = fetch_sdk_raw(provider, args.code, trade_date)
        print(f"-- SDK raw fetched result_type={type(raw_result).__name__} rows={_count_sdk_result_rows(raw_result)}", flush=True)
        print_frame("SDK raw get_long_hu_bang result", raw_df, args.max_rows)

        print("\n-- mapping SDK raw result to LongHuBangRow fields ...", flush=True)
        mapped_df = map_sdk_result(raw_result)
        print_frame("SDK mapped LongHuBangRow fields", mapped_df, args.max_rows)
    finally:
        if args.close_sdk:
            print("\n-- closing SDK provider ...", flush=True)
            provider.close()
            print("-- SDK provider closed", flush=True)

    if args.skip_db:
        print_summary(raw_result, raw_df, mapped_df, pd.DataFrame())
        return 0

    print("\n-- querying ClickHouse starlight.ad_long_hu_bang ...", flush=True)
    db_df = fetch_db_rows(runtime_path, args.code, trade_date)
    print_frame("ClickHouse starlight.ad_long_hu_bang rows", db_df, args.max_rows)
    print_summary(raw_result, raw_df, mapped_df, db_df)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
