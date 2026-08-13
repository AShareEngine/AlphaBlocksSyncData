#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Audit and backfill AmazingData tables against the historical A-share pool.

The historical source of truth is ``starlight.ad_hist_code_daily``.  A dry run
is the default.  ``--execute`` calls the existing AmazingData task runner only
for codes that have no row at all in the target table, then audits the table
again so upstream empty responses remain visible.

Examples:

  python3 scripts/backfill_amazingdata_missing_stocks.py
  python3 scripts/backfill_amazingdata_missing_stocks.py --task income
  python3 scripts/backfill_amazingdata_missing_stocks.py --execute \
    --task balance_sheet --task cash_flow --task income
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from program_bootstrap import install_sync_data_system_alias

install_sync_data_system_alias(PROJECT_ROOT)

from sync_data_system.clickhouse_client import (
    ClickHouseConfig,
    ClickHouseConnection,
    create_clickhouse_client,
)
from sync_data_system.data_models import to_ch_date
from sync_data_system.providers.amazingdata import runner


logger = logging.getLogger(__name__)
DEFAULT_BEGIN_DATE = 20100101
DEFAULT_DATABASE = "starlight"
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
AUDIT_CODE_BATCH_SIZE = 250

# These tables are expected to have broad stock coverage.  They are safe enough
# for the default --execute set because an absent code normally means a missing
# backfill rather than "the event never happened".
DEFAULT_EXECUTE_TASKS = (
    "stock_basic",
    "history_stock_status",
    "adj_factor",
    "backward_factor",
    "balance_sheet",
    "cash_flow",
    "income",
    "equity_structure",
)

# Every stock-code table is audited.  Event tables are deliberately excluded
# from DEFAULT_EXECUTE_TASKS: a stock can legitimately have no dividend,
# pledge, block trade, Dragon-Tiger event, etc.
AUDIT_TASKS = (
    "stock_basic",
    "history_stock_status",
    "adj_factor",
    "backward_factor",
    "balance_sheet",
    "cash_flow",
    "income",
    "profit_express",
    "profit_notice",
    "share_holder",
    "holder_num",
    "equity_structure",
    "equity_pledge_freeze",
    "equity_restricted",
    "dividend",
    "right_issue",
    "block_trading",
    "long_hu_bang",
    "margin_detail",
    "daily_kline",
    "minute_kline",
    "market_snapshot",
)

# These datasets describe a company rather than a historical exchange symbol.
# AmazingData assigns pre-relisting history to the successor code.  Treating an
# old/new pair as two missing companies would create duplicate statements.
COMPANY_IDENTITY_TASKS = frozenset(
    {
        "balance_sheet",
        "cash_flow",
        "income",
        "profit_express",
        "profit_notice",
        "share_holder",
        "holder_num",
        "equity_structure",
        "equity_pledge_freeze",
        "equity_restricted",
        "dividend",
        "right_issue",
    }
)

# AmazingData exposes the predecessor company's statements under the successor
# symbol for these verified Shenzhen/Shanghai code migrations.  Unlike BJ's
# mechanical 920 migration, these aliases apply only to company-level tables;
# price/status/factor history must retain both exchange symbols.
KNOWN_COMPANY_CODE_ALIASES = {
    "000022.SZ": "001872.SZ",  # 深赤湾A -> 招商港口
    "000043.SZ": "001914.SZ",  # 中航善达 -> 招商积余
    "300114.SZ": "302132.SZ",  # 中航电测 -> 中航成飞
    "601313.SH": "601360.SH",  # 江南嘉捷 -> 三六零
}


@dataclass(frozen=True)
class CoverageResult:
    task: str
    table: str
    category: str
    code_column: str
    historical_codes: int
    current_codes: int
    existing_historical_codes: int
    aliases_satisfied: int
    missing_all: int
    missing_noncurrent: int
    missing_codes: tuple[str, ...]
    missing_noncurrent_codes: tuple[str, ...]
    status: str = "ok"
    error: str = ""


@dataclass(frozen=True)
class ExecutionResult:
    task: str
    requested_codes: int
    status: str
    remaining_missing: int
    error: str = ""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="按 ad_hist_code_daily 审计并回补 AmazingData 历史股票缺口。"
    )
    parser.add_argument("--runtime-path", default=None)
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument("--begin-date", type=int, default=DEFAULT_BEGIN_DATE)
    parser.add_argument("--end-date", type=int, default=None)
    parser.add_argument(
        "--task",
        action="append",
        default=[],
        help="仅检查/执行指定任务，可重复；支持 income 或 amazingdata.income。",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="真正调用 AmazingData 并写入；默认仅审计。",
    )
    parser.add_argument(
        "--include-current",
        action="store_true",
        help="执行时也补当前股票；默认只补历史池中已不在当前代码表的股票。",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="每个任务最多执行多少个缺失代码；0 表示不限制。",
    )
    parser.add_argument("--json-report", default="")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    if args.limit < 0:
        parser.error("--limit 不能小于 0。")
    return args


def normalize_task_name(value: str) -> str:
    text = str(value or "").strip()
    if text.startswith("amazingdata."):
        text = text.split(".", 1)[1]
    if text not in AUDIT_TASKS:
        raise ValueError(
            f"不支持审计 AmazingData 任务 {value!r}；可选值={','.join(AUDIT_TASKS)}"
        )
    return text


def validate_identifier(value: str, field_name: str) -> str:
    text = str(value or "").strip()
    if not IDENTIFIER_RE.fullmatch(text):
        raise ValueError(f"{field_name} 不是合法 ClickHouse 标识符: {value!r}")
    return text


def normalize_mapped_code(value: object, suffix: str) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    return text if "." in text else f"{text}.{suffix}"


def load_historical_codes(
    connection: ClickHouseConnection,
    *,
    database: str,
    begin_date: int,
    end_date: int,
) -> set[str]:
    rows = connection.query_rows(
        f"""
        SELECT DISTINCT code
        FROM `{database}`.`ad_hist_code_daily`
        WHERE security_type = 'EXTRA_STOCK_A'
          AND trade_date >= {{begin_date:Date}}
          AND trade_date <= {{end_date:Date}}
        """,
        {
            "begin_date": to_ch_date(begin_date),
            "end_date": to_ch_date(end_date),
        },
    )
    return {str(row[0]).strip().upper() for row in rows if row and row[0]}


def load_current_codes(
    connection: ClickHouseConnection,
    *,
    database: str,
) -> set[str]:
    rows = connection.query_rows(
        f"""
        SELECT DISTINCT code
        FROM `{database}`.`ad_code_info`
        WHERE security_type = 'EXTRA_STOCK_A'
        """
    )
    return {str(row[0]).strip().upper() for row in rows if row and row[0]}


def load_code_aliases(
    connection: ClickHouseConnection,
    *,
    database: str,
    current_codes: set[str],
) -> tuple[dict[str, str], dict[str, str]]:
    """Load security migrations and broader company relisting relationships."""

    security_aliases: dict[str, str] = {}
    table_names = {
        str(row[0])
        for row in connection.query_rows(
            """
            SELECT name
            FROM system.tables
            WHERE database = {database:String}
            """,
            {"database": database},
        )
        if row
    }

    if "ad_bj_code_mapping" in table_names:
        for old_code, new_code in connection.query_rows(
            f"SELECT old_code, new_code FROM `{database}`.`ad_bj_code_mapping`"
        ):
            old = normalize_mapped_code(old_code, "BJ")
            new = normalize_mapped_code(new_code, "BJ")
            if old and new:
                security_aliases[old] = new

    company_aliases = {**security_aliases, **KNOWN_COMPANY_CODE_ALIASES}

    # Shanghai/Shenzhen relistings can be inferred when exactly one current
    # code has the same legal company name as a delisted code.
    if "ad_stock_basic" in table_names:
        rows = connection.query_rows(
            f"""
            WITH basics AS
            (
                SELECT
                    market_code,
                    argMax(comp_name, snapshot_date) AS company_name,
                    max(delist_date) AS last_delist_date
                FROM `{database}`.`ad_stock_basic`
                GROUP BY market_code
            )
            SELECT
                old.market_code AS old_code,
                any(current.market_code) AS new_code
            FROM basics AS old
            INNER JOIN basics AS current
                ON old.company_name = current.company_name
               AND old.market_code != current.market_code
            WHERE notEmpty(ifNull(old.company_name, ''))
              AND ifNull(old.last_delist_date, 0) > 0
              AND current.market_code IN {{current_codes:Array(String)}}
            GROUP BY old.market_code
            HAVING uniqExact(current.market_code) = 1
            """,
            {"current_codes": sorted(current_codes)},
        )
        for old_code, new_code in rows:
            old = str(old_code or "").strip().upper()
            new = str(new_code or "").strip().upper()
            if old and new:
                company_aliases[old] = new
    return security_aliases, company_aliases


def canonicalize_codes(codes: Iterable[str], aliases: dict[str, str]) -> set[str]:
    return {aliases.get(str(code).strip().upper(), str(code).strip().upper()) for code in codes}


def get_code_column(
    connection: ClickHouseConnection,
    *,
    database: str,
    table: str,
) -> str:
    columns = {
        str(row[0])
        for row in connection.query_rows(
            """
            SELECT name
            FROM system.columns
            WHERE database = {database:String} AND table = {table:String}
            """,
            {"database": database, "table": table},
        )
        if row
    }
    if "market_code" in columns:
        return "market_code"
    if "code" in columns:
        return "code"
    return ""


def load_existing_codes(
    connection: ClickHouseConnection,
    *,
    database: str,
    table: str,
    code_column: str,
    expected_codes: set[str],
) -> set[str]:
    if not expected_codes:
        return set()
    sorted_codes = sorted(expected_codes)
    existing: set[str] = set()
    for offset in range(0, len(sorted_codes), AUDIT_CODE_BATCH_SIZE):
        batch = sorted_codes[offset : offset + AUDIT_CODE_BATCH_SIZE]
        rows = connection.query_rows(
            f"""
            SELECT DISTINCT `{code_column}`
            FROM `{database}`.`{table}`
            WHERE `{code_column}` IN {{expected_codes:Array(String)}}
            """,
            {"expected_codes": batch},
        )
        existing.update(
            str(row[0]).strip().upper() for row in rows if row and row[0]
        )
    return existing


def audit_task(
    connection: ClickHouseConnection,
    *,
    task: str,
    database: str,
    historical_codes: set[str],
    current_codes: set[str],
    security_aliases: dict[str, str],
    company_aliases: dict[str, str],
) -> CoverageResult:
    table = runner.TASK_TARGET_TABLE_MAP[task]
    category = "company" if task in COMPANY_IDENTITY_TASKS else "security"
    code_column = get_code_column(
        connection,
        database=database,
        table=table,
    )
    if not code_column:
        return CoverageResult(
            task=task,
            table=table,
            category=category,
            code_column="",
            historical_codes=len(historical_codes),
            current_codes=len(current_codes),
            existing_historical_codes=0,
            aliases_satisfied=0,
            missing_all=0,
            missing_noncurrent=0,
            missing_codes=(),
            missing_noncurrent_codes=(),
            status="unsupported",
            error="目标表不存在或没有 code/market_code 字段",
        )

    existing_raw = load_existing_codes(
        connection,
        database=database,
        table=table,
        code_column=code_column,
        expected_codes=historical_codes | set(company_aliases.values()),
    )
    aliases = company_aliases if task in COMPANY_IDENTITY_TASKS else security_aliases
    expected = canonicalize_codes(historical_codes, aliases)
    existing = canonicalize_codes(existing_raw, aliases)
    current = canonicalize_codes(current_codes, aliases)
    missing = expected - existing
    missing_noncurrent = missing - current
    aliases_satisfied = sum(
        1
        for old_code, new_code in aliases.items()
        if old_code in historical_codes and new_code in existing
    )
    return CoverageResult(
        task=task,
        table=table,
        category=category,
        code_column=code_column,
        historical_codes=len(expected),
        current_codes=len(current),
        existing_historical_codes=len(expected & existing),
        aliases_satisfied=aliases_satisfied,
        missing_all=len(missing),
        missing_noncurrent=len(missing_noncurrent),
        missing_codes=tuple(sorted(missing)),
        missing_noncurrent_codes=tuple(sorted(missing_noncurrent)),
    )


def print_coverage(result: CoverageResult) -> None:
    if result.status != "ok":
        print(
            f"[SKIP] task={result.task} table={result.table} error={result.error}",
            flush=True,
        )
        return
    preview_codes = result.missing_noncurrent_codes or result.missing_codes
    preview = ",".join(preview_codes[:8]) or "-"
    print(
        f"[AUDIT] task={result.task} table={result.table} category={result.category} "
        f"history={result.historical_codes} covered={result.existing_historical_codes} "
        f"aliases={result.aliases_satisfied} missing_all={result.missing_all} "
        f"missing_noncurrent={result.missing_noncurrent} preview={preview}",
        flush=True,
    )


def resolve_requested_tasks(values: list[str], *, execute: bool) -> tuple[str, ...]:
    if values:
        return tuple(dict.fromkeys(normalize_task_name(item) for item in values))
    return DEFAULT_EXECUTE_TASKS if execute else AUDIT_TASKS


def execute_backfill(
    *,
    runtime_path: str | None,
    database: str,
    begin_date: int,
    end_date: int | None,
    selected: list[tuple[str, list[str]]],
) -> list[ExecutionResult]:
    if not selected:
        return []
    context = runner.build_context(runtime_path=runtime_path, database=database)
    results: list[ExecutionResult] = []
    try:
        for index, (task, codes) in enumerate(selected, start=1):
            print(
                f"[RUN] progress={index}/{len(selected)} task={task} code_count={len(codes)}",
                flush=True,
            )
            try:
                runner.execute_task_spec(
                    context,
                    runner.TaskRunSpec(
                        task=task,
                        codes_raw=",".join(codes),
                        begin_date=begin_date,
                        end_date=end_date,
                        force=True,
                        resume=False,
                        universe_mode="current",
                    ),
                )
            except Exception as exc:
                logger.exception("AmazingData missing-stock backfill failed task=%s", task)
                results.append(
                    ExecutionResult(
                        task=task,
                        requested_codes=len(codes),
                        status="failed",
                        remaining_missing=len(codes),
                        error=f"{type(exc).__name__}: {exc}",
                    )
                )
            else:
                results.append(
                    ExecutionResult(
                        task=task,
                        requested_codes=len(codes),
                        status="requested",
                        remaining_missing=-1,
                    )
                )
    finally:
        context.close()
    return results


def write_json_report(path: str, payload: dict[str, object]) -> None:
    if not path:
        return
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[REPORT] {target}", flush=True)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args.database = validate_identifier(args.database, "--database")
    if args.execute and args.database != DEFAULT_DATABASE:
        raise ValueError(
            f"--execute 目前只允许写入 {DEFAULT_DATABASE}，当前值={args.database!r}"
        )
    tasks = resolve_requested_tasks(args.task, execute=args.execute)
    audit_end = args.end_date or int(date.today().strftime("%Y%m%d"))
    connection = create_clickhouse_client(
        ClickHouseConfig.from_env(runtime_path=args.runtime_path)
    )
    before: list[CoverageResult] = []
    after: list[CoverageResult] = []
    executions: list[ExecutionResult] = []
    try:
        historical_codes = load_historical_codes(
            connection,
            database=args.database,
            begin_date=args.begin_date,
            end_date=audit_end,
        )
        if not historical_codes:
            raise RuntimeError(
                "ad_hist_code_daily 中没有请求区间内的 EXTRA_STOCK_A；"
                "请先同步 amazingdata.hist_code_list。"
            )
        current_codes = load_current_codes(connection, database=args.database)
        security_aliases, company_aliases = load_code_aliases(
            connection,
            database=args.database,
            current_codes=current_codes,
        )
        print(
            f"[POOL] begin={args.begin_date} end={audit_end} "
            f"historical={len(historical_codes)} current={len(current_codes)} "
            f"security_aliases={len(security_aliases)} "
            f"company_aliases={len(company_aliases)}",
            flush=True,
        )
        for task in tasks:
            result = audit_task(
                connection,
                task=task,
                database=args.database,
                historical_codes=historical_codes,
                current_codes=current_codes,
                security_aliases=security_aliases,
                company_aliases=company_aliases,
            )
            before.append(result)
            print_coverage(result)

        if args.execute:
            selected: list[tuple[str, list[str]]] = []
            for result in before:
                if result.status != "ok":
                    continue
                codes = list(
                    result.missing_codes
                    if args.include_current
                    else result.missing_noncurrent_codes
                )
                if args.limit > 0:
                    codes = codes[: args.limit]
                if codes:
                    selected.append((result.task, codes))
            executions = execute_backfill(
                runtime_path=args.runtime_path,
                database=args.database,
                begin_date=args.begin_date,
                end_date=args.end_date,
                selected=selected,
            )

            for result in before:
                refreshed = audit_task(
                    connection,
                    task=result.task,
                    database=args.database,
                    historical_codes=historical_codes,
                    current_codes=current_codes,
                    security_aliases=security_aliases,
                    company_aliases=company_aliases,
                )
                after.append(refreshed)
                print(
                    f"[VERIFY] task={refreshed.task} before={result.missing_all} "
                    f"after={refreshed.missing_all} "
                    f"remaining_noncurrent={refreshed.missing_noncurrent}",
                    flush=True,
                )
            after_map = {item.task: item for item in after}
            verified_executions: list[ExecutionResult] = []
            for item in executions:
                remaining = (
                    after_map[item.task].missing_noncurrent
                    if item.task in after_map
                    else -1
                )
                status = item.status
                if status != "failed" and remaining >= 0:
                    status = "complete" if remaining == 0 else "partial"
                verified_executions.append(
                    ExecutionResult(
                        task=item.task,
                        requested_codes=item.requested_codes,
                        status=status,
                        remaining_missing=remaining,
                        error=item.error,
                    )
                )
            executions = verified_executions
    finally:
        connection.close()

    report = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "database": args.database,
        "begin_date": args.begin_date,
        "end_date": audit_end,
        "execute": bool(args.execute),
        "include_current": bool(args.include_current),
        "before": [asdict(item) for item in before],
        "executions": [asdict(item) for item in executions],
        "after": [asdict(item) for item in after],
    }
    write_json_report(args.json_report, report)
    total_missing = sum(item.missing_noncurrent for item in (after or before))
    print(
        f"[SUMMARY] tasks={len(before)} execute={args.execute} "
        f"remaining_noncurrent={total_missing}",
        flush=True,
    )
    return 1 if any(item.status in {"failed", "partial"} for item in executions) else 0


if __name__ == "__main__":
    raise SystemExit(main())
