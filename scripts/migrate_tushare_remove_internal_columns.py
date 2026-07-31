#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Rebuild legacy Tushare tables without internal metadata columns."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from program_bootstrap import install_sync_data_system_alias

install_sync_data_system_alias(PROJECT_ROOT)

from sync_data_system.providers.tushare.repository import (
    LEGACY_META_COLUMNS,
    _quote_identifier,
    _safe_identifier,
)
from sync_data_system.sync_core.clickhouse import (
    ClickHouseConfig,
    ClickHouseConnection,
    create_clickhouse_client,
)


logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Remove legacy Tushare internal columns by rebuilding affected tables."
    )
    parser.add_argument("--runtime-path", default=None)
    parser.add_argument("--database", default="tushare")
    parser.add_argument(
        "--tables",
        default="",
        help="Optional comma-separated table names. Default: every affected table.",
    )
    parser.add_argument(
        "--drop-backups",
        action="store_true",
        help="Drop old backup tables after a successful swap.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def _table_ref(database: str, table: str) -> str:
    return f"{_quote_identifier(database)}.{_quote_identifier(table)}"


def _selected_tables(raw: str) -> set[str]:
    return {
        _safe_identifier(item.strip())
        for item in str(raw or "").split(",")
        if item.strip()
    }


def find_legacy_tables(
    connection: ClickHouseConnection,
    *,
    database: str,
    selected_tables: set[str],
) -> list[str]:
    rows = connection.query_rows(
        """
        SELECT table
        FROM system.columns
        WHERE database = {database:String}
          AND name IN {columns:Array(String)}
        GROUP BY table
        ORDER BY table
        """,
        {
            "database": database,
            "columns": list(LEGACY_META_COLUMNS),
        },
    )
    tables = [
        _safe_identifier(row[0])
        for row in rows
        if row
        and "__with_meta_backup_" not in str(row[0])
        and "__without_meta_" not in str(row[0])
    ]
    if selected_tables:
        tables = [table for table in tables if table in selected_tables]
    return tables


def load_business_columns(
    connection: ClickHouseConnection,
    *,
    database: str,
    table: str,
) -> list[tuple[str, str]]:
    rows = connection.query_rows(
        """
        SELECT name, type
        FROM system.columns
        WHERE database = {database:String}
          AND table = {table:String}
        ORDER BY position
        """,
        {"database": database, "table": table},
    )
    legacy = set(LEGACY_META_COLUMNS)
    columns = [
        (_safe_identifier(row[0]), str(row[1]))
        for row in rows
        if len(row) >= 2 and str(row[0]) not in legacy
    ]
    if not columns:
        raise RuntimeError(f"{database}.{table} has no business columns")
    return columns


def create_replacement_ddl(
    *,
    database: str,
    table: str,
    columns: Sequence[tuple[str, str]],
) -> str:
    column_defs = ",\n            ".join(
        f"{_quote_identifier(name)} {data_type}"
        for name, data_type in columns
    )
    row_values = ", ".join(_quote_identifier(name) for name, _ in columns)
    return f"""
    CREATE TABLE {_table_ref(database, table)}
    (
        {column_defs}
    )
    ENGINE = ReplacingMergeTree()
    PRIMARY KEY tuple()
    ORDER BY sipHash128(tuple({row_values}))
    """


def migrate_table(
    connection: ClickHouseConnection,
    *,
    database: str,
    table: str,
    suffix: str,
    drop_backup: bool,
    dry_run: bool,
) -> str:
    columns = load_business_columns(connection, database=database, table=table)
    replacement = _safe_identifier(f"{table}__without_meta_{suffix}")
    backup = _safe_identifier(f"{table}__with_meta_backup_{suffix}")
    column_list = ", ".join(_quote_identifier(name) for name, _ in columns)
    commands = [
        create_replacement_ddl(
            database=database,
            table=replacement,
            columns=columns,
        ),
        (
            f"INSERT INTO {_table_ref(database, replacement)} ({column_list}) "
            f"SELECT {column_list} FROM {_table_ref(database, table)} FINAL"
        ),
        (
            f"RENAME TABLE {_table_ref(database, table)} TO "
            f"{_table_ref(database, backup)}, "
            f"{_table_ref(database, replacement)} TO {_table_ref(database, table)}"
        ),
    ]
    if drop_backup:
        commands.append(f"DROP TABLE {_table_ref(database, backup)} SYNC")

    for command in commands:
        if dry_run:
            logger.info("dry-run SQL:\n%s", command.strip())
        else:
            connection.command(command)
    return backup


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    database = _safe_identifier(args.database)
    selected = _selected_tables(args.tables)
    config = ClickHouseConfig.from_env(runtime_path=args.runtime_path)
    connection = create_clickhouse_client(config)
    suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    try:
        tables = find_legacy_tables(
            connection,
            database=database,
            selected_tables=selected,
        )
        if not tables:
            logger.info("No Tushare tables with legacy internal columns found.")
            return 0
        for index, table in enumerate(tables, start=1):
            logger.info("Migrating table progress=%s/%s table=%s", index, len(tables), table)
            backup = migrate_table(
                connection,
                database=database,
                table=table,
                suffix=suffix,
                drop_backup=bool(args.drop_backups),
                dry_run=bool(args.dry_run),
            )
            if not args.drop_backups:
                logger.info(
                    "Kept backup table %s.%s; drop it after verification.",
                    database,
                    backup,
                )
        return 0
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
