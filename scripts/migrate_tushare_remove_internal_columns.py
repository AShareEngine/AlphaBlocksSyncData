#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Rebuild Tushare tables with their declared business-key MergeTree layout.

The filename is retained for compatibility with the earlier metadata-column
migration.  The script now handles both legacy metadata columns and the later
full-row-hash layout which could not replace corrected business records.
"""

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
    TS_SYNC_CHECKPOINT_TABLE,
    TS_SYNC_TASK_LOG_TABLE,
    _normalize_key_expression,
    _normalized_key_expression,
    _quote_identifier,
    _safe_identifier,
)
from sync_data_system.providers.tushare.specs import TUSHARE_TASK_SPECS
from sync_data_system.sync_core.clickhouse import (
    ClickHouseConfig,
    ClickHouseConnection,
    create_clickhouse_client,
)


logger = logging.getLogger(__name__)
SPECS_BY_TABLE = {spec.table_name: spec for spec in TUSHARE_TASK_SPECS.values()}
STATE_LAYOUTS: dict[str, tuple[str, tuple[str, ...], str]] = {
    TS_SYNC_TASK_LOG_TABLE: (
        "MergeTree",
        ("run_date", "task_name", "started_at", "scope_key"),
        "toYYYYMM(run_date)",
    ),
    TS_SYNC_CHECKPOINT_TABLE: (
        "ReplacingMergeTree",
        ("task_name", "scope_key"),
        "",
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild outdated Tushare tables with stable business keys. "
            "Backups are kept by default."
        )
    )
    parser.add_argument("--runtime-path", default=None)
    parser.add_argument("--database", default="tushare")
    parser.add_argument(
        "--tables",
        default="",
        help="Optional comma-separated table names. Default: every outdated registered table.",
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


def _expected_layout(table: str) -> tuple[str, tuple[str, ...], str]:
    if table in STATE_LAYOUTS:
        return STATE_LAYOUTS[table]
    spec = SPECS_BY_TABLE.get(table)
    if spec is None:
        raise ValueError(f"Tushare table is not registered: {table}")
    return "ReplacingMergeTree", spec.business_key_fields, ""


def find_outdated_tables(
    connection: ClickHouseConnection,
    *,
    database: str,
    selected_tables: set[str],
) -> list[str]:
    registered = set(SPECS_BY_TABLE) | set(STATE_LAYOUTS)
    if selected_tables:
        unknown = sorted(selected_tables - registered)
        if unknown:
            raise ValueError(f"unregistered Tushare tables requested: {unknown}")
        registered &= selected_tables
    rows = connection.query_rows(
        """
        SELECT name, engine, sorting_key, primary_key, partition_key
        FROM system.tables
        WHERE database = {database:String}
          AND name IN {tables:Array(String)}
        ORDER BY name
        """,
        {"database": database, "tables": sorted(registered)},
    )
    outdated: set[str] = set()
    for row in rows:
        if len(row) < 5:
            continue
        table = _safe_identifier(row[0])
        expected_engine, key_fields, expected_partition = _expected_layout(table)
        expected_key = _normalized_key_expression(key_fields)
        if (
            str(row[1]) != expected_engine
            or _normalize_key_expression(row[2]) != expected_key
            or _normalize_key_expression(row[3]) != expected_key
            or _normalize_key_expression(row[4])
            != _normalize_key_expression(expected_partition)
        ):
            outdated.add(table)

    legacy_rows = connection.query_rows(
        """
        SELECT table
        FROM system.columns
        WHERE database = {database:String}
          AND table IN {tables:Array(String)}
          AND name IN {columns:Array(String)}
        GROUP BY table
        """,
        {
            "database": database,
            "tables": sorted(registered),
            "columns": list(LEGACY_META_COLUMNS),
        },
    )
    outdated.update(_safe_identifier(row[0]) for row in legacy_rows if row)
    return sorted(outdated)


def find_legacy_tables(
    connection: ClickHouseConnection,
    *,
    database: str,
    selected_tables: set[str],
) -> list[str]:
    """Compatibility helper retained for callers of the original script."""
    rows = connection.query_rows(
        """
        SELECT table
        FROM system.columns
        WHERE database = {database:String}
          AND name IN {columns:Array(String)}
        GROUP BY table
        ORDER BY table
        """,
        {"database": database, "columns": list(LEGACY_META_COLUMNS)},
    )
    result = [
        _safe_identifier(row[0])
        for row in rows
        if row and str(row[0]) in (set(SPECS_BY_TABLE) | set(STATE_LAYOUTS))
    ]
    if selected_tables:
        result = [table for table in result if table in selected_tables]
    return result


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
    key_fields: Sequence[str],
    engine: str = "ReplacingMergeTree",
    version_field: str = "",
    partition_by: str = "",
) -> str:
    available = {name for name, _ in columns}
    missing = sorted(set(key_fields) - available)
    if missing:
        raise RuntimeError(
            f"{database}.{table} is missing business-key columns: {missing}"
        )
    column_defs = ",\n            ".join(
        f"{_quote_identifier(name)} {data_type}"
        for name, data_type in columns
    )
    key_sql = ", ".join(_quote_identifier(name) for name in key_fields)
    if engine == "ReplacingMergeTree" and version_field:
        engine_sql = f"ReplacingMergeTree({_quote_identifier(version_field)})"
    elif engine == "ReplacingMergeTree":
        engine_sql = "ReplacingMergeTree()"
    else:
        engine_sql = engine
    partition_sql = f"\n    PARTITION BY {partition_by}" if partition_by else ""
    return f"""
    CREATE TABLE {_table_ref(database, table)}
    (
        {column_defs}
    )
    ENGINE = {engine_sql}{partition_sql}
    PRIMARY KEY ({key_sql})
    ORDER BY ({key_sql})
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
    source_columns = load_business_columns(connection, database=database, table=table)
    engine, key_fields, partition_by = _expected_layout(table)
    columns = list(source_columns)
    existing_names = {name for name, _ in columns}
    for field in key_fields:
        if field not in existing_names:
            columns.append((field, "String"))
            existing_names.add(field)
    replacement = _safe_identifier(f"{table}__business_key_{suffix}")
    backup = _safe_identifier(f"{table}__schema_backup_{suffix}")
    column_list = ", ".join(_quote_identifier(name) for name, _ in columns)
    source_names = {name for name, _ in source_columns}
    spec = SPECS_BY_TABLE.get(table)
    defaults = spec.business_key_defaults if spec is not None else {}
    select_expressions = ", ".join(
        _quote_identifier(name)
        if name in source_names
        else "'" + str(defaults.get(name, "")).replace("'", "''") + "'"
        for name, _ in columns
    )
    commands = [
        create_replacement_ddl(
            database=database,
            table=replacement,
            columns=columns,
            key_fields=key_fields,
            engine=engine,
            version_field=("finished_at" if table == TS_SYNC_CHECKPOINT_TABLE else ""),
            partition_by=partition_by,
        ),
        (
            f"INSERT INTO {_table_ref(database, replacement)} ({column_list}) "
            f"SELECT {select_expressions} FROM {_table_ref(database, table)} FINAL"
        ),
    ]
    if engine == "ReplacingMergeTree":
        commands.append(f"OPTIMIZE TABLE {_table_ref(database, replacement)} FINAL")
    commands.append(
        f"RENAME TABLE {_table_ref(database, table)} TO "
        f"{_table_ref(database, backup)}, "
        f"{_table_ref(database, replacement)} TO {_table_ref(database, table)}"
    )
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
        tables = find_outdated_tables(
            connection,
            database=database,
            selected_tables=selected,
        )
        if not tables:
            logger.info("No outdated Tushare MergeTree layouts found.")
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
