#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ClickHouse table freshness watermark cache."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
TABLE_WATERMARK_TABLE = "sync_table_watermark"


def _identifier(value: str) -> str:
    text = str(value or "").strip()
    if not IDENTIFIER_RE.fullmatch(text):
        raise ValueError(f"invalid ClickHouse identifier: {value!r}")
    return text


def _quote_identifier(value: str) -> str:
    return f"`{_identifier(value)}`"


@dataclass(frozen=True)
class TableWatermark:
    source_database: str
    source_table: str
    latest_field: str
    latest_date: str
    has_data: bool
    source_last_update_time: str
    source_signature: str
    checked_at: Any = None


class TableWatermarkRepository:
    """Stores latest table watermarks without scanning source tables on every page load."""

    COLUMNS = (
        "source_database",
        "source_table",
        "latest_field",
        "latest_date",
        "has_data",
        "source_last_update_time",
        "source_signature",
        "checked_at",
        "_version",
    )

    def __init__(
        self,
        client: Any,
        *,
        database: str = "alphablocks",
        table: str = TABLE_WATERMARK_TABLE,
    ) -> None:
        self.client = client
        self.database = _identifier(database)
        self.table = _identifier(table)

    @property
    def table_ref(self) -> str:
        return f"{_quote_identifier(self.database)}.{_quote_identifier(self.table)}"

    def ensure_table(self) -> None:
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {_quote_identifier(self.database)}")
        self.client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_ref}
            (
                source_database String,
                source_table String,
                latest_field String,
                latest_date String,
                has_data UInt8,
                source_last_update_time String,
                source_signature String,
                checked_at DateTime64(3, 'UTC'),
                _version UInt64
            )
            ENGINE = ReplacingMergeTree(_version)
            ORDER BY (source_database, source_table)
            """
        )

    def load(
        self,
        targets: Iterable[tuple[str, str]],
    ) -> dict[tuple[str, str], TableWatermark]:
        normalized_targets = {
            (str(database).strip(), str(table).strip())
            for database, table in targets
            if str(database).strip() and str(table).strip()
        }
        if not normalized_targets:
            return {}
        databases = sorted({database for database, _ in normalized_targets})
        tables = sorted({table for _, table in normalized_targets})
        rows = self.client.query_rows(
            f"""
            SELECT
                source_database,
                source_table,
                argMax(latest_field, _version),
                argMax(latest_date, _version),
                argMax(has_data, _version),
                argMax(source_last_update_time, _version),
                argMax(source_signature, _version),
                argMax(checked_at, _version)
            FROM {self.table_ref}
            WHERE source_database IN {{databases:Array(String)}}
              AND source_table IN {{tables:Array(String)}}
            GROUP BY source_database, source_table
            """,
            {"databases": databases, "tables": tables},
        )
        result: dict[tuple[str, str], TableWatermark] = {}
        for row in rows:
            if len(row) < 8:
                continue
            key = (str(row[0]).strip(), str(row[1]).strip())
            if key not in normalized_targets:
                continue
            result[key] = TableWatermark(
                source_database=key[0],
                source_table=key[1],
                latest_field=str(row[2] or ""),
                latest_date=str(row[3] or ""),
                has_data=bool(row[4]),
                source_last_update_time=str(row[5] or ""),
                source_signature=str(row[6] or ""),
                checked_at=row[7],
            )
        return result

    def save(self, watermarks: Sequence[TableWatermark]) -> int:
        if not watermarks:
            return 0
        checked_at = datetime.now(timezone.utc).replace(tzinfo=None)
        version_base = time.time_ns()
        rows = [
            (
                watermark.source_database,
                watermark.source_table,
                watermark.latest_field,
                watermark.latest_date,
                int(watermark.has_data),
                watermark.source_last_update_time,
                watermark.source_signature,
                checked_at,
                version_base + index,
            )
            for index, watermark in enumerate(watermarks)
        ]
        self.client.insert_rows(self.table_ref, self.COLUMNS, rows)
        return len(rows)


def source_part_state(
    row: Sequence[Any] | None,
) -> tuple[bool, str, str]:
    if not row or len(row) < 4:
        return False, "", ""
    has_data = bool(row[2])
    last_update_time = str(row[3] or "")
    signature = (
        str(row[4] or "")
        if len(row) >= 5
        else f"{last_update_time}|{int(has_data)}"
    )
    return has_data, last_update_time, signature


def is_current_watermark(
    watermark: TableWatermark | None,
    *,
    latest_field: str,
    has_data: bool,
    source_signature: str,
) -> bool:
    return bool(
        watermark is not None
        and watermark.latest_field == str(latest_field or "")
        and watermark.has_data == bool(has_data)
        and watermark.source_signature == str(source_signature or "")
    )


__all__ = [
    "TABLE_WATERMARK_TABLE",
    "TableWatermark",
    "TableWatermarkRepository",
    "is_current_watermark",
    "source_part_state",
]
