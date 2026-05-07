#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ClickHouse 连接包装."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, MutableMapping, Optional, Sequence

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

from aiquantbase.runtime_config import load_runtime_config
from sync_data_system.config_paths import resolve_runtime_config_path


@dataclass(frozen=True)
class ClickHouseConfig:
    """ClickHouse 连接配置."""

    host: str
    port: int = 8123
    username: str = "default"
    password: str = ""
    database: str = "default"
    runtime_state_database: str = "alphablocks"
    secure: bool = False
    connect_timeout: int = 30
    send_receive_timeout: int = 300
    settings: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_env(
        cls,
        runtime_path: str | Path | None = None,
        prefix: str = "CLICKHOUSE_",
        fallback_prefix: str = "DB_",
        fallback_database_env: str = "DB_STARLIGHT",
    ) -> "ClickHouseConfig":
        del prefix, fallback_prefix, fallback_database_env
        runtime = load_runtime_config(resolve_runtime_config_path(runtime_path))
        datasource = runtime.datasource

        return cls(
            host=datasource.host,
            port=datasource.port,
            username=datasource.username,
            password=datasource.password,
            database=datasource.database or "default",
            runtime_state_database=runtime.runtime_state.database or "alphablocks",
            secure=datasource.secure,
        )


class ClickHouseConnection:
    """对 `clickhouse-connect` client 的轻量包装."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def command(self, sql: str, parameters: Optional[Mapping[str, Any]] = None) -> Any:
        return self._client.command(sql, parameters=parameters or {})

    def query_rows(self, sql: str, parameters: Optional[Mapping[str, Any]] = None) -> list[tuple[Any, ...]]:
        result = self._client.query(sql, parameters=parameters or {})
        return list(result.result_rows)

    def query_value(self, sql: str, parameters: Optional[Mapping[str, Any]] = None) -> Any:
        rows = self.query_rows(sql, parameters)
        if not rows:
            return None
        first_row = rows[0]
        if not first_row:
            return None
        return first_row[0]

    def query_df(self, sql: str, parameters: Optional[Mapping[str, Any]] = None):
        if pd is None:  # pragma: no cover
            raise RuntimeError("未安装 pandas，无法把 ClickHouse 查询结果转换为 DataFrame。")
        result = self._client.query(sql, parameters=parameters or {})
        return pd.DataFrame(result.result_rows, columns=result.column_names)

    def insert_rows(
        self,
        table: str,
        column_names: Sequence[str],
        rows: Sequence[Sequence[Any]],
    ) -> None:
        if not rows:
            return
        self._client.insert(table=table, data=list(rows), column_names=list(column_names))

    def close(self) -> None:
        close_fn = getattr(self._client, "close", None)
        if callable(close_fn):
            close_fn()


def create_clickhouse_client(config: ClickHouseConfig) -> ClickHouseConnection:
    """创建默认 ClickHouse 连接.

    当前实现使用 `clickhouse-connect`，因为它对 HTTP 协议和 pandas 互转都比较友好。
    """

    try:
        import clickhouse_connect
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "未安装 clickhouse-connect，请先在环境中安装该依赖。"
        ) from exc

    host = str(config.host or "").strip()
    if not host:
        raise RuntimeError("ClickHouse datasource.host is empty in runtime config")

    settings: MutableMapping[str, Any] = dict(config.settings)

    client = clickhouse_connect.get_client(
        host=host,
        port=config.port,
        username=config.username,
        password=config.password,
        database=config.database,
        secure=config.secure,
        connect_timeout=config.connect_timeout,
        send_receive_timeout=config.send_receive_timeout,
        settings=settings,
    )
    return ClickHouseConnection(client)


__all__ = [
    "ClickHouseConfig",
    "ClickHouseConnection",
    "create_clickhouse_client",
]
