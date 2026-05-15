#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Runtime configuration used by AlphaBlocksSyncData."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass(slots=True)
class LlmConfig:
    provider_name: str = ""
    base_url: str = ""
    api_key: str = ""
    model_name: str = ""
    temperature: float = 0.1
    max_tokens: int = 4096
    enabled: bool = True
    verify_ssl: bool = True


@dataclass(slots=True)
class DatasourceConfig:
    id: str = "primary"
    name: str = "Primary Data Source"
    db_type: str = "clickhouse"
    host: str = ""
    port: int = 8123
    database: str = ""
    username: str = "default"
    password: str = ""
    secure: bool = False
    extra_params: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class DiscoveryConfig:
    allow_databases: list[str] = field(default_factory=list)
    allow_tables: list[str] = field(default_factory=list)
    trading_calendar_table: str = ""


@dataclass(slots=True)
class RuntimeStateConfig:
    database: str = "alphablocks"


@dataclass(slots=True)
class SyncAmazingDataConfig:
    username: str = ""
    password: str = ""
    host: str = ""
    port: int = 0
    local_path: str = ""


@dataclass(slots=True)
class SyncBaoStockConfig:
    user_id: str = "anonymous"
    password: str = "123456"


@dataclass(slots=True)
class SyncQmtConfig:
    base_url: str = "http://172.16.2.89:8000"
    api_key: str = "dev-api-key-001"
    timeout: int = 60


@dataclass(slots=True)
class SyncConfig:
    amazingdata: SyncAmazingDataConfig = field(default_factory=SyncAmazingDataConfig)
    baostock: SyncBaoStockConfig = field(default_factory=SyncBaoStockConfig)
    qmt: SyncQmtConfig = field(default_factory=SyncQmtConfig)


@dataclass(slots=True)
class RuntimeConfig:
    llm: LlmConfig = field(default_factory=LlmConfig)
    datasource: DatasourceConfig = field(default_factory=DatasourceConfig)
    discovery: DiscoveryConfig = field(default_factory=DiscoveryConfig)
    runtime_state: RuntimeStateConfig = field(default_factory=RuntimeStateConfig)
    sync: SyncConfig = field(default_factory=SyncConfig)


def load_yaml(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML root must be a mapping: {path}")
    return data


def load_runtime_config(path: str | Path) -> RuntimeConfig:
    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(
            f"runtime config not found: {resolved_path}. "
            "Create AlphaBlocksSyncData/config/runtime.local.yaml from config/runtime.example.yaml "
            "or set SYNC_DATA_RUNTIME_CONFIG."
        )

    data = load_yaml(resolved_path)
    datasource_payload = {
        "id": "primary",
        "name": "Primary Data Source",
        **(data.get("datasource", {}) or {}),
    }
    sync_payload = data.get("sync", {}) or {}
    return RuntimeConfig(
        llm=LlmConfig(**(data.get("llm", {}) or {})),
        datasource=DatasourceConfig(**datasource_payload),
        discovery=DiscoveryConfig(**(data.get("discovery", {}) or {})),
        runtime_state=RuntimeStateConfig(**(data.get("runtime_state", {}) or {})),
        sync=SyncConfig(
            amazingdata=SyncAmazingDataConfig(**(sync_payload.get("amazingdata", {}) or {})),
            baostock=SyncBaoStockConfig(**(sync_payload.get("baostock", {}) or {})),
            qmt=SyncQmtConfig(**(sync_payload.get("qmt", {}) or {})),
        ),
    )


__all__ = [
    "DatasourceConfig",
    "DiscoveryConfig",
    "LlmConfig",
    "RuntimeConfig",
    "RuntimeStateConfig",
    "SyncAmazingDataConfig",
    "SyncBaoStockConfig",
    "SyncConfig",
    "SyncQmtConfig",
    "load_runtime_config",
]
