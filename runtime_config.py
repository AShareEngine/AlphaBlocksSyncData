#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Runtime configuration used by AlphaBlocksSyncData."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class LlmConfig:
    provider_name: str = ""
    base_url: str = ""
    api_key: str = ""
    model_name: str = ""
    temperature: float = 0.1
    max_tokens: int = 4096
    enabled: bool = True
    verify_ssl: bool = True


@dataclass
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


@dataclass
class DiscoveryConfig:
    allow_databases: list[str] = field(default_factory=list)
    allow_tables: list[str] = field(default_factory=list)
    trading_calendar_table: str = ""


@dataclass
class RuntimeStateConfig:
    database: str = "alphablocks"


@dataclass
class SyncAmazingDataConfig:
    username: str = ""
    password: str = ""
    host: str = ""
    port: int = 0
    local_path: str = ""


@dataclass
class SyncBaoStockConfig:
    user_id: str = "anonymous"
    password: str = "123456"


@dataclass
class SyncQmtConfig:
    base_url: str = ""
    api_key: str = ""
    timeout: int = 60


@dataclass
class SyncYFinanceConfig:
    proxy: str = ""
    batch_size: int = 5
    threads: bool = False
    auto_adjust: bool = False
    repair: bool = False
    timeout: int = 30
    network_retries: int = 2
    request_interval_seconds: float = 2.0
    rate_limit_retries: int = 4
    rate_limit_backoff_seconds: float = 30.0
    rate_limit_max_backoff_seconds: float = 300.0
    rate_limit_jitter_seconds: float = 3.0
    active_symbols_only: bool = True
    symbol_directory_timeout: int = 60
    default_start_date: str = "2010-01-01"
    include_otc: bool = False


@dataclass
class SyncAkshareConfig:
    proxy: str = ""
    request_interval_seconds: float = 1.0
    retries: int = 2
    retry_backoff_seconds: float = 2.0
    default_start_date: str = "2010-01-01"
    adjust: str = ""
    common_stock_only: bool = True
    include_pink: bool = False


@dataclass
class SyncTushareConfig:
    token: str = ""
    base_url: str = "https://api.tushare.pro"
    timeout: int = 60
    retries: int = 2
    retry_backoff_seconds: float = 2.0
    request_interval_seconds: float = 0.2
    default_start_date: str = "20100101"
    page_size: int = 5000
    max_requests_per_run: int = 0


@dataclass
class SyncSchedulerConfig:
    max_parallel_providers: int = 3


@dataclass
class SyncConfig:
    scheduler: SyncSchedulerConfig = field(default_factory=SyncSchedulerConfig)
    akshare: SyncAkshareConfig = field(default_factory=SyncAkshareConfig)
    amazingdata: SyncAmazingDataConfig = field(default_factory=SyncAmazingDataConfig)
    baostock: SyncBaoStockConfig = field(default_factory=SyncBaoStockConfig)
    qmt: SyncQmtConfig = field(default_factory=SyncQmtConfig)
    tushare: SyncTushareConfig = field(default_factory=SyncTushareConfig)
    yfinance: SyncYFinanceConfig = field(default_factory=SyncYFinanceConfig)


@dataclass
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
            scheduler=SyncSchedulerConfig(**(sync_payload.get("scheduler", {}) or {})),
            akshare=SyncAkshareConfig(**(sync_payload.get("akshare", {}) or {})),
            amazingdata=SyncAmazingDataConfig(**(sync_payload.get("amazingdata", {}) or {})),
            baostock=SyncBaoStockConfig(**(sync_payload.get("baostock", {}) or {})),
            qmt=SyncQmtConfig(**(sync_payload.get("qmt", {}) or {})),
            tushare=SyncTushareConfig(**(sync_payload.get("tushare", {}) or {})),
            yfinance=SyncYFinanceConfig(**(sync_payload.get("yfinance", {}) or {})),
        ),
    )


__all__ = [
    "DatasourceConfig",
    "DiscoveryConfig",
    "LlmConfig",
    "RuntimeConfig",
    "RuntimeStateConfig",
    "SyncAkshareConfig",
    "SyncAmazingDataConfig",
    "SyncBaoStockConfig",
    "SyncConfig",
    "SyncQmtConfig",
    "SyncSchedulerConfig",
    "SyncTushareConfig",
    "SyncYFinanceConfig",
    "load_runtime_config",
]
