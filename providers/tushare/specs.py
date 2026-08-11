#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Metadata-driven Tushare task definitions."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping

from sync_data_system.providers.tushare.business_keys import (
    TUSHARE_BUSINESS_KEY_DEFAULTS,
    TUSHARE_BUSINESS_KEYS,
)


CATALOG_PATH = Path(__file__).with_name("catalog.json")
SAFE_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
OUTPUT_FIELD_ALIASES: dict[str, dict[str, str]] = {
    # Some compatible Tushare gateways return the cn_pmi input spelling `m`
    # even though the documented output/business-key field is `month`.
    "cn_pmi": {"m": "month"},
}


@dataclass(frozen=True)
class TushareFieldSpec:
    name: str
    source_name: str = ""
    data_type: str = ""
    required_or_default: str = ""
    description: str = ""

    @property
    def required(self) -> bool:
        return self.required_or_default.strip().upper().startswith("Y")

    @property
    def provider_name(self) -> str:
        if self.source_name:
            return self.source_name
        # Older catalogs prefixed leading-digit provider fields with f_ so
        # they remained safe ClickHouse identifiers.
        if self.name.startswith("f_") and self.name[2:3].isdigit():
            return self.name[2:]
        return self.name

    @property
    def requestable(self) -> bool:
        """Whether the provider still accepts this documented field.

        Tushare keeps renamed fields in some documentation tables for migration
        guidance, even after the API rejects them in ``fields``. Preserve those
        entries in the catalog, but do not request or create columns for them.
        """

        return "更名停用" not in self.description


@dataclass(frozen=True)
class TushareTaskSpec:
    task: str
    table_name: str
    doc_id: str
    doc_url: str
    title: str
    category_path: tuple[str, ...]
    description: str
    input_fields: tuple[TushareFieldSpec, ...]
    output_fields: tuple[TushareFieldSpec, ...]
    cursor_field: str = ""
    request_mode: str = "snapshot"
    stopped: bool = False
    default_enabled: bool = True
    supports_pagination: bool = False
    transport: str = "http"

    @property
    def input_names(self) -> tuple[str, ...]:
        return tuple(field.name for field in self.input_fields)

    @property
    def output_names(self) -> tuple[str, ...]:
        return tuple(field.name for field in self.output_fields if field.requestable)

    @property
    def output_provider_names(self) -> tuple[str, ...]:
        return tuple(
            field.provider_name for field in self.output_fields if field.requestable
        )

    @property
    def business_key_fields(self) -> tuple[str, ...]:
        return TUSHARE_BUSINESS_KEYS[self.task]

    @property
    def business_key_defaults(self) -> Mapping[str, str]:
        return TUSHARE_BUSINESS_KEY_DEFAULTS.get(self.task, {})

    @property
    def required_input_names(self) -> tuple[str, ...]:
        return tuple(field.name for field in self.input_fields if field.required)

    @property
    def supports_incremental(self) -> bool:
        return self.request_mode in {"code_range", "date_range", "date_slice"}

    @property
    def code_field(self) -> str:
        inputs = set(self.input_names)
        for candidate in ("ts_code", "index_code", "code", "con_code", "symbol"):
            if candidate in inputs:
                return candidate
        return ""

    def provider_field_name(self, field_name: str) -> str:
        for field in self.output_fields:
            if field.name == field_name or field.provider_name == field_name:
                return field.provider_name
        return field_name

    def normalize_output_row(self, row: Mapping[str, Any]) -> dict[str, Any]:
        field_names = {
            field.provider_name: field.name
            for field in self.output_fields
        }
        normalized = {
            field_names.get(str(name), str(name)): value
            for name, value in row.items()
        }
        for source_name, target_name in OUTPUT_FIELD_ALIASES.get(self.task, {}).items():
            if source_name not in normalized:
                continue
            value = normalized.pop(source_name)
            normalized.setdefault(target_name, value)
        return normalized

    @property
    def category_root(self) -> str:
        return self.category_path[0] if self.category_path else ""


@lru_cache(maxsize=1)
def load_tushare_task_specs() -> dict[str, TushareTaskSpec]:
    payload = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    endpoints = payload.get("endpoints") or []
    specs: dict[str, TushareTaskSpec] = {}
    for raw in endpoints:
        if bool(raw.get("mutating")):
            continue
        task = str(raw.get("api_name") or "").strip()
        table_name = str(raw.get("table_name") or "").strip()
        if not SAFE_IDENTIFIER_RE.fullmatch(task) or not SAFE_IDENTIFIER_RE.fullmatch(table_name):
            raise ValueError(f"unsafe Tushare catalog identifier: task={task!r} table={table_name!r}")
        if task in specs:
            raise ValueError(f"duplicate Tushare task in catalog: {task}")
        input_fields = tuple(_field_spec(item) for item in raw.get("input_fields") or [])
        output_fields = tuple(_field_spec(item) for item in raw.get("output_fields") or [])
        for field in (*input_fields, *output_fields):
            if not SAFE_IDENTIFIER_RE.fullmatch(field.name):
                raise ValueError(f"unsafe Tushare field identifier: task={task!r} field={field.name!r}")
        specs[task] = TushareTaskSpec(
            task=task,
            table_name=table_name,
            doc_id=str(raw.get("doc_id") or ""),
            doc_url=str(raw.get("doc_url") or ""),
            title=str(raw.get("title") or task),
            category_path=tuple(str(item) for item in raw.get("category_path") or []),
            description=str(raw.get("description") or ""),
            input_fields=input_fields,
            output_fields=output_fields,
            cursor_field=str(raw.get("cursor_field") or ""),
            request_mode=str(raw.get("request_mode") or "snapshot"),
            stopped=bool(raw.get("stopped")),
            default_enabled=bool(raw.get("default_enabled", True)),
            supports_pagination=bool(raw.get("supports_pagination")),
            transport=str(raw.get("transport") or "http"),
        )
    missing_keys = sorted(set(specs) - set(TUSHARE_BUSINESS_KEYS))
    unknown_keys = sorted(set(TUSHARE_BUSINESS_KEYS) - set(specs))
    if missing_keys or unknown_keys:
        raise ValueError(
            "Tushare business-key registry does not match the catalog: "
            f"missing={missing_keys} unknown={unknown_keys}"
        )
    for spec in specs.values():
        available_fields = set(spec.output_names) | set(spec.input_names)
        unknown_fields = sorted(set(spec.business_key_fields) - available_fields)
        if unknown_fields:
            raise ValueError(
                f"Tushare business key uses undocumented output fields: "
                f"task={spec.task} fields={unknown_fields}"
            )
    return specs


def _field_spec(raw: dict[str, Any]) -> TushareFieldSpec:
    return TushareFieldSpec(
        name=str(raw.get("name") or "").strip(),
        source_name=str(raw.get("source_name") or "").strip(),
        data_type=str(raw.get("type") or "").strip(),
        required_or_default=str(raw.get("required_or_default") or "").strip(),
        description=str(raw.get("description") or "").strip(),
    )


TUSHARE_TASK_SPECS = load_tushare_task_specs()
TUSHARE_TASK_CHOICES = tuple(TUSHARE_TASK_SPECS)


__all__ = [
    "CATALOG_PATH",
    "TUSHARE_TASK_CHOICES",
    "TUSHARE_TASK_SPECS",
    "TushareFieldSpec",
    "TushareTaskSpec",
    "load_tushare_task_specs",
]
