#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Metadata-driven Tushare task definitions."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any


CATALOG_PATH = Path(__file__).with_name("catalog.json")
SAFE_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


@dataclass(frozen=True)
class TushareFieldSpec:
    name: str
    data_type: str = ""
    required_or_default: str = ""
    description: str = ""

    @property
    def required(self) -> bool:
        return self.required_or_default.strip().upper().startswith("Y")


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
        return tuple(field.name for field in self.output_fields)

    @property
    def required_input_names(self) -> tuple[str, ...]:
        return tuple(field.name for field in self.input_fields if field.required)

    @property
    def supports_incremental(self) -> bool:
        return self.request_mode in {"code_range", "date_range", "date_slice"}

    @property
    def code_field(self) -> str:
        inputs = set(self.input_names)
        outputs = set(self.output_names)
        for candidate in ("ts_code", "index_code", "code", "con_code", "symbol"):
            if candidate in inputs and candidate in outputs:
                return candidate
        return ""

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
    return specs


def _field_spec(raw: dict[str, Any]) -> TushareFieldSpec:
    return TushareFieldSpec(
        name=str(raw.get("name") or "").strip(),
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
