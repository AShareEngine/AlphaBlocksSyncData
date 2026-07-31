#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate the Tushare provider catalog from the official documentation.

The generated catalog is intentionally committed. Runtime sync never scrapes the
documentation and therefore remains deterministic/offline apart from API calls.
"""

from __future__ import annotations

import argparse
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx
from bs4 import BeautifulSoup, Tag


DOC_ROOT = "https://tushare.pro/document/2"
API_NAME_RE = re.compile(
    r"接口(?:名称)?\s*[：:]\s*([A-Za-z][A-Za-z0-9_]*)"
)
DATE_CURSOR_PREFERENCE = (
    "trade_time",
    "datetime",
    "trade_date",
    "cal_date",
    "ann_date",
    "end_date",
    "date",
    "month",
    "quarter",
    "period",
    "time",
)
MUTATING_API_NAMES = frozenset({"p_save", "p_delete"})
GLOBAL_RANGE_API_NAMES = frozenset(
    {
        "dc_daily",
        "dc_index",
        "dc_member",
        "limit_cpt_list",
        "stock_hsgt",
        "tdx_daily",
        "tdx_member",
        "ths_daily",
        "ths_member",
    }
)
DATE_SLICE_API_NAMES = frozenset(
    {
        # These endpoints accept an optional security code, but their official
        # examples recommend querying the whole market one date at a time.
        # Date slices also prevent multi-day responses from hitting row limits.
        "anns_d",
        "irm_qa_sh",
        "irm_qa_sz",
        "research_report",
    }
)
SNAPSHOT_API_NAMES = frozenset({"fut_basic"})
STOPPED_TITLE_MARKERS = ("（停）", "(停)", "（旧）", "(旧)")
DOCUMENT_ALIASES = {
    "146": {
        "api_name": "pro_bar",
        "reason": "A股复权行情是通用行情 SDK 接口 pro_bar 的专项说明。",
    }
}
SPECIAL_OUTPUT_FIELDS = {
    "pro_bar": (
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount",
        "adj_factor",
    )
}
DOCUMENT_ENDPOINT_VARIANTS = {
    "420": (
        {
            "api_name": "rt_idx_min_daily",
            "title": "指数实时分钟-日累计",
            "description": "获取单个指数当日开盘以来的分钟数据。",
        },
    ),
    "340": (
        {
            "api_name": "rt_fut_min_daily",
            "title": "期货实时分钟-日累计",
            "description": "获取单个期货合约当日开盘以来的分钟数据。",
            "extra_input_fields": (
                {
                    "name": "date_str",
                    "type": "str",
                    "required_or_default": "N",
                    "description": "回放日期（YYYY-MM-DD，默认为交易当日，支持回溯一天）",
                },
            ),
        },
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        default="providers/tushare/catalog.json",
        help="Generated JSON catalog path.",
    )
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--timeout", type=float, default=30.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output = Path(args.output).expanduser().resolve()
    navigation_html = _download(DOC_ROOT, timeout=args.timeout)
    documents = _navigation_documents(navigation_html)

    endpoints: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    non_api_documents: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {
            pool.submit(
                _parse_document,
                document,
                timeout=args.timeout,
            ): document
            for document in documents
        }
        for future in as_completed(futures):
            document = futures[future]
            try:
                endpoint = future.result()
            except Exception as exc:
                failures.append({"doc_id": document["doc_id"], "error": str(exc)})
                continue
            if endpoint is not None:
                endpoints.append(endpoint)
            else:
                non_api_documents.append(document)

    existing_api_names = {item["api_name"] for item in endpoints}
    for endpoint in tuple(endpoints):
        for variant in DOCUMENT_ENDPOINT_VARIANTS.get(endpoint["doc_id"], ()):
            if variant["api_name"] in existing_api_names:
                continue
            cloned = {
                **endpoint,
                "api_name": variant["api_name"],
                "title": variant["title"],
                "description": variant["description"],
                "table_name": f"ts_{variant['api_name']}",
                "document_api_names": [
                    *endpoint["document_api_names"],
                    variant["api_name"],
                ],
                "input_fields": [
                    *endpoint["input_fields"],
                    *variant.get("extra_input_fields", ()),
                ],
            }
            endpoints.append(cloned)
            existing_api_names.add(variant["api_name"])
    endpoints.sort(key=lambda item: (item["category_path"], item["api_name"]))
    aliases = [
        {
            **document,
            **DOCUMENT_ALIASES[document["doc_id"]],
        }
        for document in documents
        if document["doc_id"] in DOCUMENT_ALIASES
    ]
    unavailable_documents = [
        {
            **document,
            "reason": "官方文档明确说明不提供 API，仅通过 CSV 网盘单独交付。",
        }
        for document in non_api_documents
        if document["doc_id"] == "314"
    ]
    remaining_non_api = [
        document
        for document in non_api_documents
        if document["doc_id"] != "314" and document["doc_id"] not in DOCUMENT_ALIASES
    ]
    payload = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "source_url": DOC_ROOT,
        "navigation_document_count": len(documents),
        "endpoint_count": len(endpoints),
        "read_only_endpoint_count": sum(not item["mutating"] for item in endpoints),
        "mutating_endpoint_count": sum(item["mutating"] for item in endpoints),
        "failed_documents": failures,
        "document_aliases": aliases,
        "unavailable_documents": unavailable_documents,
        "non_api_documents": sorted(
            remaining_non_api,
            key=lambda item: int(item["doc_id"]),
        ),
        "endpoints": endpoints,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        f"generated {output}: documents={len(documents)} endpoints={len(endpoints)} "
        f"read_only={payload['read_only_endpoint_count']} failures={len(failures)}"
    )
    return 1 if failures else 0


def _download(url: str, *, timeout: float) -> str:
    headers = {"User-Agent": "AlphaBlocksSyncData catalog generator/1.0"}
    with httpx.Client(timeout=timeout, follow_redirects=True, headers=headers) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.text


def _navigation_documents(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    sidebar = soup.select_one("nav.sidebar")
    if sidebar is None:
        raise ValueError("official documentation sidebar not found")

    documents: dict[str, dict[str, Any]] = {}
    for anchor in sidebar.select('a[href*="doc_id="]'):
        href = str(anchor.get("href") or "")
        doc_id = (parse_qs(urlparse(href).query).get("doc_id") or [""])[0]
        if not doc_id:
            continue
        li = anchor.find_parent("li")
        if li is None or li.find("ul", recursive=False) is not None:
            continue
        category_path = _category_path(anchor)
        documents[doc_id] = {
            "doc_id": doc_id,
            "title": anchor.get_text(" ", strip=True),
            "category_path": category_path,
            "doc_url": f"{DOC_ROOT}?doc_id={doc_id}",
        }
    return sorted(documents.values(), key=lambda item: int(item["doc_id"]))


def _category_path(anchor: Tag) -> list[str]:
    categories: list[str] = []
    parent_li = anchor.find_parent("li")
    if parent_li is None:
        return categories
    for ancestor_li in parent_li.find_parents("li"):
        direct_anchor = ancestor_li.find("a", recursive=False)
        if direct_anchor is not None:
            title = direct_anchor.get_text(" ", strip=True)
            if title:
                categories.append(title)
    categories.reverse()
    return categories


def _parse_document(document: dict[str, Any], *, timeout: float) -> dict[str, Any] | None:
    if document["doc_id"] in DOCUMENT_ALIASES:
        return None
    html = _download(document["doc_url"], timeout=timeout)
    soup = BeautifulSoup(html, "html.parser")
    content = soup.select_one("div.content")
    if content is None:
        raise ValueError("document content not found")

    text = content.get_text("\n", strip=True)
    api_names = list(dict.fromkeys(API_NAME_RE.findall(text)))
    if not api_names:
        return None
    api_name = api_names[0]
    input_fields, output_fields = _parameter_tables(content)
    if not output_fields and api_name in SPECIAL_OUTPUT_FIELDS:
        output_fields = [
            {
                "name": name,
                "type": "str" if name in {"ts_code", "trade_date"} else "float",
                "required_or_default": "Y",
                "description": "SDK 动态输出字段",
            }
            for name in SPECIAL_OUTPUT_FIELDS[api_name]
        ]
    output_names = [item["name"] for item in output_fields]
    input_names = [item["name"] for item in input_fields]
    cursor_field = next(
        (candidate for candidate in DATE_CURSOR_PREFERENCE if candidate in output_names),
        "",
    )
    if not cursor_field:
        cursor_field = next(
            (
                name
                for name in output_names
                if name.endswith("_date") or name.endswith("_time")
            ),
            "",
        )
    request_mode = _request_mode(input_fields, output_fields, cursor_field)
    if api_name in GLOBAL_RANGE_API_NAMES and {"start_date", "end_date"} <= set(input_names):
        request_mode = "date_range"
    if api_name in DATE_SLICE_API_NAMES and cursor_field in input_names:
        request_mode = "date_slice"
    if api_name in SNAPSHOT_API_NAMES:
        request_mode = "snapshot"
    stopped = any(marker in document["title"] for marker in STOPPED_TITLE_MARKERS)
    return {
        **document,
        "api_name": api_name,
        "document_api_names": api_names,
        "table_name": f"ts_{_safe_identifier(api_name)}",
        "description": _description(text),
        "input_fields": input_fields,
        "output_fields": output_fields,
        "cursor_field": cursor_field,
        "request_mode": request_mode,
        "incremental_scope": "code" if request_mode == "code_range" else "global",
        "supports_incremental": request_mode in {"code_range", "date_range", "date_slice"},
        "mutating": api_name in MUTATING_API_NAMES,
        "stopped": stopped,
        "default_enabled": not stopped and api_name not in MUTATING_API_NAMES,
        "supports_pagination": "offset" in input_names or "limit" in input_names,
        "transport": "sdk" if api_name == "pro_bar" else "http",
    }


def _parameter_tables(content: Tag) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    input_fields: list[dict[str, str]] = []
    output_fields: list[dict[str, str]] = []
    current_section = ""
    for element in content.find_all(["h2", "h3", "h4", "p", "table"]):
        if element.name != "table":
            heading = element.get_text(" ", strip=True)
            if "输入参数" in heading:
                current_section = "input"
            elif "输出参数" in heading:
                current_section = "output"
            continue
        fields = _parse_parameter_table(element)
        header_text = " ".join(
            cell.get_text(" ", strip=True) for cell in element.select("thead th")
        )
        if ("必选" in header_text or current_section == "input") and not input_fields:
            input_fields = fields
        elif ("默认显示" in header_text or current_section == "output") and not output_fields:
            output_fields = fields
    return input_fields, output_fields


def _parse_parameter_table(table: Tag) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in table.select("tbody tr"):
        values = [cell.get_text(" ", strip=True) for cell in row.find_all(["td", "th"])]
        if not values:
            continue
        source_name = values[0].strip()
        name = _safe_identifier(source_name)
        if not name:
            continue
        item = {
            "name": name,
            "type": values[1].strip().lower() if len(values) > 1 else "",
            "required_or_default": values[2].strip() if len(values) > 2 else "",
            "description": values[3].strip() if len(values) > 3 else "",
        }
        if source_name != name:
            item["source_name"] = source_name
        rows.append(item)
    return rows


def _request_mode(
    input_fields: list[dict[str, str]],
    output_fields: list[dict[str, str]],
    cursor_field: str,
) -> str:
    input_names = {item["name"] for item in input_fields}
    output_names = {item["name"] for item in output_fields}
    code_field = next(
        (
            name
            for name in ("ts_code", "index_code", "code", "con_code", "symbol")
            if name in input_names and name in output_names
        ),
        "",
    )
    if code_field and {"start_date", "end_date"} <= input_names:
        return "code_range"
    if {"start_date", "end_date"} <= input_names and cursor_field:
        return "date_range"
    if cursor_field and cursor_field in input_names:
        return "date_slice"
    return "snapshot"


def _description(text: str) -> str:
    match = re.search(r"(?:描述|数据描述)\s*[：:]\s*([^\n]+)", text)
    return match.group(1).strip() if match else ""


def _safe_identifier(value: str) -> str:
    text = re.sub(r"[^0-9A-Za-z_]+", "_", str(value).strip())
    text = re.sub(r"_+", "_", text).strip("_").lower()
    if text and text[0].isdigit():
        text = f"f_{text}"
    return text


if __name__ == "__main__":
    raise SystemExit(main())
