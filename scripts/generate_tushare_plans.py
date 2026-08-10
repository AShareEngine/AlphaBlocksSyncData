#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate executable Tushare plans from the committed API catalog."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = PROJECT_ROOT / "providers" / "tushare" / "catalog.json"
DEFAULT_OUTPUT = PROJECT_ROOT / "providers" / "tushare" / "plans" / "all-historical.toml"
EXCLUDED_HISTORICAL_APIS = frozenset({"p_get", "p_list"})
UNIVERSE_SOURCE_TASKS = (
    "stock_basic",
    "bak_basic",
    "etf_basic",
    "fund_basic",
    "index_basic",
    "fut_basic",
    "opt_basic",
    "cb_basic",
    "fx_obasic",
    "hk_basic",
    "us_basic",
    "sge_basic",
)
BEGIN_DATE_OVERRIDES = {
    # The official stock historical-list service starts in 2016.
    "bak_basic": 20160101,
}
PARAM_OVERRIDES: dict[str, dict[str, Any]] = {
    "stock_basic": {"list_status": ["L", "D", "P", "G"]},
    "etf_basic": {"list_status": ["L", "D", "P"]},
    "fund_basic": {"status": ["L", "D", "I"]},
    "hk_basic": {"list_status": ["L", "D", "P"]},
    "dc_index": {"idx_type": ["行业板块", "概念板块", "地域板块"]},
    "news": {
        "src": [
            "sina",
            "wallstreetcn",
            "10jqka",
            "eastmoney",
            "yuncaijing",
            "fenghuang",
            "jinrongjie",
            "cls",
            "yicai",
        ]
    },
    "stock_hsgt": {"type": ["HK_SZ", "SZ_HK", "HK_SH", "SH_HK"]},
    "fut_basic": {
        "exchange": ["CFFEX", "DCE", "CZCE", "SHFE", "INE", "GFEX"]
    },
    "pro_bar": {"asset": "E", "freq": "D", "adj": ["", "qfq", "hfq"]},
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    endpoints = [
        endpoint
        for endpoint in payload["endpoints"]
        if not endpoint["mutating"]
        and not endpoint["stopped"]
        and endpoint["api_name"] not in EXCLUDED_HISTORICAL_APIS
        and not _is_realtime(endpoint)
    ]
    source_order = {
        task: index for index, task in enumerate(UNIVERSE_SOURCE_TASKS)
    }
    endpoints.sort(
        key=lambda endpoint: (
            0,
            source_order[endpoint["api_name"]],
        )
        if endpoint["api_name"] in source_order
        else (1, 0)
    )
    lines = [
        'source = "tushare"',
        'database = "tushare"',
        'log_level = "INFO"',
        "continue_on_error = true",
        "",
        "[defaults]",
        "begin_date = 20100101",
        "force = false",
        "resume = true",
        "page_size = 5000",
        "",
        "# 该计划覆盖官方目录中的全部非实时、非停用只读历史接口。",
        "# 高权限接口会按账号权限返回错误并继续；建议配置 max_requests_per_run 控制日配额。",
        "",
    ]
    for endpoint in endpoints:
        lines.extend(
            [
                "[[tasks]]",
                f'task = "{endpoint["api_name"]}"',
            ]
        )
        if endpoint["api_name"] in BEGIN_DATE_OVERRIDES:
            lines.append(
                f'begin_date = {BEGIN_DATE_OVERRIDES[endpoint["api_name"]]}'
            )
        params = _params_for(endpoint)
        if params:
            lines.append(f"params = {_toml_inline_table(params)}")
        if _is_minute_history(endpoint):
            lines.append("window_days = 1")
        lines.append("")

    output = Path(args.output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines), encoding="utf-8")
    print(f"generated {output}: tasks={len(endpoints)}")
    return 0


def _params_for(endpoint: dict[str, Any]) -> dict[str, Any]:
    api_name = endpoint["api_name"]
    params = dict(PARAM_OVERRIDES.get(api_name) or {})
    required = {
        field["name"]: field
        for field in endpoint["input_fields"]
        if str(field["required_or_default"]).strip().upper().startswith("Y")
    }
    if "freq" in required and "freq" not in params:
        description = str(required["freq"].get("description") or "")
        if "week" in description and "month" in description:
            params["freq"] = ["week", "month"]
        elif "1MIN" in description:
            params["freq"] = "1MIN"
        else:
            params["freq"] = "1min"
    return params


def _is_realtime(endpoint: dict[str, Any]) -> bool:
    return endpoint["api_name"].startswith("rt_") or "实时" in endpoint["title"]


def _is_minute_history(endpoint: dict[str, Any]) -> bool:
    return "分钟" in endpoint["title"] and not _is_realtime(endpoint)


def _toml_inline_table(values: dict[str, Any]) -> str:
    return "{ " + ", ".join(
        f"{key} = {_toml_value(value)}" for key, value in values.items()
    ) + " }"


def _toml_value(value: Any) -> str:
    if isinstance(value, list):
        return "[" + ", ".join(_toml_value(item) for item in value) + "]"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(str(value), ensure_ascii=False)


if __name__ == "__main__":
    raise SystemExit(main())
