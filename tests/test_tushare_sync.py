from __future__ import annotations

import json
import os
import re
from pathlib import Path

from program_bootstrap import install_sync_data_system_alias


PROJECT_ROOT = Path(__file__).resolve().parents[1]
install_sync_data_system_alias(PROJECT_ROOT)

from sync_data_system.core.providers import load_provider_registry
from sync_data_system.providers.tushare.provider import (
    TushareAPIError,
    TushareConfig,
    TushareProvider,
    TushareRequestBudgetExceeded,
)
from sync_data_system.providers.tushare.repository import TushareRepository
from sync_data_system.providers.tushare.runner import (
    UNIVERSE_DEFINITIONS,
    SyncArgs,
    _fetch_rows,
    _resolve_universe,
    _run_code_range,
    _run_date_slice,
    _run_snapshot,
    load_execution_plan_from_toml,
)
from sync_data_system.providers.tushare.specs import TUSHARE_TASK_SPECS
from scripts.migrate_tushare_remove_internal_columns import migrate_table


class FakeSDKFrame:
    columns = ("ts_code", "trade_date", "close")

    def __init__(self, records=None):
        self.records = records or [
            {"ts_code": "000001.SZ", "trade_date": "20260729", "close": 12.3}
        ]

    def to_dict(self, orient):
        assert orient == "records"
        return list(self.records)


class FakeSDKApi:
    def __init__(self, token):
        self.created_with_token = token
        self.calls = []
        self.proxy_snapshots = []
        self._DataApi__token = ""
        self._DataApi__http_url = ""
        self._DataApi__timeout = 0

    def __getattr__(self, api_name):
        def call(**kwargs):
            self.calls.append((api_name, kwargs))
            self.proxy_snapshots.append(
                {
                    key: os.environ.get(key)
                    for key in (
                        "HTTP_PROXY",
                        "HTTPS_PROXY",
                        "ALL_PROXY",
                        "http_proxy",
                        "https_proxy",
                        "all_proxy",
                    )
                }
            )
            return FakeSDKFrame()

        return call


class FakeTushareSDK:
    def __init__(self):
        self.calls = []
        self.api = None

    def pro_api(self, token):
        self.api = FakeSDKApi(token)
        return self.api

    def pro_bar(self, **kwargs):
        self.calls.append(kwargs)
        return FakeSDKFrame()


class FakeClickHouse:
    def __init__(self):
        self.commands = []
        self.inserts = []

    def command(self, sql):
        self.commands.append(sql)

    def insert_rows(self, table, columns, rows):
        self.inserts.append((table, tuple(columns), list(rows)))

    def query_value(self, sql, parameters=None):
        return None

    def query_rows(self, sql, parameters=None):
        return []


class FakeRangeProvider:
    def __init__(self):
        self.config = TushareConfig(token="token", default_start_date="20100101")
        self.calls = []

    def query_all(self, api_name, **kwargs):
        self.calls.append((api_name, kwargs))
        params = kwargs["params"]
        return [
            {
                "ts_code": params["ts_code"],
                "trade_date": params["start_date"],
                "close": "10",
            }
        ]


class FakeRangeRepository:
    def __init__(self):
        self.saved = []

    def load_latest_cursors(self, spec, codes):
        return {"000002.SZ": "20240102"}

    def save_rows(self, spec, rows, *, scope_key):
        self.saved.append((spec.task, list(rows), scope_key))
        return len(rows)


class FakeDateSliceProvider:
    def __init__(self):
        self.config = TushareConfig(token="token", default_start_date="20100101")
        self.calls = []

    def query_all(self, api_name, **kwargs):
        self.calls.append((api_name, kwargs))
        params = kwargs["params"]
        return [
            {
                "ann_date": params["ann_date"],
                "ts_code": "000001.SZ",
                "title": "公告",
                "url": f"https://example.test/{params['ann_date']}",
            }
        ]


class FakeDateSliceRepository:
    def __init__(self):
        self.saved = []

    def load_latest_cursor(self, spec):
        return None

    def save_rows(self, spec, rows, *, scope_key):
        self.saved.append((spec.task, list(rows), scope_key))
        return len(rows)


class EmptyUniverseProvider:
    def __init__(self):
        self.config = TushareConfig(token="token", default_start_date="20100101")
        self.calls = []

    def query_all(self, api_name, **kwargs):
        self.calls.append((api_name, kwargs))
        if api_name == "stock_basic":
            return []
        if api_name == "bak_basic":
            raise TushareAPIError("bak_basic", -1, "permission denied")
        params = kwargs["params"]
        return [
            {
                "ts_code": "000001.SZ",
                "trade_date": params["trade_date"],
                "close": "10",
            }
        ]


class EmptyUniverseRepository(FakeRangeRepository):
    def load_universe_codes(self, spec):
        return []

    def load_latest_cursor(self, spec):
        return None


class NumericFieldProvider:
    def __init__(self):
        self.calls = []

    def query_all(self, api_name, **kwargs):
        self.calls.append((api_name, kwargs))
        return [{"date": "20260729", "1w": "1.23"}]


def test_catalog_registers_every_read_only_document_api():
    manifest = load_provider_registry(PROJECT_ROOT).get("tushare")
    catalog = json.loads(
        (PROJECT_ROOT / "providers" / "tushare" / "catalog.json").read_text(
            encoding="utf-8"
        )
    )

    assert len(TUSHARE_TASK_SPECS) == 239
    assert len(manifest.tasks) == 239
    assert "p_save" not in TUSHARE_TASK_SPECS
    assert "p_delete" not in TUSHARE_TASK_SPECS
    assert all(spec.output_names for spec in TUSHARE_TASK_SPECS.values())
    assert catalog["navigation_document_count"] == 241
    assert catalog["endpoint_count"] == 241
    assert catalog["non_api_documents"] == []
    assert catalog["document_aliases"][0]["doc_id"] == "146"
    assert catalog["unavailable_documents"][0]["doc_id"] == "314"


def test_renamed_stopped_fields_remain_documented_but_are_never_requested():
    stopped_fields = []
    for spec in TUSHARE_TASK_SPECS.values():
        for field in spec.output_fields:
            if field.requestable:
                continue
            stopped_fields.append((spec.task, field.provider_name))
            assert field.name not in spec.output_names
            assert field.provider_name not in spec.output_provider_names

    assert stopped_fields == [("cb_basic", "maturity_put_price")]
    assert "maturity_call_price" in TUSHARE_TASK_SPECS["cb_basic"].output_provider_names


def test_local_complete_document_matches_catalog_fields_and_embedded_interfaces():
    document_path = PROJECT_ROOT / "docs" / "Tushare_数据接口完整文档.md"
    if not document_path.exists():
        return
    markdown = document_path.read_text(encoding="utf-8")
    catalog = json.loads(
        (PROJECT_ROOT / "providers" / "tushare" / "catalog.json").read_text(
            encoding="utf-8"
        )
    )
    endpoints = {item["api_name"]: item for item in catalog["endpoints"]}
    compared = 0
    for section in re.split(r'(?=^<a id="doc-\d+"></a>$)', markdown, flags=re.M):
        match = re.search(
            r"^接口[：:]\s*([A-Za-z][A-Za-z0-9_]*)",
            section,
            flags=re.M,
        )
        if match is None:
            continue
        api_name = match.group(1)
        assert api_name in endpoints
        compared += 1
        for marker, catalog_key in (
            ("输入参数", "input_fields"),
            ("输出参数", "output_fields"),
        ):
            documented = _first_documented_field_table(section, marker)
            catalog_fields = [
                (
                    field["name"],
                    field["type"],
                    field["required_or_default"],
                )
                for field in endpoints[api_name][catalog_key]
            ]
            assert documented == catalog_fields, (api_name, marker)
    assert compared == 238

    embedded_interfaces = set(
        re.findall(r"接口名[：:]\s*([A-Za-z][A-Za-z0-9_]*)", markdown)
    )
    assert embedded_interfaces <= set(endpoints)


def test_every_code_task_has_a_universe_or_safe_global_date_fallback():
    for spec in TUSHARE_TASK_SPECS.values():
        if spec.request_mode != "code_range":
            continue
        if spec.category_root in UNIVERSE_DEFINITIONS:
            continue
        assert spec.code_field not in spec.required_input_names
        assert {"start_date", "end_date"} <= set(spec.input_names)


def test_global_text_endpoints_use_date_slices_instead_of_security_universes():
    assert {
        task: TUSHARE_TASK_SPECS[task].request_mode
        for task in ("anns_d", "irm_qa_sh", "irm_qa_sz", "research_report")
    } == {
        "anns_d": "date_slice",
        "irm_qa_sh": "date_slice",
        "irm_qa_sz": "date_slice",
        "research_report": "date_slice",
    }


def test_counter_bond_quotes_use_market_date_slices_instead_of_cb_basic_codes():
    class Provider:
        def __init__(self):
            self.config = TushareConfig(token="token", default_start_date="20100101")
            self.calls = []

        def query_all(self, api_name, **kwargs):
            self.calls.append((api_name, kwargs))
            trade_date = kwargs["params"]["trade_date"]
            if api_name == "bc_bestotcqt":
                return [{"trade_date": trade_date, "ts_code": "200013.BC"}]
            if api_name == "bc_otcqt":
                return [
                    {
                        "trade_date": trade_date,
                        "bank": "招商银行",
                        "ts_code": "200013.BC",
                    }
                ]
            raise AssertionError(api_name)

    class Repository:
        def __init__(self):
            self.saved = []

        def load_latest_cursor(self, spec):
            return None

        def save_rows(self, spec, rows, *, scope_key):
            self.saved.extend(rows)
            return len(rows)

    assert {
        task: TUSHARE_TASK_SPECS[task].request_mode
        for task in ("bc_bestotcqt", "bc_otcqt")
    } == {
        "bc_bestotcqt": "date_slice",
        "bc_otcqt": "date_slice",
    }

    for task in ("bc_bestotcqt", "bc_otcqt"):
        provider = Provider()
        repository = Repository()
        inserted = _run_date_slice(
            SyncArgs(
                task=task,
                begin_date="20240329",
                end_date="20240329",
            ),
            TUSHARE_TASK_SPECS[task],
            provider,
            repository,
        )

        assert inserted == 1
        assert provider.calls == [
            (
                task,
                {
                    "params": {"trade_date": "20240329"},
                    "fields": list(TUSHARE_TASK_SPECS[task].output_provider_names),
                    "supports_pagination": False,
                    "page_size": 0,
                    "max_pages": 0,
                },
            )
        ]


def test_cb_price_change_batches_multiple_bond_codes_per_request():
    class Provider:
        def __init__(self):
            self.calls = []

        def query_all(self, api_name, **kwargs):
            self.calls.append((api_name, kwargs))
            if api_name == "cb_basic":
                return [{"ts_code": f"11{index:04d}.SH"} for index in range(45)]
            return [
                {"ts_code": code, "change_date": "20260101"}
                for code in kwargs["params"]["ts_code"].split(",")
            ]

    class Repository:
        def __init__(self):
            self.saved = []
            self.universes = {}

        def load_universe_codes(self, spec):
            return self.universes.get(spec.task, [])

        def save_rows(self, spec, rows, *, scope_key):
            self.saved.extend(rows)
            if spec.task == "cb_basic":
                self.universes[spec.task] = [row["ts_code"] for row in rows]
            return len(rows)

    provider = Provider()
    repository = Repository()

    inserted = _run_snapshot(
        SyncArgs(task="cb_price_chg"),
        TUSHARE_TASK_SPECS["cb_price_chg"],
        provider,
        repository,
        context=None,
    )

    price_calls = [kwargs for api_name, kwargs in provider.calls if api_name == "cb_price_chg"]
    assert inserted == 45
    assert [len(call["params"]["ts_code"].split(",")) for call in price_calls] == [20, 20, 5]


def test_cb_price_change_retries_individually_if_a_batch_reaches_row_limit():
    class Provider:
        def __init__(self):
            self.calls = []

        def query_all(self, api_name, **kwargs):
            params = kwargs["params"]
            self.calls.append(params["ts_code"])
            codes = params["ts_code"].split(",")
            if len(codes) > 1:
                return [
                    {"ts_code": codes[0], "change_date": f"{index:08d}"}
                    for index in range(2000)
                ]
            return [{"ts_code": codes[0], "change_date": "20260101"}]

    class Repository:
        def __init__(self):
            self.saved = []

        def save_rows(self, spec, rows, *, scope_key):
            self.saved.extend(rows)
            return len(rows)

    provider = Provider()
    repository = Repository()

    inserted = _run_snapshot(
        SyncArgs(task="cb_price_chg", codes_raw="113001.SH,113002.SH"),
        TUSHARE_TASK_SPECS["cb_price_chg"],
        provider,
        repository,
        context=None,
    )

    assert inserted == 2
    assert provider.calls == ["113001.SH,113002.SH", "113001.SH", "113002.SH"]


def test_cb_share_uses_local_cb_basic_codes_without_requesting_upstream_again():
    class Provider:
        def __init__(self):
            self.config = TushareConfig(token="token", default_start_date="20100101")
            self.calls = []

        def query_all(self, api_name, **kwargs):
            self.calls.append((api_name, kwargs))
            if api_name == "cb_basic":
                return []
            params = kwargs["params"]
            return [
                {
                    "ts_code": params["ts_code"],
                    "end_date": params["end_date"],
                }
            ]

    class Repository:
        def __init__(self):
            self.saved = []

        def load_universe_codes(self, spec):
            assert spec.task == "cb_basic"
            return ["110027.SH", "113001.SH"]

        def load_latest_cursors(self, spec, codes):
            return {}

        def save_rows(self, spec, rows, *, scope_key):
            self.saved.extend(rows)
            return len(rows)

    provider = Provider()
    repository = Repository()

    inserted = _run_code_range(
        SyncArgs(
            task="cb_share",
            begin_date="20260101",
            end_date="20260131",
        ),
        TUSHARE_TASK_SPECS["cb_share"],
        provider,
        repository,
        context=None,
    )

    cb_share_params = [
        kwargs["params"]
        for api_name, kwargs in provider.calls
        if api_name == "cb_share"
    ]
    assert inserted == 2
    assert [params["ts_code"] for params in cb_share_params] == [
        "110027.SH",
        "113001.SH",
    ]
    assert all(params["start_date"] == "20260101" for params in cb_share_params)
    assert all(params["end_date"] == "20260131" for params in cb_share_params)
    assert all(api_name != "cb_basic" for api_name, _ in provider.calls)


def test_stock_universe_uses_historical_codes_and_complements_current_codes():
    class Provider:
        def query_all(self, api_name, **kwargs):
            raise AssertionError(f"unexpected upstream request: {api_name}")

    class Repository:
        def load_universe_codes(self, spec):
            if spec.task == "bak_basic":
                return ["000001.SZ", "600001.SH"]
            if spec.task == "stock_basic":
                return ["000001.SZ", "920001.BJ"]
            raise AssertionError(spec.task)

    codes = _resolve_universe(
        TUSHARE_TASK_SPECS["daily"],
        Provider(),
        repository=Repository(),
        context=None,
    )

    assert codes == ["000001.SZ", "600001.SH", "920001.BJ"]


def test_universe_loader_uses_result_ts_code_when_api_has_no_code_input():
    class ClickHouse(FakeClickHouse):
        def query_rows(self, sql, parameters=None):
            if "SELECT DISTINCT `ts_code`" in sql:
                return [("IF2608.CFX",), ("RB2610.SHF",)]
            return []

    repository = TushareRepository(ClickHouse())

    assert repository.load_universe_codes(TUSHARE_TASK_SPECS["fut_basic"]) == [
        "IF2608.CFX",
        "RB2610.SHF",
    ]


def test_anns_d_queries_each_calendar_date_without_a_stock_code():
    provider = FakeDateSliceProvider()
    repository = FakeDateSliceRepository()
    args = SyncArgs(
        task="anns_d",
        begin_date="20260729",
        end_date="20260730",
    )

    inserted = _run_date_slice(
        args,
        TUSHARE_TASK_SPECS["anns_d"],
        provider,
        repository,
    )

    assert inserted == 2
    assert [call[1]["params"] for call in provider.calls] == [
        {"ann_date": "20260729"},
        {"ann_date": "20260730"},
    ]
    assert all("ts_code" not in call[1]["params"] for call in provider.calls)


def test_empty_optional_code_universe_falls_back_to_global_date_slices():
    provider = EmptyUniverseProvider()
    repository = EmptyUniverseRepository()
    args = SyncArgs(
        task="daily",
        begin_date="20260729",
        end_date="20260730",
    )

    inserted = _run_code_range(
        args,
        TUSHARE_TASK_SPECS["daily"],
        provider,
        repository,
        context=None,
    )

    assert inserted == 2
    daily_calls = [kwargs for api_name, kwargs in provider.calls if api_name == "daily"]
    assert [call["params"] for call in daily_calls] == [
        {"trade_date": "20260729"},
        {"trade_date": "20260730"},
    ]


def test_provider_field_names_are_mapped_back_to_safe_clickhouse_columns():
    provider = NumericFieldProvider()
    spec = TUSHARE_TASK_SPECS["shibor"]

    rows = _fetch_rows(
        SyncArgs(task="shibor"),
        spec,
        provider,
        {"date": "20260729"},
    )

    requested_fields = provider.calls[0][1]["fields"]
    assert "1w" in requested_fields
    assert "f_1w" not in requested_fields
    assert rows == [{"date": "20260729", "f_1w": "1.23"}]


def test_sdk_provider_uses_configurable_token_url_and_all_documented_fields():
    sdk = FakeTushareSDK()
    provider = TushareProvider(
        TushareConfig(
            token="secret",
            base_url="http://jiaoch.site/",
            timeout=45,
            request_interval_seconds=0,
        ),
        sleep=lambda _: None,
        tushare_module=sdk,
    )

    rows = provider.query_all(
        "daily",
        params={"ts_code": "000001.SZ", "start_date": "20260729"},
        fields=("ts_code", "trade_date", "close"),
    )

    assert rows == [{"ts_code": "000001.SZ", "trade_date": "20260729", "close": 12.3}]
    assert sdk.api.created_with_token == "secret"
    assert sdk.api._DataApi__token == "secret"
    assert sdk.api._DataApi__http_url == "http://jiaoch.site"
    assert sdk.api._DataApi__timeout == 45
    assert sdk.api.calls == [
        (
            "daily",
            {
                "ts_code": "000001.SZ",
                "start_date": "20260729",
                "fields": "ts_code,trade_date,close",
            },
        )
    ]


def test_config_supports_environment_token_and_base_url(tmp_path, monkeypatch):
    runtime_path = tmp_path / "runtime.local.yaml"
    runtime_path.write_text(
        """
sync:
  tushare:
    token: yaml-token
    base_url: https://api.tushare.pro
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("TUSHARE_TOKEN", "env-token")
    monkeypatch.setenv("TUSHARE_BASE_URL", "http://jiaoch.site/")

    config = TushareConfig.from_env(runtime_path)

    assert config.token == "env-token"
    assert config.base_url == "http://jiaoch.site"


def test_empty_proxy_forces_direct_request_and_restores_inherited_proxy(monkeypatch):
    inherited = "socks5://127.0.0.1:1080"
    proxy_keys = (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    )
    for key in proxy_keys:
        monkeypatch.setenv(key, inherited)
    sdk = FakeTushareSDK()
    provider = TushareProvider(
        TushareConfig(token="secret", request_interval_seconds=0),
        sleep=lambda _: None,
        tushare_module=sdk,
    )

    provider.query("stock_basic")

    assert sdk.api.proxy_snapshots == [{key: None for key in proxy_keys}]
    assert {key: os.environ.get(key) for key in proxy_keys} == {
        key: inherited for key in proxy_keys
    }


def test_sdk_only_pro_bar_reuses_configured_sdk_api():
    sdk = FakeTushareSDK()
    provider = TushareProvider(
        TushareConfig(token="secret", request_interval_seconds=0),
        sleep=lambda _: None,
        tushare_module=sdk,
    )

    rows = provider.query_all(
        "pro_bar",
        params={
            "ts_code": "000001.SZ",
            "start_date": "20260701",
            "end_date": "20260729",
            "adj": "qfq",
        },
    )

    assert rows[0]["trade_date"] == "20260729"
    assert sdk.calls == [
        {
            "api": sdk.api,
            "ts_code": "000001.SZ",
            "start_date": "20260701",
            "end_date": "20260729",
            "adj": "qfq",
        }
    ]


def test_request_budget_stops_before_exceeding_configured_limit():
    sdk = FakeTushareSDK()
    provider = TushareProvider(
        TushareConfig(
            token="secret",
            request_interval_seconds=0,
            max_requests_per_run=1,
        ),
        sleep=lambda _: None,
        tushare_module=sdk,
    )

    provider.query("stock_basic")
    try:
        provider.query("trade_cal")
    except TushareRequestBudgetExceeded:
        pass
    else:
        raise AssertionError("second request should exceed the configured budget")
    assert len(sdk.api.calls) == 1


def test_business_tables_use_stable_natural_key_for_corrections():
    client = FakeClickHouse()
    repository = TushareRepository(client)
    spec = TUSHARE_TASK_SPECS["daily"]
    repository.ensure_task_table(spec)
    repository.save_rows(
        spec,
        [
            {"ts_code": "000001.SZ", "trade_date": "20260729", "close": "10"},
            {"ts_code": "000001.SZ", "trade_date": "20260729", "close": "11"},
        ],
        scope_key="task=daily|code=000001.SZ",
    )

    ddl = "\n".join(client.commands)
    assert "ReplacingMergeTree()" in ddl
    assert "PRIMARY KEY (`ts_code`, `trade_date`)" in ddl
    assert "ORDER BY (`ts_code`, `trade_date`)" in ddl
    assert "sipHash128" not in ddl
    for column in ("_row_hash", "_scope_key", "_cursor_value", "_ingested_at"):
        assert column not in ddl
    inserted_rows = client.inserts[-1][2]
    assert client.inserts[-1][1] == spec.output_names
    assert len(inserted_rows[0]) == len(spec.output_names)


def test_every_tushare_table_has_an_explicit_documented_business_key():
    assert len(TUSHARE_TASK_SPECS) == 239
    for spec in TUSHARE_TASK_SPECS.values():
        assert spec.business_key_fields
        assert set(spec.business_key_fields) <= (
            set(spec.output_names) | set(spec.input_names)
        )

    assert TUSHARE_TASK_SPECS["daily"].business_key_fields == (
        "ts_code",
        "trade_date",
    )
    assert TUSHARE_TASK_SPECS["index_weight"].business_key_fields == (
        "index_code",
        "con_code",
        "trade_date",
    )
    assert TUSHARE_TASK_SPECS["income"].business_key_fields == (
        "ts_code",
        "end_date",
        "report_type",
        "comp_type",
        "end_type",
    )
    assert TUSHARE_TASK_SPECS["us_income"].business_key_fields == (
        "ts_code",
        "end_date",
        "ind_type",
        "ind_name",
        "report_type",
    )
    assert TUSHARE_TASK_SPECS["pro_bar"].business_key_fields == (
        "ts_code",
        "trade_date",
        "asset",
        "freq",
        "adj",
    )
    assert TUSHARE_TASK_SPECS["bc_otcqt"].business_key_fields == (
        "trade_date",
        "bank",
        "ts_code",
    )


def test_request_dimensions_are_materialized_for_business_keys():
    class ProBarProvider:
        def query_all(self, api_name, **kwargs):
            assert api_name == "pro_bar"
            return [{"ts_code": "000001.SZ", "trade_date": "20260729", "close": "10"}]

    rows = _fetch_rows(
        SyncArgs(task="pro_bar"),
        TUSHARE_TASK_SPECS["pro_bar"],
        ProBarProvider(),
        {"asset": "E", "freq": "D", "adj": "qfq"},
    )

    assert rows == [
        {
            "ts_code": "000001.SZ",
            "trade_date": "20260729",
            "close": "10",
            "asset": "E",
            "freq": "D",
            "adj": "qfq",
        }
    ]


def test_state_tables_have_log_and_checkpoint_semantics():
    client = FakeClickHouse()
    repository = TushareRepository(client)

    repository.ensure_tables()

    ddl = "\n".join(client.commands)
    assert "ENGINE = MergeTree" in ddl
    assert "ORDER BY (run_date, task_name, started_at, scope_key)" in ddl
    assert "ENGINE = ReplacingMergeTree(finished_at)" in ddl
    assert "ORDER BY (task_name, scope_key)" in ddl
    assert "ORDER BY (task_name, scope_key, run_date, finished_at)" not in ddl


def test_outdated_full_row_hash_layout_must_be_migrated():
    class OutdatedClickHouse(FakeClickHouse):
        def query_rows(self, sql, parameters=None):
            if "system.tables" in sql:
                return [
                    (
                        "ReplacingMergeTree",
                        "sipHash128(tuple(ts_code, trade_date, close))",
                        "tuple()",
                        "",
                    )
                ]
            return []

    repository = TushareRepository(OutdatedClickHouse())

    try:
        repository.ensure_task_table(TUSHARE_TASK_SPECS["daily"])
    except RuntimeError as exc:
        assert "outdated MergeTree layout" in str(exc)
        assert "migrate_tushare_remove_internal_columns.py" in str(exc)
    else:
        raise AssertionError("full-row-hash Tushare schema should require migration")


def test_migration_adds_missing_request_dimensions_and_keeps_backup():
    class MigrationClickHouse(FakeClickHouse):
        def query_rows(self, sql, parameters=None):
            if "FROM system.columns" in sql:
                return [
                    ("ts_code", "String"),
                    ("trade_date", "String"),
                    ("close", "String"),
                ]
            return []

    client = MigrationClickHouse()
    backup = migrate_table(
        client,
        database="tushare",
        table="ts_pro_bar",
        suffix="20260729000000",
        drop_backup=False,
        dry_run=False,
    )

    sql = "\n".join(client.commands)
    assert "`asset` String" in sql
    assert "`freq` String" in sql
    assert "`adj` String" in sql
    assert "PRIMARY KEY (`ts_code`, `trade_date`, `asset`, `freq`, `adj`)" in sql
    assert "SELECT `ts_code`, `trade_date`, `close`, 'E', 'D', ''" in sql
    assert "OPTIMIZE TABLE" in sql
    assert backup == "ts_pro_bar__schema_backup_20260729000000"


def test_legacy_internal_columns_must_be_migrated_before_sync():
    class LegacyClickHouse(FakeClickHouse):
        def query_rows(self, sql, parameters=None):
            if "system.columns" in sql:
                return [("_row_hash",), ("_ingested_at",)]
            return []

    repository = TushareRepository(LegacyClickHouse())
    spec = TUSHARE_TASK_SPECS["daily"]

    try:
        repository.ensure_task_table(spec)
    except RuntimeError as exc:
        assert "migrate_tushare_remove_internal_columns.py" in str(exc)
    else:
        raise AssertionError("legacy Tushare schema should require migration")


def test_code_incremental_uses_each_security_cursor_and_2010_for_missing_security():
    provider = FakeRangeProvider()
    repository = FakeRangeRepository()
    args = SyncArgs(
        task="daily",
        codes_raw="000001.SZ,000002.SZ",
        end_date="20240105",
    )

    inserted = _run_code_range(
        args,
        TUSHARE_TASK_SPECS["daily"],
        provider,
        repository,
        context=None,
    )

    assert inserted == 2
    assert [call[1]["params"]["start_date"] for call in provider.calls] == [
        "20100101",
        "20240102",
    ]
    assert [call[1]["params"]["end_date"] for call in provider.calls] == [
        "20240105",
        "20240105",
    ]


def test_generated_plans_are_executable_and_start_in_2010():
    daily = load_execution_plan_from_toml(
        str(PROJECT_ROOT / "providers" / "tushare" / "plans" / "daily.toml")
    )
    historical = load_execution_plan_from_toml(
        str(
            PROJECT_ROOT
            / "providers"
            / "tushare"
            / "plans"
            / "all-historical.toml"
        )
    )

    assert all(task.begin_date == "20100101" for task in daily.tasks)
    assert all(
        task.begin_date == ("20160101" if task.task == "bak_basic" else "20100101")
        for task in historical.tasks
    )
    expected_sources = [
        source.task
        for category_sources in UNIVERSE_DEFINITIONS.values()
        for source in category_sources
    ]
    assert [task.task for task in historical.tasks[: len(expected_sources)]] == expected_sources
    assert len(historical.tasks) >= 200


def _first_documented_field_table(
    section: str,
    marker: str,
) -> list[tuple[str, str, str]]:
    lines = section.splitlines()
    for index, line in enumerate(lines):
        heading = line.strip().lstrip("#").strip()
        if heading.startswith("**") and heading.endswith("**"):
            heading = heading[2:-2].strip()
        elif not line.lstrip().startswith("#"):
            continue
        if not heading.endswith(marker):
            continue
        rows: list[tuple[str, str, str]] = []
        started = False
        for raw_line in lines[index + 1 :]:
            stripped = raw_line.strip()
            if stripped.startswith("|"):
                started = True
                cells = [
                    cell.strip()
                    for cell in stripped.strip("|").split("|")
                ]
                if (
                    len(cells) >= 3
                    and cells[0] != "名称"
                    and not re.fullmatch(r"-+", cells[0] or "")
                ):
                    rows.append(
                        (
                            _documented_safe_identifier(cells[0]),
                            cells[1].lower(),
                            cells[2],
                        )
                    )
            elif started or stripped:
                break
        return rows
    raise AssertionError(f"missing documented field table: {marker}")


def _documented_safe_identifier(value: str) -> str:
    text = re.sub(r"[^0-9A-Za-z_]+", "_", value.strip())
    text = re.sub(r"_+", "_", text).strip("_").lower()
    if text and text[0].isdigit():
        return f"f_{text}"
    return text
