from __future__ import annotations

import json
from pathlib import Path

from program_bootstrap import install_sync_data_system_alias


PROJECT_ROOT = Path(__file__).resolve().parents[1]
install_sync_data_system_alias(PROJECT_ROOT)

from sync_data_system.core.providers import load_provider_registry
from sync_data_system.providers.tushare.provider import (
    TushareConfig,
    TushareProvider,
    TushareRequestBudgetExceeded,
)
from sync_data_system.providers.tushare.repository import TushareRepository
from sync_data_system.providers.tushare.runner import (
    SyncArgs,
    _run_code_range,
    load_execution_plan_from_toml,
)
from sync_data_system.providers.tushare.specs import TUSHARE_TASK_SPECS


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
        self._DataApi__token = ""
        self._DataApi__http_url = ""
        self._DataApi__timeout = 0

    def __getattr__(self, api_name):
        def call(**kwargs):
            self.calls.append((api_name, kwargs))
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


def test_catalog_registers_every_read_only_document_api():
    manifest = load_provider_registry(PROJECT_ROOT).get("tushare")
    catalog = json.loads(
        (PROJECT_ROOT / "providers" / "tushare" / "catalog.json").read_text(
            encoding="utf-8"
        )
    )

    assert len(TUSHARE_TASK_SPECS) == 237
    assert len(manifest.tasks) == 237
    assert "p_save" not in TUSHARE_TASK_SPECS
    assert "p_delete" not in TUSHARE_TASK_SPECS
    assert all(spec.output_names for spec in TUSHARE_TASK_SPECS.values())
    assert catalog["navigation_document_count"] == 241
    assert catalog["endpoint_count"] == 239
    assert catalog["non_api_documents"] == []
    assert catalog["document_aliases"][0]["doc_id"] == "146"
    assert catalog["unavailable_documents"][0]["doc_id"] == "314"


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


def test_replacing_merge_tree_uses_full_row_hash_instead_of_incomplete_business_key():
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
    assert "ReplacingMergeTree(_ingested_at)" in ddl
    assert "ORDER BY (_row_hash)" in ddl
    inserted_rows = client.inserts[-1][2]
    row_hash_index = client.inserts[-1][1].index("_row_hash")
    assert inserted_rows[0][row_hash_index] != inserted_rows[1][row_hash_index]

    repository.save_rows(
        spec,
        [{"ts_code": "000001.SZ", "trade_date": "20260729", "close": "10"}],
        scope_key=(
            'task=daily|params={"end_date":"20260729",'
            '"start_date":"20100101","ts_code":"000001.SZ"}'
        ),
    )
    first_window_hash = client.inserts[-1][2][0][row_hash_index]
    repository.save_rows(
        spec,
        [{"ts_code": "000001.SZ", "trade_date": "20260729", "close": "10"}],
        scope_key=(
            'task=daily|params={"end_date":"20260730",'
            '"start_date":"20260729","ts_code":"000001.SZ"}'
        ),
    )
    overlapping_window_hash = client.inserts[-1][2][0][row_hash_index]
    assert first_window_hash == overlapping_window_hash


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
    assert all(task.begin_date == "20100101" for task in historical.tasks)
    assert len(historical.tasks) >= 200
