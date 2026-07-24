# Provider Development

这个项目把同步能力分成两层：

- `core/`：配置解析、provider manifest 加载、注册、日志、增量和写库公共能力
- `providers/<name>/`：单个数据源的 provider、repository、runner、specs、plan 示例

新增 provider 时，不要改核心同步入口的分发逻辑。新增一个目录并提供 `provider.toml`、`runner.py`、`repository.py`、`specs.py` 和 plan 示例。

当前核心目录约定：

```text
core/
  config.py       # TOML 加载和 source 识别
  engine.py       # 按 source 分发到 provider runner
  registry.py     # provider registry 导出
  providers.py    # provider.toml 解析和校验
  clickhouse.py   # ClickHouse 公共层导出
  incremental.py  # 增量公共层导出
  logging.py      # 同步日志公共层导出
```

## 目录结构

```text
providers/<name>/
  provider.toml
  __init__.py
  provider.py
  repository.py
  runner.py
  specs.py
  plans/
    sample.toml
```

## provider.toml

```toml
name = "qmt"
display_name = "QMT REST"
version = "0.1.0"
module = "sync_data_system.providers.qmt"
runtime_config_key = "qmt"
default_database = "qmt"
plans_path = "plans"
dependencies = ["clickhouse-connect"]
import_modules = []
plan_fields = ["codes", "begin_date", "end_date", "period", "limit", "force", "continue_on_error"]

[entrypoints]
provider = "provider:QmtProvider"
repository = "repository:QmtRepository"
runner = "runner:run_sync_args"
config_runner = "runner:run_config_file"
context_builder = "runner:build_context"
registered_task_runner = "runner:run_registered_task"
specs = "specs:QMT_TASK_SPECS"

[[tasks]]
name = "kline_history"
target = "qmt_kline_history"
supports_incremental = true
cursor_field = "time_ms"
request_fields = ["name", "codes", "begin_date", "end_date", "period", "limit", "force", "log_level"]
```

约定：

- `source = "<name>"` 的同步计划会分发到同名 provider
- `config_runner` 必须是 `module:object` 格式，并接收 `run_config_file(path, log_level_override=None)`
- `context_builder` 用于 API 单任务执行，签名为 `build_context(runtime_path=None, database="<default>")`
- `registered_task_runner` 用于 API 单任务执行，签名为 `run_registered_task(probe)`
- `supports_incremental = true` 的任务必须有 `cursor_field`
- `dependencies` 声明可用 pip 安装的 provider 额外依赖
- `import_modules` 声明运行时必须能 import 的模块；适合 AmazingData 这类不一定能直接从公开 pip 源安装的 SDK
- `plan_fields` 声明 `run_sync*.toml` 中 `[defaults]` 和 `[[tasks]]` 允许出现的 provider 参数字段；跨 provider 的证券列表字段统一叫 `codes`

API 任务元数据会从 `provider.toml` 的 `[[tasks]]` 自动注册。新增 provider 后，任务名会以 `<provider>.<task>` 形式暴露，例如 `qmt.kline_history`。

当前内置 provider：

- `amazingdata`：API 任务名形如 `amazingdata.daily_kline`
- `baostock`：API 任务名形如 `baostock.daily_kline`
- `qmt`：API 任务名形如 `qmt.kline_history`
- `yfinance`：API 任务名形如 `yfinance.daily_kline`

`scripts/run_provider_sync.py` 是统一 CLI 入口；provider 实现应放到 `providers/<name>/runner.py`。

## 新增 Provider 流程

1. 新建 `providers/<name>/`，补齐 `provider.toml`、`provider.py`、`repository.py`、`runner.py`、`specs.py`
2. 在 `provider.toml` 声明依赖、运行时 import、同步计划字段、入口和 `[[tasks]]`
3. 在 `runner.py` 提供：
   - `run_config_file(path, log_level_override=None)`
   - `build_context(runtime_path=None, database="<default>")`
   - `run_registered_task(probe)`
4. 把示例计划放到 `providers/<name>/plans/`
5. 执行 manifest 校验和入口导入校验

## 校验

```bash
python3 scripts/validate_provider.py
python3 scripts/validate_provider.py --provider qmt --load-entrypoints
python3 scripts/validate_sync_config.py
python3 scripts/validate_sync_config.py config/sync/plans/run_sync.qmt.sample.toml
```

`--load-entrypoints` 会导入 provider 配置的入口对象，用来发现 import 路径错误。
`validate_sync_config.py` 会校验同步计划的顶层字段、`[defaults]`、`[[tasks]]`、任务名、字段白名单和基础类型。

## 依赖

Provider 的额外依赖写在 `provider.toml` 的 `dependencies`；运行时 import 依赖写在 `import_modules`。启用 provider 前可以检查或安装：

```bash
python3 scripts/install_provider_deps.py qmt --check
python3 scripts/install_provider_deps.py qmt --dry-run
python3 scripts/install_provider_deps.py qmt --install
python3 scripts/install_provider_deps.py --all --check
```

默认不会安装任何东西；必须显式传 `--install` 才会调用 `pip install`。
`--install` 只安装 `dependencies`，`import_modules` 仍按当前 Python 环境检查。
