# Provider 模板

复制本目录中的模板到 `providers/<name>/`，再替换 provider 名称、任务、表结构和请求逻辑。

最小步骤：

```bash
mkdir -p providers/demo/plans
cp docs/provider-template/provider.toml providers/demo/provider.toml
cp docs/provider-template/runner.py providers/demo/runner.py
touch providers/demo/__init__.py providers/demo/provider.py providers/demo/repository.py providers/demo/specs.py
```

完成后运行：

```bash
python3 scripts/validate_provider.py --provider demo --load-entrypoints
python3 scripts/validate_sync_config.py
```

注意：

- 同步计划里的证券列表统一使用 `codes`。
- `provider.toml` 的 `plan_fields` 必须覆盖 sample plan 中会出现的字段。
- `run_registered_task(probe)` 用于 API 单任务执行。
- `run_config_file(path, log_level_override=None)` 用于 TOML 配置执行。
