# AlphaBlocksSyncData

Independent data sync service used by AlphaBlocks.

## Config Ownership

This project owns all sync-service configs:

- Runtime config template: `config/runtime.example.yaml`
- Local runtime config: `config/runtime.local.yaml`
- Sync plan configs: `config/sync/plans/run_sync*.toml`

`config/runtime.local.yaml` contains credentials and is intentionally ignored by git. Create it on each deployment host from the example file.

## Server Setup

```bash
cd AlphaBlocksSyncData
cp config/runtime.example.yaml config/runtime.local.yaml
vim config/runtime.local.yaml
```

Fill at least:

- `datasource.host`
- `datasource.database`
- `datasource.username`
- `datasource.password`
- `sync.amazingdata.username`
- `sync.amazingdata.password`
- `sync.amazingdata.host`
- `sync.amazingdata.port`
- `sync.amazingdata.local_path`
- `sync.qmt.base_url`
- `sync.qmt.api_key`

## PM2

```bash
cd AlphaBlocksSyncData
pm2 start ecosystem.config.js
```

The PM2 config defaults `SYNC_DATA_RUNTIME_CONFIG` to:

```text
<repo>/config/runtime.local.yaml
```

If you use a different path:

```bash
SYNC_DATA_RUNTIME_CONFIG=/path/to/runtime.local.yaml pm2 restart alphablocks-sync-data --update-env
```

## API

Default API service:

```text
http://<host>:8010/api/sync
```

Useful checks:

```bash
curl http://127.0.0.1:8010/health
curl http://127.0.0.1:8010/api/sync/meta/configs
curl http://127.0.0.1:8010/api/sync/meta/tasks
curl http://127.0.0.1:8010/api/sync/meta/providers
```

## Providers

Provider implementations live under `providers/<name>/` and are described by `provider.toml`.
See `docs/provider-development.md` before adding a new data provider.

Useful provider commands:

```bash
python3 scripts/validate_provider.py --load-entrypoints
python3 scripts/install_provider_deps.py --all --check
python3 scripts/install_provider_deps.py qmt --check
python3 scripts/install_provider_deps.py qmt --install
python3 scripts/run_provider_sync.py qmt.kline_history --codes 600000.SH --begin-date 20240101 --end-date 20240131 --period 1d
```
