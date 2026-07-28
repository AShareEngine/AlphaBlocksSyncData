# AKShare 美股数据接入

`providers/akshare/` 按 AKShare 1.18.80 文档中的公开接口补充美股数据，不需要 API Key。数据写入独立的
ClickHouse `akshare` 数据库，不会覆盖现有 `yfinance` 表。

## 数据任务和表

| 任务 | ClickHouse 表 | 内容 |
| --- | --- | --- |
| `akshare.us_spot` | `ak_us_spot` | 美股实时快照、名称、价格、成交量、市值、市盈率及证券类型 |
| `akshare.us_daily_kline` | `ak_us_daily_kline` | 股票日线 OHLC、成交量、成交额、涨跌幅、换手率 |
| `akshare.us_minute_kline` | `ak_us_minute_kline` | AKShare 当前可取得的近期分钟行情 |
| `akshare.us_company_profile` | `ak_us_company_profile` | 雪球个股资料接口返回的公司资料键值快照 |
| `akshare.us_financial_statement` | `ak_us_financial_statement` | 资产负债表、综合损益表、现金流量表的长表数据 |
| `akshare.us_financial_indicator` | `ak_us_financial_indicator` | 收入、利润、EPS、ROE、流动比率等财务指标 |
| `akshare.us_valuation` | `ak_us_valuation` | 总市值、市盈率、市净率、市现率等历史估值 |
| `akshare.us_index_daily` | `ak_us_index_daily` | 纳斯达克综指、道琼斯、标普 500、纳斯达克 100 日线 |

其中 `ak_sync_task_log`、`ak_sync_checkpoint` 和 `ak_symbol_cursor` 分别记录任务日志、同步
checkpoint 和逐证券日线游标。

## 安装

必须使用实际执行同步任务的 Python 安装依赖。例如 PM2 使用
`/home/mubin/.miniconda3/envs/amazing_data/bin/python3`：

```bash
/home/mubin/.miniconda3/envs/amazing_data/bin/python3 \
  scripts/install_provider_deps.py akshare --install
```

检查：

```bash
/home/mubin/.miniconda3/envs/amazing_data/bin/python3 \
  scripts/install_provider_deps.py akshare --check
```

## Runtime 配置

在 `config/runtime.local.yaml` 的 `sync` 下加入：

```yaml
sync:
  akshare:
    request_interval_seconds: 1.0
    retries: 2
    retry_backoff_seconds: 2.0
    default_start_date: "2010-01-01"
    adjust: ""
    common_stock_only: true
    include_pink: false
```

- `request_interval_seconds`：AKShare 请求的最小间隔。
- `retries` / `retry_backoff_seconds`：网络失败时使用指数退避重试。
- `adjust`：日线复权方式，可设为空字符串、`qfq` 或 `hfq`。
- `common_stock_only`：默认排除 ETF、权证、优先股等非普通股。
- `include_pink`：是否保留 Eastmoney 市场编号为 `153` 的 Pink/OTC 证券。

## 运行

先用少量股票验证：

```bash
python3 scripts/run_provider_sync.py akshare.us_spot --limit 20 --force
python3 scripts/run_provider_sync.py akshare.us_daily_kline \
  --codes AAPL,MSFT --begin-date 20240101 --force
python3 scripts/run_provider_sync.py akshare.us_index_daily \
  --index-code .INX,.IXIC --begin-date 20240101 --force
```

按计划运行：

```bash
python3 scripts/run_provider_sync.py --config providers/akshare/plans/full.toml
python3 scripts/run_provider_sync.py --config providers/akshare/plans/daily.toml
python3 scripts/run_provider_sync.py --config providers/akshare/plans/enrichment.sample.toml
```

`full.toml` 会同步全量普通股历史日线，首次运行时间较长。建议先在计划的 `[defaults]` 中设置
`limit = 20` 验证网络、表结构和运行环境，再改回 `0`。

## 使用边界

- AKShare 聚合多个公开网页接口，免费但不提供生产 SLA，上游字段或访问策略变化时可能需要升级
  AKShare 或调整适配器。
- 分钟行情接口只提供上游当前保留的近期数据，不能作为完整历史分钟库。
- 公司资料、财报、财务指标和估值都是逐证券请求，不适合无节制地对全市场高频执行。
- 该 provider 没有把 ETF 代表行业伪装成官方行业/概念分类。板块和概念仍保留现有 yfinance
  provider 的 ETF 口径；如需正式成分关系，应再接入有明确分类与成分定义的数据源。
