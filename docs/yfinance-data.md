# 免费美股数据接入

`providers/yfinance/` 在不接入 AKShare、无需付费 API Key 的前提下提供美股日线和分类数据：

- [yfinance](https://github.com/ranaroussi/yfinance)：日线、分红拆股、板块/概念 ETF 行情和 ETF Top Holdings
- [FinanceDatabase](https://github.com/JerBouma/FinanceDatabase)：美股 symbol 主表、交易所、Sector、Industry Group 和 Industry

## 任务与表

| 任务 | ClickHouse 表 | 数据说明 |
| --- | --- | --- |
| `yfinance.symbol_master` | `yf_symbol_master` | 美国主要交易所证券主表 |
| `yfinance.daily_kline` | `yf_daily_kline` | 未复权 OHLC、Adj Close、成交量 |
| `yfinance.corporate_actions` | `yf_corporate_actions` | 分红、拆股、Capital Gain |
| `yfinance.industry_membership` | `yf_industry_membership` | Sector / Industry Group / Industry 快照 |
| `yfinance.sector_daily` | `yf_sector_daily` | 11 个标准板块 ETF 日线 |
| `yfinance.concept_daily` | `yf_concept_daily` | AI、半导体、网络安全、清洁能源、生物科技代表 ETF 日线 |
| `yfinance.concept_membership` | `yf_concept_membership` | 代表 ETF 披露的 Top Holdings |

板块基准使用 `XLC/XLY/XLP/XLE/XLF/XLV/XLI/XLB/XLRE/XLK/XLU`。概念使用：

- 人工智能：`AIQ`，成分参考 `AIQ/BOTZ/ROBO`
- 半导体：`SMH`，成分参考 `SMH/SOXX`
- 网络安全：`CIBR`，成分参考 `CIBR/HACK`
- 清洁能源：`ICLN`，成分参考 `ICLN/TAN`
- 生物科技：`XBI`，成分参考 `XBI/IBB`

`concept_membership.membership_scope` 固定为 `top_holdings`。它是 ETF 公开的主要持仓，不是完整、权威的“概念股全集”。

## 配置

```yaml
sync:
  yfinance:
    proxy: "http://127.0.0.1:7890"
    batch_size: 5
    threads: false
    auto_adjust: false
    repair: false
    timeout: 30
    network_retries: 2
    request_interval_seconds: 2.0
    rate_limit_retries: 4
    rate_limit_backoff_seconds: 30.0
    rate_limit_max_backoff_seconds: 300.0
    rate_limit_jitter_seconds: 3.0
    active_symbols_only: true
    symbol_directory_timeout: 60
    default_start_date: "2010-01-01"
    include_otc: false
```

`proxy` 会写入 yfinance 的全局网络配置，支持普通 HTTP/HTTPS 代理地址；留空表示直连。代理必须能从实际运行 PM2 任务的服务器访问，本机的 `127.0.0.1` 代理不会自动转发到远程服务器。

Provider 会串行化 Yahoo 请求，每次调用至少间隔 `request_interval_seconds`。遇到 HTTP 429 或 `YFRateLimitError` 时，最多额外重试 `rate_limit_retries` 次，按 `rate_limit_backoff_seconds` 指数退避，并受最大退避时间和随机抖动限制。`network_retries` 是 yfinance 自身针对瞬时网络错误的重试次数。共享代理出口仍可能被 Yahoo 限制，建议保持 `threads: false`。

`active_symbols_only: true` 会使用 Nasdaq Trader 当日证券目录筛选当前正常上市的普通证券，排除 ETF、测试证券、异常上市状态、权证、Rights、Units、优先股和交易所上市债券。目录下载同样使用 `proxy`，获取失败时会停止生成股票池，避免退回 FinanceDatabase 的历史全集后污染日线任务。

FinanceDatabase 会保留退市代码供历史研究，因此不建议关闭 `active_symbols_only`。关闭时只按精确的 NASDAQ、NYSE 和 NYSE American 市场名称过滤；`include_otc: true` 仅在关闭当前证券目录过滤时生效。

## 运行

先安装该 Provider 的依赖：

```bash
python3 scripts/install_provider_deps.py yfinance --install
```

首次建议先做小批量验证：

```bash
python3 scripts/run_provider_sync.py yfinance.symbol_master --limit 20
python3 scripts/run_provider_sync.py yfinance.daily_kline --codes AAPL,MSFT --begin-date 20240101
```

执行完整计划：

```bash
python3 scripts/run_provider_sync.py --config providers/yfinance/plans/full.toml
python3 scripts/run_provider_sync.py --config providers/yfinance/plans/daily.toml
```

日线、公司行动、板块和概念行情会按 ClickHouse 中各 symbol/ETF 的最新日期增量续传。`--force` 会忽略游标，按传入的日期范围重跑。

## 使用边界

yfinance 是对 Yahoo Finance 公开接口的开源封装。代码可以免费使用，但 Yahoo Finance 数据通常面向个人研究用途；生产商用、对外分发或对 SLA 有要求的场景，应另行确认数据授权并准备付费数据源作为替代。
