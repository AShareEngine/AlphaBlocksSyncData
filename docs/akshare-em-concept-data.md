# AKShare 东方财富概念板块接入

`providers/akshare/` 提供以下三个东方财富概念板块任务：

| 任务 | ClickHouse 表 | 内容 |
| --- | --- | --- |
| `akshare.stock_board_concept_name_em` | `akshare.ak_stock_board_concept_name_em` | 东方财富概念名称和板块代码的每日目录快照 |
| `akshare.stock_board_concept_cons_em` | `akshare.ak_stock_board_concept_cons_em` | 每个概念当前的成份股及实时行情快照 |
| `akshare.stock_board_concept_hist_em` | `akshare.ak_stock_board_concept_hist_em` | 每个概念的日线、周线或月线历史行情 |

成份股表保存 AKShare `stock_board_concept_cons_em` 返回的全部字段，包括序号、股票代码、名称、
最新价、涨跌、成交量、成交额、振幅、OHLC、换手率、动态市盈率和市净率。主键范围为
`snapshot_date + concept_code + symbol`，所以一只股票同时属于多个概念时会保留多条板块归属。

历史表保存 `stock_board_concept_hist_em` 的 OHLC、涨跌、成交量、成交额、振幅和换手率。
`period` 支持 `daily`、`weekly`、`monthly`；`adjust` 读取
`runtime.local.yaml` 中的 `sync.akshare.adjust`，可设为空字符串、`qfq` 或 `hfq`。
历史表按 `concept_code + period + adjust + trade_date` 去重，增量游标也隔离周期与复权模式。

默认先读取当天已经落库的东方财富概念目录；目录不存在或显式指定的概念不在旧目录中时，
会调用 `stock_board_concept_name_em()` 刷新。`codes` 既支持板块名称，也支持 `BK0655`
一类东方财富板块代码。

```bash
python3 scripts/run_provider_sync.py akshare.stock_board_concept_name_em --force

python3 scripts/run_provider_sync.py akshare.stock_board_concept_cons_em \
  --codes 融资融券,BK0655 --force

python3 scripts/run_provider_sync.py akshare.stock_board_concept_hist_em \
  --codes 绿色电力 --begin-date 20220101 --period daily --force
```

同步全部概念可以直接运行独立计划：

```bash
python3 scripts/run_provider_sync.py --config providers/akshare/plans/concept-em.toml
```

成份股和历史行情都需要逐个板块请求东方财富。首次全量运行建议先使用 `--limit 3`
验证网络与表结构，再取消限制；请求间隔、重试和代理继续使用 `runtime.local.yaml` 的
`sync.akshare.request_interval_seconds`、`retries`、`retry_backoff_seconds` 和 `proxy`。
