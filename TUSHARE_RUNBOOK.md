# Tushare Pro 同步说明

## 覆盖范围

Tushare Provider 的任务目录由官方数据接口文档生成，当前目录快照包含：

- 241 个叶子文档；
- 239 个接口，其中 237 个只读接口已注册为同步任务；
- `p_save`、`p_delete` 会修改远端自选组合，出于安全原因不注册为同步任务；
- “A股复权行情”与“通用行情接口”实际都指向 SDK 的 `pro_bar`，合并为一个任务；
- “期货 Tick 行情”官方明确不提供 API，只能单独购买 CSV 网盘交付，因此无法通过 Token 自动同步。

完整机器可读目录在 `providers/tushare/catalog.json`。所有接口统一通过
`tushare>=1.4.29` SDK 调用；Token 和 API 根地址均从运行时配置读取。
`pro_bar` 按官方要求通过模块级函数并传入同一个已配置的 `api` 客户端。

## Linux 配置

安装依赖：

```bash
python3 -m pip install -r requirements.txt
python3 -m pip install --upgrade 'tushare>=1.4.29'
python3 scripts/install_provider_deps.py tushare --check
```

在 `config/runtime.local.yaml` 中增加：

```yaml
sync:
  tushare:
    token: YOUR_TUSHARE_TOKEN
    base_url: https://api.tushare.pro
    timeout: 60
    retries: 2
    retry_backoff_seconds: 2.0
    request_interval_seconds: 0.2
    default_start_date: '20100101'
    page_size: 5000
    max_requests_per_run: 50000
```

使用兼容 Tushare SDK 的代理服务时，只需修改根地址：

```yaml
sync:
  tushare:
    token: YOUR_TUSHARE_TOKEN
    base_url: http://jiaoch.site
```

SDK 会自动请求 `http://jiaoch.site/daily` 等接口路径，不要把 `/daily`
写入 `base_url`。各 Provider 的代理彼此隔离；Tushare 当前没有代理配置，
请求会强制直连，并在请求期间忽略 PM2、systemd 或 Shell 继承的
`HTTP_PROXY`、`HTTPS_PROXY`、`ALL_PROXY` 及对应的小写变量。

也可以不把 Token 写入文件：

```bash
export TUSHARE_TOKEN='YOUR_TUSHARE_TOKEN'
export TUSHARE_BASE_URL='http://jiaoch.site'
```

`max_requests_per_run` 为 `0` 时不在本地限流。设置为正数后，达到预算会停止后续任务；已经按代码落库的数据会在下次运行时继续。

## 执行

日常核心任务：

```bash
python3 scripts/run_provider_sync.py \
  --config providers/tushare/plans/daily.toml
```

全部非实时、非停用历史接口：

```bash
python3 scripts/run_provider_sync.py \
  --config providers/tushare/plans/all-historical.toml
```

全历史计划包含 220 个以上的任务，很多接口要求单独权限，而且逐代码首轮回填会消耗大量请求。建议先设置请求预算，多次运行；无权限任务会记录错误并继续。

单接口示例：

```bash
python3 scripts/run_provider_sync.py tushare.daily \
  --codes 000001.SZ,000005.SZ \
  --begin-date 20100101

python3 scripts/run_provider_sync.py tushare.stock_hsgt \
  --begin-date 20250812 \
  --params '{"type":["HK_SZ","SZ_HK","HK_SH","SH_HK"]}'
```

所有 237 个只读任务也会出现在同步任务 API 和 freshness 页面中。需要接口专用参数时，可在 HTTP 请求的 `params` 对象中传入。

## 增量规则

代码型历史接口不会使用整张表的最大日期：

1. 先从对应目标表一次性查询 `code -> max(cursor)`；
2. 某只代码完全无数据时，从 `20100101` 开始；
3. 有数据时，从该代码自己的最大日期开始，故意保留一个日期重叠以接收当天修订；
4. 每只代码请求完成后立即落库，进程中断不会丢失已完成进度；
5. 未显式传入代码时，只从对应类目的本地基础表读取遍历池；基础表为空会先同步基础接口、落库，再重新读取，不直接拿临时 API 返回值遍历；
6. 股票池以 `bak_basic` 历史列表中的全部历史代码为主体，再用 `stock_basic` 的 `L/D/P/G` 补入尚未进入历史列表的新代码，因此覆盖曾上市、已退市、暂停上市和当前上市股票，且不混入指数。

全市场型接口按自己的日期游标增量；支持 `offset/limit` 的接口会自动分页。分钟接口在全历史计划中按天切窗，避免单次返回上限截断。

类目基础表映射如下：股票 `ts_bak_basic + ts_stock_basic`、ETF
`ts_etf_basic`、基金 `ts_fund_basic`、指数 `ts_index_basic`、期货
`ts_fut_basic`、期权 `ts_opt_basic`、可转债接口 `ts_cb_basic`、外汇
`ts_fx_obasic`、港股 `ts_hk_basic`、美股 `ts_us_basic`、现货
`ts_sge_basic`。全历史计划会先同步这些基础表；`bak_basic` 从官方有数据的
`20160101` 开始。柜台流通式债券没有对应基础列表，`bc_bestotcqt` 和
`bc_otcqt` 使用独立的 `.BC` 代码体系，因此按 `trade_date` 全市场切片同步，
不使用 `ts_cb_basic` 的可转债代码。

## ReplacingMergeTree 设计

每个接口按需创建独立的 `ts_<api_name>` 表，文档输出字段全部保存为
`String`，不附加 `source`、`fetched_at` 或内部写入时间。每个接口在
`providers/tushare/business_keys.py` 中声明自己的稳定业务键，例如：

- `daily`：`(ts_code, trade_date)`；
- `index_weight`：`(index_code, con_code, trade_date)`；
- `income`：`(ts_code, end_date, report_type, comp_type, end_type)`；
- `us_income`：`(ts_code, end_date, ind_type, ind_name, report_type)`。

业务表引擎为：

```sql
ENGINE = ReplacingMergeTree()
PRIMARY KEY (<接口业务键>)
ORDER BY (<接口业务键>)
```

同一业务键再次写入时，新行会在 ClickHouse 后台合并后替换旧行；查询必须立即得到
合并结果时使用 `FINAL`。静态列表通常以证券代码为键，行情以证券代码和时间为键，
成分关系会加入母代码和成分代码，财务长表会加入指标名和报告类型，避免过窄键吞掉
合法记录。新增 Tushare 接口如果没有登记业务键，目录加载会直接失败，不会猜测建表。

状态表采用不同语义：`ts_sync_task_log` 使用普通 `MergeTree` 保留每次执行记录；
`ts_sync_checkpoint` 使用 `ReplacingMergeTree(finished_at)`，但版本字段不进入
`ORDER BY (task_name, scope_key)`，因此每个任务范围只保留最新检查点。

已有旧表不会被 `CREATE TABLE IF NOT EXISTS` 自动改变。同步启动时会检测整行哈希、
旧内部字段和错误状态表键，并提示执行迁移：

```bash
# 先查看受影响的表和 SQL
python3 scripts/migrate_tushare_remove_internal_columns.py --dry-run

# 原子换表迁移；默认保留 __schema_backup_<时间> 备份
python3 scripts/migrate_tushare_remove_internal_columns.py
```

迁移会把旧数据复制到新业务键表、执行 `OPTIMIZE ... FINAL` 后原子换名。由于整行哈希
旧表没有可靠写入版本，在旧数据已经存在同一业务键多个不同版本时，无法从旧表字段
判断哪一行最后写入；迁移默认保留备份，建议迁移后强制同步相关接口，再核对并删除备份。

## 更新官方目录

官方文档更新后执行：

```bash
python3 scripts/generate_tushare_catalog.py
python3 scripts/generate_tushare_plans.py
python3 scripts/validate_provider.py --provider tushare --load-entrypoints
python3 scripts/validate_sync_config.py \
  providers/tushare/plans/daily.toml \
  providers/tushare/plans/all-historical.toml
```
