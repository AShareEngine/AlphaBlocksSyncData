# 前端对接文档

## 1. 目标

本文档给前端使用，覆盖当前项目已经实现的两类接口：

1. 普通同步任务管理 API
2. 宽表发现 / 规划 / 状态 / 触发作业 API

当前后端能力已经支持：

- 列出任务
- 查询任务元信息
- 启动同步任务
- 查询任务列表 / 详情 / 日志
- 取消同步任务
- 宽表 spec 列表 / 详情
- 宽表 planning
- 宽表状态查询
- 宽表作业触发

---

## 2. 服务地址

默认启动方式：

```bash
python3 scripts/run_api_service.py
```

默认监听：

- `http://127.0.0.1:18080`

Swagger：

- `GET /docs`

OpenAPI：

- `GET /openapi.json`

---

## 3. 当前约束

### 3.1 单活约束

当前后端只允许一个同步任务运行。

如果已经有任务在运行，再发起新的同步请求会返回：

- HTTP `409`

错误示例：

```json
{
  "detail": "another sync job is running job_id=... task=...; cancel it first"
}
```

前端建议：

1. 在发起新任务前，先查询 `GET /api/jobs?status=running`
2. 如果有运行中的任务，提示用户先取消或等待

### 3.2 鉴权

当前版本没有鉴权。

如果前端部署到非内网环境，需要额外加认证层。

### 3.3 宽表执行能力边界

宽表相关 API 目前已经支持真实宽表执行，但当前执行语义仍然偏保守：

- 已支持：
  - 发现 spec
  - 规划 action
  - 检查目标表是否存在
  - 真实 ClickHouse 建表
  - 真实 ClickHouse 插入执行
  - 状态表更新
- 当前限制：
  - 还不支持增量同步
  - 还不是 rename-swap rebuild
  - 当前是“存在即删表再重建”的整表刷新语义

所以当前宽表 `run` 接口的意义是：

- 触发真实宽表同步作业

当前 `create_and_sync / rebuild / sync` 三类动作在实际执行前都会：

1. 检查目标表是否存在
2. 存在则先 `DROP TABLE IF EXISTS`
3. 然后重新 `CREATE TABLE`
4. 再执行整表 `INSERT INTO ... SELECT ...`

如果 spec 缺 `materialization_bundle`，会明确失败，不会伪造成功。

---

## 4. 通用响应约定

### 4.1 时间字段

所有时间字段统一返回 ISO8601 字符串，例如：

```json
"2026-04-21T09:05:00+00:00"
```

### 4.2 Job 状态

当前任务状态枚举：

- `running`
- `cancelling`
- `cancelled`
- `success`
- `failed`
- `interrupted`

### 4.3 Job 类型

当前任务类型枚举：

- `config`
- `task`
- `registered_task`
- `wide_table_sync`

---

## 5. 前端推荐对接流程

### 5.1 普通同步任务页

推荐流程：

1. 页面初始化调用 `GET /api/meta/tasks`
2. 用户选择任务后调用 `GET /api/meta/tasks/{task_name}`
3. 渲染表单
4. 提交时调用 `POST /api/jobs/run-task`
5. 记录返回的 `job_id`
6. 用 `GET /api/jobs/{job_id}` 轮询任务状态
7. 如需完整日志，再调用 `GET /api/jobs/{job_id}/logs`

### 5.2 配置任务页

推荐流程：

1. 页面初始化调用 `GET /api/meta/configs`
2. 用户选择配置后调用 `POST /api/jobs/run-config`
3. 拿到 `job_id`
4. 轮询 `GET /api/jobs/{job_id}`

### 5.3 宽表管理页

推荐流程：

1. 页面初始化调用 `GET /api/wide-tables/specs`
2. 点击某个宽表调用 `GET /api/wide-tables/specs/{wide_table_name}`
3. 用户点击“规划”时调用 `POST /api/wide-tables/plan`
4. 用户点击“触发作业”时调用：
   - `POST /api/wide-tables/run/{wide_table_name}`
   或
   - `POST /api/wide-tables/run`
5. 再用 `GET /api/jobs/{job_id}` 跟踪后台作业
6. 状态页调用 `GET /api/wide-tables/states` 或 `GET /api/wide-tables/states/{wide_table_name}`

---

## 6. 普通同步任务 API

### 6.1 健康检查

`GET /health`

返回：

```json
{
  "status": "ok"
}
```

### 6.2 列出全部任务

`GET /api/meta/tasks`

返回结构：

```json
{
  "tasks": ["daily_kline", "minute_kline", "..."],
  "registered_tasks": [
    {
      "name": "daily_kline",
      "source": "amazingdata",
      "target": "ad_market_kline_daily",
      "input_resolver": "market_kline_defaults",
      "request_fields": [
        "name",
        "codes",
        "begin_date",
        "end_date",
        "limit",
        "force",
        "resume",
        "log_level"
      ],
      "probe_fields": [
        "name",
        "source",
        "target",
        "job_id",
        "runtime_path",
        "input_codes",
        "input_begin_date",
        "input_end_date",
        "limit",
        "force",
        "resume",
        "log_level",
        "codes",
        "begin_date",
        "end_date",
        "row_count",
        "status",
        "message",
        "log_path"
      ]
    }
  ]
}
```

说明：

- `tasks` 是全部可用任务名
- `registered_tasks` 是当前已经挂到装饰器注册表里的任务
- 前端以 `registered_tasks` 为主展示更合适

### 6.3 查询单个任务元信息

`GET /api/meta/tasks/{task_name}`

示例：

```bash
curl http://127.0.0.1:18080/api/meta/tasks/daily_kline
```

如果是注册任务，会返回完整元信息。  
如果是普通任务但未注册，也会返回基础占位结构。  
如果任务不存在，返回：

- HTTP `404`

### 6.4 列出配置文件

`GET /api/meta/configs`

返回：

```json
{
  "configs": [
    "run_sync.full.toml",
    "run_sync.amazingdata.full.toml",
    "run_sync.baostock.full.toml"
  ]
}
```

### 6.5 启动配置任务

`POST /api/jobs/run-config`

请求体：

```json
{
  "config": "run_sync.full.toml",
  "log_level": "INFO"
}
```

返回：

```json
{
  "job_id": "abc123def456",
  "kind": "config",
  "status": "running",
  "config": "run_sync.full.toml",
  "created_at": "2026-04-21T09:00:00+00:00",
  "started_at": "2026-04-21T09:00:00+00:00",
  "finished_at": null,
  "cwd": "/path/to/project",
  "command": ["python3", "..."],
  "log_path": "/path/to/project/.service_state/logs/abc123def456.log",
  "config_path": "run_sync.full.toml",
  "task": null,
  "source": null,
  "target": null,
  "pid": 12345,
  "return_code": null,
  "error": null,
  "request_payload": {
    "config": "run_sync.full.toml",
    "log_level": "INFO"
  }
}
```

### 6.6 启动单任务

`POST /api/jobs/run-task`

请求体字段：

- `name`
  推荐使用
- `task`
  兼容字段，不推荐新代码使用
- `codes: string[]`
- `begin_date: int | null`
- `end_date: int | null`
- `limit: int`
- `force: bool`
- `resume: bool`
- `log_level: string | null`

请求示例：

```json
{
  "name": "daily_kline",
  "codes": ["000001.SZ", "510300.SH"],
  "begin_date": 20240101,
  "end_date": 20240131,
  "limit": 20,
  "force": false,
  "resume": false,
  "log_level": "INFO"
}
```

返回示例：

```json
{
  "job_id": "abc123def456",
  "kind": "registered_task",
  "status": "running",
  "created_at": "2026-04-21T09:05:00+00:00",
  "started_at": "2026-04-21T09:05:00+00:00",
  "finished_at": null,
  "cwd": "/path/to/project",
  "command": ["python3", "..."],
  "log_path": "/path/to/project/.service_state/logs/abc123def456.log",
  "config_path": null,
  "task": "daily_kline",
  "source": "amazingdata",
  "target": "ad_market_kline_daily",
  "pid": 12346,
  "return_code": null,
  "error": null,
  "request_payload": {
    "name": "daily_kline",
    "codes": ["000001.SZ", "510300.SH"],
    "begin_date": 20240101,
    "end_date": 20240131,
    "limit": 20,
    "force": false,
    "resume": false,
    "log_level": "INFO",
    "runtime_path": null
  },
  "task_metadata": {
    "name": "daily_kline",
    "source": "amazingdata",
    "target": "ad_market_kline_daily",
    "input_resolver": "market_kline_defaults",
    "request_fields": ["name", "codes", "begin_date", "end_date", "limit", "force", "resume", "log_level"],
    "probe_fields": ["name", "source", "target", "..."]
  }
}
```

### 6.7 列出任务列表

`GET /api/jobs`

支持查询参数：

- `status`
- `task`
- `kind`

示例：

```bash
curl http://127.0.0.1:18080/api/jobs
curl http://127.0.0.1:18080/api/jobs?status=running
curl http://127.0.0.1:18080/api/jobs?task=daily_kline
curl http://127.0.0.1:18080/api/jobs?kind=registered_task
```

返回：

```json
{
  "jobs": [
    {
      "job_id": "abc123def456",
      "kind": "registered_task",
      "status": "running",
      "created_at": "2026-04-21T09:05:00+00:00",
      "started_at": "2026-04-21T09:05:00+00:00",
      "finished_at": null,
      "cwd": "/path/to/project",
      "command": ["python3", "..."],
      "log_path": "/path/to/project/.service_state/logs/abc123def456.log",
      "config_path": null,
      "task": "daily_kline",
      "source": "amazingdata",
      "target": "ad_market_kline_daily",
      "pid": 12346,
      "return_code": null,
      "error": null,
      "request_payload": {
        "name": "daily_kline",
        "codes": [],
        "begin_date": 20240101,
        "end_date": 20240131,
        "limit": 20,
        "force": false,
        "resume": false,
        "log_level": "INFO",
        "runtime_path": null
      }
    }
  ]
}
```

### 6.8 查询单个任务详情

`GET /api/jobs/{job_id}`

支持参数：

- `tail_lines`

返回：

```json
{
  "job_id": "abc123def456",
  "kind": "registered_task",
  "status": "running",
  "created_at": "2026-04-21T09:05:00+00:00",
  "started_at": "2026-04-21T09:05:00+00:00",
  "finished_at": null,
  "cwd": "/path/to/project",
  "command": ["python3", "..."],
  "log_path": "/path/to/project/.service_state/logs/abc123def456.log",
  "config_path": null,
  "task": "daily_kline",
  "source": "amazingdata",
  "target": "ad_market_kline_daily",
  "pid": 12346,
  "return_code": null,
  "error": null,
  "request_payload": {
    "name": "daily_kline",
    "codes": [],
    "begin_date": 20240101,
    "end_date": 20240131,
    "limit": 20,
    "force": false,
    "resume": false,
    "log_level": "INFO",
    "runtime_path": null
  },
  "logs_tail": "...\n..."
}
```

### 6.9 查询单个任务完整日志

`GET /api/jobs/{job_id}/logs`

支持参数：

- `tail_lines`

返回：

```json
{
  "job_id": "abc123def456",
  "logs": "...\n..."
}
```

### 6.10 取消任务

`POST /api/jobs/{job_id}/cancel`

当前行为：

- 普通同步任务和注册表任务都是真正进程托管
- 调用后会发送 `terminate()`

返回示例：

```json
{
  "job_id": "abc123def456",
  "kind": "registered_task",
  "status": "cancelling",
  "created_at": "2026-04-21T09:05:00+00:00",
  "started_at": "2026-04-21T09:05:00+00:00",
  "finished_at": null,
  "cwd": "/path/to/project",
  "command": ["python3", "..."],
  "log_path": "/path/to/project/.service_state/logs/abc123def456.log",
  "config_path": null,
  "task": "daily_kline",
  "source": "amazingdata",
  "target": "ad_market_kline_daily",
  "pid": 12346,
  "return_code": null,
  "error": null
}
```

最终状态会变成：

- `cancelled`

---

## 7. 宽表 API

### 7.1 列出宽表 spec

`GET /api/wide-tables/specs`

返回：

```json
{
  "specs": [
    {
      "spec_path": "/abs/path/wide_table_specs/stock_daily_real.yaml",
      "spec_name": "stock_daily_real",
      "wide_table_id": "wide::stock_daily_real::c83034b0",
      "source_node": "stock_daily_real",
      "target": {
        "database": "starlight",
        "table": "ad_market_kline_daily",
        "engine": "Memory",
        "partition_by": [],
        "order_by": ["code", "trade_time"],
        "version_field": ""
      },
      "fields": ["code", "trade_time", "..."],
      "key_fields": ["code", "trade_time"],
      "status": "enabled"
    }
  ]
}
```

### 7.2 查询单个宽表 spec

`GET /api/wide-tables/specs/{wide_table_name}`

示例：

```bash
curl http://127.0.0.1:18080/api/wide-tables/specs/stock_daily_real
```

### 7.3 宽表 planning

`POST /api/wide-tables/plan`

请求体：

```json
{
  "clickhouse_live": false,
  "write_state": false,
  "state_database": null
}
```

说明：

- `clickhouse_live=false`
  只做本地 planning
- `clickhouse_live=true`
  会连 ClickHouse 做目标表存在性检查和历史签名比对
- `write_state=true`
  需要同时设置 `clickhouse_live=true`

返回：

```json
{
  "plans": [
    {
      "spec_path": "/abs/path/wide_table_specs/stock_daily_real.yaml",
      "wide_table_name": "stock_daily_real",
      "target_database": "starlight",
      "target_table": "ad_market_kline_daily",
      "wide_table_signature": "...",
      "plan_signature": "...",
      "action": "invalid",
      "validation": {
        "ok": false,
        "messages": ["materialization_bundle missing"]
      },
      "reason": "materialization_bundle missing"
    }
  ]
}
```

### 7.4 查询宽表状态列表

`GET /api/wide-tables/states`

可选参数：

- `state_database`

返回：

```json
{
  "states": [
    {
      "wide_table_id": "wide::stock_daily_real::c83034b0",
      "wide_table_name": "stock_daily_real",
      "source_node": "stock_daily_real",
      "target_database": "starlight",
      "target_table": "ad_market_kline_daily",
      "spec_path": "/abs/path/wide_table_specs/stock_daily_real.yaml",
      "wide_table_signature": "...",
      "plan_signature": "...",
      "last_status": "failed",
      "last_action": "invalid",
      "last_message": "materialization_bundle missing",
      "last_started_at": null,
      "last_finished_at": null,
      "updated_at": "2026-04-22T12:00:00+00:00"
    }
  ]
}
```

### 7.5 查询单个宽表状态

`GET /api/wide-tables/states/{wide_table_name}`

示例：

```bash
curl http://127.0.0.1:18080/api/wide-tables/states/stock_daily_real
```

### 7.6 触发宽表作业

批量：

`POST /api/wide-tables/run`

请求体：

```json
{
  "wide_table_names": ["stock_daily_real"],
  "state_database": "default"
}
```

单个：

`POST /api/wide-tables/run/{wide_table_name}`

示例：

```bash
curl -X POST http://127.0.0.1:18080/api/wide-tables/run/stock_daily_real
```

当前行为说明：

- 会创建后台 job
- 会执行：
  - spec 读取
  - live planning
  - 状态表更新
- 还不会真正做数据物化

所以当前 `run` 接口更准确地说是：

- **触发宽表 planning + 状态更新作业**

返回示例：

```json
{
  "job_id": "wide123job456",
  "kind": "wide_table_sync",
  "status": "running",
  "created_at": "2026-04-22T12:00:00+00:00",
  "started_at": "2026-04-22T12:00:00+00:00",
  "finished_at": null,
  "cwd": "/path/to/project",
  "command": ["python3", ".../scripts/run_wide_table_sync.py", "--json", "--wide-table-name", "stock_daily_real"],
  "log_path": "/path/to/project/.service_state/logs/wide123job456.log",
  "config_path": null,
  "task": "stock_daily_real",
  "source": "wide_table",
  "target": null,
  "pid": 54321,
  "return_code": null,
  "error": null,
  "request_payload": {
    "wide_table_names": ["stock_daily_real"],
    "state_database": "default"
  },
  "wide_table_names": ["stock_daily_real"]
}
```

---

## 8. 错误处理建议

前端需要重点处理这些状态码：

- `200`
  正常
- `400`
  参数错误 / 配置错误 / 后端校验失败
- `404`
  任务 / spec / 状态不存在
- `409`
  当前已有同步任务运行，触发了单活限制

推荐错误处理：

### `409`

提示：

- 当前已有同步任务在运行
- 先查看运行中的任务
- 让用户决定取消还是等待

### `400`

直接展示后端返回的 `detail`

---

## 9. 前端建议实现

### 任务中心

页面最小功能：

1. 任务列表
2. 配置列表
3. 任务启动
4. 任务列表筛选
5. 任务详情 + 日志
6. 取消任务

### 宽表管理页

页面最小功能：

1. spec 列表
2. spec 详情
3. planning
4. 状态列表
5. 触发宽表作业

### 推荐轮询

普通任务和宽表作业都建议轮询：

- `GET /api/jobs/{job_id}`

推荐轮询间隔：

- 运行中：`2~5 秒`
- `cancelling`：`1~2 秒`
- 结束后停止轮询

---

## 10. 一句话结论

前端现在已经可以直接对接：

1. 普通同步任务管理
2. 宽表 spec / planning / state / run

但要明确：

- 宽表 `run` 当前是 planning/state 作业，不是最终物化执行器
