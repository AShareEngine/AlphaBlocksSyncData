# Wide Table Sync Implementation Handoff

## 状态

这份文档已经不只是 handoff 说明，仓库里现在有一版最小实现骨架：

- 核心模块：`wide_table_sync.py`
- 规划脚本：`scripts/plan_wide_table_sync.py`

当前这版先完成：

1. 扫描宽表 YAML
2. 解析 `wide_table`
3. 计算 `wide_table_signature`
4. 计算 `plan_signature`
5. 生成动作规划：
   - `invalid`
   - `create_and_sync`
   - `rebuild`
   - `sync`

当前这版**还没有**完成：

1. 实际建表
2. 实际物化 SQL 执行
3. 实际状态表落库
4. 宽表数据同步

所以它目前是 **P0 的 planning / validation skeleton**，不是最终同步器。

---

## 1. 当前仓库里的实际协议

当前对接方式仍然是：

**目录 + YAML 文件协议**

AIQuantBase 负责：

1. 设计宽表节点
2. 导出 YAML 到同步项目目录

同步项目负责：

1. 扫描目录
2. 读取 YAML
3. 做校验 / 规划
4. 后续再执行同步

---

## 2. 当前目录约定

文档里原本写的是：

```text
wide_table_specs/
```

但当前仓库实际目录是：

```text
wide_table_spec/
```

目前实现为了兼容两种命名，都会扫描：

- `wide_table_spec/*.yaml`
- `wide_table_specs/*.yaml`

当前仓库已有示例：

```text
wide_table_spec/stock_daily_real.yaml
```

---

## 3. 当前 YAML 实际情况

原 handoff 里假设 YAML 包含：

1. `wide_table`
2. `graph_bundle`
3. `materialization_bundle`
4. `export_meta`

但当前仓库里的样例 `wide_table_spec/stock_daily_real.yaml` 实际只看到：

1. `wide_table`
2. `graph_bundle`
3. `export_meta`

也就是说：

- `materialization_bundle` 在当前样例里缺失

因此当前实现的处理策略是：

1. `wide_table_signature`
   仍然照常计算
2. `plan_signature`
   如果 `materialization_bundle` 存在，就优先基于它计算
3. 如果 `materialization_bundle` 不存在，就 fallback 到：
   - `graph_bundle.scope`
   - `graph_bundle.snapshot`
4. 同时把校验结果标记为：
   - `materialization_bundle missing`

因此，当前样例文件的动作规划大概率会是：

- `invalid`

这是符合当前实际数据状态的，不是实现 bug。

---

## 4. 当前已实现模块

### 4.1 `wide_table_sync.py`

当前包含：

1. `discover_wide_table_specs(project_root)`
2. `load_wide_table_yaml(path)`
3. `parse_wide_table_metadata(path, payload)`
4. `validate_wide_table_payload(metadata, payload)`
5. `compute_wide_table_signature(payload)`
6. `compute_plan_signature(payload)`
7. `plan_wide_table_sync(...)`
8. `load_and_plan_specs(...)`

### 4.2 `scripts/plan_wide_table_sync.py`

用于直接查看规划结果。

示例：

```bash
python3 scripts/plan_wide_table_sync.py
```

或输出 JSON：

```bash
python3 scripts/plan_wide_table_sync.py --json
```

---

## 5. 宽表签名规则

### 5.1 `wide_table_signature`

基于：

1. `wide_table.target.database`
2. `wide_table.target.table`
3. `wide_table.target.engine`
4. `wide_table.target.partition_by`
5. `wide_table.target.order_by`
6. `wide_table.target.version_field`
7. `wide_table.fields`
8. `wide_table.key_fields`

### 5.2 `plan_signature`

优先基于：

1. `materialization_bundle.query_plan`
2. `materialization_bundle.base_context`

如果缺失，则 fallback 到：

1. `graph_bundle.scope`
2. `graph_bundle.snapshot`

---

## 6. 动作规划规则

当前 `plan_wide_table_sync(...)` 产出 4 类动作：

### `invalid`

出现条件：

1. `wide_table` 缺失
2. `wide_table.target` 缺失
3. `fields` / `key_fields` 缺失
4. `materialization_bundle` 缺失或不完整

### `create_and_sync`

出现条件：

1. 目标表不存在
2. 基础校验通过

### `rebuild`

出现条件：

1. 目标表存在
2. `wide_table_signature` 变化
   或
3. `plan_signature` 变化

### `sync`

出现条件：

1. 目标表存在
2. 两类签名都未变化

---

## 7. 当前缺的实现

这部分现在已经完成第一版落地，实现内容包括：

1. 目标表存在性检查
   - 已对接 ClickHouse `system.tables`
2. 状态表
   - 已落地 `wide_table_sync_state`
3. 建表 SQL 生成
4. 同步动作执行
   - `create_and_sync`
   - `rebuild`
   - `sync`
5. 状态回写

### 当前真实执行语义

虽然规划动作仍然区分：

1. `create_and_sync`
2. `rebuild`
3. `sync`

但当前真实执行路径统一收敛成：

1. 先检查目标表是否存在
2. 如果存在，先执行 `DROP TABLE IF EXISTS target`
3. 重新 `CREATE TABLE`
4. 再执行整表 `INSERT INTO target SELECT ...`

也就是说，当前版本本质上是：

1. 全量重建
2. 非增量同步
3. 非 rename-swap rebuild

---

## 8. 推荐下一步实施顺序

### P0

当前已经完成：

1. 扫描 YAML
2. 校验结构
3. 输出动作规划
4. 状态表落库
5. 真实建表与同步执行

### P1

下一步建议优先补：

1. 增量同步窗口
2. 基于 `base_context.time_key` 的推进
3. 更细粒度的重刷范围控制

### P2

再推进：

1. rename-swap rebuild
2. 非 destructive 重建
3. 更稳定的大表执行策略

---

## 9. 一句话结论

当前仓库里，这条链路已经不再只是 planning skeleton。

现在已经支持：

1. 扫描 / 校验 / 签名 / 动作规划
2. 目标表存在性检查
3. `wide_table_sync_state` 状态落库
4. 真实 ClickHouse 建表与同步执行

但当前执行语义仍然是：

1. 目标表存在则先删表
2. 然后重建并整表重刷

如果继续推进，下一步重点应该放在：

1. 增量同步
2. rename-swap rebuild
3. 更细的执行窗口控制
